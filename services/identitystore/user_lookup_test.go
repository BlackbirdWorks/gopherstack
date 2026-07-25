package identitystore_test

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/identitystore"
)

// TestUserExternalIDAndEmailLookup covers GetUserId lookups via ExternalId and the
// email index, including index maintenance on UpdateUser.
func TestUserExternalIDAndEmailLookup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *identitystore.Handler)
		name string
	}{
		{
			// ExternalIds is not settable via CreateUser -- the real
			// CreateUserRequest smithy shape has no ExternalIds member (see
			// the doc comment on createUserRequest in handler_users.go) --
			// so this seeds it via UpdateUser's AttributeOperations instead,
			// the only real-AWS-shaped way to set it.
			name: "create_user_with_external_ids",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "ext.user",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				userID := parseResponse(t, rec)["UserId"].(string)

				updRec := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{
							"AttributePath": "externalIds",
							"AttributeValue": []map[string]any{
								{"Issuer": "okta", "Id": "okta-abc-123"},
							},
						},
					},
				})
				require.Equal(t, http.StatusOK, updRec.Code)

				descRec := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				resp := parseResponse(t, descRec)
				extIDs, ok := resp["ExternalIds"].([]any)
				require.True(t, ok)
				require.Len(t, extIDs, 1)

				first := extIDs[0].(map[string]any)
				assert.Equal(t, "okta", first["Issuer"])
				assert.Equal(t, "okta-abc-123", first["Id"])
			},
		},
		{
			name: "get_user_id_by_external_id",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "extlookup.user",
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				wantID := parseResponse(t, createRec)["UserId"].(string)

				updRec := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          wantID,
					"Operations": []map[string]any{
						{
							"AttributePath": "externalIds",
							"AttributeValue": []map[string]any{
								{"Issuer": "idp", "Id": "idp-xyz-789"},
							},
						},
					},
				})
				require.Equal(t, http.StatusOK, updRec.Code)

				rec := doRequest(t, h, "GetUserId", map[string]any{
					"IdentityStoreId": testStoreID,
					"AlternateIdentifier": map[string]any{
						"ExternalId": map[string]any{
							"Issuer": "idp",
							"Id":     "idp-xyz-789",
						},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, wantID, parseResponse(t, rec)["UserId"])
			},
		},
		{
			name: "get_user_id_by_email_index",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "email.index.user",
					"Emails": []map[string]any{
						{"Value": "primary@example.com", "Type": "work", "Primary": true},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				wantID := parseResponse(t, createRec)["UserId"].(string)

				rec := doRequest(t, h, "GetUserId", map[string]any{
					"IdentityStoreId": testStoreID,
					"AlternateIdentifier": map[string]any{
						"UniqueAttribute": map[string]any{
							"AttributePath":  "emails.value",
							"AttributeValue": "primary@example.com",
						},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, wantID, parseResponse(t, rec)["UserId"])
			},
		},
		{
			name: "email_index_updated_on_user_update",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "email.update.user",
					"Emails": []map[string]any{
						{"Value": "old@example.com", "Primary": true},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				// Update email
				patchRec := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{
							"AttributePath": "emails",
							"AttributeValue": []map[string]any{
								{"Value": "new@example.com", "Primary": true},
							},
						},
					},
				})
				require.Equal(t, http.StatusOK, patchRec.Code)

				// Old email should no longer work
				oldRec := doRequest(t, h, "GetUserId", map[string]any{
					"IdentityStoreId": testStoreID,
					"AlternateIdentifier": map[string]any{
						"UniqueAttribute": map[string]any{
							"AttributePath":  "emails.value",
							"AttributeValue": "old@example.com",
						},
					},
				})
				assert.Equal(t, http.StatusNotFound, oldRec.Code)

				// New email should work
				newRec := doRequest(t, h, "GetUserId", map[string]any{
					"IdentityStoreId": testStoreID,
					"AlternateIdentifier": map[string]any{
						"UniqueAttribute": map[string]any{
							"AttributePath":  "emails.value",
							"AttributeValue": "new@example.com",
						},
					},
				})
				require.Equal(t, http.StatusOK, newRec.Code)
				assert.Equal(t, userID, parseResponse(t, newRec)["UserId"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newTestHandler())
		})
	}
}

// TestGetUserIDExternalIDIssuerIsolation verifies that GetUserId ExternalId lookups
// match on Issuer+Id together, not Id alone.
func TestGetUserIDExternalIDIssuerIsolation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// ExternalIds is not settable via CreateUser (see the doc comment on
	// createUserRequest in handler_users.go), so both users are seeded via a
	// follow-up UpdateUser AttributeOperations call instead.
	createRec1 := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "user-issuer-a",
	})
	require.Equal(t, http.StatusOK, createRec1.Code)
	userA := parseResponse(t, createRec1)["UserId"].(string)

	updRec1 := doRequest(t, h, "UpdateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userA,
		"Operations": []map[string]any{
			{
				"AttributePath": "externalIds",
				"AttributeValue": []map[string]any{
					{"Issuer": "https://idp-a.example.com", "Id": "shared-user-ext"},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, updRec1.Code)

	createRec2 := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "user-issuer-b",
	})
	require.Equal(t, http.StatusOK, createRec2.Code)
	userB := parseResponse(t, createRec2)["UserId"].(string)

	updRec2 := doRequest(t, h, "UpdateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userB,
		"Operations": []map[string]any{
			{
				"AttributePath": "externalIds",
				"AttributeValue": []map[string]any{
					{"Issuer": "https://idp-b.example.com", "Id": "shared-user-ext"},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, updRec2.Code)

	recA := doRequest(t, h, "GetUserId", map[string]any{
		"IdentityStoreId": testStoreID,
		"AlternateIdentifier": map[string]any{
			"ExternalId": map[string]string{
				"Issuer": "https://idp-a.example.com",
				"Id":     "shared-user-ext",
			},
		},
	})
	require.Equal(t, http.StatusOK, recA.Code)
	assert.Equal(t, userA, parseResponse(t, recA)["UserId"])

	recB := doRequest(t, h, "GetUserId", map[string]any{
		"IdentityStoreId": testStoreID,
		"AlternateIdentifier": map[string]any{
			"ExternalId": map[string]string{
				"Issuer": "https://idp-b.example.com",
				"Id":     "shared-user-ext",
			},
		},
	})
	require.Equal(t, http.StatusOK, recB.Code)
	assert.Equal(t, userB, parseResponse(t, recB)["UserId"])
}

// TestListUsersFilters verifies ListUsers Filters (AttributePath/AttributeValue).
func TestListUsersFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *identitystore.Handler)
		name string
	}{
		{
			name: "list_users_filter_by_username",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				for _, name := range []string{"filter.alice", "filter.bob", "filter.carol"} {
					rec := doRequest(t, h, "CreateUser", map[string]any{
						"IdentityStoreId": testStoreID,
						"UserName":        name,
					})
					require.Equal(t, http.StatusOK, rec.Code)
				}

				rec := doRequest(t, h, "ListUsers", map[string]any{
					"IdentityStoreId": testStoreID,
					"Filters": []map[string]any{
						{"AttributePath": "UserName", "AttributeValue": "filter.bob"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				users, ok := parseResponse(t, rec)["Users"].([]any)
				require.True(t, ok)
				require.Len(t, users, 1)
				assert.Equal(t, "filter.bob", users[0].(map[string]any)["UserName"])
			},
		},
		{
			name: "list_users_filter_by_email",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				for _, email := range []string{"aa@ex.com", "bb@ex.com"} {
					rec := doRequest(t, h, "CreateUser", map[string]any{
						"IdentityStoreId": testStoreID,
						"UserName":        "femail-" + email,
						"Emails": []map[string]any{
							{"Value": email, "Primary": true},
						},
					})
					require.Equal(t, http.StatusOK, rec.Code)
				}

				rec := doRequest(t, h, "ListUsers", map[string]any{
					"IdentityStoreId": testStoreID,
					"Filters": []map[string]any{
						{"AttributePath": "emails.value", "AttributeValue": "bb@ex.com"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				users, ok := parseResponse(t, rec)["Users"].([]any)
				require.True(t, ok)
				require.Len(t, users, 1)
			},
		},
		{
			// Regression test: matchUserSingleValueFilter's switch default
			// previously returned true for an unrecognized AttributePath.
			// "birthdate" is a syntactically valid AttributePath (passes
			// validateFilters) that matchUserSingleValueFilter's switch does
			// NOT have a case for, so before the fix this filter would have
			// matched every user in the store instead of none. See users.go's
			// matchUserSingleValueFilter doc comment.
			name: "list_users_filter_unrecognized_path_matches_nothing",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				for _, name := range []string{"unrec.alice", "unrec.bob"} {
					rec := doRequest(t, h, "CreateUser", map[string]any{
						"IdentityStoreId": testStoreID,
						"UserName":        name,
					})
					require.Equal(t, http.StatusOK, rec.Code)
				}

				rec := doRequest(t, h, "ListUsers", map[string]any{
					"IdentityStoreId": testStoreID,
					"Filters": []map[string]any{
						{"AttributePath": "birthdate", "AttributeValue": "1990-01-01"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				users, ok := parseResponse(t, rec)["Users"].([]any)
				require.True(t, ok)
				assert.Empty(t, users)
			},
		},
		{
			name: "list_users_filter_malformed_attribute_path_is_validation_error",
			run: func(t *testing.T, h *identitystore.Handler) {
				t.Helper()

				rec := doRequest(t, h, "ListUsers", map[string]any{
					"IdentityStoreId": testStoreID,
					"Filters": []map[string]any{
						{"AttributePath": "not valid!", "AttributeValue": "x"},
					},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newTestHandler())
		})
	}
}

// TestListUsersPagination verifies ListUsers MaxResults + NextToken pagination.
func TestListUsersPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for i := range 5 {
		rec := doRequest(t, h, "CreateUser", map[string]any{
			"IdentityStoreId": testStoreID,
			"UserName":        fmt.Sprintf("page.user.%d", i),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// First page of 2
	rec1 := doRequest(t, h, "ListUsers", map[string]any{
		"IdentityStoreId": testStoreID,
		"MaxResults":      2,
	})
	require.Equal(t, http.StatusOK, rec1.Code)
	resp1 := parseResponse(t, rec1)

	users1, ok := resp1["Users"].([]any)
	require.True(t, ok)
	assert.Len(t, users1, 2)

	token, hasToken := resp1["NextToken"].(string)
	require.True(t, hasToken, "expected NextToken to be non-nil")
	require.NotEmpty(t, token)

	// Verify token is base64
	_, err := base64.StdEncoding.DecodeString(token)
	require.NoError(t, err)

	// Second page of 2
	rec2 := doRequest(t, h, "ListUsers", map[string]any{
		"IdentityStoreId": testStoreID,
		"MaxResults":      2,
		"NextToken":       token,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	resp2 := parseResponse(t, rec2)
	users2, ok := resp2["Users"].([]any)
	require.True(t, ok)
	assert.Len(t, users2, 2)

	// Remaining page of 1
	token2 := resp2["NextToken"].(string)
	rec3 := doRequest(t, h, "ListUsers", map[string]any{
		"IdentityStoreId": testStoreID,
		"MaxResults":      2,
		"NextToken":       token2,
	})
	require.Equal(t, http.StatusOK, rec3.Code)
	resp3 := parseResponse(t, rec3)
	users3, ok := resp3["Users"].([]any)
	require.True(t, ok)
	assert.Len(t, users3, 1)
	assert.Nil(t, resp3["NextToken"])
}

// TestListUsersMaxResultsBound verifies ListUsers rejects a MaxResults
// outside the AWS 1-100 range with a ValidationException, while an unset or
// in-range value is accepted.
func TestListUsersMaxResultsBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxResults any
		name       string
		wantStatus int
	}{
		{name: "unset_ok", maxResults: nil, wantStatus: http.StatusOK},
		{name: "in_range_ok", maxResults: 50, wantStatus: http.StatusOK},
		{name: "at_upper_bound_ok", maxResults: 100, wantStatus: http.StatusOK},
		{name: "over_bound_rejected", maxResults: 101, wantStatus: http.StatusBadRequest},
		{name: "negative_rejected", maxResults: -1, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			body := map[string]any{"IdentityStoreId": testStoreID}
			if tt.maxResults != nil {
				body["MaxResults"] = tt.maxResults
			}

			rec := doRequest(t, h, "ListUsers", body)
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestUserIDFormat verifies that generated user IDs are UUID format and unique.
func TestUserIDFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "user_id_is_uuid_format",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				rec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "uuid.user",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				userID := parseResponse(t, rec)["UserId"].(string)
				// UUID format: 8-4-4-4-12 hex chars separated by hyphens
				assert.Len(t, userID, 36, "UserID should be UUID format (36 chars)")
				assert.Equal(t, '-', rune(userID[8]))
				assert.Equal(t, '-', rune(userID[13]))
				assert.Equal(t, '-', rune(userID[18]))
				assert.Equal(t, '-', rune(userID[23]))
			},
		},
		{
			name: "user_ids_are_unique",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				seen := make(map[string]bool)
				for i := range 10 {
					rec := doRequest(t, h, "CreateUser", map[string]any{
						"IdentityStoreId": testStoreID,
						"UserName":        fmt.Sprintf("unique%d", i),
					})
					require.Equal(t, http.StatusOK, rec.Code)
					id := parseResponse(t, rec)["UserId"].(string)
					assert.False(t, seen[id], "Generated IDs should be unique")
					seen[id] = true
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// TestUserListSorting verifies ListUsers returns deterministic sorted results.
func TestUserListSorting(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	names := []string{"zebra", "alpha", "mango", "bravo", "kilo"}
	for _, n := range names {
		doRequest(t, h, "CreateUser", map[string]any{
			"IdentityStoreId": testStoreID,
			"UserName":        n,
		})
	}

	rec1 := doRequest(t, h, "ListUsers", map[string]any{
		"IdentityStoreId": testStoreID,
	})
	rec2 := doRequest(t, h, "ListUsers", map[string]any{
		"IdentityStoreId": testStoreID,
	})

	require.Equal(t, http.StatusOK, rec1.Code)
	require.Equal(t, http.StatusOK, rec2.Code)

	users1 := parseResponse(t, rec1)["Users"].([]any)
	users2 := parseResponse(t, rec2)["Users"].([]any)

	require.Len(t, users1, len(names))
	require.Len(t, users2, len(names))

	for i := range users1 {
		id1 := users1[i].(map[string]any)["UserId"].(string)
		id2 := users2[i].(map[string]any)["UserId"].(string)
		assert.Equal(t, id1, id2, "ListUsers order must be deterministic across calls")
	}
}
