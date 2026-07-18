package identitystore_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/identitystore"
)

// TestUserBirthdate verifies Birthdate is stored and returned on users.
func TestUserBirthdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create_user_with_birthdate",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				rec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "birth.user",
					"Birthdate":       "1990-06-15",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				userID := parseResponse(t, rec)["UserId"].(string)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				require.Equal(t, http.StatusOK, desc.Code)
				assert.Equal(t, "1990-06-15", parseResponse(t, desc)["Birthdate"])
			},
		},
		{
			name: "create_user_without_birthdate_returns_empty",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				rec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "no.birth",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				userID := parseResponse(t, rec)["UserId"].(string)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				require.Equal(t, http.StatusOK, desc.Code)
				resp := parseResponse(t, desc)
				_, hasBirthdate := resp["Birthdate"]
				assert.False(t, hasBirthdate, "Birthdate should be absent when not set")
			},
		},
		{
			name: "invalid_birthdate_format_rejected",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				rec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "bad.birth",
					"Birthdate":       "15/06/1990",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, parseResponse(t, rec)["message"], "Birthdate")
			},
		},
		{
			name: "update_user_birthdate",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "update.birth",
					"Birthdate":       "1985-01-01",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				upd := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{"AttributePath": "birthdate", "AttributeValue": "1990-06-15"},
					},
				})
				require.Equal(t, http.StatusOK, upd.Code)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				assert.Equal(t, "1990-06-15", parseResponse(t, desc)["Birthdate"])
			},
		},
		{
			name: "list_users_includes_birthdate",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "b1",
					"Birthdate":       "2000-03-20",
				})

				listRec := doRequest(t, h, "ListUsers", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				require.Equal(t, http.StatusOK, listRec.Code)

				users := parseResponse(t, listRec)["Users"].([]any)
				require.Len(t, users, 1)
				assert.Equal(t, "2000-03-20", users[0].(map[string]any)["Birthdate"])
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

// TestBirthdateEdgeCases exercises birthdate boundary conditions.
func TestBirthdateEdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		birthdate string
		wantOK    bool
	}{
		{name: "valid_date", birthdate: "1990-06-15", wantOK: true},
		{name: "epoch", birthdate: "1970-01-01", wantOK: true},
		{name: "recent_date", birthdate: "2024-12-31", wantOK: true},
		{name: "slash_format", birthdate: "1990/06/15", wantOK: false},
		{name: "dd_mm_yyyy", birthdate: "15-06-1990", wantOK: false},
		{name: "too_short", birthdate: "1990-6-5", wantOK: false},
		{name: "no_separators", birthdate: "19900615", wantOK: false},
		{name: "only_year", birthdate: "1990", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()

			rec := doRequest(t, h, "CreateUser", map[string]any{
				"IdentityStoreId": testStoreID,
				"UserName":        "birth." + tt.name,
				"Birthdate":       tt.birthdate,
			})

			if tt.wantOK {
				assert.Equal(t, http.StatusOK, rec.Code, "expected 200 for valid birthdate %q", tt.birthdate)
			} else {
				assert.Equal(t, http.StatusBadRequest, rec.Code, "expected 400 for invalid birthdate %q", tt.birthdate)
			}
		})
	}
}

// TestUserWebsite verifies Website field is stored and returned.
func TestUserWebsite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create_user_with_website",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				rec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "web.user",
					"Website":         "https://example.com/~web.user",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				userID := parseResponse(t, rec)["UserId"].(string)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				require.Equal(t, http.StatusOK, desc.Code)
				assert.Equal(t, "https://example.com/~web.user", parseResponse(t, desc)["Website"])
			},
		},
		{
			name: "update_user_website",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "update.web",
					"Website":         "https://old.example.com",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				upd := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{"AttributePath": "website", "AttributeValue": "https://new.example.com"},
					},
				})
				require.Equal(t, http.StatusOK, upd.Code)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				assert.Equal(t, "https://new.example.com", parseResponse(t, desc)["Website"])
			},
		},
		{
			name: "clear_user_website",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "clear.web",
					"Website":         "https://example.com",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				upd := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{"AttributePath": "website", "AttributeValue": ""},
					},
				})
				require.Equal(t, http.StatusOK, upd.Code)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				resp := parseResponse(t, desc)
				_, hasWebsite := resp["Website"]
				assert.False(t, hasWebsite, "Website should be absent after clearing")
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

// TestBirthdateWebsiteInListUsers verifies new fields visible in ListUsers.
func TestBirthdateWebsiteInListUsers(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "bdweb.user",
		"Birthdate":       "1995-07-04",
		"Website":         "https://bdweb.example.com",
	})

	listRec := doRequest(t, h, "ListUsers", map[string]any{
		"IdentityStoreId": testStoreID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	users := parseResponse(t, listRec)["Users"].([]any)
	require.Len(t, users, 1)

	u := users[0].(map[string]any)
	assert.Equal(t, "1995-07-04", u["Birthdate"])
	assert.Equal(t, "https://bdweb.example.com", u["Website"])
	assert.Equal(t, "ENABLED", u["UserStatus"])
}

// TestUserStatus verifies UserStatus defaults to ENABLED and can be toggled.
func TestUserStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "new_user_defaults_to_enabled",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				rec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "status.default",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				userID := parseResponse(t, rec)["UserId"].(string)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				require.Equal(t, http.StatusOK, desc.Code)
				assert.Equal(t, "ENABLED", parseResponse(t, desc)["UserStatus"])
			},
		},
		{
			name: "update_user_status_to_disabled",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "status.change",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				upd := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{"AttributePath": "userStatus", "AttributeValue": "DISABLED"},
					},
				})
				require.Equal(t, http.StatusOK, upd.Code)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				assert.Equal(t, "DISABLED", parseResponse(t, desc)["UserStatus"])
			},
		},
		{
			name: "re_enable_user",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "status.reenable",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{"AttributePath": "userStatus", "AttributeValue": "DISABLED"},
					},
				})

				upd := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{"AttributePath": "userStatus", "AttributeValue": "ENABLED"},
					},
				})
				require.Equal(t, http.StatusOK, upd.Code)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				assert.Equal(t, "ENABLED", parseResponse(t, desc)["UserStatus"])
			},
		},
		{
			name: "invalid_user_status_ignored",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "status.invalid",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				upd := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{"AttributePath": "userStatus", "AttributeValue": "SUSPENDED"},
					},
				})
				require.Equal(t, http.StatusOK, upd.Code)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				// Invalid value should be silently ignored; status stays ENABLED.
				assert.Equal(t, "ENABLED", parseResponse(t, desc)["UserStatus"])
			},
		},
		{
			name: "user_status_in_list_users",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "status.list",
				})

				listRec := doRequest(t, h, "ListUsers", map[string]any{
					"IdentityStoreId": testStoreID,
				})
				require.Equal(t, http.StatusOK, listRec.Code)

				users := parseResponse(t, listRec)["Users"].([]any)
				require.Len(t, users, 1)
				assert.Equal(t, "ENABLED", users[0].(map[string]any)["UserStatus"])
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

// TestUserStatusInDescribeAndList verifies UserStatus appears in all read operations.
func TestUserStatusInDescribeAndList(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createRec := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "status.everywhere",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	userID := parseResponse(t, createRec)["UserId"].(string)

	// Toggle to DISABLED.
	doRequest(t, h, "UpdateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
		"Operations": []map[string]any{
			{"AttributePath": "userStatus", "AttributeValue": "DISABLED"},
		},
	})

	// DescribeUser should reflect DISABLED.
	desc := doRequest(t, h, "DescribeUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
	})
	assert.Equal(t, "DISABLED", parseResponse(t, desc)["UserStatus"])

	// ListUsers should also reflect DISABLED.
	listRec := doRequest(t, h, "ListUsers", map[string]any{
		"IdentityStoreId": testStoreID,
	})
	users := parseResponse(t, listRec)["Users"].([]any)
	require.Len(t, users, 1)
	assert.Equal(t, "DISABLED", users[0].(map[string]any)["UserStatus"])
}

// TestUserStatusDefaultsAfterRestore verifies UserStatus preserved via snapshot.
func TestUserStatusDefaultsAfterRestore(t *testing.T) {
	t.Parallel()

	backend1 := identitystore.NewInMemoryBackend("000000000000", "us-east-1")
	h1 := identitystore.NewHandler(backend1)

	createRec := doRequest(t, h1, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "status.restore",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	userID := parseResponse(t, createRec)["UserId"].(string)

	doRequest(t, h1, "UpdateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
		"Operations": []map[string]any{
			{"AttributePath": "userStatus", "AttributeValue": "DISABLED"},
		},
	})

	snapshot := h1.Snapshot(t.Context())

	backend2 := identitystore.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := identitystore.NewHandler(backend2)
	require.NoError(t, h2.Restore(t.Context(), snapshot))

	desc := doRequest(t, h2, "DescribeUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
	})
	require.Equal(t, http.StatusOK, desc.Code)
	assert.Equal(t, "DISABLED", parseResponse(t, desc)["UserStatus"])
}

// TestUpdateUserExternalIDs verifies the externalIds attribute path in UpdateUser.
func TestUpdateUserExternalIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "update_external_ids_via_attribute_path",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "extid.update",
					"ExternalIds": []map[string]any{
						{"Issuer": "okta", "Id": "okta-old-123"},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				upd := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{
							"AttributePath": "externalIds",
							"AttributeValue": []map[string]any{
								{"Issuer": "okta", "Id": "okta-new-456"},
								{"Issuer": "azure", "Id": "azure-abc"},
							},
						},
					},
				})
				require.Equal(t, http.StatusOK, upd.Code)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				extIDs := parseResponse(t, desc)["ExternalIds"].([]any)
				require.Len(t, extIDs, 2)
				assert.Equal(t, "okta-new-456", extIDs[0].(map[string]any)["Id"])
				assert.Equal(t, "azure-abc", extIDs[1].(map[string]any)["Id"])
			},
		},
		{
			name: "clear_external_ids_via_attribute_path",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler()

				createRec := doRequest(t, h, "CreateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserName":        "extid.clear",
					"ExternalIds": []map[string]any{
						{"Issuer": "saml", "Id": "saml-xyz"},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				userID := parseResponse(t, createRec)["UserId"].(string)

				upd := doRequest(t, h, "UpdateUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
					"Operations": []map[string]any{
						{"AttributePath": "externalIds", "AttributeValue": nil},
					},
				})
				require.Equal(t, http.StatusOK, upd.Code)

				desc := doRequest(t, h, "DescribeUser", map[string]any{
					"IdentityStoreId": testStoreID,
					"UserId":          userID,
				})
				resp := parseResponse(t, desc)
				_, hasExt := resp["ExternalIds"]
				assert.False(t, hasExt, "ExternalIds should be absent after clearing")
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

// TestMultipleOperationsOnNewFields verifies multiple operations in a single UpdateUser.
func TestMultipleOperationsOnNewFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createRec := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "multi.update",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	userID := parseResponse(t, createRec)["UserId"].(string)

	upd := doRequest(t, h, "UpdateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
		"Operations": []map[string]any{
			{"AttributePath": "birthdate", "AttributeValue": "1980-08-08"},
			{"AttributePath": "website", "AttributeValue": "https://multi.example.com"},
			{"AttributePath": "userStatus", "AttributeValue": "DISABLED"},
			{
				"AttributePath": "photos",
				"AttributeValue": []map[string]any{
					{"Value": "https://multi.example.com/p.jpg", "Primary": true},
				},
			},
			{
				"AttributePath": "roles",
				"AttributeValue": []map[string]any{
					{"Value": "Lead", "Primary": true},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, upd.Code)

	desc := doRequest(t, h, "DescribeUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserId":          userID,
	})
	require.Equal(t, http.StatusOK, desc.Code)

	resp := parseResponse(t, desc)
	assert.Equal(t, "1980-08-08", resp["Birthdate"])
	assert.Equal(t, "https://multi.example.com", resp["Website"])
	assert.Equal(t, "DISABLED", resp["UserStatus"])

	photos := resp["Photos"].([]any)
	assert.Len(t, photos, 1)

	roles := resp["Roles"].([]any)
	assert.Len(t, roles, 1)
}
