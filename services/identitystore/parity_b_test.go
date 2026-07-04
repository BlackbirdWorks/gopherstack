package identitystore_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_ListGroups_MaxResultsBound verifies ListGroups rejects MaxResults outside 1-100.
// Previously ListGroups had no MaxResults validation; ListUsers did.
func TestParity_ListGroups_MaxResultsBound(t *testing.T) {
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

			rec := doRequest(t, h, "ListGroups", body)
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestParity_ListGroupMemberships_MaxResultsBound verifies ListGroupMemberships validates MaxResults.
func TestParity_ListGroupMemberships_MaxResultsBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxResults any
		name       string
		wantStatus int
	}{
		{name: "unset_ok", maxResults: nil, wantStatus: http.StatusOK},
		{name: "in_range_ok", maxResults: 10, wantStatus: http.StatusOK},
		{name: "over_bound_rejected", maxResults: 101, wantStatus: http.StatusBadRequest},
		{name: "zero_as_unset_ok", maxResults: 0, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create a group to satisfy the required GroupId.
			createRec := doRequest(t, h, "CreateGroup", map[string]any{
				"IdentityStoreId": testStoreID,
				"DisplayName":     "parity-mb-group-" + tt.name,
			})
			require.Equal(t, http.StatusOK, createRec.Code)
			groupID := parseResponse(t, createRec)["GroupId"].(string)

			body := map[string]any{
				"IdentityStoreId": testStoreID,
				"GroupId":         groupID,
			}
			if tt.maxResults != nil {
				body["MaxResults"] = tt.maxResults
			}

			rec := doRequest(t, h, "ListGroupMemberships", body)
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestParity_ListGroupMembershipsForMember_MaxResultsBound verifies ListGroupMembershipsForMember
// validates MaxResults.
func TestParity_ListGroupMembershipsForMember_MaxResultsBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxResults any
		name       string
		wantStatus int
	}{
		{name: "unset_ok", maxResults: nil, wantStatus: http.StatusOK},
		{name: "in_range_ok", maxResults: 10, wantStatus: http.StatusOK},
		{name: "over_bound_rejected", maxResults: 200, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create a user to satisfy the required MemberId.
			createRec := doRequest(t, h, "CreateUser", map[string]any{
				"IdentityStoreId": testStoreID,
				"UserName":        "parity-lfm-" + tt.name,
			})
			require.Equal(t, http.StatusOK, createRec.Code)
			userID := parseResponse(t, createRec)["UserId"].(string)

			body := map[string]any{
				"IdentityStoreId": testStoreID,
				"MemberId":        map[string]string{"UserId": userID},
			}
			if tt.maxResults != nil {
				body["MaxResults"] = tt.maxResults
			}

			rec := doRequest(t, h, "ListGroupMembershipsForMember", body)
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestParity_GetGroupId_ExternalId verifies GetGroupId resolves a group by ExternalId
// (Issuer + Id compound key). Previously GetGroupId only supported displayName lookups.
func TestParity_GetGroupId_ExternalId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		issuer     string
		id         string
		wantStatus int
	}{
		{
			name:       "resolves_by_issuer_and_id",
			issuer:     "https://sso.example.com",
			id:         "ext-group-001",
			wantStatus: http.StatusOK,
		},
		{
			name:       "wrong_id_returns_not_found",
			issuer:     "https://sso.example.com",
			id:         "ext-group-999",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create a group with ExternalIds.
			createRec := doRequest(t, h, "CreateGroup", map[string]any{
				"IdentityStoreId": testStoreID,
				"DisplayName":     "ext-group-" + tt.name,
				"ExternalIds": []map[string]string{
					{"Issuer": "https://sso.example.com", "Id": "ext-group-001"},
				},
			})
			require.Equal(t, http.StatusOK, createRec.Code)
			createdGroupID := parseResponse(t, createRec)["GroupId"].(string)

			rec := doRequest(t, h, "GetGroupId", map[string]any{
				"IdentityStoreId": testStoreID,
				"AlternateIdentifier": map[string]any{
					"ExternalId": map[string]string{
						"Issuer": tt.issuer,
						"Id":     tt.id,
					},
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body.String())

			if tt.wantStatus == http.StatusOK {
				resp := parseResponse(t, rec)
				assert.Equal(t, createdGroupID, resp["GroupId"])
			}
		})
	}
}

// TestParity_GetGroupId_ExternalId_IssuerIsolation verifies that ExternalId lookups match
// on Issuer+Id together — two groups with the same Id but different Issuers are distinct.
func TestParity_GetGroupId_ExternalId_IssuerIsolation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create two groups with the same Id but different Issuers.
	createRec1 := doRequest(t, h, "CreateGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"DisplayName":     "group-issuer-a",
		"ExternalIds": []map[string]string{
			{"Issuer": "https://issuer-a.example.com", "Id": "shared-ext-id"},
		},
	})
	require.Equal(t, http.StatusOK, createRec1.Code)
	groupA := parseResponse(t, createRec1)["GroupId"].(string)

	createRec2 := doRequest(t, h, "CreateGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"DisplayName":     "group-issuer-b",
		"ExternalIds": []map[string]string{
			{"Issuer": "https://issuer-b.example.com", "Id": "shared-ext-id"},
		},
	})
	require.Equal(t, http.StatusOK, createRec2.Code)
	groupB := parseResponse(t, createRec2)["GroupId"].(string)

	// Lookup by issuer-a must return group A.
	recA := doRequest(t, h, "GetGroupId", map[string]any{
		"IdentityStoreId": testStoreID,
		"AlternateIdentifier": map[string]any{
			"ExternalId": map[string]string{
				"Issuer": "https://issuer-a.example.com",
				"Id":     "shared-ext-id",
			},
		},
	})
	require.Equal(t, http.StatusOK, recA.Code)
	assert.Equal(t, groupA, parseResponse(t, recA)["GroupId"])

	// Lookup by issuer-b must return group B.
	recB := doRequest(t, h, "GetGroupId", map[string]any{
		"IdentityStoreId": testStoreID,
		"AlternateIdentifier": map[string]any{
			"ExternalId": map[string]string{
				"Issuer": "https://issuer-b.example.com",
				"Id":     "shared-ext-id",
			},
		},
	})
	require.Equal(t, http.StatusOK, recB.Code)
	assert.Equal(t, groupB, parseResponse(t, recB)["GroupId"])
}

// TestParity_GetUserId_ExternalId_IssuerIsolation verifies that GetUserId ExternalId lookups
// match on Issuer+Id together, not Id alone.
func TestParity_GetUserId_ExternalId_IssuerIsolation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createRec1 := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "user-issuer-a",
		"ExternalIds": []map[string]string{
			{"Issuer": "https://idp-a.example.com", "Id": "shared-user-ext"},
		},
	})
	require.Equal(t, http.StatusOK, createRec1.Code)
	userA := parseResponse(t, createRec1)["UserId"].(string)

	createRec2 := doRequest(t, h, "CreateUser", map[string]any{
		"IdentityStoreId": testStoreID,
		"UserName":        "user-issuer-b",
		"ExternalIds": []map[string]string{
			{"Issuer": "https://idp-b.example.com", "Id": "shared-user-ext"},
		},
	})
	require.Equal(t, http.StatusOK, createRec2.Code)
	userB := parseResponse(t, createRec2)["UserId"].(string)

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
