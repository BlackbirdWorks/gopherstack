package identitystore_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListGroupsFilters verifies ListGroups Filters (AttributePath/AttributeValue).
func TestListGroupsFilters(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for _, name := range []string{"Alpha Team", "Beta Team", "Gamma Team"} {
		rec := doRequest(t, h, "CreateGroup", map[string]any{
			"IdentityStoreId": testStoreID,
			"DisplayName":     name,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListGroups", map[string]any{
		"IdentityStoreId": testStoreID,
		"Filters": []map[string]any{
			{"AttributePath": "displayName", "AttributeValue": "Beta Team"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	groups, ok := parseResponse(t, rec)["Groups"].([]any)
	require.True(t, ok)
	require.Len(t, groups, 1)
	assert.Equal(t, "Beta Team", groups[0].(map[string]any)["DisplayName"])
}

// TestListGroupsPagination verifies ListGroups MaxResults + NextToken pagination.
func TestListGroupsPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for i := range 4 {
		rec := doRequest(t, h, "CreateGroup", map[string]any{
			"IdentityStoreId": testStoreID,
			"DisplayName":     fmt.Sprintf("Paged Group %d", i),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec1 := doRequest(t, h, "ListGroups", map[string]any{
		"IdentityStoreId": testStoreID,
		"MaxResults":      2,
	})
	require.Equal(t, http.StatusOK, rec1.Code)
	resp1 := parseResponse(t, rec1)
	g1, _ := resp1["Groups"].([]any)
	assert.Len(t, g1, 2)

	token, hasToken := resp1["NextToken"].(string)
	require.True(t, hasToken)

	rec2 := doRequest(t, h, "ListGroups", map[string]any{
		"IdentityStoreId": testStoreID,
		"MaxResults":      2,
		"NextToken":       token,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	resp2 := parseResponse(t, rec2)
	g2, _ := resp2["Groups"].([]any)
	assert.Len(t, g2, 2)
	assert.Nil(t, resp2["NextToken"])
}

// TestListGroupsMaxResultsBound verifies ListGroups rejects MaxResults outside 1-100.
// Previously ListGroups had no MaxResults validation; ListUsers did.
func TestListGroupsMaxResultsBound(t *testing.T) {
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

// TestGetGroupIDExternalID verifies GetGroupId resolves a group by ExternalId
// (Issuer + Id compound key). Previously GetGroupId only supported displayName lookups.
func TestGetGroupIDExternalID(t *testing.T) {
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

// TestGetGroupIDExternalIDIssuerIsolation verifies that ExternalId lookups match
// on Issuer+Id together — two groups with the same Id but different Issuers are distinct.
func TestGetGroupIDExternalIDIssuerIsolation(t *testing.T) {
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

// TestGroupIDFormat verifies that generated group IDs are UUID format.
func TestGroupIDFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doRequest(t, h, "CreateGroup", map[string]any{
		"IdentityStoreId": testStoreID,
		"DisplayName":     "UUIDGroup",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	groupID := parseResponse(t, rec)["GroupId"].(string)
	assert.Len(t, groupID, 36, "GroupID should be UUID format (36 chars)")
}

// TestGroupListSorting verifies ListGroups returns deterministic sorted results.
func TestGroupListSorting(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	displayNames := []string{"Zeta", "Alpha", "Mu", "Beta"}
	for _, n := range displayNames {
		doRequest(t, h, "CreateGroup", map[string]any{
			"IdentityStoreId": testStoreID,
			"DisplayName":     n,
		})
	}

	rec1 := doRequest(t, h, "ListGroups", map[string]any{
		"IdentityStoreId": testStoreID,
	})
	rec2 := doRequest(t, h, "ListGroups", map[string]any{
		"IdentityStoreId": testStoreID,
	})

	require.Equal(t, http.StatusOK, rec1.Code)
	require.Equal(t, http.StatusOK, rec2.Code)

	groups1 := parseResponse(t, rec1)["Groups"].([]any)
	groups2 := parseResponse(t, rec2)["Groups"].([]any)

	require.Len(t, groups1, len(displayNames))

	for i := range groups1 {
		id1 := groups1[i].(map[string]any)["GroupId"].(string)
		id2 := groups2[i].(map[string]any)["GroupId"].(string)
		assert.Equal(t, id1, id2, "ListGroups order must be deterministic across calls")
	}
}

// TestGroupSortingWithManyGroups verifies sort stability with many groups.
func TestGroupSortingWithManyGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for i := range 15 {
		doRequest(t, h, "CreateGroup", map[string]any{
			"IdentityStoreId": testStoreID,
			"DisplayName":     fmt.Sprintf("Group-%03d", i),
		})
	}

	rec1 := doRequest(t, h, "ListGroups", map[string]any{
		"IdentityStoreId": testStoreID,
	})
	rec2 := doRequest(t, h, "ListGroups", map[string]any{
		"IdentityStoreId": testStoreID,
	})

	g1 := parseResponse(t, rec1)["Groups"].([]any)
	g2 := parseResponse(t, rec2)["Groups"].([]any)

	require.Len(t, g1, 15)
	for i := range g1 {
		assert.Equal(t, g1[i].(map[string]any)["GroupId"], g2[i].(map[string]any)["GroupId"])
	}
}
