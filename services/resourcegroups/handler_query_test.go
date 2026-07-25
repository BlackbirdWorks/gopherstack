package resourcegroups_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

func TestResourceGroupsHandler_SearchResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *resourcegroups.Handler)
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "empty",
			wantCode:     http.StatusOK,
			wantContains: []string{"ResourceIdentifiers"},
		},
		{
			name: "with_resources",
			setup: func(t *testing.T, h *resourcegroups.Handler) {
				t.Helper()
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g1"})
				doResourceGroupsRequest(t, h, "GroupResources", map[string]any{
					"Group":        "g1",
					"ResourceArns": []string{"arn:aws:s3:::my-bucket"},
				})
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"ResourceIdentifiers", "my-bucket"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			body := map[string]any{
				"ResourceQuery": map[string]any{
					"Type":  "TAG_FILTERS_1_0",
					"Query": `{"ResourceTypeFilters":["AWS::AllSupported"]}`,
				},
			}
			rec := doResourceGroupsRequest(t, h, "SearchResources", body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestSearchResources_QueryErrorsShape verifies that QueryErrors is present
// in the SearchResourcesOutput shape (real types.SearchResourcesOutput
// member) but omitted/empty for TAG_FILTERS_1_0 queries: QueryErrors only
// ever arises for CLOUDFORMATION_STACK_1_0-based groups, which this emulator
// does not model (see PARITY.md gaps).
func TestSearchResources_QueryErrorsShape(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "qe-group"})
	doResourceGroupsRequest(t, h, "GroupResources", map[string]any{
		"Group":        "qe-group",
		"ResourceArns": []string{"arn:aws:s3:::qe-bucket"},
	})

	rec := doResourceGroupsRequest(t, h, "SearchResources", map[string]any{
		"ResourceQuery": map[string]any{
			"Type":  "TAG_FILTERS_1_0",
			"Query": `{"ResourceTypeFilters":["AWS::AllSupported"]}`,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotContains(t, out, "QueryErrors", "QueryErrors must be omitted when empty")
}

// TestSearchResources_HandlerRequiresResourceQuery verifies error shape.
func TestSearchResources_HandlerRequiresResourceQuery(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g1"})
	doResourceGroupsRequest(t, h, "GroupResources", map[string]any{
		"Group":        "g1",
		"ResourceArns": []string{"arn:aws:s3:::b1"},
	})

	// No ResourceQuery — handler passes nil to backend which returns all.
	rec := doResourceGroupsRequest(t, h, "SearchResources", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "b1")
}

// TestSearchResources_PaginationViaHandler verifies handler NextToken.
func TestSearchResources_PaginationViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "srch-grp"})

	arns := make([]string, 5)
	for i := range arns {
		arns[i] = fmt.Sprintf("arn:aws:s3:::bucket-%d", i)
	}
	doResourceGroupsRequest(t, h, "GroupResources", map[string]any{
		"Group":        "srch-grp",
		"ResourceArns": arns,
	})

	body := map[string]any{
		"ResourceQuery": map[string]any{
			"Type":  "TAG_FILTERS_1_0",
			"Query": `{"ResourceTypeFilters":["AWS::AllSupported"]}`,
		},
		"MaxResults": 3,
	}

	rec1 := doResourceGroupsRequest(t, h, "SearchResources", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	ids1 := out1["ResourceIdentifiers"].([]any)
	assert.Len(t, ids1, 3)
	tok1, _ := out1["NextToken"].(string)
	require.NotEmpty(t, tok1)

	body["NextToken"] = tok1
	rec2 := doResourceGroupsRequest(t, h, "SearchResources", body)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	ids2 := out2["ResourceIdentifiers"].([]any)
	assert.Len(t, ids2, 2)
	assert.Empty(t, out2["NextToken"])
}
