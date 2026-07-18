package resourcegroups_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

func TestResourceGroupsHandler_CreateGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *resourcegroups.Handler)
		name         string
		groupName    string
		description  string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			groupName:    "my-group",
			description:  "test group",
			wantCode:     http.StatusOK,
			wantContains: []string{"Group"},
		},
		{
			name:      "duplicate",
			groupName: "my-group",
			setup: func(t *testing.T, h *resourcegroups.Handler) {
				t.Helper()
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			body := map[string]any{"Name": tt.groupName}
			if tt.description != "" {
				body["Description"] = tt.description
			}
			rec := doResourceGroupsRequest(t, h, "CreateGroup", body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestCreateGroup_NameRequired verifies that missing Name returns 400.
func TestCreateGroup_NameRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "empty_name",
			body:     map[string]any{"Name": ""},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_name",
			body:     map[string]any{"Description": "no name given"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "valid",
			body:     map[string]any{"Name": "my-group"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			rec := doResourceGroupsRequest(t, h, "CreateGroup", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestCreateGroup_NameValidation asserts 400 for invalid or empty group names.
func TestCreateGroup_NameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		groupName string
		wantCode  int
	}{
		{name: "valid_name", groupName: "my-group", wantCode: http.StatusOK},
		{name: "empty_name", groupName: "", wantCode: http.StatusBadRequest},
		{name: "too_long_name", groupName: string(make([]byte, 129)), wantCode: http.StatusBadRequest},
		{name: "invalid_chars", groupName: "bad name!", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": tt.groupName})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusBadRequest {
				require.NotEmpty(t, rec.Body.String())
			}
		})
	}
}

// TestCreateGroup_MutualExclusivity verifies that setting both ResourceQuery
// and Configuration returns an error.
func TestCreateGroup_MutualExclusivity(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // field order optimized for readability
		name     string
		body     map[string]any
		wantCode int
	}{
		{
			name: "query_only_ok",
			body: map[string]any{
				"Name": "query-only",
				"ResourceQuery": map[string]any{
					"Type":  "TAG_FILTERS_1_0",
					"Query": `{"TagFilters":[]}`,
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "config_only_ok",
			body: map[string]any{
				"Name":          "config-only",
				"Configuration": []map[string]any{{"Type": "AWS::EC2::CapacityReservationPool"}},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "both_query_and_config_rejected",
			body: map[string]any{
				"Name": "both-group",
				"ResourceQuery": map[string]any{
					"Type":  "TAG_FILTERS_1_0",
					"Query": `{"TagFilters":[]}`,
				},
				"Configuration": []map[string]any{{"Type": "AWS::EC2::CapacityReservationPool"}},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			rec := doResourceGroupsRequest(t, h, "CreateGroup", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

func TestResourceGroupsHandler_ListGroups(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g1"})
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g2"})

	rec := doResourceGroupsRequest(t, h, "ListGroups", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GroupIdentifiers")
}

// TestListGroups_Sorted verifies groups are returned sorted by name.
func TestListGroups_Sorted(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	h := resourcegroups.NewHandler(b)

	for _, name := range []string{"z-group", "a-group", "m-group"} {
		doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": name})
	}

	groups, _ := b.ListGroups(context.Background(), nil, "", 0)
	require.Len(t, groups, 3)
	assert.Equal(t, "a-group", groups[0].Name)
	assert.Equal(t, "m-group", groups[1].Name)
	assert.Equal(t, "z-group", groups[2].Name)
}

// TestListGroups_PaginationViaHandler verifies handler-level NextToken flow.
func TestListGroups_PaginationViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)

	for i := range 6 {
		doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
			"Name": fmt.Sprintf("pg-%02d", i),
		})
	}

	// Page 1: MaxResults=3.
	rec1 := doResourceGroupsRequest(t, h, "ListGroups", map[string]any{"MaxResults": 3})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	groups1 := out1["Groups"].([]any)
	assert.Len(t, groups1, 3)
	token1, _ := out1["NextToken"].(string)
	require.NotEmpty(t, token1)

	// Page 2: resume with NextToken.
	rec2 := doResourceGroupsRequest(t, h, "ListGroups", map[string]any{
		"MaxResults": 3,
		"NextToken":  token1,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	groups2 := out2["Groups"].([]any)
	assert.Len(t, groups2, 3)
	assert.Empty(t, out2["NextToken"])

	// All 6 groups are covered across both pages, no overlap.
	names1 := make(map[string]bool)
	for _, g := range groups1 {
		names1[g.(map[string]any)["Name"].(string)] = true
	}
	for _, g := range groups2 {
		name := g.(map[string]any)["Name"].(string)
		assert.False(t, names1[name], "group %s appeared in both pages", name)
	}
}

// TestListGroupsNamePrefixFilterViaHandler verifies the handler-level name-prefix filter.
func TestListGroupsNamePrefixFilterViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)

	for _, name := range []string{"web-frontend", "web-backend", "db-primary", "cache-main"} {
		doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": name})
	}

	rec := doResourceGroupsRequest(t, h, "ListGroups", map[string]any{
		"Filters": []map[string]any{
			{"Name": "name-prefix", "Values": []string{"web"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "web-frontend")
	assert.Contains(t, body, "web-backend")
	assert.NotContains(t, body, "db-primary")
	assert.NotContains(t, body, "cache-main")
}

// TestListGroupsNamePrefixFilterCases verifies the name-prefix filter on ListGroups
// across several prefixes and non-matching cases.
func TestListGroupsNamePrefixFilterCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		prefix       string
		wantContains []string
		wantExcludes []string
	}{
		{
			name:         "prefix_alpha",
			prefix:       "alpha",
			wantContains: []string{"alpha-prod", "alpha-dev"},
			wantExcludes: []string{"beta-prod"},
		},
		{
			name:         "prefix_beta",
			prefix:       "beta",
			wantContains: []string{"beta-prod"},
			wantExcludes: []string{"alpha-prod", "alpha-dev"},
		},
		{
			name:         "no_match_prefix",
			prefix:       "gamma",
			wantExcludes: []string{"alpha-prod", "beta-prod"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			for _, n := range []string{"alpha-prod", "alpha-dev", "beta-prod"} {
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": n})
			}

			rec := doResourceGroupsRequest(t, h, "ListGroups", map[string]any{
				"Filters": []map[string]any{
					{"Name": "name-prefix", "Values": []string{tt.prefix}},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)
			body := rec.Body.String()

			for _, want := range tt.wantContains {
				assert.Contains(t, body, want)
			}

			for _, notWant := range tt.wantExcludes {
				assert.NotContains(t, body, notWant)
			}
		})
	}
}

func TestResourceGroupsHandler_GetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *resourcegroups.Handler)
		name         string
		groupName    string
		wantContains []string
		wantCode     int
	}{
		{
			name:      "success",
			groupName: "my-group",
			setup: func(t *testing.T, h *resourcegroups.Handler) {
				t.Helper()
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"Group"},
		},
		{
			name:      "not_found",
			groupName: "nonexistent",
			wantCode:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doResourceGroupsRequest(t, h, "GetGroup", map[string]any{"GroupName": tt.groupName})
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestResourceGroupsHandler_DeleteGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(t *testing.T, h *resourcegroups.Handler)
		groupName string
		wantCode  int
	}{
		{
			name:      "success",
			groupName: "my-group",
			setup: func(t *testing.T, h *resourcegroups.Handler) {
				t.Helper()
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})
			},
			wantCode: http.StatusOK,
		},
		{
			name:      "not_found",
			groupName: "nonexistent",
			wantCode:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestResourceGroupsHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doResourceGroupsRequest(t, h, "DeleteGroup", map[string]any{"GroupName": tt.groupName})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestDeleteGroup_Cascade verifies cascaded deletion of resources, config, and tasks.
func TestDeleteGroup_Cascade(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	h := resourcegroups.NewHandler(b)

	// Create a group and populate it.
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g1"})
	doResourceGroupsRequest(t, h, "GroupResources", map[string]any{
		"Group":        "g1",
		"ResourceArns": []string{"arn:aws:s3:::bucket1"},
	})
	doResourceGroupsRequest(t, h, "PutGroupConfiguration", map[string]any{
		"Group":         "g1",
		"Configuration": []map[string]any{{"Type": "AWS::EC2::CapacityReservationPool"}},
	})
	doResourceGroupsRequest(t, h, "StartTagSyncTask", map[string]any{
		"Group":   "g1",
		"RoleArn": "arn:aws:iam::000000000000:role/role1",
	})

	assert.Equal(t, 1, resourcegroups.GroupCount(b))
	assert.Equal(t, 1, resourcegroups.GroupResourceCount(b))
	assert.Equal(t, 1, resourcegroups.GroupConfigurationCount(b))
	assert.Equal(t, 1, resourcegroups.TagSyncTaskCount(b))

	// Delete the group.
	rec := doResourceGroupsRequest(t, h, "DeleteGroup", map[string]any{"Group": "g1"})
	require.Equal(t, http.StatusOK, rec.Code)

	// All cascade-deleted.
	assert.Equal(t, 0, resourcegroups.GroupCount(b))
	assert.Equal(t, 0, resourcegroups.GroupResourceCount(b))
	assert.Equal(t, 0, resourcegroups.GroupConfigurationCount(b))
	assert.Equal(t, 0, resourcegroups.TagSyncTaskCount(b))
}

// TestGroupCount_AfterCreateAndDelete verifies count helpers work.
func TestGroupCount_AfterCreateAndDelete(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	h := resourcegroups.NewHandler(b)

	assert.Equal(t, 0, resourcegroups.GroupCount(b))

	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g1"})
	assert.Equal(t, 1, resourcegroups.GroupCount(b))

	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g2"})
	assert.Equal(t, 2, resourcegroups.GroupCount(b))

	doResourceGroupsRequest(t, h, "DeleteGroup", map[string]any{"Group": "g1"})
	assert.Equal(t, 1, resourcegroups.GroupCount(b))
}

func TestResourceGroupsHandler_RESTCreateGroup(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRESTRequest(t, h, "/groups", map[string]any{"Name": "rest-group"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestResourceGroupsHandler_RESTGetGroup(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRESTRequest(t, h, "/groups", map[string]any{"Name": "rest-group"})
	rec := doResourceGroupsRESTRequest(t, h, "/get-group", map[string]any{"GroupName": "rest-group"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestResourceGroupsHandler_RESTDeleteGroup(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRESTRequest(t, h, "/groups", map[string]any{"Name": "rest-group"})
	rec := doResourceGroupsRESTRequest(t, h, "/delete-group", map[string]any{"GroupName": "rest-group"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestResourceGroupsHandler_RESTListGroups(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRESTRequest(t, h, "/groups-list", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestResourceGroupsHandler_UpdateGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, h *resourcegroups.Handler)
		body        map[string]any
		name        string
		wantContain string
		wantCode    int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *resourcegroups.Handler) {
				t.Helper()
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
					"Name": "my-group", "Description": "old",
				})
			},
			body:        map[string]any{"GroupName": "my-group", "Description": "new-desc"},
			wantCode:    http.StatusOK,
			wantContain: "new-desc",
		},
		{
			name:     "not_found",
			body:     map[string]any{"GroupName": "missing"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doResourceGroupsRequest(t, h, "UpdateGroup", tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContain)
			}
		})
	}
}

// TestUpdateGroup_NameRequired verifies that missing group name returns 400.
func TestUpdateGroup_NameRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *resourcegroups.Handler)
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "no_name",
			body:     map[string]any{"Description": "updated"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "with_group_field",
			setup: func(t *testing.T, h *resourcegroups.Handler) {
				t.Helper()
				rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"Group": "my-group", "Description": "updated"},
			wantCode: http.StatusOK,
		},
		{
			name: "with_group_name_field",
			setup: func(t *testing.T, h *resourcegroups.Handler) {
				t.Helper()
				rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "other-group"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"GroupName": "other-group", "Description": "updated"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doResourceGroupsRequest(t, h, "UpdateGroup", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestUpdateGroup_CriticalityBoundary verifies boundary values 1 and 5.
func TestUpdateGroup_CriticalityBoundary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		criticality int
		wantCode    int
	}{
		{name: "boundary_1", criticality: 1, wantCode: http.StatusOK},
		{name: "boundary_5", criticality: 5, wantCode: http.StatusOK},
		{name: "too_low_minus1", criticality: -1, wantCode: http.StatusBadRequest},
		{name: "too_high_6", criticality: 6, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "crit-group"})
			rec := doResourceGroupsRequest(t, h, "UpdateGroup", map[string]any{
				"Group":       "crit-group",
				"Criticality": tt.criticality,
			})
			assert.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

func TestResourceGroupsHandler_UpdateGroupQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, h *resourcegroups.Handler)
		body        map[string]any
		name        string
		wantContain string
		wantCode    int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *resourcegroups.Handler) {
				t.Helper()
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
					"Name": "q-group",
					"ResourceQuery": map[string]any{
						"Type": "TAG_FILTERS_1_0", "Query": "{}",
					},
				})
			},
			body: map[string]any{
				"GroupName": "q-group",
				"ResourceQuery": map[string]any{
					"Type": "TAG_FILTERS_1_0", "Query": "{\"updated\":true}",
				},
			},
			wantCode:    http.StatusOK,
			wantContain: "updated",
		},
		{
			name:     "not_found",
			body:     map[string]any{"GroupName": "no-such"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doResourceGroupsRequest(t, h, "UpdateGroupQuery", tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContain)
			}
		})
	}
}

// TestUpdateGroupQuery_NameRequired verifies that missing group name returns 400.
func TestUpdateGroupQuery_NameRequired(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRequest(t, h, "UpdateGroupQuery", map[string]any{
		"ResourceQuery": map[string]string{"Type": "TAG_FILTERS_1_0", "Query": "{}"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestResourceGroupsHandler_DeleteGroup_EchoesGroup verifies DeleteGroupOutput
// echoes back the deleted group's description, matching AWS.
func TestResourceGroupsHandler_DeleteGroup_EchoesGroup(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name":        "echo-group",
		"Description": "echo desc",
	})

	rec := doResourceGroupsRequest(t, h, "DeleteGroup", map[string]any{"Group": "echo-group"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Group struct {
			Name        string `json:"Name"`
			Description string `json:"Description"`
		} `json:"Group"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "echo-group", out.Group.Name)
	assert.Equal(t, "echo desc", out.Group.Description)
}

// TestResourceGroupsHandler_ListGroups_IdentifierFields verifies that
// GroupIdentifiers include DisplayName, Criticality, and Owner alongside
// GroupName/GroupArn/Description, matching types.GroupIdentifier.
func TestResourceGroupsHandler_ListGroups_IdentifierFields(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "ident-group"})
	doResourceGroupsRequest(t, h, "UpdateGroup", map[string]any{
		"Group":       "ident-group",
		"DisplayName": "Ident Group",
		"Criticality": 2,
	})

	rec := doResourceGroupsRequest(t, h, "ListGroups", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		GroupIdentifiers []map[string]any `json:"GroupIdentifiers"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.GroupIdentifiers, 1)
	assert.Equal(t, "Ident Group", out.GroupIdentifiers[0]["DisplayName"])
	criticality, ok := out.GroupIdentifiers[0]["Criticality"].(float64)
	require.True(t, ok, "Criticality must be a JSON number")
	assert.Equal(t, 2, int(criticality))
}
