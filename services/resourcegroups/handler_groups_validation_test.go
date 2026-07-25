package resourcegroups_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

// TestGroupNameValidation covers group name rules: must match
// [a-zA-Z0-9_.−]+, 1-300 chars, no aws/AWS prefix.
func TestGroupNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		wantCode int
	}{
		{name: "valid_simple", input: "my-group", wantCode: http.StatusOK},
		{name: "valid_with_dot", input: "my.group", wantCode: http.StatusOK},
		{name: "valid_with_underscore", input: "my_group", wantCode: http.StatusOK},
		{name: "valid_alphanumeric", input: "Group123", wantCode: http.StatusOK},
		{name: "empty_name", input: "", wantCode: http.StatusBadRequest},
		{name: "reserved_aws_lower", input: "aws-group", wantCode: http.StatusBadRequest},
		{name: "reserved_aws_upper", input: "AWS-group", wantCode: http.StatusBadRequest},
		{name: "reserved_aws_mixed", input: "Aws-group", wantCode: http.StatusBadRequest},
		{name: "invalid_spaces", input: "my group", wantCode: http.StatusBadRequest},
		{name: "invalid_slash", input: "my/group", wantCode: http.StatusBadRequest},
		{name: "invalid_at", input: "my@group", wantCode: http.StatusBadRequest},
		{name: "too_long_301", input: strings.Repeat("a", 301), wantCode: http.StatusBadRequest},
		{name: "exactly_300", input: strings.Repeat("g", 300), wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": tt.input})
			assert.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestResourceQueryValidation covers that ResourceQuery.Type must be
// TAG_FILTERS_1_0 or CLOUDFORMATION_STACK_1_0; Query must be valid JSON.
func TestResourceQueryValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		query    map[string]any
		name     string
		wantCode int
	}{
		{
			name: "valid_tag_filters",
			query: map[string]any{
				"Type":  "TAG_FILTERS_1_0",
				"Query": `{"ResourceTypeFilters":[],"TagFilters":[]}`,
			},
			wantCode: http.StatusOK,
		},
		{
			name: "valid_cloudformation",
			query: map[string]any{
				"Type":  "CLOUDFORMATION_STACK_1_0",
				"Query": `{"StackIdentifier":"arn:aws:cloudformation:us-east-1:123:stack/s/id"}`,
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "invalid_type",
			query:    map[string]any{"Type": "UNKNOWN_TYPE_1_0", "Query": `{}`},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_type",
			query:    map[string]any{"Type": "", "Query": `{}`},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_query_string",
			query:    map[string]any{"Type": "TAG_FILTERS_1_0", "Query": ""},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_json_query",
			query:    map[string]any{"Type": "TAG_FILTERS_1_0", "Query": "not-json"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			body := map[string]any{"Name": "q-group-" + tt.name, "ResourceQuery": tt.query}
			rec := doResourceGroupsRequest(t, h, "CreateGroup", body)
			assert.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestGroupFields covers that GetGroup returns Criticality and DisplayName
// fields; Tags are NOT included in the Group body. It also verifies the real
// wire field for the group owner is "Owner" (never the legacy "OwnerId"),
// and that it is unset by default since neither CreateGroup nor UpdateGroup
// accept an Owner input in this emulator.
func TestGroupFields(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name":        "field-group",
		"Description": "test desc",
		"Tags":        map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	updRec := doResourceGroupsRequest(t, h, "UpdateGroup", map[string]any{
		"Group":       "field-group",
		"DisplayName": "Field Group",
		"Criticality": 3,
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	rec2 := doResourceGroupsRequest(t, h, "GetGroup", map[string]any{"Group": "field-group"})
	require.Equal(t, http.StatusOK, rec2.Code)

	body := rec2.Body.String()
	assert.Contains(t, body, `"DisplayName":"Field Group"`)
	assert.Contains(t, body, `"Criticality":3`)
	assert.Contains(t, body, `"Name"`)
	assert.Contains(t, body, `"field-group"`)
	assert.NotContains(t, body, `"Tags"`, "GetGroup should NOT include Tags in the Group body")
	assert.NotContains(t, body, `"OwnerId"`, "the AWS wire field is Owner, not the legacy OwnerId")
	assert.NotContains(t, body, `"Owner"`, "Owner is unset by default")
}

// TestUpdateGroupCriticalityDisplayName covers that UpdateGroup accepts
// Criticality (1-10, per the real API's documented "scale of 1 to 10") and
// DisplayName in addition to Description.
func TestUpdateGroupCriticalityDisplayName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update      map[string]any
		name        string
		wantInBody  string
		wantNotBody string
		wantCode    int
	}{
		{
			name:       "update_description_only",
			update:     map[string]any{"Group": "upd-group", "Description": "new desc"},
			wantCode:   http.StatusOK,
			wantInBody: "new desc",
		},
		{
			name:       "update_display_name",
			update:     map[string]any{"Group": "upd-group", "DisplayName": "My Group"},
			wantCode:   http.StatusOK,
			wantInBody: "My Group",
		},
		{
			name:       "update_criticality_valid",
			update:     map[string]any{"Group": "upd-group", "Criticality": 3},
			wantCode:   http.StatusOK,
			wantInBody: `"Criticality":3`,
		},
		{
			name:     "criticality_too_low",
			update:   map[string]any{"Group": "upd-group", "Criticality": -1},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "criticality_too_high",
			update:   map[string]any{"Group": "upd-group", "Criticality": 11},
			wantCode: http.StatusBadRequest,
		},
		{
			name:       "criticality_boundary_1",
			update:     map[string]any{"Group": "upd-group", "Criticality": 1},
			wantCode:   http.StatusOK,
			wantInBody: `"Criticality":1`,
		},
		{
			name:       "criticality_boundary_10",
			update:     map[string]any{"Group": "upd-group", "Criticality": 10},
			wantCode:   http.StatusOK,
			wantInBody: `"Criticality":10`,
		},
		{
			name:       "update_owner",
			update:     map[string]any{"Group": "upd-group", "Owner": "team-x@example.com"},
			wantCode:   http.StatusOK,
			wantInBody: `"Owner":"team-x@example.com"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "upd-group"})

			rec := doResourceGroupsRequest(t, h, "UpdateGroup", tt.update)
			assert.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())

			if tt.wantInBody != "" && rec.Code == http.StatusOK {
				assert.Contains(t, rec.Body.String(), tt.wantInBody)
			}
		})
	}
}

// TestListGroupsBothFields covers that ListGroups must return both Groups
// (legacy) and GroupIdentifiers fields.
func TestListGroupsBothFields(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "list-a"})
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "list-b"})

	rec := doResourceGroupsRequest(t, h, "ListGroups", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, `"Groups"`, "ListGroups must include Groups field for SDK compat")
	assert.Contains(t, body, `"GroupIdentifiers"`, "ListGroups must include GroupIdentifiers field")
	assert.Contains(t, body, "list-a")
	assert.Contains(t, body, "list-b")
}

// TestListGroupsFilters covers that ListGroups must filter by
// configuration-type and resource-type.
func TestListGroupsFilters(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)

	// Create groups with different configurations.
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name":          "host-mgmt-group",
		"Configuration": []map[string]any{{"Type": "AWS::EC2::HostManagement"}},
	})
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name":          "capacity-pool-group",
		"Configuration": []map[string]any{{"Type": "AWS::EC2::CapacityReservationPool"}},
	})
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name": "query-group",
		"ResourceQuery": map[string]any{
			"Type":  "TAG_FILTERS_1_0",
			"Query": `{}`,
		},
	})
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name": "generic-group",
		"Configuration": []map[string]any{
			{
				"Type": "AWS::ResourceGroups::Generic",
				"Parameters": []map[string]any{
					{"Name": "allowed-resource-types", "Values": []string{"AWS::EC2::Instance"}},
				},
			},
		},
	})
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name":        "owned-group",
		"Owner":       "team-x@example.com",
		"DisplayName": "Owned Group",
		"Criticality": 5,
	})

	tests := []struct {
		name         string
		filters      []map[string]any
		wantContains []string
		wantExcludes []string
	}{
		{
			name:    "no_filter_returns_all",
			filters: nil,
			wantContains: []string{
				"host-mgmt-group",
				"capacity-pool-group",
				"query-group",
				"generic-group",
				"owned-group",
			},
		},
		{
			name: "filter_by_configuration_type_host_management",
			filters: []map[string]any{
				{"Name": "configuration-type", "Values": []string{"AWS::EC2::HostManagement"}},
			},
			wantContains: []string{"host-mgmt-group"},
			wantExcludes: []string{"capacity-pool-group", "query-group"},
		},
		{
			name: "filter_by_configuration_type_capacity_pool",
			filters: []map[string]any{
				{
					"Name":   "configuration-type",
					"Values": []string{"AWS::EC2::CapacityReservationPool"},
				},
			},
			wantContains: []string{"capacity-pool-group"},
			wantExcludes: []string{"host-mgmt-group", "query-group"},
		},
		{
			name: "filter_by_resource_type",
			filters: []map[string]any{
				{"Name": "resource-type", "Values": []string{"AWS::EC2::Instance"}},
			},
			wantContains: []string{"generic-group"},
			wantExcludes: []string{"host-mgmt-group", "query-group"},
		},
		{
			name: "filter_no_match",
			filters: []map[string]any{
				{"Name": "configuration-type", "Values": []string{"AWS::NonExistent::Type"}},
			},
			wantExcludes: []string{"host-mgmt-group", "capacity-pool-group", "generic-group"},
		},
		{
			name: "filter_by_owner",
			filters: []map[string]any{
				{"Name": "owner", "Values": []string{"team-x@example.com"}},
			},
			wantContains: []string{"owned-group"},
			wantExcludes: []string{"host-mgmt-group", "capacity-pool-group", "generic-group"},
		},
		{
			name: "filter_by_display_name",
			filters: []map[string]any{
				{"Name": "display-name", "Values": []string{"Owned Group"}},
			},
			wantContains: []string{"owned-group"},
			wantExcludes: []string{"host-mgmt-group", "capacity-pool-group", "generic-group"},
		},
		{
			name: "filter_by_criticality",
			filters: []map[string]any{
				{"Name": "criticality", "Values": []string{"5"}},
			},
			wantContains: []string{"owned-group"},
			wantExcludes: []string{"host-mgmt-group", "capacity-pool-group", "generic-group"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doResourceGroupsRequest(
				t,
				h,
				"ListGroups",
				map[string]any{"Filters": tt.filters},
			)
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

// TestListGroupsOutputShape verifies the exact JSON shape of ListGroups output.
func TestListGroupsOutputShape(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name":        "shape-group",
		"Description": "shape desc",
	})

	rec := doResourceGroupsRequest(t, h, "ListGroups", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	groups, ok := out["Groups"].([]any)
	require.True(t, ok, "Groups must be a list")
	require.Len(t, groups, 1)

	g := groups[0].(map[string]any)
	assert.Equal(t, "shape-group", g["Name"])
	assert.Contains(t, g["GroupArn"].(string), "shape-group")

	identifiers, ok := out["GroupIdentifiers"].([]any)
	require.True(t, ok, "GroupIdentifiers must be a list")
	require.Len(t, identifiers, 1)
	ident := identifiers[0].(map[string]any)
	assert.Equal(t, "shape-group", ident["GroupName"])
}

// TestDescriptionLengthValidation covers that Description must be at most
// 512 characters.
func TestDescriptionLengthValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		desc     string
		wantCode int
	}{
		{name: "empty_desc", desc: "", wantCode: http.StatusOK},
		{name: "exactly_512", desc: strings.Repeat("d", 512), wantCode: http.StatusOK},
		{name: "too_long_513", desc: strings.Repeat("d", 513), wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
				"Name":        "desc-group-" + tt.name,
				"Description": tt.desc,
			})
			assert.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestUpdateGroupQueryValidation covers that UpdateGroupQuery must also
// validate the ResourceQuery.
func TestUpdateGroupQueryValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		query    map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "valid_query",
			query:    map[string]any{"Type": "TAG_FILTERS_1_0", "Query": `{"TagFilters":[]}`},
			wantCode: http.StatusOK,
		},
		{
			name:     "invalid_type",
			query:    map[string]any{"Type": "BAD_TYPE", "Query": `{}`},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "non_json_query",
			query:    map[string]any{"Type": "TAG_FILTERS_1_0", "Query": "bad-json"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
				"Name": "uq-group",
				"ResourceQuery": map[string]any{
					"Type":  "TAG_FILTERS_1_0",
					"Query": `{}`,
				},
			})

			rec := doResourceGroupsRequest(t, h, "UpdateGroupQuery", map[string]any{
				"Group":         "uq-group",
				"ResourceQuery": tt.query,
			})
			assert.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestOwnerId verifies the account ID appears in the group's ARN (the only
// place AWS embeds account ownership), and that the Owner field -- a
// free-form, caller-supplied identifier unrelated to the account ID -- is
// left unset rather than fabricated from the account ID.
func TestOwnerId(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("111111111111", "us-east-1")
	h := resourcegroups.NewHandler(b)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "owner-group"})

	rec := doResourceGroupsRequest(t, h, "GetGroup", map[string]any{"Group": "owner-group"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	group, ok := out["Group"].(map[string]any)
	require.True(t, ok)

	assert.Contains(t, group["GroupArn"], "111111111111")
	assert.NotContains(t, group, "Owner")
	assert.NotContains(t, group, "OwnerId")
}

// TestCreateGroupIdentityFields verifies CreateGroup accepts Owner,
// DisplayName, and Criticality at creation time (all documented members of
// the real CreateGroupInput) and echoes them back in CreateGroupOutput.Group.
func TestCreateGroupIdentityFields(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)

	rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name":        "identity-handler-group",
		"Owner":       "team-x@example.com",
		"DisplayName": "Identity Handler Group",
		"Criticality": 8,
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	group, ok := out["Group"].(map[string]any)
	require.True(t, ok)

	assert.Equal(t, "team-x@example.com", group["Owner"])
	assert.Equal(t, "Identity Handler Group", group["DisplayName"])
	assert.InEpsilon(t, float64(8), group["Criticality"], 0)
}

// TestListGroupsGroupIdentifiersShape verifies exact shape of GroupIdentifiers.
func TestListGroupsGroupIdentifiersShape(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name":        "shape-grp",
		"Description": "shape desc",
	})

	rec := doResourceGroupsRequest(t, h, "ListGroups", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	identifiers := out["GroupIdentifiers"].([]any)
	require.Len(t, identifiers, 1)

	ident := identifiers[0].(map[string]any)
	assert.Equal(t, "shape-grp", ident["GroupName"])
	assert.Contains(t, ident["GroupArn"].(string), "shape-grp")
	assert.Equal(t, "shape desc", ident["Description"])
}
