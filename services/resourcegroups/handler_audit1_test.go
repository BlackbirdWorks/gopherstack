package resourcegroups_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

// TestAudit1_GroupNameValidation covers issue #8: group name must match
// [a-zA-Z0-9_.−]+, 1-300 chars, no aws/AWS prefix.
func TestAudit1_GroupNameValidation(t *testing.T) {
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

// TestAudit1_ResourceQueryValidation covers issue #3: ResourceQuery.Type must
// be TAG_FILTERS_1_0 or CLOUDFORMATION_STACK_1_0; Query must be valid JSON.
func TestAudit1_ResourceQueryValidation(t *testing.T) {
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

// TestAudit1_CreateGroupWithConfiguration covers issue #4: CreateGroup must
// atomically persist Configuration alongside the group.
func TestAudit1_CreateGroupWithConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantCfgType string
		config      []map[string]any
		wantCode    int
	}{
		{
			name: "valid_generic_type",
			config: []map[string]any{
				{"Type": "AWS::ResourceGroups::Generic"},
			},
			wantCode:    http.StatusOK,
			wantCfgType: "AWS::ResourceGroups::Generic",
		},
		{
			name: "valid_ec2_capacity_pool",
			config: []map[string]any{
				{"Type": "AWS::EC2::CapacityReservationPool"},
			},
			wantCode:    http.StatusOK,
			wantCfgType: "AWS::EC2::CapacityReservationPool",
		},
		{
			name: "valid_appregistry_application",
			config: []map[string]any{
				{"Type": "AWS::AppRegistry::Application"},
			},
			wantCode:    http.StatusOK,
			wantCfgType: "AWS::AppRegistry::Application",
		},
		{
			name: "invalid_unknown_type",
			config: []map[string]any{
				{"Type": "AWS::S3::Bucket"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "valid_ec2_host_management_with_params",
			config: []map[string]any{
				{
					"Type": "AWS::EC2::HostManagement",
					"Parameters": []map[string]any{
						{"Name": "allowed-resource-types", "Values": []string{"AWS::EC2::Host"}},
					},
				},
			},
			wantCode:    http.StatusOK,
			wantCfgType: "AWS::EC2::HostManagement",
		},
		{
			name: "invalid_unknown_param",
			config: []map[string]any{
				{
					"Type": "AWS::EC2::CapacityReservationPool",
					"Parameters": []map[string]any{
						{"Name": "unknown-param", "Values": []string{"v"}},
					},
				},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			body := map[string]any{"Name": "cfg-" + tt.name, "Configuration": tt.config}
			rec := doResourceGroupsRequest(t, h, "CreateGroup", body)
			assert.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())

			if tt.wantCfgType != "" {
				// Verify configuration was stored atomically: GetGroupConfiguration returns it.
				rec2 := doResourceGroupsRequest(
					t,
					h,
					"GetGroupConfiguration",
					map[string]any{"Group": "cfg-" + tt.name},
				)
				require.Equal(t, http.StatusOK, rec2.Code)
				assert.Contains(t, rec2.Body.String(), tt.wantCfgType)
			}
		})
	}
}

// TestAudit1_ConfigurationTypeValidation covers issue #5: PutGroupConfiguration
// must validate Type and Parameters against the allow-list.
func TestAudit1_ConfigurationTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantInMsg string
		config    []map[string]any
		wantCode  int
	}{
		{
			name:     "valid_generic_no_params",
			config:   []map[string]any{{"Type": "AWS::ResourceGroups::Generic"}},
			wantCode: http.StatusOK,
		},
		{
			name: "valid_generic_allowed_param",
			config: []map[string]any{
				{
					"Type": "AWS::ResourceGroups::Generic",
					"Parameters": []map[string]any{
						{
							"Name":   "allowed-resource-types",
							"Values": []string{"AWS::EC2::Instance"},
						},
					},
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "valid_capacity_pool_no_params",
			config:   []map[string]any{{"Type": "AWS::EC2::CapacityReservationPool"}},
			wantCode: http.StatusOK,
		},
		{
			name: "invalid_capacity_pool_with_params",
			config: []map[string]any{
				{
					"Type": "AWS::EC2::CapacityReservationPool",
					"Parameters": []map[string]any{
						{"Name": "not-allowed", "Values": []string{"v"}},
					},
				},
			},
			wantCode:  http.StatusBadRequest,
			wantInMsg: "does not accept any parameters",
		},
		{
			name:      "invalid_type_s3_bucket",
			config:    []map[string]any{{"Type": "AWS::S3::Bucket"}},
			wantCode:  http.StatusBadRequest,
			wantInMsg: "unsupported configuration type",
		},
		{
			name:      "invalid_type_lambda",
			config:    []map[string]any{{"Type": "AWS::Lambda::Function"}},
			wantCode:  http.StatusBadRequest,
			wantInMsg: "unsupported configuration type",
		},
		{
			name: "invalid_param_name_for_generic",
			config: []map[string]any{
				{
					"Type": "AWS::ResourceGroups::Generic",
					"Parameters": []map[string]any{
						{"Name": "bad-param", "Values": []string{"v"}},
					},
				},
			},
			wantCode:  http.StatusBadRequest,
			wantInMsg: "not valid for configuration type",
		},
		{
			name: "valid_host_management_with_deletion_protection",
			config: []map[string]any{
				{
					"Type": "AWS::EC2::HostManagement",
					"Parameters": []map[string]any{
						{"Name": "deletion-protection", "Values": []string{"enabled"}},
					},
				},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "cfg-g"})
			rec := doResourceGroupsRequest(t, h, "PutGroupConfiguration", map[string]any{
				"Group":         "cfg-g",
				"Configuration": tt.config,
			})
			assert.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())

			if tt.wantInMsg != "" {
				assert.Contains(t, rec.Body.String(), tt.wantInMsg)
			}
		})
	}
}

// TestAudit1_GroupFields covers issue #6: GetGroup returns Criticality and
// DisplayName fields; Tags are NOT included in the Group body. It also
// verifies the real wire field for the group owner is "Owner" (never the
// legacy "OwnerId"), and that it is unset by default since neither
// CreateGroup nor UpdateGroup accept an Owner input in this emulator.
func TestAudit1_GroupFields(t *testing.T) {
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

// TestAudit1_UpdateGroupCriticalityDisplayName covers issue #7: UpdateGroup
// accepts Criticality (1-5) and DisplayName in addition to Description.
func TestAudit1_UpdateGroupCriticalityDisplayName(t *testing.T) {
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
			update:   map[string]any{"Group": "upd-group", "Criticality": 6},
			wantCode: http.StatusBadRequest,
		},
		{
			name:       "criticality_boundary_1",
			update:     map[string]any{"Group": "upd-group", "Criticality": 1},
			wantCode:   http.StatusOK,
			wantInBody: `"Criticality":1`,
		},
		{
			name:       "criticality_boundary_5",
			update:     map[string]any{"Group": "upd-group", "Criticality": 5},
			wantCode:   http.StatusOK,
			wantInBody: `"Criticality":5`,
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

// TestAudit1_ListGroupsBothFields covers issue #10: ListGroups must return
// both Groups (legacy) and GroupIdentifiers fields.
func TestAudit1_ListGroupsBothFields(t *testing.T) {
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

// TestAudit1_ListGroupsFilters covers issue #16: ListGroups must filter by
// configuration-type and resource-type.
func TestAudit1_ListGroupsFilters(t *testing.T) {
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

// TestAudit1_ReservedTagNamespace covers issue #14: tag keys with "aws:" prefix
// must be rejected on CreateGroup and AddTagsByARN.
func TestAudit1_ReservedTagNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "valid_tag",
			tags:     map[string]string{"env": "prod"},
			wantCode: http.StatusOK,
		},
		{
			name:     "reserved_aws_prefix",
			tags:     map[string]string{"aws:custom": "val"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "reserved_aws_prefix_upper",
			tags:     map[string]string{"AWS:custom": "val"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "reserved_aws_mixed_case",
			tags:     map[string]string{"Aws:custom": "val"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			body := map[string]any{
				"Name": "tag-group-" + tt.name,
				"Tags": tt.tags,
			}
			rec := doResourceGroupsRequest(t, h, "CreateGroup", body)
			assert.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestAudit1_ReservedTagNamespaceOnAddTags verifies tag key validation on AddTagsByARN.
func TestAudit1_ReservedTagNamespaceOnAddTags(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "tag-test-group"})

	b := h.Backend
	g, err := b.GetGroup(context.Background(), "tag-test-group")
	require.NoError(t, err)

	_, err = b.AddTagsByARN(context.Background(), g.ARN, map[string]string{"aws:reserved": "val"})
	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroups.ErrValidation)
}

// TestAudit1_CancelTagSyncTaskState verifies that CancelTagSyncTask deletes
// the task rather than transitioning it to a fabricated CANCELLED status:
// TagSyncTaskStatus's only documented wire values are ACTIVE and ERROR, and
// AWS documents CancelTagSyncTask as taking "the TaskArn of the tag-sync task
// you want to delete".
func TestAudit1_CancelTagSyncTaskState(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "sync-group"})

	startRec := doResourceGroupsRequest(t, h, "StartTagSyncTask", map[string]any{
		"Group":    "sync-group",
		"RoleArn":  "arn:aws:iam::000000000000:role/sync-role",
		"TagKey":   "app",
		"TagValue": "myapp",
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startOut map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	taskARN := startOut["TaskArn"].(string)

	cancelRec := doResourceGroupsRequest(
		t,
		h,
		"CancelTagSyncTask",
		map[string]any{"TaskArn": taskARN},
	)
	assert.Equal(t, http.StatusOK, cancelRec.Code)

	// Task must be gone after cancellation, not lingering with an invalid status.
	getRec := doResourceGroupsRequest(t, h, "GetTagSyncTask", map[string]any{"TaskArn": taskARN})
	assert.Equal(
		t,
		http.StatusNotFound,
		getRec.Code,
		"cancelled task must no longer be retrievable: %s",
		getRec.Body.String(),
	)

	// Must no longer appear in ListTagSyncTasks either.
	listRec := doResourceGroupsRequest(t, h, "ListTagSyncTasks", map[string]any{})
	assert.Equal(t, http.StatusOK, listRec.Code)
	assert.NotContains(t, listRec.Body.String(), taskARN)
}

// TestAudit1_UngroupResourcesFailures covers issue #25: UngroupResources must
// return failures for ARNs that are not currently in the group.
func TestAudit1_UngroupResourcesFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		grouped       []string
		ungroup       []string
		wantSucceeded []string
		wantFailCount int
	}{
		{
			name:          "all_members",
			grouped:       []string{"arn:aws:s3:::b1", "arn:aws:s3:::b2"},
			ungroup:       []string{"arn:aws:s3:::b1", "arn:aws:s3:::b2"},
			wantSucceeded: []string{"arn:aws:s3:::b1", "arn:aws:s3:::b2"},
			wantFailCount: 0,
		},
		{
			name:          "one_non_member",
			grouped:       []string{"arn:aws:s3:::b1"},
			ungroup:       []string{"arn:aws:s3:::b1", "arn:aws:s3:::nonexistent"},
			wantSucceeded: []string{"arn:aws:s3:::b1"},
			wantFailCount: 1,
		},
		{
			name:          "all_non_members",
			grouped:       []string{},
			ungroup:       []string{"arn:aws:s3:::b1", "arn:aws:s3:::b2"},
			wantSucceeded: []string{},
			wantFailCount: 2,
		},
		{
			name:          "empty_input",
			grouped:       []string{"arn:aws:s3:::b1"},
			ungroup:       []string{},
			wantSucceeded: []string{},
			wantFailCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "ug-group"})

			if len(tt.grouped) > 0 {
				doResourceGroupsRequest(t, h, "GroupResources", map[string]any{
					"Group":        "ug-group",
					"ResourceArns": tt.grouped,
				})
			}

			rec := doResourceGroupsRequest(t, h, "UngroupResources", map[string]any{
				"Group":        "ug-group",
				"ResourceArns": tt.ungroup,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			succeeded, _ := out["Succeeded"].([]any)
			failed, _ := out["Failed"].([]any)

			assert.Len(
				t,
				succeeded,
				len(tt.wantSucceeded),
				"succeeded count mismatch: %s",
				rec.Body.String(),
			)
			assert.Len(t, failed, tt.wantFailCount, "failed count mismatch: %s", rec.Body.String())

			for _, s := range tt.wantSucceeded {
				found := false
				for _, item := range succeeded {
					if item.(string) == s {
						found = true
					}
				}
				assert.True(t, found, "expected %s in Succeeded", s)
			}
		})
	}
}

// TestAudit1_UntagViaDeleteVerb covers issue #12: DELETE /resources/{Arn}/tags
// must work as the Untag operation.
func TestAudit1_UntagViaDeleteVerb(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name": "delete-tag-group",
		"Tags": map[string]string{"env": "prod", "team": "platform"},
	})

	b := h.Backend
	g, err := b.GetGroup(context.Background(), "delete-tag-group")
	require.NoError(t, err)

	// DELETE /resources/{Arn}/tags with JSON body.
	e := echo.New()
	bodyBytes, _ := json.Marshal(map[string]any{"Keys": []string{"env"}})
	req := httptest.NewRequest(
		http.MethodDelete,
		"/resources/"+g.ARN+"/tags",
		strings.NewReader(string(bodyBytes)),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err = h.Handler()(c)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	// Verify "env" tag was removed.
	tagMap, err := b.GetTagsByARN(context.Background(), g.ARN)
	require.NoError(t, err)
	assert.NotContains(t, tagMap, "env")
	assert.Contains(t, tagMap, "team")
}

// TestAudit1_DescriptionLengthValidation covers issue #8: Description must be
// at most 512 characters.
func TestAudit1_DescriptionLengthValidation(t *testing.T) {
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

// TestAudit1_UpdateGroupQueryValidation covers issue #3 for UpdateGroupQuery:
// it must also validate the ResourceQuery.
func TestAudit1_UpdateGroupQueryValidation(t *testing.T) {
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

// TestAudit1_OwnerId verifies the account ID appears in the group's ARN (the
// only place AWS embeds account ownership), and that the Owner field -- a
// free-form, caller-supplied identifier unrelated to the account ID -- is
// left unset rather than fabricated from the account ID.
func TestAudit1_OwnerId(t *testing.T) {
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

// TestAudit1_BackendCreateGroupWithReservedTag verifies backend-level tag validation.
func TestAudit1_BackendCreateGroupWithReservedTag(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup(context.Background(),
		"my-group",
		"desc",
		nil,
		tags.FromMap("test.rg", map[string]string{"aws:reserved": "v"}),
		nil,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroups.ErrValidation)
}

// TestAudit1_GroupingStatusOnUngroup verifies that failed ungroup operations
// are recorded in grouping status history.
func TestAudit1_GroupingStatusOnUngroup(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateGroup(context.Background(), "status-group", "", nil, nil, nil)
	_, _ = b.GroupResources(context.Background(), "status-group", []string{"arn:aws:s3:::b1"})

	result, err := b.UngroupResources(context.Background(),
		"status-group",
		[]string{"arn:aws:s3:::b1", "arn:aws:s3:::nonmember"},
	)
	require.NoError(t, err)

	assert.Len(t, result.Succeeded, 1)
	assert.Len(t, result.Failed, 1)
	assert.Equal(t, "arn:aws:s3:::nonmember", result.Failed[0].ResourceArn)
	assert.Equal(t, "RESOURCE_NOT_FOUND", result.Failed[0].ErrorCode)

	statuses, _, err := b.ListGroupingStatuses(context.Background(), "status-group", "", 0)
	require.NoError(t, err)

	var successCount, failCount int
	for _, s := range statuses {
		if s.Action == "UNGROUP" {
			switch s.Status {
			case "SUCCESS":
				successCount++
			case "FAILED":
				failCount++
			}
		}
	}

	assert.Equal(t, 1, successCount)
	assert.Equal(t, 1, failCount)
}

// TestAudit1_CancelTagSyncTask_NotFound verifies 404 for unknown task ARN.
func TestAudit1_CancelTagSyncTask_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRequest(t, h, "CancelTagSyncTask", map[string]any{
		"TaskArn": "arn:aws:resource-groups:us-east-1:000000000000:tag-sync-task/nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestAudit1_ListGroupsOutputShape verifies the exact JSON shape of ListGroups output.
func TestAudit1_ListGroupsOutputShape(t *testing.T) {
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

// TestAudit1_CreateGroupAtomicConfig verifies that if configuration storage fails
// validation, the group is also not created (atomic).
func TestAudit1_CreateGroupAtomicConfig(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateGroup(context.Background(), "atomic-group", "", nil, nil, []resourcegroups.GroupConfigurationItem{
		{Type: "AWS::Invalid::Type"},
	})
	require.Error(t, err)

	// Group must not have been created.
	_, err = b.GetGroup(context.Background(), "atomic-group")
	assert.ErrorIs(t, err, resourcegroups.ErrNotFound)
}

// TestAudit1_GetGroupCreateGroupOutput verifies CreateGroup response includes
// GroupConfiguration when provided.
func TestAudit1_GetGroupCreateGroupOutput(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name": "cfg-output-group",
		"Configuration": []map[string]any{
			{"Type": "AWS::EC2::CapacityReservationPool"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "GroupConfiguration")
	assert.Contains(t, body, "AWS::EC2::CapacityReservationPool")
	assert.Contains(t, body, "UPDATE_COMPLETE")
}
