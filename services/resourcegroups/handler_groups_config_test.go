package resourcegroups_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

func TestResourceGroupsHandler_GetGroupQuery(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name": "qgroup",
		"ResourceQuery": map[string]any{
			"Type":  "TAG_FILTERS_1_0",
			"Query": `{"ResourceTypeFilters":["AWS::AllSupported"]}`,
		},
	})

	rec := doResourceGroupsRequest(t, h, "GetGroupQuery", map[string]any{"GroupName": "qgroup"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestGetGroupQuery_ReturnsNilForNoQuery verifies nil ResourceQuery is represented.
func TestGetGroupQuery_ReturnsNilForNoQuery(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name":          "no-query-group",
		"Configuration": []map[string]any{{"Type": "AWS::EC2::CapacityReservationPool"}},
	})

	rec := doResourceGroupsRequest(t, h, "GetGroupQuery", map[string]any{"Group": "no-query-group"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	groupQuery := out["GroupQuery"].(map[string]any)
	assert.Equal(t, "no-query-group", groupQuery["GroupName"])
	// ResourceQuery should be null when not set.
	_, hasQuery := groupQuery["ResourceQuery"]
	if hasQuery {
		assert.Nil(t, groupQuery["ResourceQuery"])
	}
}

// TestGetGroupQuery_NilForConfigGroup verifies that GetGroupQuery returns a
// nil ResourceQuery for configuration-based groups (no query set).
func TestGetGroupQuery_NilForConfigGroup(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name    string
		wantNil bool
		body    map[string]any
	}{
		{
			name: "query_group_has_query",
			body: map[string]any{
				"Name": "query-grp",
				"ResourceQuery": map[string]any{
					"Type":  "TAG_FILTERS_1_0",
					"Query": `{"TagFilters":[]}`,
				},
			},
			wantNil: false,
		},
		{
			name: "config_group_no_query",
			body: map[string]any{
				"Name":          "cfg-grp",
				"Configuration": []map[string]any{{"Type": "AWS::ResourceGroups::Generic"}},
			},
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			doResourceGroupsRequest(t, h, "CreateGroup", tt.body)

			groupName, _ := tt.body["Name"].(string)
			rec := doResourceGroupsRequest(t, h, "GetGroupQuery", map[string]any{"Group": groupName})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				GroupQuery struct {
					ResourceQuery *struct {
						Type  string `json:"Type"`
						Query string `json:"Query"`
					} `json:"ResourceQuery"`
				} `json:"GroupQuery"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			if tt.wantNil {
				assert.Nil(t, out.GroupQuery.ResourceQuery)
			} else {
				assert.NotNil(t, out.GroupQuery.ResourceQuery)
			}
		})
	}
}

func TestResourceGroupsHandler_GetGroupConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "cfggroup"})

	rec := doResourceGroupsRequest(t, h, "GetGroupConfiguration", map[string]any{"GroupName": "cfggroup"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestResourceGroupsHandler_GetGroupConfiguration_Status verifies the
// GetGroupConfiguration response matches types.GroupConfiguration: it carries
// a Status field ("UPDATE_COMPLETE" once a configuration is set) and no
// fabricated GroupName field.
func TestResourceGroupsHandler_GetGroupConfiguration_Status(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name": "cfg-status-group",
		"Configuration": []map[string]any{
			{"Type": "AWS::ResourceGroups::Generic"},
		},
	})

	rec := doResourceGroupsRequest(t, h, "GetGroupConfiguration", map[string]any{"Group": "cfg-status-group"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		GroupConfiguration struct {
			Status string `json:"Status"`
		} `json:"GroupConfiguration"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "UPDATE_COMPLETE", out.GroupConfiguration.Status)
	assert.NotContains(t, rec.Body.String(), "GroupName")
}

// TestGetGroupConfiguration_Empty verifies an empty list is returned when no
// configuration has been stored for a group.
func TestGetGroupConfiguration_Empty(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "no-config"})

	rec := doResourceGroupsRequest(t, h, "GetGroupConfiguration", map[string]any{
		"Group": "no-config",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GroupConfiguration")
}

// TestGetGroupConfiguration_ReflectsUpdate verifies config is updated by PutGroupConfiguration.
func TestGetGroupConfiguration_ReflectsUpdate(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "update-cfg"})

	doResourceGroupsRequest(t, h, "PutGroupConfiguration", map[string]any{
		"Group": "update-cfg",
		"Configuration": []map[string]any{
			{"Type": "AWS::ResourceGroups::Generic"},
		},
	})

	rec := doResourceGroupsRequest(t, h, "GetGroupConfiguration", map[string]any{"Group": "update-cfg"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AWS::ResourceGroups::Generic")

	// Update to a different type.
	doResourceGroupsRequest(t, h, "PutGroupConfiguration", map[string]any{
		"Group": "update-cfg",
		"Configuration": []map[string]any{
			{"Type": "AWS::EC2::CapacityReservationPool"},
		},
	})

	rec2 := doResourceGroupsRequest(t, h, "GetGroupConfiguration", map[string]any{"Group": "update-cfg"})
	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "AWS::EC2::CapacityReservationPool")
	assert.NotContains(t, rec2.Body.String(), "AWS::ResourceGroups::Generic")
}

func TestResourceGroupsHandler_RESTGetGroupQuery(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRESTRequest(t, h, "/groups", map[string]any{"Name": "qgroup2"})
	rec := doResourceGroupsRESTRequest(t, h, "/get-group-query", map[string]any{"GroupName": "qgroup2"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestResourceGroupsHandler_RESTGetGroupConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRESTRequest(t, h, "/groups", map[string]any{"Name": "cfggroup2"})
	rec := doResourceGroupsRESTRequest(t, h, "/get-group-configuration", map[string]any{"GroupName": "cfggroup2"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestResourceGroupsHandler_PutGroupConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *resourcegroups.Handler)
		name         string
		group        string
		wantContains []string
		wantCode     int
	}{
		{
			name:  "success",
			group: "my-group",
			setup: func(t *testing.T, h *resourcegroups.Handler) {
				t.Helper()
				doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "my-group"})
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			group:    "nonexistent",
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

			body := map[string]any{
				"Group":         tt.group,
				"Configuration": []map[string]any{{"Type": "AWS::NetworkFirewall::RuleGroup"}},
			}
			rec := doResourceGroupsRequest(t, h, "PutGroupConfiguration", body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestPutGroupConfiguration_ValidTypes verifies all supported config types.
func TestPutGroupConfiguration_ValidTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config []map[string]any
	}{
		{
			name:   "ec2_host_management",
			config: []map[string]any{{"Type": "AWS::EC2::HostManagement"}},
		},
		{
			name:   "ec2_capacity_pool",
			config: []map[string]any{{"Type": "AWS::EC2::CapacityReservationPool"}},
		},
		{
			name: "generic_with_allowed_types",
			config: []map[string]any{{
				"Type": "AWS::ResourceGroups::Generic",
				"Parameters": []map[string]any{
					{"Name": "allowed-resource-types", "Values": []string{"AWS::EC2::Instance", "AWS::S3::Bucket"}},
				},
			}},
		},
		{
			name:   "appregistry_application",
			config: []map[string]any{{"Type": "AWS::AppRegistry::Application"}},
		},
		{
			name:   "servicecat_appregistry",
			config: []map[string]any{{"Type": "AWS::ServiceCatalogAppRegistry::Application"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "cfg-" + tt.name})
			rec := doResourceGroupsRequest(t, h, "PutGroupConfiguration", map[string]any{
				"Group":         "cfg-" + tt.name,
				"Configuration": tt.config,
			})
			assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestCreateGroupWithConfiguration covers CreateGroup atomically persisting
// Configuration alongside the group.
func TestCreateGroupWithConfiguration(t *testing.T) {
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

// TestConfigurationTypeValidation covers that PutGroupConfiguration must
// validate Type and Parameters against the allow-list.
func TestConfigurationTypeValidation(t *testing.T) {
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

// TestGetGroupCreateGroupOutput verifies CreateGroup response includes
// GroupConfiguration when provided.
func TestGetGroupCreateGroupOutput(t *testing.T) {
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

// TestCreateGroup_ResponseShape verifies complete CreateGroup response.
func TestCreateGroup_ResponseShape(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name":        "shape-test",
		"Description": "test desc",
		"Tags":        map[string]string{"env": "test"},
		"ResourceQuery": map[string]any{
			"Type":  "TAG_FILTERS_1_0",
			"Query": `{"TagFilters":[{"Key":"env","Values":["test"]}]}`,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	group, ok := out["Group"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "shape-test", group["Name"])
	assert.Contains(t, group["GroupArn"].(string), "shape-test")
	assert.Equal(t, "test desc", group["Description"])
	// Owner defaults to unset (AWS never fabricates it from the account ID),
	// and the real wire key is "Owner", never the legacy "OwnerId".
	assert.NotContains(t, group, "OwnerId")
	assert.NotContains(t, group, "Owner")
	// types.Group carries no Tags/ResourceQuery members of its own.
	assert.NotContains(t, group, "Tags")
	assert.NotContains(t, group, "ResourceQuery")

	// ResourceQuery should appear at top level.
	rq, ok := out["ResourceQuery"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "TAG_FILTERS_1_0", rq["Type"])

	// Tags should appear at top level too, as a sibling of Group.
	wireTags, ok := out["Tags"].(map[string]any)
	require.True(t, ok, "Tags must appear at the top level of CreateGroupOutput: %s", rec.Body.String())
	assert.Equal(t, "test", wireTags["env"])
}
