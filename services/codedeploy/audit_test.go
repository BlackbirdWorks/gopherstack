package codedeploy_test

// audit_test.go — table-driven tests covering the AWS-accuracy improvements
// implemented for issue go-grqv (GitHub #1778).

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/codedeploy"
)

// ---------------------------------------------------------------------------
// Item 1 — ComputePlatform validation on CreateApplication
// ---------------------------------------------------------------------------

func TestAudit_CreateApplication_ComputePlatformValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		platform    string
		wantStatus  int
		wantErrType string
	}{
		{name: "server_valid", platform: "Server", wantStatus: http.StatusOK},
		{name: "lambda_valid", platform: "Lambda", wantStatus: http.StatusOK},
		{name: "ecs_valid", platform: "ECS", wantStatus: http.StatusOK},
		{name: "empty_defaults_to_server", platform: "", wantStatus: http.StatusOK},
		{
			name:        "invalid_platform",
			platform:    "Docker",
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidComputePlatformException",
		},
		{
			name:        "case_sensitive",
			platform:    "server",
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidComputePlatformException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateApplication", map[string]any{
				"applicationName": "app-" + tt.name,
				"computePlatform": tt.platform,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErrType != "" {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantErrType, resp["__type"])
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Item 2 / Item 3 — DeploymentGroup rich fields + UpdateDeploymentGroup
// ---------------------------------------------------------------------------

func TestAudit_CreateDeploymentGroup_RichFields_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.CreateApplication("my-app", "Server", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, "CreateDeploymentGroup", map[string]any{
		"applicationName":     "my-app",
		"deploymentGroupName": "rich-dg",
		"serviceRoleArn":      "arn:aws:iam::123:role/role",
		"deploymentConfigName": "CodeDeployDefault.AllAtOnce",
		"ec2TagFilters": []map[string]string{
			{"Key": "env", "Value": "prod", "Type": "EQUALS"},
		},
		"autoScalingGroups": []map[string]string{
			{"name": "my-asg"},
		},
		"deploymentStyle": map[string]string{
			"deploymentType":   "BLUE_GREEN",
			"deploymentOption": "WITH_TRAFFIC_CONTROL",
		},
		"alarmConfiguration": map[string]any{
			"enabled": true,
			"alarms":  []map[string]string{{"name": "cpu-alarm"}},
		},
		"autoRollbackConfiguration": map[string]any{
			"enabled": true,
			"events":  []string{"DEPLOYMENT_FAILURE"},
		},
		"triggerConfigurations": []map[string]any{
			{
				"triggerName":      "deploy-trigger",
				"triggerTargetArn": "arn:aws:sns:us-east-1:123:topic",
				"triggerEvents":    []string{"DeploymentSuccess"},
			},
		},
		"outdatedInstancesStrategy": "UPDATE",
		"terminationHookEnabled":    true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Retrieve via GetDeploymentGroup and verify round-trip.
	rec2 := doRequest(t, h, "GetDeploymentGroup", map[string]any{
		"applicationName":     "my-app",
		"deploymentGroupName": "rich-dg",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp struct {
		DeploymentGroupInfo struct {
			ServiceRoleArn    string `json:"serviceRoleArn"`
			ComputePlatform   string `json:"computePlatform"`
			DeploymentStyle   *struct {
				DeploymentType   string `json:"deploymentType"`
				DeploymentOption string `json:"deploymentOption"`
			} `json:"deploymentStyle"`
			AlarmConfiguration *struct {
				Enabled bool `json:"enabled"`
				Alarms  []struct {
					Name string `json:"name"`
				} `json:"alarms"`
			} `json:"alarmConfiguration"`
			AutoRollbackConfiguration *struct {
				Enabled bool     `json:"enabled"`
				Events  []string `json:"events"`
			} `json:"autoRollbackConfiguration"`
			Ec2TagFilters []struct {
				Key   string `json:"Key"`
				Value string `json:"Value"`
				Type  string `json:"Type"`
			} `json:"ec2TagFilters"`
			TriggerConfigurations []struct {
				TriggerName   string   `json:"triggerName"`
				TriggerEvents []string `json:"triggerEvents"`
			} `json:"triggerConfigurations"`
			OutdatedInstancesStrategy string `json:"outdatedInstancesStrategy"`
			TerminationHookEnabled    bool   `json:"terminationHookEnabled"`
		} `json:"deploymentGroupInfo"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	info := resp.DeploymentGroupInfo
	assert.Equal(t, "arn:aws:iam::123:role/role", info.ServiceRoleArn)
	assert.Equal(t, "Server", info.ComputePlatform)
	require.NotNil(t, info.DeploymentStyle)
	assert.Equal(t, "BLUE_GREEN", info.DeploymentStyle.DeploymentType)
	assert.Equal(t, "WITH_TRAFFIC_CONTROL", info.DeploymentStyle.DeploymentOption)
	require.NotNil(t, info.AlarmConfiguration)
	assert.True(t, info.AlarmConfiguration.Enabled)
	require.Len(t, info.AlarmConfiguration.Alarms, 1)
	assert.Equal(t, "cpu-alarm", info.AlarmConfiguration.Alarms[0].Name)
	require.NotNil(t, info.AutoRollbackConfiguration)
	assert.True(t, info.AutoRollbackConfiguration.Enabled)
	assert.Equal(t, []string{"DEPLOYMENT_FAILURE"}, info.AutoRollbackConfiguration.Events)
	require.Len(t, info.Ec2TagFilters, 1)
	assert.Equal(t, "env", info.Ec2TagFilters[0].Key)
	assert.Equal(t, "prod", info.Ec2TagFilters[0].Value)
	assert.Equal(t, "EQUALS", info.Ec2TagFilters[0].Type)
	require.Len(t, info.TriggerConfigurations, 1)
	assert.Equal(t, "deploy-trigger", info.TriggerConfigurations[0].TriggerName)
	assert.Equal(t, "UPDATE", info.OutdatedInstancesStrategy)
	assert.True(t, info.TerminationHookEnabled)
}

func TestAudit_UpdateDeploymentGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		setup           func(h *codedeploy.Handler)
		input           map[string]any
		wantStatus      int
		wantHooksClean  bool
	}{
		{
			name: "rename",
			setup: func(h *codedeploy.Handler) {
				_, _ = h.Backend.CreateApplication("app", "Server", nil)
				_, _ = createDG(h.Backend, "app", "old-name", "", "", nil)
			},
			input: map[string]any{
				"applicationName":            "app",
				"currentDeploymentGroupName": "old-name",
				"newDeploymentGroupName":     "new-name",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update_fields",
			setup: func(h *codedeploy.Handler) {
				_, _ = h.Backend.CreateApplication("app2", "Server", nil)
				_, _ = createDG(h.Backend, "app2", "dg", "", "", nil)
			},
			input: map[string]any{
				"applicationName":            "app2",
				"currentDeploymentGroupName": "dg",
				"serviceRoleArn":             "arn:aws:iam::123:role/new-role",
				"deploymentConfigName":       "CodeDeployDefault.OneAtATime",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "hooks_cleaned_on_alarm_removal",
			setup: func(h *codedeploy.Handler) {
				_, _ = h.Backend.CreateApplication("app3", "Server", nil)
				// Create DG with alarms enabled.
				_, _ = h.Backend.CreateDeploymentGroup("app3", "dg", codedeploy.DeploymentGroupInput{
					AlarmConfiguration: &codedeploy.AlarmConfiguration{
						Enabled: true,
						Alarms:  []codedeploy.Alarm{{Name: "cpu"}},
					},
				}, nil)
			},
			input: map[string]any{
				"applicationName":            "app3",
				"currentDeploymentGroupName": "dg",
				// alarmConfiguration omitted → removing alarms
			},
			wantStatus:     http.StatusOK,
			wantHooksClean: true,
		},
		{
			name: "not_found",
			input: map[string]any{
				"applicationName":            "missing-app",
				"currentDeploymentGroupName": "dg",
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_required_fields",
			input: map[string]any{
				"applicationName": "app",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "UpdateDeploymentGroup", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK && tt.wantHooksClean {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				hooksNotCleanedUp, _ := resp["hooksNotCleanedUp"].(bool)
				assert.True(t, hooksNotCleanedUp, "expected hooksNotCleanedUp=true when alarms were removed")
			}
		})
	}
}

func TestAudit_UpdateDeploymentGroup_Rename_Verifiable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, _ = h.Backend.CreateApplication("app", "Server", nil)
	_, _ = createDG(h.Backend, "app", "old-name", "", "", nil)

	rec := doRequest(t, h, "UpdateDeploymentGroup", map[string]any{
		"applicationName":            "app",
		"currentDeploymentGroupName": "old-name",
		"newDeploymentGroupName":     "new-name",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Old name should no longer exist.
	rec2 := doRequest(t, h, "GetDeploymentGroup", map[string]any{
		"applicationName":     "app",
		"deploymentGroupName": "old-name",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)

	// New name should exist.
	rec3 := doRequest(t, h, "GetDeploymentGroup", map[string]any{
		"applicationName":     "app",
		"deploymentGroupName": "new-name",
	})
	assert.Equal(t, http.StatusOK, rec3.Code)
}

// ---------------------------------------------------------------------------
// Item 13 — DeploymentConfig MinimumHealthyHosts / TrafficRouting / ZonalConfig
// ---------------------------------------------------------------------------

func TestAudit_CreateDeploymentConfig_SubStructs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		input      map[string]any
		wantStatus int
		checkFn    func(t *testing.T, info map[string]any)
	}{
		{
			name: "minimum_healthy_hosts",
			input: map[string]any{
				"deploymentConfigName": "cfg-mhh",
				"computePlatform":      "Server",
				"minimumHealthyHosts": map[string]any{
					"type":  "FLEET_PERCENT",
					"value": 75,
				},
			},
			wantStatus: http.StatusOK,
			checkFn: func(t *testing.T, info map[string]any) {
				t.Helper()
				mhh, ok := info["minimumHealthyHosts"].(map[string]any)
				require.True(t, ok, "minimumHealthyHosts should be present")
				assert.Equal(t, "FLEET_PERCENT", mhh["type"])
				assert.InDelta(t, 75, mhh["value"], 0.1)
			},
		},
		{
			name: "canary_traffic_routing",
			input: map[string]any{
				"deploymentConfigName": "cfg-canary",
				"computePlatform":      "Lambda",
				"trafficRoutingConfig": map[string]any{
					"type": "TimeBasedCanary",
					"timeBasedCanary": map[string]any{
						"canaryPercentage": 10,
						"canaryInterval":   5,
					},
				},
			},
			wantStatus: http.StatusOK,
			checkFn: func(t *testing.T, info map[string]any) {
				t.Helper()
				trc, ok := info["trafficRoutingConfig"].(map[string]any)
				require.True(t, ok, "trafficRoutingConfig should be present")
				assert.Equal(t, "TimeBasedCanary", trc["type"])
				canary, ok := trc["timeBasedCanary"].(map[string]any)
				require.True(t, ok)
				assert.InDelta(t, 10, canary["canaryPercentage"], 0.1)
				assert.InDelta(t, 5, canary["canaryInterval"], 0.1)
			},
		},
		{
			name: "linear_traffic_routing",
			input: map[string]any{
				"deploymentConfigName": "cfg-linear",
				"computePlatform":      "Lambda",
				"trafficRoutingConfig": map[string]any{
					"type": "TimeBasedLinear",
					"timeBasedLinear": map[string]any{
						"linearPercentage": 10,
						"linearInterval":   1,
					},
				},
			},
			wantStatus: http.StatusOK,
			checkFn: func(t *testing.T, info map[string]any) {
				t.Helper()
				trc, ok := info["trafficRoutingConfig"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "TimeBasedLinear", trc["type"])
			},
		},
		{
			name: "zonal_config",
			input: map[string]any{
				"deploymentConfigName": "cfg-zonal",
				"computePlatform":      "Server",
				"zonalConfig": map[string]any{
					"firstZoneMonitorDurationInSeconds": 60,
					"monitorDurationInSeconds":          30,
					"minimumHealthyHostsPerZone": map[string]any{
						"type":  "HOST_COUNT",
						"value": 2,
					},
				},
			},
			wantStatus: http.StatusOK,
			checkFn: func(t *testing.T, info map[string]any) {
				t.Helper()
				zc, ok := info["zonalConfig"].(map[string]any)
				require.True(t, ok, "zonalConfig should be present")
				assert.InDelta(t, 60, zc["firstZoneMonitorDurationInSeconds"], 0.1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateDeploymentConfig", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkFn != nil && tt.wantStatus == http.StatusOK {
				name, _ := tt.input["deploymentConfigName"].(string)
				rec2 := doRequest(t, h, "GetDeploymentConfig", map[string]any{
					"deploymentConfigName": name,
				})
				require.Equal(t, http.StatusOK, rec2.Code)

				var resp map[string]map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
				tt.checkFn(t, resp["deploymentConfigInfo"])
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Item 14 — Pre-seeded CodeDeployDefault.* configs
// ---------------------------------------------------------------------------

func TestAudit_DefaultDeploymentConfigs_Seeded(t *testing.T) {
	t.Parallel()

	expectedDefaults := []string{
		"CodeDeployDefault.AllAtOnce",
		"CodeDeployDefault.OneAtATime",
		"CodeDeployDefault.HalfAtATime",
		"CodeDeployDefault.LambdaAllAtOnce",
		"CodeDeployDefault.LambdaCanary10Percent5Minutes",
		"CodeDeployDefault.LambdaLinear10PercentEvery1Minute",
		"CodeDeployDefault.ECSAllAtOnce",
		"CodeDeployDefault.ECSCanary10Percent5Minutes",
		"CodeDeployDefault.ECSLinear10PercentEvery1Minute",
	}

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListDeploymentConfigs", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		DeploymentConfigsList []string `json:"deploymentConfigsList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	for _, name := range expectedDefaults {
		assert.Contains(t, resp.DeploymentConfigsList, name, "missing default config: %s", name)
	}
}

func TestAudit_DefaultDeploymentConfigs_GetReturnsSubStructs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		configName      string
		wantPlatform    string
		wantRoutingType string
	}{
		{
			name:            "AllAtOnce",
			configName:      "CodeDeployDefault.AllAtOnce",
			wantPlatform:    "Server",
			wantRoutingType: "AllAtOnce",
		},
		{
			name:            "LambdaCanary10Percent5Minutes",
			configName:      "CodeDeployDefault.LambdaCanary10Percent5Minutes",
			wantPlatform:    "Lambda",
			wantRoutingType: "TimeBasedCanary",
		},
		{
			name:            "ECSLinear",
			configName:      "CodeDeployDefault.ECSLinear10PercentEvery1Minute",
			wantPlatform:    "ECS",
			wantRoutingType: "TimeBasedLinear",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetDeploymentConfig", map[string]any{
				"deploymentConfigName": tt.configName,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				DeploymentConfigInfo struct {
					ComputePlatform      string `json:"computePlatform"`
					TrafficRoutingConfig *struct {
						Type string `json:"type"`
					} `json:"trafficRoutingConfig"`
				} `json:"deploymentConfigInfo"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantPlatform, resp.DeploymentConfigInfo.ComputePlatform)
			require.NotNil(t, resp.DeploymentConfigInfo.TrafficRoutingConfig)
			assert.Equal(t, tt.wantRoutingType, resp.DeploymentConfigInfo.TrafficRoutingConfig.Type)
		})
	}
}

func TestAudit_DefaultDeploymentConfigs_CannotDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DeleteDeploymentConfig", map[string]any{
		"deploymentConfigName": "CodeDeployDefault.AllAtOnce",
	})
	assert.Equal(t, http.StatusConflict, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "DeploymentConfigInUseException", resp["__type"])
}

// ---------------------------------------------------------------------------
// Item 17 — OnPremisesInstance IAM ARN validation
// ---------------------------------------------------------------------------

func TestAudit_RegisterOnPremisesInstance_IamValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		instanceName  string
		iamSessionArn string
		iamUserArn    string
		wantStatus    int
		wantErrType   string
	}{
		{
			name:          "session_arn_only",
			instanceName:  "server-01",
			iamSessionArn: "arn:aws:sts::123:assumed-role/role/session",
			wantStatus:    http.StatusOK,
		},
		{
			name:         "user_arn_only",
			instanceName: "server-02",
			iamUserArn:   "arn:aws:iam::123:user/user1",
			wantStatus:   http.StatusOK,
		},
		{
			name:          "both_arns_set",
			instanceName:  "server-03",
			iamSessionArn: "arn:aws:sts::123:assumed-role/role/session",
			iamUserArn:    "arn:aws:iam::123:user/user1",
			wantStatus:    http.StatusBadRequest,
			wantErrType:   "MultipleIamArnsProvidedException",
		},
		{
			name:         "no_arns_set",
			instanceName: "server-04",
			wantStatus:   http.StatusBadRequest,
			wantErrType:  "IamArnRequiredException",
		},
		{
			name:         "invalid_name_has_spaces",
			instanceName: "server 05",
			iamUserArn:   "arn:aws:iam::123:user/user1",
			wantStatus:   http.StatusBadRequest,
			wantErrType:  "InvalidParameterValueException",
		},
		{
			name:         "name_too_long",
			instanceName: strings.Repeat("x", 101),
			iamUserArn:   "arn:aws:iam::123:user/user1",
			wantStatus:   http.StatusBadRequest,
			wantErrType:  "InvalidParameterValueException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "RegisterOnPremisesInstance", map[string]any{
				"instanceName":  tt.instanceName,
				"iamSessionArn": tt.iamSessionArn,
				"iamUserArn":    tt.iamUserArn,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErrType != "" {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantErrType, resp["__type"])
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Item 19 — GitHub token storage
// ---------------------------------------------------------------------------

func TestAudit_GitHubAccountTokens(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// List should be empty initially.
	rec := doRequest(t, h, "ListGitHubAccountTokenNames", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		TokenNameList []string `json:"tokenNameList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.TokenNameList)

	// Add token via internal method and verify it appears.
	h.Backend.AddGitHubAccountTokenInternal("my-token")

	rec = doRequest(t, h, "ListGitHubAccountTokenNames", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Equal(t, []string{"my-token"}, listResp.TokenNameList)

	// Delete the token.
	rec = doRequest(t, h, "DeleteGitHubAccountToken", map[string]any{"tokenName": "my-token"})
	require.Equal(t, http.StatusOK, rec.Code)

	// List should be empty again.
	rec = doRequest(t, h, "ListGitHubAccountTokenNames", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.TokenNameList)
}

func TestAudit_DeleteGitHubAccountToken_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DeleteGitHubAccountToken", map[string]any{"tokenName": "nonexistent"})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "GitHubAccountTokenDoesNotExistException", resp["__type"])
}

func TestAudit_DeleteGitHubAccountToken_MissingTokenName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DeleteGitHubAccountToken", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Item 22 — ListDeployments filters (status, createTimeRange)
// ---------------------------------------------------------------------------

func TestAudit_ListDeployments_StatusFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, _ = h.Backend.CreateApplication("app", "Server", nil)
	_, _ = createDG(h.Backend, "app", "dg", "", "", nil)

	// Create one deployment then stop it.
	d1, _ := createDeploy(h.Backend, "app", "dg", "", "")
	_ = h.Backend.StopDeployment(d1.DeploymentID)

	// Create another deployment (stays Succeeded).
	_, _ = createDeploy(h.Backend, "app", "dg", "", "")

	tests := []struct {
		name    string
		filter  []string
		wantLen int
	}{
		{name: "all", filter: nil, wantLen: 2},
		{name: "succeeded_only", filter: []string{"Succeeded"}, wantLen: 1},
		{name: "stopped_only", filter: []string{"Stopped"}, wantLen: 1},
		{name: "failed_only", filter: []string{"Failed"}, wantLen: 0},
		{name: "stopped_or_succeeded", filter: []string{"Stopped", "Succeeded"}, wantLen: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			input := map[string]any{"applicationName": "app", "deploymentGroupName": "dg"}
			if tt.filter != nil {
				input["includeOnlyStatuses"] = tt.filter
			}

			rec := doRequest(t, h, "ListDeployments", input)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Deployments []string `json:"deployments"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.Deployments, tt.wantLen)
		})
	}
}

// ---------------------------------------------------------------------------
// Item 24 — UpdateApplication updates deployment.ApplicationName
// ---------------------------------------------------------------------------

func TestAudit_UpdateApplication_UpdatesDeployments(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, _ = h.Backend.CreateApplication("old-app", "Server", nil)
	_, _ = createDG(h.Backend, "old-app", "dg", "", "", nil)
	d, _ := createDeploy(h.Backend, "old-app", "dg", "", "")

	// Rename the application.
	rec := doRequest(t, h, "UpdateApplication", map[string]any{
		"applicationName":    "old-app",
		"newApplicationName": "new-app",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// The deployment should now reference the new application name.
	got, err := h.Backend.GetDeployment(d.DeploymentID)
	require.NoError(t, err)
	assert.Equal(t, "new-app", got.ApplicationName)

	// Old application name should not exist.
	rec2 := doRequest(t, h, "GetApplication", map[string]any{"applicationName": "old-app"})
	assert.Equal(t, http.StatusNotFound, rec2.Code)

	// New application name should exist.
	rec3 := doRequest(t, h, "GetApplication", map[string]any{"applicationName": "new-app"})
	assert.Equal(t, http.StatusOK, rec3.Code)
}

// ---------------------------------------------------------------------------
// Item 26 — TagResource tag limits
// ---------------------------------------------------------------------------

func TestAudit_TagResource_TagLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		tags        []map[string]string
		wantStatus  int
		wantErrType string
	}{
		{
			name: "within_limit",
			tags: []map[string]string{
				{"Key": "k1", "Value": "v1"},
				{"Key": "k2", "Value": "v2"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:        "reserved_prefix",
			tags:        []map[string]string{{"Key": "aws:tag", "Value": "v"}},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidParameterValueException",
		},
		{
			name: "key_too_long",
			tags: []map[string]string{
				{"Key": strings.Repeat("k", 129), "Value": "v"},
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidParameterValueException",
		},
		{
			name: "value_too_long",
			tags: []map[string]string{
				{"Key": "k", "Value": strings.Repeat("v", 257)},
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "InvalidParameterValueException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, _ = h.Backend.CreateApplication("tag-app", "Server", nil)
			appARN := h.Backend.ApplicationARN("tag-app")

			rec := doRequest(t, h, "TagResource", map[string]any{
				"resourceArn": appARN,
				"tags":        tt.tags,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErrType != "" {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantErrType, resp["__type"])
			}
		})
	}
}

func TestAudit_TagResource_ExceedsMaxTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, _ = h.Backend.CreateApplication("tag-app2", "Server", nil)
	appARN := h.Backend.ApplicationARN("tag-app2")

	// Add 50 tags.
	tags := make([]map[string]string, 50)
	for i := 0; i < 50; i++ {
		tags[i] = map[string]string{"Key": "k" + string(rune('a'+i%26)) + string(rune('0'+i/26)), "Value": "v"}
	}
	rec := doRequest(t, h, "TagResource", map[string]any{"resourceArn": appARN, "tags": tags})
	require.Equal(t, http.StatusOK, rec.Code)

	// Adding one more should fail.
	rec = doRequest(t, h, "TagResource", map[string]any{
		"resourceArn": appARN,
		"tags":        []map[string]string{{"Key": "extra-key", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "TagLimitExceededException", resp["__type"])
}

// ---------------------------------------------------------------------------
// Item 27 — DeploymentID format d-[A-Z0-9]{9}
// ---------------------------------------------------------------------------

var deployIDRe = regexp.MustCompile(`^d-[A-Z0-9]{9}$`)

func TestAudit_CreateDeployment_IDFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, _ = h.Backend.CreateApplication("app", "Server", nil)
	_, _ = createDG(h.Backend, "app", "dg", "", "", nil)

	for i := 0; i < 10; i++ {
		rec := doRequest(t, h, "CreateDeployment", map[string]any{
			"applicationName":     "app",
			"deploymentGroupName": "dg",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]string
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		id := resp["deploymentId"]
		assert.True(t, deployIDRe.MatchString(id), "deploymentId %q does not match pattern d-[A-Z0-9]{9}", id)
	}
}

// ---------------------------------------------------------------------------
// Item 28 — CreateDeployment extra fields persisted
// ---------------------------------------------------------------------------

func TestAudit_CreateDeployment_ExtraFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, _ = h.Backend.CreateApplication("app", "Server", nil)
	_, _ = createDG(h.Backend, "app", "dg", "", "", nil)

	rec := doRequest(t, h, "CreateDeployment", map[string]any{
		"applicationName":               "app",
		"deploymentGroupName":           "dg",
		"description":                   "my-description",
		"fileExistsBehavior":            "OVERWRITE",
		"updateOutdatedInstancesOnly":   true,
		"ignoreApplicationStopFailures": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	rec2 := doRequest(t, h, "GetDeployment", map[string]any{"deploymentId": createResp["deploymentId"]})
	require.Equal(t, http.StatusOK, rec2.Code)

	var getResp struct {
		DeploymentInfo struct {
			Description                   string `json:"description"`
			FileExistsBehavior            string `json:"fileExistsBehavior"`
			UpdateOutdatedInstancesOnly   bool   `json:"updateOutdatedInstancesOnly"`
			IgnoreApplicationStopFailures bool   `json:"ignoreApplicationStopFailures"`
		} `json:"deploymentInfo"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &getResp))

	assert.Equal(t, "my-description", getResp.DeploymentInfo.Description)
	assert.Equal(t, "OVERWRITE", getResp.DeploymentInfo.FileExistsBehavior)
	assert.True(t, getResp.DeploymentInfo.UpdateOutdatedInstancesOnly)
	assert.True(t, getResp.DeploymentInfo.IgnoreApplicationStopFailures)
}

// ---------------------------------------------------------------------------
// Item 29 — ComputePlatform inherited on DeploymentGroup from Application
// ---------------------------------------------------------------------------

func TestAudit_DeploymentGroup_InheritsComputePlatform(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		platform string
	}{
		{name: "server", platform: "Server"},
		{name: "lambda", platform: "Lambda"},
		{name: "ecs", platform: "ECS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, _ = h.Backend.CreateApplication("app-"+tt.name, tt.platform, nil)
			_, _ = createDG(h.Backend, "app-"+tt.name, "dg", "", "", nil)

			rec := doRequest(t, h, "GetDeploymentGroup", map[string]any{
				"applicationName":     "app-" + tt.name,
				"deploymentGroupName": "dg",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				DeploymentGroupInfo struct {
					ComputePlatform string `json:"computePlatform"`
				} `json:"deploymentGroupInfo"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.platform, resp.DeploymentGroupInfo.ComputePlatform)
		})
	}
}

// ---------------------------------------------------------------------------
// Item 30 — Persistence snapshot tags survive round-trip
// ---------------------------------------------------------------------------

func TestAudit_Persistence_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	b := codedeploy.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)

	_, err := b.CreateApplication("tagged-app", "Server", map[string]string{"env": "prod", "team": "core"})
	require.NoError(t, err)

	_, err = b.CreateDeploymentGroup("tagged-app", "tagged-dg",
		codedeploy.DeploymentGroupInput{ServiceRoleArn: "arn:role"},
		map[string]string{"tier": "blue"})
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := codedeploy.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	require.NoError(t, b2.Restore(snap))

	appARN := b2.ApplicationARN("tagged-app")
	appTags, err := b2.ListTagsForResource(appARN)
	require.NoError(t, err)
	assert.Equal(t, "prod", appTags["env"])
	assert.Equal(t, "core", appTags["team"])

	dgARN := b2.DeploymentGroupARN("tagged-app", "tagged-dg")
	dgTags, err := b2.ListTagsForResource(dgARN)
	require.NoError(t, err)
	assert.Equal(t, "blue", dgTags["tier"])
}

func TestAudit_Persistence_GitHubTokensRoundTrip(t *testing.T) {
	t.Parallel()

	b := codedeploy.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	b.AddGitHubAccountTokenInternal("token-alpha")
	b.AddGitHubAccountTokenInternal("token-beta")

	snap := b.Snapshot()

	b2 := codedeploy.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	require.NoError(t, b2.Restore(snap))

	tokens := b2.ListGitHubAccountTokenNames()
	assert.Contains(t, tokens, "token-alpha")
	assert.Contains(t, tokens, "token-beta")
}

// ---------------------------------------------------------------------------
// BatchGetDeploymentInstances — missing deployment returns error (item 23)
// ---------------------------------------------------------------------------

func TestAudit_BatchGetDeploymentInstances_MissingDeployment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "BatchGetDeploymentInstances", map[string]any{
		"deploymentId": "d-NOTEXIST1",
		"instanceIds":  []string{"i-abc"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "DeploymentDoesNotExistException", resp["__type"])
}

// ---------------------------------------------------------------------------
// ListOnPremisesInstances tag filter (item 18)
// ---------------------------------------------------------------------------

func TestAudit_ListOnPremisesInstances_TagFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Register instances.
	require.NoError(t, h.Backend.RegisterOnPremisesInstance("server-a", "arn:aws:sts::123:assumed-role/r/s", ""))
	require.NoError(t, h.Backend.RegisterOnPremisesInstance("server-b", "arn:aws:sts::123:assumed-role/r/s2", ""))
	require.NoError(t, h.Backend.AddTagsToOnPremisesInstances([]string{"server-a"}, map[string]string{"tier": "web"}))
	require.NoError(t, h.Backend.AddTagsToOnPremisesInstances([]string{"server-b"}, map[string]string{"tier": "db"}))

	tests := []struct {
		name       string
		tagFilters []map[string]string
		wantNames  []string
	}{
		{
			name:       "no_filter",
			tagFilters: nil,
			wantNames:  []string{"server-a", "server-b"},
		},
		{
			name:       "equals_web",
			tagFilters: []map[string]string{{"Key": "tier", "Value": "web", "Type": "EQUALS"}},
			wantNames:  []string{"server-a"},
		},
		{
			name:       "key_only",
			tagFilters: []map[string]string{{"Key": "tier", "Type": "KEY_ONLY"}},
			wantNames:  []string{"server-a", "server-b"},
		},
		{
			name:       "value_only_db",
			tagFilters: []map[string]string{{"Value": "db", "Type": "VALUE_ONLY"}},
			wantNames:  []string{"server-b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			input := map[string]any{}
			if tt.tagFilters != nil {
				input["tagFilters"] = tt.tagFilters
			}

			rec := doRequest(t, h, "ListOnPremisesInstances", input)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				InstanceNames []string `json:"instanceNames"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.ElementsMatch(t, tt.wantNames, resp.InstanceNames)
		})
	}
}

// ---------------------------------------------------------------------------
// BatchGetDeploymentGroups includes rich fields (item 2)
// ---------------------------------------------------------------------------

func TestAudit_BatchGetDeploymentGroups_RichFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, _ = h.Backend.CreateApplication("app", "ECS", nil)
	_, _ = h.Backend.CreateDeploymentGroup("app", "dg1", codedeploy.DeploymentGroupInput{
		ECSServices: []codedeploy.ECSService{{ServiceName: "my-svc", ClusterName: "my-cluster"}},
		DeploymentStyle: &codedeploy.DeploymentStyle{
			DeploymentType:   "BLUE_GREEN",
			DeploymentOption: "WITH_TRAFFIC_CONTROL",
		},
	}, nil)

	rec := doRequest(t, h, "BatchGetDeploymentGroups", map[string]any{
		"applicationName":      "app",
		"deploymentGroupNames": []string{"dg1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		DeploymentGroupsInfo []struct {
			ComputePlatform string `json:"computePlatform"`
			DeploymentStyle *struct {
				DeploymentType string `json:"deploymentType"`
			} `json:"deploymentStyle"`
			ECSServices []struct {
				ServiceName string `json:"serviceName"`
			} `json:"ecsServices"`
		} `json:"deploymentGroupsInfo"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.DeploymentGroupsInfo, 1)

	info := resp.DeploymentGroupsInfo[0]
	assert.Equal(t, "ECS", info.ComputePlatform)
	require.NotNil(t, info.DeploymentStyle)
	assert.Equal(t, "BLUE_GREEN", info.DeploymentStyle.DeploymentType)
	require.Len(t, info.ECSServices, 1)
	assert.Equal(t, "my-svc", info.ECSServices[0].ServiceName)
}
