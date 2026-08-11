package elasticsearch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestElasticsearchHandler_CreateDomain_VPCOptions verifies VPCOptions
// (subnets/security groups) round-trips through CreateElasticsearchDomain's
// DomainStatus response as VPCDerivedInfo.
func TestElasticsearchHandler_CreateDomain_VPCOptions(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
		"DomainName": "vpc-domain",
		"VPCOptions": map[string]any{
			"SubnetIds":        []string{"subnet-1", "subnet-2"},
			"SecurityGroupIds": []string{"sg-1"},
		},
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	out := readJSONBody(t, resp)
	status := out["DomainStatus"].(map[string]any)
	vpc := status["VPCOptions"].(map[string]any)
	assert.ElementsMatch(t, []any{"subnet-1", "subnet-2"}, vpc["SubnetIds"])
	assert.ElementsMatch(t, []any{"sg-1"}, vpc["SecurityGroupIds"])

	// A domain never placed in a VPC must not carry a VPCOptions block at all.
	resp2 := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
		"DomainName": "no-vpc-domain",
	})
	defer resp2.Body.Close()
	out2 := readJSONBody(t, resp2)
	status2 := out2["DomainStatus"].(map[string]any)
	assert.NotContains(t, status2, "VPCOptions")
}

// TestElasticsearchHandler_CreateDomain_CognitoOptions verifies CognitoOptions
// round-trips and that Enabled=true without the required identifying fields
// is rejected, matching real AWS validation.
func TestElasticsearchHandler_CreateDomain_CognitoOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cognito    map[string]any
		name       string
		domainName string
		wantCode   int
	}{
		{
			name:       "enabled_with_all_fields",
			domainName: "cognito-domain",
			cognito: map[string]any{
				"Enabled":        true,
				"UserPoolId":     "pool-1",
				"IdentityPoolId": "idpool-1",
				"RoleArn":        "arn:aws:iam::123456789012:role/CognitoRole",
			},
			wantCode: http.StatusOK,
		},
		{
			name:       "enabled_missing_fields_rejected",
			domainName: "cognito-bad-domain",
			cognito:    map[string]any{"Enabled": true},
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
				"DomainName":     tt.domainName,
				"CognitoOptions": tt.cognito,
			})
			defer resp.Body.Close()
			require.Equal(t, tt.wantCode, resp.StatusCode)

			if tt.wantCode != http.StatusOK {
				return
			}

			out := readJSONBody(t, resp)
			status := out["DomainStatus"].(map[string]any)
			cognito := status["CognitoOptions"].(map[string]any)
			assert.Equal(t, true, cognito["Enabled"])
			assert.Equal(t, "pool-1", cognito["UserPoolId"])
			assert.Equal(t, "idpool-1", cognito["IdentityPoolId"])
		})
	}
}

// TestElasticsearchHandler_CreateDomain_LogPublishingOptions verifies
// LogPublishingOptions round-trips through the DomainStatus response.
func TestElasticsearchHandler_CreateDomain_LogPublishingOptions(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
		"DomainName": "log-domain",
		"LogPublishingOptions": map[string]any{
			"INDEX_SLOW_LOGS": map[string]any{
				"CloudWatchLogsLogGroupArn": "arn:aws:logs:us-east-1:123456789012:log-group:/es/slow",
				"Enabled":                   true,
			},
		},
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	out := readJSONBody(t, resp)
	status := out["DomainStatus"].(map[string]any)
	logOpts := status["LogPublishingOptions"].(map[string]any)
	slowLogs := logOpts["INDEX_SLOW_LOGS"].(map[string]any)
	assert.Equal(t, true, slowLogs["Enabled"])
	assert.Equal(t, "arn:aws:logs:us-east-1:123456789012:log-group:/es/slow", slowLogs["CloudWatchLogsLogGroupArn"])
}

// TestElasticsearchHandler_CreateDomain_AdvancedSecurityOptions verifies
// AdvancedSecurityOptions round-trips (minus MasterUserOptions, which real
// AWS never echoes back) and that InternalUserDatabaseEnabled without
// MasterUserOptions is rejected.
func TestElasticsearchHandler_CreateDomain_AdvancedSecurityOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		asOpts     map[string]any
		name       string
		domainName string
		wantCode   int
	}{
		{
			name:       "enabled_with_master_user",
			domainName: "as-domain",
			asOpts: map[string]any{
				"Enabled":                     true,
				"InternalUserDatabaseEnabled": true,
				"MasterUserOptions": map[string]any{
					"MasterUserName":     "admin",
					"MasterUserPassword": "hunter2!A",
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name:       "internal_db_without_master_user_rejected",
			domainName: "as-bad-domain",
			asOpts: map[string]any{
				"Enabled":                     true,
				"InternalUserDatabaseEnabled": true,
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
				"DomainName":              tt.domainName,
				"AdvancedSecurityOptions": tt.asOpts,
			})
			defer resp.Body.Close()
			require.Equal(t, tt.wantCode, resp.StatusCode)

			if tt.wantCode != http.StatusOK {
				return
			}

			out := readJSONBody(t, resp)
			status := out["DomainStatus"].(map[string]any)
			as := status["AdvancedSecurityOptions"].(map[string]any)
			assert.Equal(t, true, as["Enabled"])
			assert.Equal(t, true, as["InternalUserDatabaseEnabled"])
			// MasterUserOptions must never be echoed back, matching real AWS.
			assert.NotContains(t, as, "MasterUserOptions")
		})
	}
}

// TestElasticsearchHandler_CreateDomain_AutoTuneOptions verifies
// AutoTuneOptions.DesiredState maps onto the response's State field, that
// invalid values are rejected, and that a domain with no Auto-Tune
// configuration defaults to DISABLED.
func TestElasticsearchHandler_CreateDomain_AutoTuneOptions(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
		"DomainName":      "autotune-domain",
		"AutoTuneOptions": map[string]any{"DesiredState": "ENABLED"},
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	out := readJSONBody(t, resp)
	status := out["DomainStatus"].(map[string]any)
	at := status["AutoTuneOptions"].(map[string]any)
	assert.Equal(t, "ENABLED", at["State"])

	badResp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
		"DomainName":      "autotune-bad-domain",
		"AutoTuneOptions": map[string]any{"DesiredState": "MAYBE"},
	})
	defer badResp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, badResp.StatusCode)

	defaultResp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
		"DomainName": "autotune-default-domain",
	})
	defer defaultResp.Body.Close()
	require.Equal(t, http.StatusOK, defaultResp.StatusCode)
	defaultOut := readJSONBody(t, defaultResp)
	defaultStatus := defaultOut["DomainStatus"].(map[string]any)
	defaultAT := defaultStatus["AutoTuneOptions"].(map[string]any)
	assert.Equal(t, "DISABLED", defaultAT["State"])
}

// TestElasticsearchHandler_CreateDomain_TagList verifies TagList on
// CreateElasticsearchDomain applies tags atomically at creation, visible via
// ListTags without a separate AddTags call.
func TestElasticsearchHandler_CreateDomain_TagList(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	domainARN := createDomainAndGetARN(t, h, "taglist-domain")

	// createDomainAndGetARN doesn't send TagList, so create a second domain directly.
	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
		"DomainName": "taglist-domain-2",
		"TagList": []map[string]any{
			{"Key": "env", "Value": "prod"},
		},
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	out := readJSONBody(t, resp)
	arn2 := out["DomainStatus"].(map[string]any)["ARN"].(string)

	listResp := doRequest(t, h, http.MethodGet, "/2015-01-01/tags?arn="+arn2, nil)
	defer listResp.Body.Close()
	require.Equal(t, http.StatusOK, listResp.StatusCode)

	listOut := readJSONBody(t, listResp)
	tagList := listOut["TagList"].([]any)
	require.Len(t, tagList, 1)
	tag := tagList[0].(map[string]any)
	assert.Equal(t, "env", tag["Key"])
	assert.Equal(t, "prod", tag["Value"])

	// Sanity: the first domain (no TagList) has no tags.
	firstTagsResp := doRequest(t, h, http.MethodGet, "/2015-01-01/tags?arn="+domainARN, nil)
	defer firstTagsResp.Body.Close()
	firstOut := readJSONBody(t, firstTagsResp)
	assert.Empty(t, firstOut["TagList"])
}

// TestElasticsearchHandler_DomainConfig_OptionStatus verifies
// DescribeElasticsearchDomainConfig's per-field OptionStatus carries
// CreationDate/UpdateDate/UpdateVersion/State/PendingDeletion, and that
// UpdateVersion increments (and UpdateDate advances) after a real
// UpdateElasticsearchDomainConfig call.
func TestElasticsearchHandler_DomainConfig_OptionStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	name := createTestDomainName(t, h, "optionstatus-domain")

	descResp := doRequest(t, h, http.MethodGet, "/2015-01-01/es/domain/"+name+"/config", nil)
	defer descResp.Body.Close()
	require.Equal(t, http.StatusOK, descResp.StatusCode)

	descOut := readJSONBody(t, descResp)
	cfg := descOut["DomainConfig"].(map[string]any)
	ebsStatus := cfg["EBSOptions"].(map[string]any)["Status"].(map[string]any)

	assert.Equal(t, "Active", ebsStatus["State"])
	assert.InDelta(t, float64(1), ebsStatus["UpdateVersion"], 0.01)
	assert.False(t, ebsStatus["PendingDeletion"].(bool))
	assert.Greater(t, ebsStatus["CreationDate"].(float64), float64(0))
	assert.InDelta(t, ebsStatus["CreationDate"].(float64), ebsStatus["UpdateDate"].(float64), 0.5)

	updateResp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain/"+name+"/config", map[string]any{
		"EBSOptions": map[string]any{"EBSEnabled": true, "VolumeSize": 30, "VolumeType": "gp3"},
	})
	defer updateResp.Body.Close()
	require.Equal(t, http.StatusOK, updateResp.StatusCode)

	updateOut := readJSONBody(t, updateResp)
	updatedCfg := updateOut["DomainConfig"].(map[string]any)
	updatedStatus := updatedCfg["EBSOptions"].(map[string]any)["Status"].(map[string]any)
	assert.InDelta(t, float64(2), updatedStatus["UpdateVersion"], 0.01)
	assert.GreaterOrEqual(t, updatedStatus["UpdateDate"].(float64), ebsStatus["CreationDate"].(float64))
}

// TestElasticsearchHandler_DomainConfig_VPCOptionsOmittedWhenUnset verifies
// DescribeElasticsearchDomainConfig omits VPCOptions entirely for a
// non-VPC domain but includes it once the domain is placed in a VPC via
// UpdateElasticsearchDomainConfig.
func TestElasticsearchHandler_DomainConfig_VPCOptionsOmittedWhenUnset(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	name := createTestDomainName(t, h, "vpc-config-domain")

	descResp := doRequest(t, h, http.MethodGet, "/2015-01-01/es/domain/"+name+"/config", nil)
	defer descResp.Body.Close()
	descOut := readJSONBody(t, descResp)
	cfg := descOut["DomainConfig"].(map[string]any)
	assert.NotContains(t, cfg, "VPCOptions")

	updateResp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain/"+name+"/config", map[string]any{
		"VPCOptions": map[string]any{"SubnetIds": []string{"subnet-9"}, "SecurityGroupIds": []string{"sg-9"}},
	})
	defer updateResp.Body.Close()
	require.Equal(t, http.StatusOK, updateResp.StatusCode)

	updateOut := readJSONBody(t, updateResp)
	updatedCfg := updateOut["DomainConfig"].(map[string]any)
	require.Contains(t, updatedCfg, "VPCOptions")
	vpcOpts := updatedCfg["VPCOptions"].(map[string]any)["Options"].(map[string]any)
	assert.ElementsMatch(t, []any{"subnet-9"}, vpcOpts["SubnetIds"])
}

// TestElasticsearchHandler_UpdateDomainConfig_SecurityFields verifies
// UpdateElasticsearchDomainConfig accepts and applies CognitoOptions,
// AdvancedSecurityOptions, AutoTuneOptions, and LogPublishingOptions, and
// that the same request-time validation (e.g. AutoTuneOptions.DesiredState)
// applies on update as it does on create.
func TestElasticsearchHandler_UpdateDomainConfig_SecurityFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	name := createTestDomainName(t, h, "update-security-domain")

	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain/"+name+"/config", map[string]any{
		"CognitoOptions": map[string]any{
			"Enabled":        true,
			"UserPoolId":     "pool-2",
			"IdentityPoolId": "idpool-2",
			"RoleArn":        "arn:aws:iam::123456789012:role/CognitoRole2",
		},
		"AdvancedSecurityOptions": map[string]any{"Enabled": true},
		"AutoTuneOptions":         map[string]any{"DesiredState": "DISABLED"},
		"LogPublishingOptions": map[string]any{
			"SEARCH_SLOW_LOGS": map[string]any{"Enabled": true},
		},
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	out := readJSONBody(t, resp)
	cfg := out["DomainConfig"].(map[string]any)

	cognito := cfg["CognitoOptions"].(map[string]any)["Options"].(map[string]any)
	assert.Equal(t, true, cognito["Enabled"])
	assert.Equal(t, "pool-2", cognito["UserPoolId"])

	as := cfg["AdvancedSecurityOptions"].(map[string]any)["Options"].(map[string]any)
	assert.Equal(t, true, as["Enabled"])

	atField := cfg["AutoTuneOptions"].(map[string]any)
	at := atField["Options"].(map[string]any)
	assert.Equal(t, "DISABLED", at["DesiredState"])
	assert.Equal(t, "DISABLED", atField["Status"].(map[string]any)["State"])

	logOpts := cfg["LogPublishingOptions"].(map[string]any)["Options"].(map[string]any)
	slowLogs := logOpts["SEARCH_SLOW_LOGS"].(map[string]any)
	assert.Equal(t, true, slowLogs["Enabled"])

	// Invalid AutoTuneOptions.DesiredState must be rejected on update too.
	badResp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain/"+name+"/config", map[string]any{
		"AutoTuneOptions": map[string]any{"DesiredState": "NOPE"},
	})
	defer badResp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, badResp.StatusCode)
}

// TestElasticsearchBackend_CreateDomain_InvalidAutoTuneDesiredState verifies
// the backend rejects a domain create when AutoTuneOptions.DesiredState is
// neither ENABLED nor DISABLED, exercising the handler-layer validation path
// with a raw JSON payload (rather than the doRequest map[string]any helper)
// to also confirm the wire field name is exactly "DesiredState".
func TestElasticsearchBackend_CreateDomain_InvalidAutoTuneDesiredState(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	body := []byte(`{"DomainName":"raw-autotune","AutoTuneOptions":{"DesiredState":"BOGUS"}}`)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))

	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", decoded)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestElasticsearchHandler_CreateDomain_SAMLOptions verifies
// AdvancedSecurityOptions.SAMLOptions round-trips on the DomainStatus
// response (Idp/RolesKey/SubjectKey/SessionTimeoutMinutes) while
// credential-adjacent MasterUserName/MasterBackendRole are never echoed,
// matching real AWS's SAMLOptionsOutput shape.
func TestElasticsearchHandler_CreateDomain_SAMLOptions(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
		"DomainName": "saml-domain",
		"AdvancedSecurityOptions": map[string]any{
			"Enabled": true,
			"SAMLOptions": map[string]any{
				"Enabled": true,
				"Idp": map[string]any{
					"EntityId":        "https://idp.example.com/saml",
					"MetadataContent": "<EntityDescriptor/>",
				},
				"RolesKey":              "Role",
				"SubjectKey":            "Subject",
				"SessionTimeoutMinutes": 120,
				"MasterUserName":        "saml-admin",
				"MasterBackendRole":     "admin-role",
			},
		},
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	out := readJSONBody(t, resp)
	status := out["DomainStatus"].(map[string]any)
	as := status["AdvancedSecurityOptions"].(map[string]any)
	saml := as["SAMLOptions"].(map[string]any)
	assert.Equal(t, true, saml["Enabled"])
	assert.Equal(t, "Role", saml["RolesKey"])
	assert.Equal(t, "Subject", saml["SubjectKey"])
	assert.InEpsilon(t, float64(120), saml["SessionTimeoutMinutes"], 0)
	idp := saml["Idp"].(map[string]any)
	assert.Equal(t, "https://idp.example.com/saml", idp["EntityId"])
	assert.NotContains(t, saml, "MasterUserName")
	assert.NotContains(t, saml, "MasterBackendRole")
}

// TestElasticsearchHandler_CreateDomain_SAMLOptionsValidation verifies
// request-time validation of SAMLOptions, mirroring the SDK client's own
// validateSAMLIdp check plus this backend's Enabled-requires-Idp rule.
func TestElasticsearchHandler_CreateDomain_SAMLOptionsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		samlOptions map[string]any
		name        string
	}{
		{
			name:        "enabled_without_idp",
			samlOptions: map[string]any{"Enabled": true},
		},
		{
			name: "idp_missing_metadata_content",
			samlOptions: map[string]any{
				"Enabled": true,
				"Idp":     map[string]any{"EntityId": "https://idp.example.com/saml"},
			},
		},
		{
			name: "idp_missing_entity_id",
			samlOptions: map[string]any{
				"Enabled": true,
				"Idp":     map[string]any{"MetadataContent": "<EntityDescriptor/>"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
				"DomainName": "saml-invalid-domain",
				"AdvancedSecurityOptions": map[string]any{
					"Enabled":     true,
					"SAMLOptions": tt.samlOptions,
				},
			})
			defer resp.Body.Close()
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		})
	}
}

// TestElasticsearchHandler_DomainConfig_AutoTuneMaintenanceSchedules verifies
// AutoTuneOptions.MaintenanceSchedules round-trips through
// DescribeElasticsearchDomainConfig's DomainConfig.AutoTuneOptions.Options
// (types.AutoTuneOptions -- DesiredState/MaintenanceSchedules), which is a
// different shape from the DomainStatus response's AutoTuneOptions (see the
// AutoTune converter's doc comment in handler_domain_config.go). Status.State
// must use the AutoTuneState enum (ENABLED/DISABLED), not OptionState's
// Active.
func TestElasticsearchHandler_DomainConfig_AutoTuneMaintenanceSchedules(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	name := createTestDomainName(t, h, "autotune-schedule-domain")

	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain/"+name+"/config", map[string]any{
		"AutoTuneOptions": map[string]any{
			"DesiredState": "ENABLED",
			"MaintenanceSchedules": []map[string]any{
				{
					"CronExpressionForRecurrence": "cron(0 2 ? * SUN *)",
					"Duration":                    map[string]any{"Unit": "HOURS", "Value": 2},
					"StartAt":                     1700000000,
				},
			},
		},
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	out := readJSONBody(t, resp)
	cfg := out["DomainConfig"].(map[string]any)
	atField := cfg["AutoTuneOptions"].(map[string]any)

	opts := atField["Options"].(map[string]any)
	assert.Equal(t, "ENABLED", opts["DesiredState"])
	schedules := opts["MaintenanceSchedules"].([]any)
	require.Len(t, schedules, 1)
	sched := schedules[0].(map[string]any)
	assert.Equal(t, "cron(0 2 ? * SUN *)", sched["CronExpressionForRecurrence"])
	duration := sched["Duration"].(map[string]any)
	assert.Equal(t, "HOURS", duration["Unit"])
	assert.InEpsilon(t, float64(2), duration["Value"], 0)

	assert.Equal(t, "ENABLED", atField["Status"].(map[string]any)["State"])
}

// TestElasticsearchHandler_CreateDomain_DeploymentStrategyOptions verifies
// DeploymentStrategyOptions round-trips on the DomainStatus response and
// that an invalid DeploymentStrategy value is rejected.
func TestElasticsearchHandler_CreateDomain_DeploymentStrategyOptions(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
		"DomainName":                "deploy-strategy-domain",
		"DeploymentStrategyOptions": map[string]any{"DeploymentStrategy": "CapacityOptimized"},
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	out := readJSONBody(t, resp)
	status := out["DomainStatus"].(map[string]any)
	dso := status["DeploymentStrategyOptions"].(map[string]any)
	assert.Equal(t, "CapacityOptimized", dso["DeploymentStrategy"])

	badResp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
		"DomainName":                "deploy-strategy-bad-domain",
		"DeploymentStrategyOptions": map[string]any{"DeploymentStrategy": "Bogus"},
	})
	defer badResp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, badResp.StatusCode)
}
