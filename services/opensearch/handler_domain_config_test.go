package opensearch_test

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenSearchHandler_UpdateDomainConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create domain.
	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{"DomainName": "config-domain", "EngineVersion": "OpenSearch_2.7"})
	resp.Body.Close()

	// Update config.
	upResp := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/domain/config-domain/config",
		map[string]any{"EngineVersion": "OpenSearch_2.9"})
	defer upResp.Body.Close()

	assert.Equal(t, http.StatusOK, upResp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(upResp.Body).Decode(&out))

	domainConfig, ok := out["DomainConfig"].(map[string]any)
	require.True(t, ok)
	engineVersion, ok := domainConfig["EngineVersion"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "OpenSearch_2.9", engineVersion["Options"])
}

// Test_UpdateDomainConfig_DryRun verifies UpdateDomainConfig with DryRun=true
// previews the resulting config (and returns DryRunResults) without mutating
// the stored domain, matching aws-sdk-go-v2 UpdateDomainConfigInput.DryRun.
func Test_UpdateDomainConfig_DryRun(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{"DomainName": "dryrun-domain", "EngineVersion": "OpenSearch_2.7"})
	resp.Body.Close()

	dryResp := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/domain/dryrun-domain/config",
		map[string]any{"EngineVersion": "OpenSearch_2.9", "DryRun": true})
	defer dryResp.Body.Close()
	assert.Equal(t, http.StatusOK, dryResp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(dryResp.Body).Decode(&out))

	domainConfig, ok := out["DomainConfig"].(map[string]any)
	require.True(t, ok)
	engineVersion, ok := domainConfig["EngineVersion"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "OpenSearch_2.9", engineVersion["Options"], "dry run should preview the requested change")

	dryRunResults, ok := out["DryRunResults"].(map[string]any)
	require.True(t, ok, "DryRunResults missing")
	assert.NotEmpty(t, dryRunResults["DeploymentType"])

	// The dry run must not have mutated the stored domain.
	descResp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain/dryrun-domain", nil)
	defer descResp.Body.Close()

	var descOut map[string]any
	require.NoError(t, json.NewDecoder(descResp.Body).Decode(&descOut))
	status, ok := descOut["DomainStatus"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "OpenSearch_2.7", status["EngineVersion"], "dry run must not persist the change")
}

func TestOpenSearchHandler_UpdateDomainConfig_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/domain/nonexistent/config",
		map[string]any{"EngineVersion": "OpenSearch_2.9"})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestOpenSearchHandler_DefaultApplicationSetting(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// GET before any setting → empty ARN.
	resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/defaultApplicationSetting", nil)
	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Empty(t, out["applicationArn"])

	appArn := "arn:aws:es:us-east-1:123456789012:application/app-1"

	// PUT with SetAsDefault=true sets the default.
	putResp := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/defaultApplicationSetting",
		map[string]any{"applicationArn": appArn, "setAsDefault": true})
	defer putResp.Body.Close()
	assert.Equal(t, http.StatusOK, putResp.StatusCode)

	var putOut map[string]any
	require.NoError(t, json.NewDecoder(putResp.Body).Decode(&putOut))
	assert.Equal(t, appArn, putOut["applicationArn"])

	// GET after PUT → should return the stored ARN.
	getResp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/defaultApplicationSetting", nil)
	defer getResp.Body.Close()

	assert.Equal(t, http.StatusOK, getResp.StatusCode)

	var out2 map[string]any
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&out2))
	assert.Equal(t, appArn, out2["applicationArn"])

	// PUT with SetAsDefault=false clears the default.
	clearResp := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/defaultApplicationSetting",
		map[string]any{"applicationArn": appArn, "setAsDefault": false})
	defer clearResp.Body.Close()
	assert.Equal(t, http.StatusOK, clearResp.StatusCode)

	var clearOut map[string]any
	require.NoError(t, json.NewDecoder(clearResp.Body).Decode(&clearOut))
	assert.Empty(t, clearOut["applicationArn"])
}

func TestOpenSearchHandler_DefaultApplicationSetting_MissingArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/defaultApplicationSetting",
		map[string]any{"setAsDefault": true})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestDescribeDomainConfig_RealData(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
		map[string]any{
			"DomainName":    "cfg-domain",
			"EngineVersion": "OpenSearch_2.3",
		})
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	cfgResp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain/cfg-domain/config", nil)
	defer cfgResp.Body.Close()
	require.Equal(t, http.StatusOK, cfgResp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(cfgResp.Body).Decode(&out))

	domainConfig, ok := out["DomainConfig"].(map[string]any)
	require.True(t, ok)
	ev, ok := domainConfig["EngineVersion"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "OpenSearch_2.3", ev["Options"])
}

func TestCancelDomainConfigChange_DryRun(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDomainInternal("my-domain", "")

	ids, dryRun, err := b.CancelDomainConfigChange("my-domain", true)
	require.NoError(t, err)
	assert.Empty(t, ids)
	assert.True(t, dryRun)
}

// TestCancelDomainConfigChange_DryRunDoesNotMutate asserts that DryRun=true
// reports what would be cancelled without actually clearing the pending
// change -- the same class of bug UpdateDomainConfig's DryRun had (see
// PARITY.md), now fixed on CancelDomainConfigChange too.
func TestCancelDomainConfigChange_DryRunDoesNotMutate(t *testing.T) {
	t.Parallel()

	b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: "dryrun-cancel-domain"})
	require.NoError(t, err)

	// Trigger a pending config change.
	_, err = b.UpdateDomainConfig("dryrun-cancel-domain", opensearch.UpdateDomainConfigInput{
		AccessPolicies: "{}",
	})
	require.NoError(t, err)

	// Dry-run cancel reports the pending change ID but must not clear it.
	ids, dryRun, err := b.CancelDomainConfigChange("dryrun-cancel-domain", true)
	require.NoError(t, err)
	require.Len(t, ids, 1)
	assert.True(t, dryRun)

	// A real (non-dry-run) cancel afterward must still see the same pending change.
	ids2, dryRun2, err := b.CancelDomainConfigChange("dryrun-cancel-domain", false)
	require.NoError(t, err)
	assert.Equal(t, ids, ids2)
	assert.False(t, dryRun2)

	// Now it's actually cleared.
	ids3, _, err := b.CancelDomainConfigChange("dryrun-cancel-domain", false)
	require.NoError(t, err)
	assert.Empty(t, ids3)
}

// TestAudit1_UpdateDomainConfig_AllOptions verifies UpdateDomainConfig can set all new fields.
func TestUpdateDomainConfig_AllOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updateBody map[string]any
		verify     func(t *testing.T, status map[string]any)
		name       string
	}{
		{
			name: "update_ebs_options",
			updateBody: map[string]any{
				"EBSOptions": map[string]any{
					"EBSEnabled": true,
					"VolumeType": "gp3",
					"VolumeSize": 200,
				},
			},
			verify: func(t *testing.T, status map[string]any) {
				t.Helper()
				dc, ok := status["DomainConfig"].(map[string]any)
				require.True(t, ok)
				ebs, ok := dc["EBSOptions"].(map[string]any)
				require.True(t, ok, "EBSOptions must be in DomainConfig")
				opts, ok := ebs["Options"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, true, opts["EBSEnabled"])
				assert.Equal(t, "gp3", opts["VolumeType"])
			},
		},
		{
			name: "update_encryption_at_rest",
			updateBody: map[string]any{
				"EncryptionAtRestOptions": map[string]any{
					"Enabled":  true,
					"KMSKeyId": "arn:aws:kms:us-east-1:123456789012:key/new-key",
				},
			},
			verify: func(t *testing.T, status map[string]any) {
				t.Helper()
				dc, ok := status["DomainConfig"].(map[string]any)
				require.True(t, ok)
				enc, ok := dc["EncryptionAtRestOptions"].(map[string]any)
				require.True(t, ok, "EncryptionAtRestOptions must be in DomainConfig")
				opts, ok := enc["Options"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, true, opts["Enabled"])
			},
		},
		{
			name: "update_node_to_node_encryption",
			updateBody: map[string]any{
				"NodeToNodeEncryptionOptions": map[string]any{
					"Enabled": true,
				},
			},
			verify: func(t *testing.T, status map[string]any) {
				t.Helper()
				dc, ok := status["DomainConfig"].(map[string]any)
				require.True(t, ok)
				n2n, ok := dc["NodeToNodeEncryptionOptions"].(map[string]any)
				require.True(t, ok, "NodeToNodeEncryptionOptions must be in DomainConfig")
				opts, ok := n2n["Options"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, true, opts["Enabled"])
			},
		},
		{
			name: "update_domain_endpoint_options",
			updateBody: map[string]any{
				"DomainEndpointOptions": map[string]any{
					"EnforceHTTPS":      true,
					"TLSSecurityPolicy": "Policy-Min-TLS-1-2-2019-07",
				},
			},
			verify: func(t *testing.T, status map[string]any) {
				t.Helper()
				dc, ok := status["DomainConfig"].(map[string]any)
				require.True(t, ok)
				deo, ok := dc["DomainEndpointOptions"].(map[string]any)
				require.True(t, ok, "DomainEndpointOptions must be in DomainConfig")
				opts, ok := deo["Options"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, true, opts["EnforceHTTPS"])
			},
		},
		{
			name: "update_advanced_security_options",
			updateBody: map[string]any{
				"AdvancedSecurityOptions": map[string]any{
					"Enabled":                     true,
					"InternalUserDatabaseEnabled": true,
				},
			},
			verify: func(t *testing.T, status map[string]any) {
				t.Helper()
				dc, ok := status["DomainConfig"].(map[string]any)
				require.True(t, ok)
				aso, ok := dc["AdvancedSecurityOptions"].(map[string]any)
				require.True(t, ok, "AdvancedSecurityOptions must be in DomainConfig")
				opts, ok := aso["Options"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, true, opts["Enabled"])
				assert.Equal(t, true, opts["InternalUserDatabaseEnabled"])
			},
		},
		{
			name: "update_vpc_options",
			updateBody: map[string]any{
				"VPCOptions": map[string]any{
					"SubnetIds":        []string{"subnet-updated"},
					"SecurityGroupIds": []string{"sg-updated"},
				},
			},
			verify: func(t *testing.T, status map[string]any) {
				t.Helper()
				dc, ok := status["DomainConfig"].(map[string]any)
				require.True(t, ok)
				vpc, ok := dc["VPCOptions"].(map[string]any)
				require.True(t, ok, "VPCOptions must be in DomainConfig")
				opts, ok := vpc["Options"].(map[string]any)
				require.True(t, ok)
				assert.NotNil(t, opts["SubnetIds"])
			},
		},
		{
			name: "update_cognito_options",
			updateBody: map[string]any{
				"CognitoOptions": map[string]any{
					"Enabled":        true,
					"UserPoolId":     "us-east-1_updated",
					"IdentityPoolId": "us-east-1:updated-id",
					"RoleArn":        "arn:aws:iam::123456789012:role/UpdatedRole",
				},
			},
			verify: func(t *testing.T, status map[string]any) {
				t.Helper()
				dc, ok := status["DomainConfig"].(map[string]any)
				require.True(t, ok)
				cog, ok := dc["CognitoOptions"].(map[string]any)
				require.True(t, ok, "CognitoOptions must be in DomainConfig")
				opts, ok := cog["Options"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, true, opts["Enabled"])
				assert.Equal(t, "us-east-1_updated", opts["UserPoolId"])
			},
		},
		{
			name: "update_log_publishing_options",
			updateBody: map[string]any{
				"LogPublishingOptions": map[string]any{
					"SEARCH_SLOW_LOGS": map[string]any{
						"Enabled":                   true,
						"CloudWatchLogsLogGroupArn": "arn:aws:logs:us-east-1:123456789012:log-group:/updated",
					},
				},
			},
			verify: func(t *testing.T, status map[string]any) {
				t.Helper()
				dc, ok := status["DomainConfig"].(map[string]any)
				require.True(t, ok)
				logs, ok := dc["LogPublishingOptions"].(map[string]any)
				require.True(t, ok, "LogPublishingOptions must be in DomainConfig")
				opts, ok := logs["Options"].(map[string]any)
				require.True(t, ok)
				assert.Contains(t, opts, "SEARCH_SLOW_LOGS")
			},
		},
		{
			name: "update_access_policies",
			updateBody: map[string]any{
				"AccessPolicies": policyRootAllowAll,
			},
			verify: func(t *testing.T, status map[string]any) {
				t.Helper()
				dc, ok := status["DomainConfig"].(map[string]any)
				require.True(t, ok)
				ap, ok := dc["AccessPolicies"].(map[string]any)
				require.True(t, ok, "AccessPolicies must be in DomainConfig")
				assert.NotEmpty(t, ap["Options"])
			},
		},
		{
			name: "update_cluster_config_with_dedicated_master",
			updateBody: map[string]any{
				"ClusterConfig": map[string]any{
					"InstanceType":           "r6g.large.search",
					"InstanceCount":          3,
					"DedicatedMasterEnabled": true,
					"DedicatedMasterType":    "m6g.large.search",
					"DedicatedMasterCount":   3,
				},
			},
			verify: func(t *testing.T, status map[string]any) {
				t.Helper()
				dc, ok := status["DomainConfig"].(map[string]any)
				require.True(t, ok)
				cc, ok := dc["ClusterConfig"].(map[string]any)
				require.True(t, ok, "ClusterConfig must be in DomainConfig")
				opts, ok := cc["Options"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, true, opts["DedicatedMasterEnabled"])
				assert.Equal(t, "m6g.large.search", opts["DedicatedMasterType"])
				assert.InDelta(t, float64(3), opts["DedicatedMasterCount"], 0)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createResp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
				map[string]any{"DomainName": "upd-test"})
			createResp.Body.Close()
			require.Equal(t, http.StatusOK, createResp.StatusCode)

			upResp := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/domain/upd-test/config",
				tt.updateBody)
			defer upResp.Body.Close()
			require.Equal(t, http.StatusOK, upResp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(upResp.Body).Decode(&out))
			tt.verify(t, out)
		})
	}
}

// TestAudit1_DescribeDomainConfig_FullConfig verifies DescribeDomainConfig returns all new fields.
func TestDescribeDomainConfig_FullConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody map[string]any
		name       string
		wantKeys   []string
	}{
		{
			name: "with_ebs_options",
			createBody: map[string]any{
				"DomainName": "cfg-ebs",
				"EBSOptions": map[string]any{
					"EBSEnabled": true,
					"VolumeType": "gp3",
					"VolumeSize": 100,
				},
			},
			wantKeys: []string{"EngineVersion", "ClusterConfig", "EBSOptions"},
		},
		{
			name: "with_encryption",
			createBody: map[string]any{
				"DomainName":                  "cfg-enc",
				"EncryptionAtRestOptions":     map[string]any{"Enabled": true},
				"NodeToNodeEncryptionOptions": map[string]any{"Enabled": true},
			},
			wantKeys: []string{
				"EngineVersion", "ClusterConfig",
				"EncryptionAtRestOptions", "NodeToNodeEncryptionOptions",
			},
		},
		{
			name: "with_advanced_security",
			createBody: map[string]any{
				"DomainName": "cfg-aso",
				"AdvancedSecurityOptions": map[string]any{
					"Enabled":                     true,
					"InternalUserDatabaseEnabled": true,
				},
			},
			wantKeys: []string{"EngineVersion", "ClusterConfig", "AdvancedSecurityOptions"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createResp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", tt.createBody)
			createResp.Body.Close()
			require.Equal(t, http.StatusOK, createResp.StatusCode)

			domainName := tt.createBody["DomainName"].(string)
			cfgResp := doRequest(t, h, http.MethodGet,
				"/2021-01-01/opensearch/domain/"+domainName+"/config", nil)
			defer cfgResp.Body.Close()
			require.Equal(t, http.StatusOK, cfgResp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(cfgResp.Body).Decode(&out))
			dc, ok := out["DomainConfig"].(map[string]any)
			require.True(t, ok)

			for _, key := range tt.wantKeys {
				assert.Contains(t, dc, key, "DomainConfig should contain %s", key)
			}
		})
	}
}

func TestUpdateDomainConfig_MutatesState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updateBody map[string]any
		verifyFn   func(t *testing.T, domain map[string]any)
		name       string
	}{
		{
			name: "update_cluster_config",
			updateBody: map[string]any{
				"ClusterConfig": map[string]any{
					"InstanceType":  "m6g.large.search",
					"InstanceCount": 3,
				},
			},
			verifyFn: func(t *testing.T, status map[string]any) {
				t.Helper()
				dc := status["DomainStatus"].(map[string]any)
				cc := dc["ClusterConfig"].(map[string]any)
				assert.Equal(t, "m6g.large.search", cc["InstanceType"])
				assert.InDelta(t, float64(3), cc["InstanceCount"], 0)
			},
		},
		{
			name: "update_ebs_options",
			updateBody: map[string]any{
				"EBSOptions": map[string]any{
					"EBSEnabled": true,
					"VolumeType": "gp3",
					"VolumeSize": 200,
				},
			},
			verifyFn: func(t *testing.T, status map[string]any) {
				t.Helper()
				dc := status["DomainStatus"].(map[string]any)
				ebs := dc["EBSOptions"].(map[string]any)
				assert.Equal(t, true, ebs["EBSEnabled"])
				assert.Equal(t, "gp3", ebs["VolumeType"])
				assert.InDelta(t, float64(200), ebs["VolumeSize"], 0)
			},
		},
		{
			name: "update_access_policies",
			updateBody: map[string]any{
				"AccessPolicies": `{"Version":"2012-10-17","Statement":[]}`,
			},
			verifyFn: func(t *testing.T, status map[string]any) {
				t.Helper()
				dc := status["DomainStatus"].(map[string]any)
				assert.NotEmpty(t, dc["AccessPolicies"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			cr := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
				map[string]any{"DomainName": "mut-test"})
			cr.Body.Close()
			require.Equal(t, http.StatusOK, cr.StatusCode)

			upResp := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/domain/mut-test/config",
				tt.updateBody)
			upResp.Body.Close()
			require.Equal(t, http.StatusOK, upResp.StatusCode)

			// Now describe to verify state was mutated.
			descResp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain/mut-test", nil)
			defer descResp.Body.Close()
			require.Equal(t, http.StatusOK, descResp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(descResp.Body).Decode(&out))
			tt.verifyFn(t, out)
		})
	}
}

func TestCancelDomainConfigChange_ReturnsLastChangeID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		domainName    string
		triggerUpdate bool
		wantEmptyIDs  bool
	}{
		{
			name:          "no pending change returns empty",
			domainName:    "cancel-domain-empty",
			triggerUpdate: false,
			wantEmptyIDs:  true,
		},
		{
			name:          "pending change returned and cleared",
			domainName:    "cancel-domain-pending",
			triggerUpdate: true,
			wantEmptyIDs:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
				"DomainName": tt.domainName,
			})
			resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			if tt.triggerUpdate {
				resp = doRequest(
					t,
					h,
					http.MethodPut,
					"/2021-01-01/opensearch/domain/"+tt.domainName+"/config",
					map[string]any{
						"AccessPolicies": `{"Version":"2012-10-17"}`,
					},
				)
				resp.Body.Close()
				require.Equal(t, http.StatusOK, resp.StatusCode)
			}

			resp = doRequest(
				t,
				h,
				http.MethodPost,
				"/2021-01-01/opensearch/domain/"+tt.domainName+"/config/cancel",
				map[string]any{"DryRun": false},
			)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var got struct {
				CancelledChangeIDs []string `json:"CancelledChangeIds"`
			}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))

			if tt.wantEmptyIDs {
				assert.Empty(t, got.CancelledChangeIDs)
			} else {
				assert.NotEmpty(t, got.CancelledChangeIDs, "expected a change ID to be returned")
			}
		})
	}
}

func TestOpenSearchHandler_DescribeDomainConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *opensearch.Handler)
		name         string
		domainName   string
		wantContains []string
		wantCode     int
	}{
		{
			name:       "success",
			domainName: "config-domain",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "config-domain"})
				r.Body.Close()
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DomainConfig", "EngineVersion", "ClusterConfig"},
		},
		{
			name:       "not_found",
			domainName: "nonexistent",
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}
			resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain/"+tt.domainName+"/config", nil)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)
			if len(tt.wantContains) > 0 {
				bodyBytes, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				for _, s := range tt.wantContains {
					assert.Contains(t, string(bodyBytes), s)
				}
			}
		})
	}
}

func TestOpenSearchHandler_CancelDomainConfigChange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *opensearch.Handler)
		name         string
		domainName   string
		wantContains []string
		wantCode     int
		dryRun       bool
	}{
		{
			name:       "success",
			domainName: "my-domain",
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "my-domain"})
				r.Body.Close()
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"CancelledChangeIds"},
		},
		{
			name:       "dry_run",
			domainName: "dr-domain",
			dryRun:     true,
			setup: func(t *testing.T, h *opensearch.Handler) {
				t.Helper()
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
					map[string]any{"DomainName": "dr-domain"})
				r.Body.Close()
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"true"},
		},
		{
			name:       "domain_not_found",
			domainName: "nonexistent",
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			body := map[string]any{"DryRun": tt.dryRun}
			path := "/2021-01-01/opensearch/domain/" + tt.domainName + "/config/cancel"
			resp := doRequest(t, h, http.MethodPost, path, body)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode)

			if len(tt.wantContains) > 0 {
				bodyBytes, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				for _, s := range tt.wantContains {
					assert.Contains(t, string(bodyBytes), s)
				}
			}
		})
	}
}
