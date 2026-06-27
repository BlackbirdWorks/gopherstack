package elasticsearch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_DomainDedicatedMaster verifies dedicated master fields are stored and returned.
func TestParity_DomainDedicatedMaster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                    string
		body                    map[string]any
		wantDedicatedMasterType string
		wantDedicatedCount      float64
		wantDedicatedMaster     bool
	}{
		{
			name: "dedicated_master_enabled",
			body: map[string]any{
				"DomainName": "dm-domain",
				"ElasticsearchClusterConfig": map[string]any{
					"DedicatedMasterEnabled": true,
					"DedicatedMasterType":    "m5.large.elasticsearch",
					"DedicatedMasterCount":   3,
				},
			},
			wantDedicatedMaster:     true,
			wantDedicatedMasterType: "m5.large.elasticsearch",
			wantDedicatedCount:      3,
		},
		{
			name: "dedicated_master_disabled",
			body: map[string]any{
				"DomainName": "nodm-domain",
				"ElasticsearchClusterConfig": map[string]any{
					"DedicatedMasterEnabled": false,
				},
			},
			wantDedicatedMaster: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", tc.body)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

			status := out["DomainStatus"].(map[string]any)
			cfg := status["ElasticsearchClusterConfig"].(map[string]any)
			assert.Equal(t, tc.wantDedicatedMaster, cfg["DedicatedMasterEnabled"])

			if tc.wantDedicatedMaster {
				assert.Equal(t, tc.wantDedicatedMasterType, cfg["DedicatedMasterType"])
				assert.InDelta(t, tc.wantDedicatedCount, cfg["DedicatedMasterCount"], 0.01)
			}
		})
	}
}

// TestParity_DomainZoneAwareness verifies zone awareness fields are stored and returned.
func TestParity_DomainZoneAwareness(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name          string
		body          map[string]any
		wantAZCount   float64
		wantZoneAware bool
	}{
		{
			name: "zone_awareness_with_config",
			body: map[string]any{
				"DomainName": "za-domain",
				"ElasticsearchClusterConfig": map[string]any{
					"ZoneAwarenessEnabled": true,
					"ZoneAwarenessConfig":  map[string]any{"AvailabilityZoneCount": 3},
				},
			},
			wantZoneAware: true,
			wantAZCount:   3,
		},
		{
			name: "zone_awareness_disabled",
			body: map[string]any{
				"DomainName": "noza-domain",
				"ElasticsearchClusterConfig": map[string]any{
					"ZoneAwarenessEnabled": false,
				},
			},
			wantZoneAware: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", tc.body)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

			status := out["DomainStatus"].(map[string]any)
			cfg := status["ElasticsearchClusterConfig"].(map[string]any)
			assert.Equal(t, tc.wantZoneAware, cfg["ZoneAwarenessEnabled"])

			if tc.wantZoneAware {
				azCfg := cfg["ZoneAwarenessConfig"].(map[string]any)
				assert.InDelta(t, tc.wantAZCount, azCfg["AvailabilityZoneCount"], 0.01)
			}
		})
	}
}

// TestParity_DomainWarm verifies warm storage fields are stored and returned.
func TestParity_DomainWarm(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		body          map[string]any
		wantWarmType  string
		wantWarmCount float64
		wantWarm      bool
	}{
		{
			name: "warm_enabled",
			body: map[string]any{
				"DomainName": "warm-domain",
				"ElasticsearchClusterConfig": map[string]any{
					"WarmEnabled": true,
					"WarmType":    "ultrawarm1.medium.elasticsearch",
					"WarmCount":   2,
				},
			},
			wantWarm:      true,
			wantWarmType:  "ultrawarm1.medium.elasticsearch",
			wantWarmCount: 2,
		},
		{
			name: "warm_disabled",
			body: map[string]any{
				"DomainName": "nowarm-domain",
				"ElasticsearchClusterConfig": map[string]any{
					"WarmEnabled": false,
				},
			},
			wantWarm: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", tc.body)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

			status := out["DomainStatus"].(map[string]any)
			cfg := status["ElasticsearchClusterConfig"].(map[string]any)
			assert.Equal(t, tc.wantWarm, cfg["WarmEnabled"])

			if tc.wantWarm {
				assert.Equal(t, tc.wantWarmType, cfg["WarmType"])
				assert.InDelta(t, tc.wantWarmCount, cfg["WarmCount"], 0.01)
			}
		})
	}
}

// TestParity_DomainEBSIopsAndThroughput verifies Iops and Throughput fields are stored and returned.
func TestParity_DomainEBSIopsAndThroughput(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name           string
		body           map[string]any
		wantIops       float64
		wantThroughput float64
	}{
		{
			name: "with_iops_and_throughput",
			body: map[string]any{
				"DomainName": "ebs-iops-domain",
				"EBSOptions": map[string]any{
					"EBSEnabled": true,
					"VolumeType": "gp3",
					"VolumeSize": 100,
					"Iops":       3000,
					"Throughput": 125,
				},
			},
			wantIops:       3000,
			wantThroughput: 125,
		},
		{
			name: "without_iops",
			body: map[string]any{
				"DomainName": "ebs-nops-domain",
				"EBSOptions": map[string]any{
					"EBSEnabled": true,
					"VolumeType": "gp2",
					"VolumeSize": 20,
				},
			},
			wantIops:       0,
			wantThroughput: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", tc.body)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

			status := out["DomainStatus"].(map[string]any)
			ebs := status["EBSOptions"].(map[string]any)
			assert.InDelta(t, tc.wantIops, ebs["Iops"], 0.01)
			assert.InDelta(t, tc.wantThroughput, ebs["Throughput"], 0.01)
		})
	}
}

// TestParity_DomainEncryptionAtRest verifies EncryptionAtRestOptions is stored and returned.
func TestParity_DomainEncryptionAtRest(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name        string
		body        map[string]any
		wantEnabled bool
	}{
		{
			name: "encryption_enabled",
			body: map[string]any{
				"DomainName":              "enc-domain",
				"EncryptionAtRestOptions": map[string]any{"Enabled": true},
			},
			wantEnabled: true,
		},
		{
			name: "encryption_disabled",
			body: map[string]any{
				"DomainName":              "noenc-domain",
				"EncryptionAtRestOptions": map[string]any{"Enabled": false},
			},
			wantEnabled: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", tc.body)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

			status := out["DomainStatus"].(map[string]any)
			enc := status["EncryptionAtRestOptions"].(map[string]any)
			assert.Equal(t, tc.wantEnabled, enc["Enabled"])
		})
	}
}

// TestParity_DomainNodeToNodeEncryption verifies NodeToNodeEncryptionOptions is stored.
func TestParity_DomainNodeToNodeEncryption(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name        string
		body        map[string]any
		wantEnabled bool
	}{
		{
			name: "n2n_enabled",
			body: map[string]any{
				"DomainName":                  "n2n-domain",
				"NodeToNodeEncryptionOptions": map[string]any{"Enabled": true},
			},
			wantEnabled: true,
		},
		{
			name: "n2n_disabled",
			body: map[string]any{
				"DomainName":                  "non2n-domain",
				"NodeToNodeEncryptionOptions": map[string]any{"Enabled": false},
			},
			wantEnabled: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", tc.body)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

			status := out["DomainStatus"].(map[string]any)
			n2n := status["NodeToNodeEncryptionOptions"].(map[string]any)
			assert.Equal(t, tc.wantEnabled, n2n["Enabled"])
		})
	}
}

// TestParity_DomainEndpointOptions verifies DomainEndpointOptions (EnforceHTTPS, TLS policy) are stored.
func TestParity_DomainEndpointOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		body             map[string]any
		wantTLSPolicy    string
		wantEnforceHTTPS bool
	}{
		{
			name: "enforce_https_with_policy",
			body: map[string]any{
				"DomainName": "https-domain",
				"DomainEndpointOptions": map[string]any{
					"EnforceHTTPS":      true,
					"TLSSecurityPolicy": "Policy-Min-TLS-1-2-2019-07",
				},
			},
			wantEnforceHTTPS: true,
			wantTLSPolicy:    "Policy-Min-TLS-1-2-2019-07",
		},
		{
			name: "no_https",
			body: map[string]any{
				"DomainName": "nohttps-domain",
				"DomainEndpointOptions": map[string]any{
					"EnforceHTTPS": false,
				},
			},
			wantEnforceHTTPS: false,
			wantTLSPolicy:    "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", tc.body)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

			status := out["DomainStatus"].(map[string]any)
			endpoint := status["DomainEndpointOptions"].(map[string]any)
			assert.Equal(t, tc.wantEnforceHTTPS, endpoint["EnforceHTTPS"])
			tlsPolicy, _ := endpoint["TLSSecurityPolicy"].(string)
			assert.Equal(t, tc.wantTLSPolicy, tlsPolicy)
		})
	}
}

// TestParity_DomainSnapshotOptions verifies SnapshotOptions is persisted and updatable.
func TestParity_DomainSnapshotOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		createHr int
		updateHr int
		wantHr   float64
	}{
		{
			name:     "snapshot_hour_set_on_create",
			createHr: 3,
			updateHr: 0,
			wantHr:   3,
		},
		{
			name:     "snapshot_hour_updated",
			createHr: 0,
			updateHr: 12,
			wantHr:   12,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			createBody := map[string]any{
				"DomainName": "snap-domain",
			}
			if tc.createHr != 0 {
				createBody["SnapshotOptions"] = map[string]any{
					"AutomatedSnapshotStartHour": tc.createHr,
				}
			}

			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", createBody)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			if tc.updateHr != 0 {
				updateResp := doRequest(
					t,
					h,
					http.MethodPost,
					"/2015-01-01/es/domain/snap-domain/config",
					map[string]any{
						"SnapshotOptions": map[string]any{"AutomatedSnapshotStartHour": tc.updateHr},
					},
				)
				defer updateResp.Body.Close()
				require.Equal(t, http.StatusOK, updateResp.StatusCode)
			}

			descResp := doRequest(t, h, http.MethodGet, "/2015-01-01/es/domain/snap-domain", nil)
			defer descResp.Body.Close()
			require.Equal(t, http.StatusOK, descResp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(descResp.Body).Decode(&out))

			status := out["DomainStatus"].(map[string]any)
			snap := status["SnapshotOptions"].(map[string]any)
			assert.InDelta(t, tc.wantHr, snap["AutomatedSnapshotStartHour"], 0.01)
		})
	}
}

// TestParity_DomainAdvancedOptions verifies AdvancedOptions is stored and updatable.
func TestParity_DomainAdvancedOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createOpts map[string]string
		updateOpts map[string]string
		wantKey    string
		wantVal    string
	}{
		{
			name:       "advanced_opts_on_create",
			createOpts: map[string]string{"rest.action.multi.allow_explicit_index": "true"},
			wantKey:    "rest.action.multi.allow_explicit_index",
			wantVal:    "true",
		},
		{
			name:       "advanced_opts_on_update",
			updateOpts: map[string]string{"override_main_response_version": "false"},
			wantKey:    "override_main_response_version",
			wantVal:    "false",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			createBody := map[string]any{"DomainName": "adv-domain"}
			if tc.createOpts != nil {
				createBody["AdvancedOptions"] = tc.createOpts
			}

			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", createBody)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			if tc.updateOpts != nil {
				updateResp := doRequest(
					t,
					h,
					http.MethodPost,
					"/2015-01-01/es/domain/adv-domain/config",
					map[string]any{
						"AdvancedOptions": tc.updateOpts,
					},
				)
				defer updateResp.Body.Close()
				require.Equal(t, http.StatusOK, updateResp.StatusCode)
			}

			descResp := doRequest(t, h, http.MethodGet, "/2015-01-01/es/domain/adv-domain", nil)
			defer descResp.Body.Close()
			require.Equal(t, http.StatusOK, descResp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(descResp.Body).Decode(&out))

			status := out["DomainStatus"].(map[string]any)
			advOpts := status["AdvancedOptions"].(map[string]any)
			assert.Equal(t, tc.wantVal, advOpts[tc.wantKey])
		})
	}
}

// TestParity_DomainAccessPolicies verifies AccessPolicies is stored and updatable.
func TestParity_DomainAccessPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		createPolicy string
		updatePolicy string
		wantPolicy   string
	}{
		{
			name:         "policy_on_create",
			createPolicy: `{"Version":"2012-10-17"}`,
			wantPolicy:   `{"Version":"2012-10-17"}`,
		},
		{
			name:         "policy_on_update",
			updatePolicy: `{"Version":"2012-10-17","Statement":[]}`,
			wantPolicy:   `{"Version":"2012-10-17","Statement":[]}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			createBody := map[string]any{"DomainName": "pol-domain"}
			if tc.createPolicy != "" {
				createBody["AccessPolicies"] = tc.createPolicy
			}

			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", createBody)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			if tc.updatePolicy != "" {
				updateResp := doRequest(
					t,
					h,
					http.MethodPost,
					"/2015-01-01/es/domain/pol-domain/config",
					map[string]any{
						"AccessPolicies": tc.updatePolicy,
					},
				)
				defer updateResp.Body.Close()
				require.Equal(t, http.StatusOK, updateResp.StatusCode)
			}

			descResp := doRequest(t, h, http.MethodGet, "/2015-01-01/es/domain/pol-domain", nil)
			defer descResp.Body.Close()
			require.Equal(t, http.StatusOK, descResp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(descResp.Body).Decode(&out))

			status := out["DomainStatus"].(map[string]any)
			assert.Equal(t, tc.wantPolicy, status["AccessPolicies"])
		})
	}
}

// TestParity_ListTagsNotFound verifies ListTags returns 404 for unknown domain ARN.
func TestParity_ListTagsNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arn  string
	}{
		{
			name: "unknown_arn",
			arn:  "arn:aws:es:us-east-1:123456789012:domain/no-such-domain",
		},
		{
			name: "empty_arn",
			arn:  "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodGet, "/2015-01-01/tags?arn="+tc.arn, nil)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusNotFound, resp.StatusCode)
		})
	}
}

// TestParity_ListElasticsearchVersions verifies all 22 valid versions are returned.
func TestParity_ListElasticsearchVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		expectedVersion string
	}{
		{name: "7.17", expectedVersion: "7.17"},
		{name: "7.10", expectedVersion: "7.10"},
		{name: "6.8", expectedVersion: "6.8"},
		{name: "5.1", expectedVersion: "5.1"},
		{name: "1.5", expectedVersion: "1.5"},
	}

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodGet, "/2015-01-01/es/versions", nil)
	t.Cleanup(func() { resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out struct {
		ElasticsearchVersions []string `json:"ElasticsearchVersions"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.ElasticsearchVersions, 22, "expected all 22 versions")

	versionSet := make(map[string]bool)
	for _, v := range out.ElasticsearchVersions {
		versionSet[v] = true
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.True(t, versionSet[tc.expectedVersion], "version %s should be in the list", tc.expectedVersion)
		})
	}
}

// TestParity_CompatibleVersionsDomain verifies GetCompatibleVersions respects domainName param.
func TestParity_CompatibleVersionsDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		domainVersion     string
		wantSourceVersion string
	}{
		{
			name:              "6.8_domain",
			domainVersion:     "6.8",
			wantSourceVersion: "6.8",
		},
		{
			name:              "7.10_domain",
			domainVersion:     "7.10",
			wantSourceVersion: "7.10",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
				"DomainName":           "compat-domain",
				"ElasticsearchVersion": tc.domainVersion,
			})
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			compatResp := doRequest(
				t,
				h,
				http.MethodGet,
				"/2015-01-01/es/compatibleVersions?domainName=compat-domain",
				nil,
			)
			defer compatResp.Body.Close()
			require.Equal(t, http.StatusOK, compatResp.StatusCode)

			var out struct {
				CompatibleElasticsearchVersions []struct {
					SourceVersion  string   `json:"SourceVersion"`
					TargetVersions []string `json:"TargetVersions"`
				} `json:"CompatibleElasticsearchVersions"`
			}
			require.NoError(t, json.NewDecoder(compatResp.Body).Decode(&out))
			require.Len(t, out.CompatibleElasticsearchVersions, 1)
			assert.Equal(t, tc.wantSourceVersion, out.CompatibleElasticsearchVersions[0].SourceVersion)
		})
	}
}

// TestParity_UpdateDomainNewFields verifies UpdateDomainConfig applies new fields correctly.
func TestParity_UpdateDomainNewFields(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name       string
		updateBody map[string]any
		checkFn    func(t *testing.T, status map[string]any)
	}{
		{
			name: "update_encryption_at_rest",
			updateBody: map[string]any{
				"EncryptionAtRestOptions": map[string]any{"Enabled": true},
			},
			checkFn: func(t *testing.T, status map[string]any) {
				t.Helper()

				enc := status["EncryptionAtRestOptions"].(map[string]any)
				assert.Equal(t, true, enc["Enabled"])
			},
		},
		{
			name: "update_node_to_node_encryption",
			updateBody: map[string]any{
				"NodeToNodeEncryptionOptions": map[string]any{"Enabled": true},
			},
			checkFn: func(t *testing.T, status map[string]any) {
				t.Helper()

				n2n := status["NodeToNodeEncryptionOptions"].(map[string]any)
				assert.Equal(t, true, n2n["Enabled"])
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
			checkFn: func(t *testing.T, status map[string]any) {
				t.Helper()

				ep := status["DomainEndpointOptions"].(map[string]any)
				assert.Equal(t, true, ep["EnforceHTTPS"])
				assert.Equal(t, "Policy-Min-TLS-1-2-2019-07", ep["TLSSecurityPolicy"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			createResp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
				"DomainName": "upd-domain",
			})
			defer createResp.Body.Close()
			require.Equal(t, http.StatusOK, createResp.StatusCode)

			updateResp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain/upd-domain/config", tc.updateBody)
			defer updateResp.Body.Close()
			require.Equal(t, http.StatusOK, updateResp.StatusCode)

			descResp := doRequest(t, h, http.MethodGet, "/2015-01-01/es/domain/upd-domain", nil)
			defer descResp.Body.Close()
			require.Equal(t, http.StatusOK, descResp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(descResp.Body).Decode(&out))

			status := out["DomainStatus"].(map[string]any)
			tc.checkFn(t, status)
		})
	}
}
