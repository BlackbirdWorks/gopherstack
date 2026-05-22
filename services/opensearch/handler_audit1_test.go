package opensearch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAudit1_CreateDomain_FullClusterConfig verifies all ClusterConfig fields round-trip through
// CreateDomain and are reflected in DescribeDomain.
func TestAudit1_CreateDomain_FullClusterConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		clusterConfig map[string]any
		wantCC        map[string]any
		name          string
	}{
		{
			name: "dedicated_master_enabled",
			clusterConfig: map[string]any{
				"InstanceType":           "m6g.large.search",
				"InstanceCount":          3,
				"DedicatedMasterEnabled": true,
				"DedicatedMasterType":    "m6g.large.search",
				"DedicatedMasterCount":   3,
			},
			wantCC: map[string]any{
				"DedicatedMasterEnabled": true,
				"DedicatedMasterType":    "m6g.large.search",
				"DedicatedMasterCount":   float64(3),
			},
		},
		{
			name: "zone_awareness_enabled",
			clusterConfig: map[string]any{
				"InstanceType":         "r6g.large.search",
				"InstanceCount":        6,
				"ZoneAwarenessEnabled": true,
				"ZoneAwarenessConfig":  map[string]any{"AvailabilityZoneCount": 3},
			},
			wantCC: map[string]any{
				"ZoneAwarenessEnabled": true,
			},
		},
		{
			name: "warm_nodes_enabled",
			clusterConfig: map[string]any{
				"InstanceType": "r6g.large.search",
				"WarmEnabled":  true,
				"WarmType":     "ultrawarm1.medium.search",
				"WarmCount":    2,
			},
			wantCC: map[string]any{
				"WarmEnabled": true,
				"WarmType":    "ultrawarm1.medium.search",
				"WarmCount":   float64(2),
			},
		},
		{
			name: "cold_storage_enabled",
			clusterConfig: map[string]any{
				"InstanceType":       "r6g.large.search",
				"WarmEnabled":        true,
				"WarmType":           "ultrawarm1.medium.search",
				"WarmCount":          2,
				"ColdStorageEnabled": true,
			},
			wantCC: map[string]any{
				"ColdStorageEnabled": true,
			},
		},
		{
			name: "multi_az_with_standby",
			clusterConfig: map[string]any{
				"InstanceType":              "m6g.large.search",
				"InstanceCount":             3,
				"MultiAZWithStandbyEnabled": true,
			},
			wantCC: map[string]any{
				"MultiAZWithStandbyEnabled": true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			body := map[string]any{
				"DomainName":    "cc-domain-" + tt.name,
				"ClusterConfig": tt.clusterConfig,
			}
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", body)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
			status, ok := out["DomainStatus"].(map[string]any)
			require.True(t, ok)
			cc, ok := status["ClusterConfig"].(map[string]any)
			require.True(t, ok)

			for k, v := range tt.wantCC {
				assert.Equal(t, v, cc[k], "ClusterConfig.%s", k)
			}
		})
	}
}

// TestAudit1_CreateDomain_EBSOptions verifies EBSOptions are stored and returned.
func TestAudit1_CreateDomain_EBSOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ebsOptions map[string]any
		wantEBS    map[string]any
		name       string
	}{
		{
			name: "ebs_enabled_gp3",
			ebsOptions: map[string]any{
				"EBSEnabled": true,
				"VolumeType": "gp3",
				"VolumeSize": 100,
				"Iops":       3000,
				"Throughput": 125,
			},
			wantEBS: map[string]any{
				"EBSEnabled": true,
				"VolumeType": "gp3",
				"VolumeSize": float64(100),
				"Iops":       float64(3000),
				"Throughput": float64(125),
			},
		},
		{
			name: "ebs_enabled_io1_with_kms",
			ebsOptions: map[string]any{
				"EBSEnabled": true,
				"VolumeType": "io1",
				"VolumeSize": 200,
				"Iops":       10000,
				"KMSKeyId":   "arn:aws:kms:us-east-1:123456789012:key/abc123",
			},
			wantEBS: map[string]any{
				"EBSEnabled": true,
				"VolumeType": "io1",
				"KMSKeyId":   "arn:aws:kms:us-east-1:123456789012:key/abc123",
			},
		},
		{
			name: "ebs_disabled",
			ebsOptions: map[string]any{
				"EBSEnabled": false,
			},
			wantEBS: map[string]any{
				"EBSEnabled": false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
				"DomainName": "ebs-" + tt.name,
				"EBSOptions": tt.ebsOptions,
			})
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
			status, ok := out["DomainStatus"].(map[string]any)
			require.True(t, ok)
			ebs, ok := status["EBSOptions"].(map[string]any)
			require.True(t, ok, "EBSOptions should be present in response")

			for k, v := range tt.wantEBS {
				assert.Equal(t, v, ebs[k], "EBSOptions.%s", k)
			}
		})
	}
}

// TestAudit1_CreateDomain_EncryptionOptions verifies encryption at rest and node-to-node options.
func TestAudit1_CreateDomain_EncryptionOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body              map[string]any
		wantEncryptFields map[string]any
		name              string
		wantN2NEnabled    bool
	}{
		{
			name: "encrypt_at_rest_enabled",
			body: map[string]any{
				"DomainName": "enc-enabled",
				"EncryptionAtRestOptions": map[string]any{
					"Enabled":  true,
					"KMSKeyId": "arn:aws:kms:us-east-1:123456789012:key/test-key",
				},
			},
			wantEncryptFields: map[string]any{
				"Enabled":  true,
				"KMSKeyId": "arn:aws:kms:us-east-1:123456789012:key/test-key",
			},
		},
		{
			name: "encrypt_at_rest_disabled",
			body: map[string]any{
				"DomainName": "enc-disabled",
				"EncryptionAtRestOptions": map[string]any{
					"Enabled": false,
				},
			},
			wantEncryptFields: map[string]any{
				"Enabled": false,
			},
		},
		{
			name: "node_to_node_encryption",
			body: map[string]any{
				"DomainName": "n2n-enc",
				"NodeToNodeEncryptionOptions": map[string]any{
					"Enabled": true,
				},
			},
			wantN2NEnabled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", tt.body)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
			status, ok := out["DomainStatus"].(map[string]any)
			require.True(t, ok)

			if len(tt.wantEncryptFields) > 0 {
				enc, encOk := status["EncryptionAtRestOptions"].(map[string]any)
				require.True(t, encOk, "EncryptionAtRestOptions should be present")
				for k, v := range tt.wantEncryptFields {
					assert.Equal(t, v, enc[k], "EncryptionAtRestOptions.%s", k)
				}
			}

			if tt.wantN2NEnabled {
				n2n, n2nOk := status["NodeToNodeEncryptionOptions"].(map[string]any)
				require.True(t, n2nOk, "NodeToNodeEncryptionOptions should be present")
				assert.Equal(t, true, n2n["Enabled"])
			}
		})
	}
}

// TestAudit1_CreateDomain_DomainEndpointOptions verifies HTTPS and custom endpoint options.
func TestAudit1_CreateDomain_DomainEndpointOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		options map[string]any
		wantDEO map[string]any
		name    string
	}{
		{
			name: "enforce_https",
			options: map[string]any{
				"EnforceHTTPS":      true,
				"TLSSecurityPolicy": "Policy-Min-TLS-1-2-2019-07",
			},
			wantDEO: map[string]any{
				"EnforceHTTPS":      true,
				"TLSSecurityPolicy": "Policy-Min-TLS-1-2-2019-07",
			},
		},
		{
			name: "custom_endpoint_enabled",
			options: map[string]any{
				"EnforceHTTPS":                 true,
				"CustomEndpointEnabled":        true,
				"CustomEndpoint":               "search.example.com",
				"CustomEndpointCertificateArn": "arn:aws:acm:us-east-1:123456789012:certificate/abc",
			},
			wantDEO: map[string]any{
				"CustomEndpointEnabled":        true,
				"CustomEndpoint":               "search.example.com",
				"CustomEndpointCertificateArn": "arn:aws:acm:us-east-1:123456789012:certificate/abc",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
				"DomainName":            "deo-" + tt.name,
				"DomainEndpointOptions": tt.options,
			})
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
			status, ok := out["DomainStatus"].(map[string]any)
			require.True(t, ok)
			deo, ok := status["DomainEndpointOptions"].(map[string]any)
			require.True(t, ok, "DomainEndpointOptions should be present")

			for k, v := range tt.wantDEO {
				assert.Equal(t, v, deo[k], "DomainEndpointOptions.%s", k)
			}
		})
	}
}

// TestAudit1_CreateDomain_AdvancedSecurityOptions verifies FGAC settings including SAML.
func TestAudit1_CreateDomain_AdvancedSecurityOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		options map[string]any
		wantASO map[string]any
		name    string
	}{
		{
			name: "internal_userdb_enabled",
			options: map[string]any{
				"Enabled":                     true,
				"InternalUserDatabaseEnabled": true,
			},
			wantASO: map[string]any{
				"Enabled":                     true,
				"InternalUserDatabaseEnabled": true,
			},
		},
		{
			name: "saml_enabled",
			options: map[string]any{
				"Enabled":                     true,
				"InternalUserDatabaseEnabled": false,
				"SAMLOptions": map[string]any{
					"Enabled":               true,
					"SessionTimeoutMinutes": 60,
				},
			},
			wantASO: map[string]any{
				"Enabled": true,
			},
		},
		{
			name: "anonymous_auth_enabled",
			options: map[string]any{
				"Enabled":              true,
				"AnonymousAuthEnabled": true,
			},
			wantASO: map[string]any{
				"Enabled":              true,
				"AnonymousAuthEnabled": true,
			},
		},
		{
			name: "disabled",
			options: map[string]any{
				"Enabled": false,
			},
			wantASO: map[string]any{
				"Enabled": false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
				"DomainName":              "aso-" + tt.name,
				"AdvancedSecurityOptions": tt.options,
			})
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
			status, ok := out["DomainStatus"].(map[string]any)
			require.True(t, ok)
			aso, ok := status["AdvancedSecurityOptions"].(map[string]any)
			require.True(t, ok, "AdvancedSecurityOptions should be present")

			for k, v := range tt.wantASO {
				assert.Equal(t, v, aso[k], "AdvancedSecurityOptions.%s", k)
			}
		})
	}
}

// TestAudit1_CreateDomain_VPCOptions verifies VPC configuration is stored and returned.
func TestAudit1_CreateDomain_VPCOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vpcOptions map[string]any
		wantVPC    map[string]any
		name       string
	}{
		{
			name: "single_subnet_sg",
			vpcOptions: map[string]any{
				"SubnetIds":        []string{"subnet-abc123"},
				"SecurityGroupIds": []string{"sg-abc123"},
			},
			wantVPC: map[string]any{
				"SubnetIds":        []any{"subnet-abc123"},
				"SecurityGroupIds": []any{"sg-abc123"},
			},
		},
		{
			name: "multi_subnet_multi_sg",
			vpcOptions: map[string]any{
				"SubnetIds":        []string{"subnet-aaa", "subnet-bbb", "subnet-ccc"},
				"SecurityGroupIds": []string{"sg-one", "sg-two"},
				"VPCId":            "vpc-12345",
			},
			wantVPC: map[string]any{
				"VPCId": "vpc-12345",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
				"DomainName": "vpc-" + tt.name,
				"VPCOptions": tt.vpcOptions,
			})
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
			status, ok := out["DomainStatus"].(map[string]any)
			require.True(t, ok)
			vpc, ok := status["VPCOptions"].(map[string]any)
			require.True(t, ok, "VPCOptions should be present")

			for k, v := range tt.wantVPC {
				assert.Equal(t, v, vpc[k], "VPCOptions.%s", k)
			}
		})
	}
}

// TestAudit1_CreateDomain_CognitoOptions verifies Cognito / Kibana auth options.
func TestAudit1_CreateDomain_CognitoOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cognitoOpts map[string]any
		wantCognito map[string]any
		name        string
	}{
		{
			name: "cognito_enabled",
			cognitoOpts: map[string]any{
				"Enabled":        true,
				"UserPoolId":     "us-east-1_abc123",
				"IdentityPoolId": "us-east-1:abcd-1234",
				"RoleArn":        "arn:aws:iam::123456789012:role/CognitoRole",
			},
			wantCognito: map[string]any{
				"Enabled":        true,
				"UserPoolId":     "us-east-1_abc123",
				"IdentityPoolId": "us-east-1:abcd-1234",
				"RoleArn":        "arn:aws:iam::123456789012:role/CognitoRole",
			},
		},
		{
			name: "cognito_disabled",
			cognitoOpts: map[string]any{
				"Enabled": false,
			},
			wantCognito: map[string]any{
				"Enabled": false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
				"DomainName":     "cognito-" + tt.name,
				"CognitoOptions": tt.cognitoOpts,
			})
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
			status, ok := out["DomainStatus"].(map[string]any)
			require.True(t, ok)
			cognito, ok := status["CognitoOptions"].(map[string]any)
			require.True(t, ok, "CognitoOptions should be present")

			for k, v := range tt.wantCognito {
				assert.Equal(t, v, cognito[k], "CognitoOptions.%s", k)
			}
		})
	}
}

// TestAudit1_CreateDomain_LogPublishingOptions verifies all 4 log type options.
func TestAudit1_CreateDomain_LogPublishingOptions(t *testing.T) {
	t.Parallel()

	allLogTypes := []string{
		"INDEX_SLOW_LOGS",
		"SEARCH_SLOW_LOGS",
		"ES_APPLICATION_LOGS",
		"AUDIT_LOGS",
	}

	tests := []struct {
		logPublishingOpts map[string]any
		name              string
		wantLogTypes      []string
	}{
		{
			name: "single_log_type",
			logPublishingOpts: map[string]any{
				"INDEX_SLOW_LOGS": map[string]any{
					"Enabled":                   true,
					"CloudWatchLogsLogGroupArn": "arn:aws:logs:us-east-1:123456789012:log-group:/aws/opensearch/index-slow",
				},
			},
			wantLogTypes: []string{"INDEX_SLOW_LOGS"},
		},
		{
			name: "all_four_log_types",
			logPublishingOpts: (func() map[string]any {
				opts := map[string]any{}
				for _, lt := range allLogTypes {
					opts[lt] = map[string]any{
						"Enabled":                   true,
						"CloudWatchLogsLogGroupArn": "arn:aws:logs:us-east-1:123456789012:log-group:/aws/opensearch/" + lt,
					}
				}
				return opts
			})(),
			wantLogTypes: allLogTypes,
		},
		{
			name: "log_type_disabled",
			logPublishingOpts: map[string]any{
				"AUDIT_LOGS": map[string]any{
					"Enabled": false,
				},
			},
			wantLogTypes: []string{"AUDIT_LOGS"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
				"DomainName":           "logs-" + tt.name,
				"LogPublishingOptions": tt.logPublishingOpts,
			})
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
			status, ok := out["DomainStatus"].(map[string]any)
			require.True(t, ok)
			logs, ok := status["LogPublishingOptions"].(map[string]any)
			require.True(t, ok, "LogPublishingOptions should be present")

			for _, logType := range tt.wantLogTypes {
				assert.Contains(t, logs, logType, "LogPublishingOptions should contain %s", logType)
			}
		})
	}
}

// TestAudit1_CreateDomain_SnapshotOptions verifies automated snapshot start hour.
func TestAudit1_CreateDomain_SnapshotOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                       string
		automatedSnapshotStartHour int
	}{
		{name: "hour_0", automatedSnapshotStartHour: 0},
		{name: "hour_1", automatedSnapshotStartHour: 1},
		{name: "hour_23", automatedSnapshotStartHour: 23},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
				"DomainName": "snap-" + tt.name,
				"SnapshotOptions": map[string]any{
					"AutomatedSnapshotStartHour": tt.automatedSnapshotStartHour,
				},
			})
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
			status, ok := out["DomainStatus"].(map[string]any)
			require.True(t, ok)
			snapOpts, ok := status["SnapshotOptions"].(map[string]any)
			require.True(t, ok, "SnapshotOptions should be present")
			assert.Equal(t, float64(tt.automatedSnapshotStartHour), snapOpts["AutomatedSnapshotStartHour"])
		})
	}
}

// TestAudit1_CreateDomain_AccessPolicies verifies the access policies JSON field.
func TestAudit1_CreateDomain_AccessPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		accessPolicies string
		wantPolicies   string
	}{
		{
			name:           "empty_policy",
			accessPolicies: "",
			wantPolicies:   "",
		},
		{
			name:           "allow_all_policy",
			accessPolicies: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"es:*","Resource":"*"}]}`,
			wantPolicies:   `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"es:*","Resource":"*"}]}`,
		},
		{
			name:           "ip_based_policy",
			accessPolicies: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"es:ESHttp*","Condition":{"IpAddress":{"aws:SourceIp":["203.0.113.0/24"]}}}]}`,
			wantPolicies:   `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"es:ESHttp*","Condition":{"IpAddress":{"aws:SourceIp":["203.0.113.0/24"]}}}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			body := map[string]any{"DomainName": "ap-" + tt.name}
			if tt.accessPolicies != "" {
				body["AccessPolicies"] = tt.accessPolicies
			}
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", body)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
			status, ok := out["DomainStatus"].(map[string]any)
			require.True(t, ok)
			gotPolicies, _ := status["AccessPolicies"].(string)
			assert.Equal(t, tt.wantPolicies, gotPolicies)
		})
	}
}

// TestAudit1_UpdateDomainConfig_AllOptions verifies UpdateDomainConfig can set all new fields.
func TestAudit1_UpdateDomainConfig_AllOptions(t *testing.T) {
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
				"AccessPolicies": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:root"},"Action":"es:*","Resource":"*"}]}`,
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
				assert.Equal(t, float64(3), opts["DedicatedMasterCount"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createResp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain",
				map[string]any{"DomainName": "upd-" + tt.name})
			createResp.Body.Close()
			require.Equal(t, http.StatusOK, createResp.StatusCode)

			upResp := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/domain/upd-"+tt.name+"/config",
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
func TestAudit1_DescribeDomainConfig_FullConfig(t *testing.T) {
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
