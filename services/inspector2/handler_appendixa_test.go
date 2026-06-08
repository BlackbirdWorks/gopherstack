package inspector2_test

// Appendix A: table-driven tests for all 62 missing ops (go-314i).
//
// Covers:
//   - Members: AssociateMember, DisassociateMember, GetMember, ListMembers
//   - DelegatedAdminAccounts: Enable/Disable/Get/List
//   - OrganizationConfiguration: Describe/Update
//   - EC2 Deep Inspection: Get/Update config, org update, batch get/update member status
//   - EncryptionKey: Get/Reset/Update
//   - CIS Scan Configuration: Create/Delete/Update/List
//   - CIS Session: Start/Stop/Health/Telemetry + report/result queries
//   - Code Security Integration: Create/Delete/Get/Update/List
//   - Code Security Scan Configuration: Create/Delete/Get/Update/List + associations
//   - Code Security Scan: Start/Get
//   - Findings Report: Create/Cancel/Status
//   - SBOM Export: Create/Cancel/Get
//   - Coverage: List/Statistics
//   - FindingAggregations, UsageTotals, AccountPermissions
//   - SearchVulnerabilities
//   - BatchGetCodeSnippet, BatchGetFindingDetails, BatchGetFreeTrialInfo
//   - GetClustersForImage

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

// --- helper: new fresh handler for appendix A tests ---

func axHandler(t *testing.T) *inspector2.Handler {
	t.Helper()

	return inspector2.NewHandler(inspector2.NewInMemoryBackend("123456789012", "us-east-1"))
}

// --- Members ---

func TestAppendixA_Members(t *testing.T) {
	t.Parallel()

	type step struct {
		body   any
		check  func(t *testing.T, code int, body []byte)
		name   string
		method string
		path   string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "AssociateMember returns accountId",
			steps: []step{
				{
					name:   "associate",
					method: http.MethodPost,
					path:   "/members/associate",
					body:   map[string]any{"accountId": "111111111111"},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						assert.Equal(t, "111111111111", resp["accountId"])
					},
				},
			},
		},
		{
			name: "Associate/Get/Disassociate full cycle",
			steps: []step{
				{
					name:   "associate",
					method: http.MethodPost,
					path:   "/members/associate",
					body:   map[string]any{"accountId": "222222222222"},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get after associate",
					method: http.MethodPost,
					path:   "/members/get",
					body:   map[string]any{"accountId": "222222222222"},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						m, _ := resp["member"].(map[string]any)
						assert.Equal(t, "222222222222", m["accountId"])
					},
				},
				{
					name:   "list shows member",
					method: http.MethodPost,
					path:   "/members/list",
					body:   map[string]any{},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						members, _ := resp["members"].([]any)
						assert.Len(t, members, 1)
					},
				},
				{
					name:   "disassociate",
					method: http.MethodPost,
					path:   "/members/disassociate",
					body:   map[string]any{"accountId": "222222222222"},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get after disassociate returns 404",
					method: http.MethodPost,
					path:   "/members/get",
					body:   map[string]any{"accountId": "222222222222"},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusNotFound, code)
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := axHandler(t)

			for _, s := range tc.steps {
				rec := auditDo(t, h, s.method, s.path, s.body)
				s.check(t, rec.Code, rec.Body.Bytes())
			}
		})
	}
}

// --- Delegated Admin Accounts ---

func TestAppendixA_DelegatedAdmin(t *testing.T) {
	t.Parallel()

	type step struct {
		body   any
		check  func(t *testing.T, code int, body []byte)
		name   string
		method string
		path   string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "Enable/Get/Disable/List full cycle",
			steps: []step{
				{
					name:   "enable",
					method: http.MethodPost,
					path:   "/delegatedadminaccounts/enable",
					body:   map[string]any{"delegatedAdminAccountId": "333333333333"},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						assert.Equal(t, "333333333333", resp["delegatedAdminAccountId"])
						assert.Equal(t, "ENABLED", resp["status"])
					},
				},
				{
					name:   "get delegated admin",
					method: http.MethodPost,
					path:   "/delegatedadminaccounts/get",
					body:   map[string]any{},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						d, _ := resp["delegatedAdmin"].(map[string]any)
						assert.Equal(t, "333333333333", d["accountId"])
					},
				},
				{
					name:   "list delegated admins",
					method: http.MethodPost,
					path:   "/delegatedadminaccounts/list",
					body:   map[string]any{},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						admins, _ := resp["delegatedAdminAccounts"].([]any)
						assert.Len(t, admins, 1)
					},
				},
				{
					name:   "disable delegated admin",
					method: http.MethodPost,
					path:   "/delegatedadminaccounts/disable",
					body:   map[string]any{"delegatedAdminAccountId": "333333333333"},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get after disable returns 404",
					method: http.MethodPost,
					path:   "/delegatedadminaccounts/get",
					body:   map[string]any{},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusNotFound, code)
					},
				},
			},
		},
		{
			name: "Duplicate enable returns conflict",
			steps: []step{
				{
					name:   "first enable",
					method: http.MethodPost,
					path:   "/delegatedadminaccounts/enable",
					body:   map[string]any{"delegatedAdminAccountId": "444444444444"},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "second enable same account",
					method: http.MethodPost,
					path:   "/delegatedadminaccounts/enable",
					body:   map[string]any{"delegatedAdminAccountId": "444444444444"},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusConflict, code)
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := axHandler(t)

			for _, s := range tc.steps {
				rec := auditDo(t, h, s.method, s.path, s.body)
				s.check(t, rec.Code, rec.Body.Bytes())
			}
		})
	}
}

// --- Organization Configuration ---

func TestAppendixA_OrgConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		check  func(t *testing.T, code int, body []byte)
		name   string
		method string
		path   string
	}{
		{
			name:   "Describe returns defaults",
			method: http.MethodPost,
			path:   "/organizationconfiguration/describe",
			body:   map[string]any{},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				_, ok := resp["autoEnable"]
				assert.True(t, ok)
			},
		},
		{
			name:   "Update returns OK",
			method: http.MethodPost,
			path:   "/organizationconfiguration/update",
			body:   map[string]any{"autoEnable": map[string]any{"ec2": true}},
			check: func(t *testing.T, code int, _ []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := axHandler(t)
			rec := auditDo(t, h, tc.method, tc.path, tc.body)
			tc.check(t, rec.Code, rec.Body.Bytes())
		})
	}
}

// --- EC2 Deep Inspection ---

func TestAppendixA_Ec2DeepInspection(t *testing.T) {
	t.Parallel()

	type step struct {
		body   any
		check  func(t *testing.T, code int, body []byte)
		name   string
		method string
		path   string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "Get/Update round-trip",
			steps: []step{
				{
					name:   "get initial config",
					method: http.MethodPost,
					path:   "/ec2deepinspectionconfiguration/get",
					body:   map[string]any{},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						_, ok := resp["status"]
						assert.True(t, ok)
					},
				},
				{
					name:   "update with paths",
					method: http.MethodPost,
					path:   "/ec2deepinspectionconfiguration/update",
					body:   map[string]any{"packagePaths": []string{"/opt/app", "/usr/local"}},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						paths, _ := resp["packagePaths"].([]any)
						assert.Len(t, paths, 2)
					},
				},
				{
					name:   "org update",
					method: http.MethodPost,
					path:   "/ec2deepinspectionconfiguration/org/update",
					body:   map[string]any{"orgPackagePaths": []string{"/shared"}},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
			},
		},
		{
			name: "Batch get/update member status",
			steps: []step{
				{
					name:   "batch get unknown accounts returns defaults",
					method: http.MethodPost,
					path:   "/ec2deepinspectionstatus/member/batch/get",
					body:   map[string]any{"accountIds": []string{"555555555555"}},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						members, _ := resp["members"].([]any)
						assert.Len(t, members, 1)
					},
				},
				{
					name:   "batch update member status",
					method: http.MethodPost,
					path:   "/ec2deepinspectionstatus/member/batch/update",
					body: map[string]any{
						"accountEc2DeepInspectionStatuses": []any{
							map[string]any{
								"accountId":    "555555555555",
								"packagePaths": []string{"/opt"},
							},
						},
					},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						accounts, _ := resp["accounts"].([]any)
						assert.Len(t, accounts, 1)
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := axHandler(t)

			for _, s := range tc.steps {
				rec := auditDo(t, h, s.method, s.path, s.body)
				s.check(t, rec.Code, rec.Body.Bytes())
			}
		})
	}
}

// --- Encryption Key ---

func TestAppendixA_EncryptionKey(t *testing.T) {
	t.Parallel()

	type step struct {
		body   any
		check  func(t *testing.T, code int, body []byte)
		name   string
		method string
		path   string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "Get returns default key",
			steps: []step{
				{
					name:   "get default",
					method: http.MethodGet,
					path:   "/encryptionkey/get?resourceType=EC2&scanType=NETWORK",
					body:   nil,
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						assert.Equal(t, "AWS_OWNED_KEY", resp["kmsKeyId"])
					},
				},
			},
		},
		{
			name: "Update/Reset cycle",
			steps: []step{
				{
					name:   "update key",
					method: http.MethodPut,
					path:   "/encryptionkey/update",
					body: map[string]any{
						"kmsKeyId":     "arn:aws:kms:us-east-1:123456789012:key/abc",
						"resourceType": "EC2",
						"scanType":     "NETWORK",
					},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "reset key",
					method: http.MethodPut,
					path:   "/encryptionkey/reset",
					body: map[string]any{
						"resourceType": "EC2",
						"scanType":     "NETWORK",
					},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := axHandler(t)

			for _, s := range tc.steps {
				rec := auditDo(t, h, s.method, s.path, s.body)
				s.check(t, rec.Code, rec.Body.Bytes())
			}
		})
	}
}

// --- CIS Scan Configuration ---

func TestAppendixA_CisScanConfiguration(t *testing.T) {
	t.Parallel()

	type step struct {
		body   any
		check  func(t *testing.T, code int, body []byte)
		name   string
		method string
		path   string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "Create/List/Update/Delete cycle",
			steps: []step{
				{
					name:   "create",
					method: http.MethodPost,
					path:   "/cis/scan-configuration/create",
					body: map[string]any{
						"scanName": "my-cis-scan",
						"schedule": map[string]any{"daily": map[string]any{"startTime": "12:00"}},
						"targets":  map[string]any{"accountIds": []string{"123456789012"}},
						"tags":     map[string]string{"env": "prod"},
					},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						assert.NotEmpty(t, resp["scanConfigurationArn"])
					},
				},
				{
					name:   "list shows config",
					method: http.MethodPost,
					path:   "/cis/scan-configuration/list",
					body:   map[string]any{},
					check: func(t *testing.T, code int, body []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						var resp map[string]any
						require.NoError(t, json.Unmarshal(body, &resp))
						cfgs, _ := resp["scanConfigurations"].([]any)
						assert.Len(t, cfgs, 1)
					},
				},
				{
					name:   "update config name",
					method: http.MethodPost,
					path:   "/cis/scan-configuration/update",
					body: func() any {
						// We need to know the ARN; just pass an empty one to test error path in a separate test.
						// This step will be tested with a real ARN in the sequential flow below.
						return map[string]any{
							"scanConfigurationArn": "placeholder-see-subtests",
							"scanName":             "updated-name",
						}
					}(),
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						// Placeholder ARN will return 404 — that's expected in this static table test.
						assert.Equal(t, http.StatusNotFound, code)
					},
				},
			},
		},
		{
			name: "Create/Delete cycle",
			steps: []step{
				{
					name:   "create",
					method: http.MethodPost,
					path:   "/cis/scan-configuration/create",
					body:   map[string]any{"scanName": "to-delete"},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "delete unknown ARN returns 404",
					method: http.MethodPost,
					path:   "/cis/scan-configuration/delete",
					body: map[string]any{
						"scanConfigurationArn": "arn:aws:inspector2:us-east-1:123456789012:cis-scan-configuration/nonexistent",
					},
					check: func(t *testing.T, code int, _ []byte) {
						t.Helper()
						assert.Equal(t, http.StatusNotFound, code)
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := axHandler(t)

			for _, s := range tc.steps {
				rec := auditDo(t, h, s.method, s.path, s.body)
				s.check(t, rec.Code, rec.Body.Bytes())
			}
		})
	}
}

// Full CIS scan config create/update/delete with real ARN.
func TestAppendixA_CisScanConfiguration_FullCycle(t *testing.T) {
	t.Parallel()

	h := axHandler(t)

	rec := auditDo(t, h, http.MethodPost, "/cis/scan-configuration/create", map[string]any{
		"scanName": "full-cycle-cis",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	cfgARN, _ := createResp["scanConfigurationArn"].(string)
	require.NotEmpty(t, cfgARN)

	rec = auditDo(t, h, http.MethodPost, "/cis/scan-configuration/update", map[string]any{
		"scanConfigurationArn": cfgARN,
		"scanName":             "renamed-cis",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = auditDo(t, h, http.MethodPost, "/cis/scan-configuration/delete", map[string]any{
		"scanConfigurationArn": cfgARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = auditDo(t, h, http.MethodPost, "/cis/scan-configuration/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	cfgs, _ := listResp["scanConfigurations"].([]any)
	assert.Empty(t, cfgs)
}

// --- CIS Session ---

func TestAppendixA_CisSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		check  func(t *testing.T, code int, body []byte)
		name   string
		method string
		path   string
	}{
		{
			name:   "StartCisSession returns OK",
			method: http.MethodPut,
			path:   "/cissession/start",
			body:   map[string]any{"scanJobId": "job-001", "message": map[string]any{"sessionToken": "tok"}},
			check: func(t *testing.T, code int, _ []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
			},
		},
		{
			name:   "StopCisSession unknown job returns 404",
			method: http.MethodPut,
			path:   "/cissession/stop",
			body:   map[string]any{"scanJobId": "nonexistent"},
			check: func(t *testing.T, code int, _ []byte) {
				t.Helper()
				assert.Equal(t, http.StatusNotFound, code)
			},
		},
		{
			name:   "SendCisSessionHealth returns OK",
			method: http.MethodPut,
			path:   "/cissession/health/send",
			body:   map[string]any{"scanJobId": "job-002"},
			check: func(t *testing.T, code int, _ []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
			},
		},
		{
			name:   "SendCisSessionTelemetry returns OK",
			method: http.MethodPut,
			path:   "/cissession/telemetry/send",
			body:   map[string]any{"scanJobId": "job-002"},
			check: func(t *testing.T, code int, _ []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
			},
		},
		{
			name:   "GetCisScanReport returns status",
			method: http.MethodPost,
			path:   "/cis/scan/report/get",
			body:   map[string]any{"scanArn": "arn:aws:inspector2:us-east-1:123456789012:cis-scan/test"},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "SUCCEEDED", resp["status"])
			},
		},
		{
			name:   "GetCisScanResultDetails returns checkResults",
			method: http.MethodPost,
			path:   "/cis/scan-result/details/get",
			body:   map[string]any{"scanArn": "arn:aws:inspector2:us-east-1:123456789012:cis-scan/test"},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				_, ok := resp["checkResults"]
				assert.True(t, ok)
			},
		},
		{
			name:   "ListCisScans returns empty list",
			method: http.MethodPost,
			path:   "/cis/scan/list",
			body:   map[string]any{},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				scans, _ := resp["cisScans"].([]any)
				assert.Empty(t, scans)
			},
		},
		{
			name:   "ListCisScanResultsAggregatedByChecks returns empty",
			method: http.MethodPost,
			path:   "/cis/scan-result/check/list",
			body:   map[string]any{"scanArn": "arn:aws:inspector2:us-east-1:123456789012:cis-scan/test"},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				_, ok := resp["checkAggregations"]
				assert.True(t, ok)
			},
		},
		{
			name:   "ListCisScanResultsAggregatedByTargetResource returns empty",
			method: http.MethodPost,
			path:   "/cis/scan-result/resource/list",
			body:   map[string]any{"scanArn": "arn:aws:inspector2:us-east-1:123456789012:cis-scan/test"},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				_, ok := resp["targetResourceAggregations"]
				assert.True(t, ok)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := axHandler(t)
			rec := auditDo(t, h, tc.method, tc.path, tc.body)
			tc.check(t, rec.Code, rec.Body.Bytes())
		})
	}
}

// Start/Stop cycle with real session.
func TestAppendixA_CisSession_StartStop(t *testing.T) {
	t.Parallel()

	h := axHandler(t)

	rec := auditDo(t, h, http.MethodPut, "/cissession/start", map[string]any{
		"scanJobId": "job-ss-001",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = auditDo(t, h, http.MethodPut, "/cissession/stop", map[string]any{
		"scanJobId": "job-ss-001",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Code Security Integration ---

func TestAppendixA_CodeSecurityIntegration(t *testing.T) {
	t.Parallel()

	h := axHandler(t)

	// Create
	rec := auditDo(t, h, http.MethodPost, "/codesecurity/integration/create", map[string]any{
		"name": "my-integration",
		"type": "GITHUB",
		"tags": map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	integARN, _ := createResp["integrationArn"].(string)
	require.NotEmpty(t, integARN)
	assert.Equal(t, "ACTIVE", createResp["status"])

	// Get
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/integration/get", map[string]any{
		"integrationArn": integARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, integARN, getResp["integrationArn"])

	// List
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/integration/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	integrations, _ := listResp["integrations"].([]any)
	assert.Len(t, integrations, 1)

	// Update
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/integration/update", map[string]any{
		"integrationArn": integARN,
		"details":        map[string]any{"webhookSecret": "new-secret"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/integration/delete", map[string]any{
		"integrationArn": integARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get after delete returns 404
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/integration/get", map[string]any{
		"integrationArn": integARN,
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- Code Security Scan Configuration ---

func TestAppendixA_CodeSecurityScanConfiguration(t *testing.T) {
	t.Parallel()

	h := axHandler(t)

	// Create
	rec := auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/create", map[string]any{
		"name":          "my-scan-config",
		"scopeSettings": map[string]any{"projectSelectionScope": "ALL"},
		"tags":          map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	cfgARN, _ := createResp["scanConfigurationArn"].(string)
	require.NotEmpty(t, cfgARN)

	// Get
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/get", map[string]any{
		"scanConfigurationArn": cfgARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/list", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	cfgs, _ := listResp["scanConfigurations"].([]any)
	assert.Len(t, cfgs, 1)

	// Update
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/update", map[string]any{
		"scanConfigurationArn": cfgARN,
		"scopeSettings":        map[string]any{"projectSelectionScope": "SPECIFIC"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Batch associate
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/batch/associate", map[string]any{
		"scanConfigurationArn": cfgARN,
		"associateConfigurationRequests": []any{
			map[string]any{"resource": "arn:aws:codecommit:us-east-1:123456789012:my-repo"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List associations
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/associations/list", map[string]any{
		"scanConfigurationArn": cfgARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var assocResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &assocResp))
	assocs, _ := assocResp["associations"].([]any)
	assert.Len(t, assocs, 1)

	// Batch disassociate
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/batch/disassociate", map[string]any{
		"scanConfigurationArn": cfgARN,
		"disassociateConfigurationRequests": []any{
			map[string]any{"resource": "arn:aws:codecommit:us-east-1:123456789012:my-repo"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List associations now empty
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/associations/list", map[string]any{
		"scanConfigurationArn": cfgARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var assocResp2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &assocResp2))
	assocs2, _ := assocResp2["associations"].([]any)
	assert.Empty(t, assocs2)

	// Delete
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan-configuration/delete", map[string]any{
		"scanConfigurationArn": cfgARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// --- Code Security Scan ---

func TestAppendixA_CodeSecurityScan(t *testing.T) {
	t.Parallel()

	h := axHandler(t)

	// Start
	rec := auditDo(t, h, http.MethodPost, "/codesecurity/scan/start", map[string]any{
		"resourceId": "arn:aws:codecommit:us-east-1:123456789012:my-repo",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	scanID, _ := startResp["scanId"].(string)
	require.NotEmpty(t, scanID)

	// Get
	rec = auditDo(t, h, http.MethodPost, "/codesecurity/scan/get", map[string]any{
		"scanId": scanID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, scanID, getResp["scanId"])
}

// --- Findings Report ---

func TestAppendixA_FindingsReport(t *testing.T) {
	t.Parallel()

	h := axHandler(t)

	// Create
	rec := auditDo(t, h, http.MethodPost, "/reporting/create", map[string]any{
		"reportFormat":  "CSV",
		"s3Destination": map[string]any{"bucketName": "my-bucket", "keyPrefix": "reports/"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	reportID, _ := createResp["reportId"].(string)
	require.NotEmpty(t, reportID)

	// Get status
	rec = auditDo(t, h, http.MethodPost, "/reporting/status/get", map[string]any{
		"reportId": reportID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var statusResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &statusResp))
	assert.Equal(t, reportID, statusResp["reportId"])
	assert.Equal(t, "SUCCEEDED", statusResp["status"])

	// Cancel
	rec = auditDo(t, h, http.MethodPost, "/reporting/cancel", map[string]any{
		"reportId": reportID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Cancel unknown report returns 404
	rec = auditDo(t, h, http.MethodPost, "/reporting/cancel", map[string]any{
		"reportId": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- SBOM Export ---

func TestAppendixA_SbomExport(t *testing.T) {
	t.Parallel()

	h := axHandler(t)

	// Create
	rec := auditDo(t, h, http.MethodPost, "/sbomexport/create", map[string]any{
		"sbomFormat":    "CYCLONEDX_1_4",
		"s3Destination": map[string]any{"bucketName": "my-bucket", "keyPrefix": "sbom/"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	reportID, _ := createResp["reportId"].(string)
	require.NotEmpty(t, reportID)

	// Get
	rec = auditDo(t, h, http.MethodPost, "/sbomexport/get", map[string]any{
		"reportId": reportID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "SUCCEEDED", getResp["status"])

	// Cancel
	rec = auditDo(t, h, http.MethodPost, "/sbomexport/cancel", map[string]any{
		"reportId": reportID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get unknown returns 404
	rec = auditDo(t, h, http.MethodPost, "/sbomexport/get", map[string]any{
		"reportId": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- Coverage ---

func TestAppendixA_Coverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		check  func(t *testing.T, code int, body []byte)
		name   string
		method string
		path   string
	}{
		{
			name:   "ListCoverage returns empty covered resources",
			method: http.MethodPost,
			path:   "/coverage/list",
			body:   map[string]any{},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				_, ok := resp["coveredResources"]
				assert.True(t, ok)
			},
		},
		{
			name:   "ListCoverageStatistics returns totals",
			method: http.MethodPost,
			path:   "/coverage/statistics/list",
			body:   map[string]any{},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				_, ok := resp["totalCounts"]
				assert.True(t, ok)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := axHandler(t)
			rec := auditDo(t, h, tc.method, tc.path, tc.body)
			tc.check(t, rec.Code, rec.Body.Bytes())
		})
	}
}

// --- Finding Aggregations, Usage, Permissions ---

func TestAppendixA_AggregationsUsagePermissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		check  func(t *testing.T, code int, body []byte)
		name   string
		method string
		path   string
	}{
		{
			name:   "ListFindingAggregations returns responses",
			method: http.MethodPost,
			path:   "/findings/aggregation/list",
			body:   map[string]any{"aggregationType": "ACCOUNT"},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				_, ok := resp["responses"]
				assert.True(t, ok)
			},
		},
		{
			name:   "ListUsageTotals returns totals array",
			method: http.MethodPost,
			path:   "/usage/list",
			body:   map[string]any{},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				totals, _ := resp["totals"].([]any)
				assert.NotEmpty(t, totals)
			},
		},
		{
			name:   "ListAccountPermissions returns empty permissions",
			method: http.MethodPost,
			path:   "/accountpermissions/list",
			body:   map[string]any{},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				_, ok := resp["permissions"]
				assert.True(t, ok)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := axHandler(t)
			rec := auditDo(t, h, tc.method, tc.path, tc.body)
			tc.check(t, rec.Code, rec.Body.Bytes())
		})
	}
}

// --- Search Vulnerabilities ---

func TestAppendixA_SearchVulnerabilities(t *testing.T) {
	t.Parallel()

	h := axHandler(t)
	rec := auditDo(t, h, http.MethodPost, "/vulnerabilities/search", map[string]any{
		"filterCriteria": map[string]any{
			"vulnerabilityIds": []string{"CVE-2023-1234"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, ok := resp["vulnerabilities"]
	assert.True(t, ok)
}

// --- Batch ops ---

func TestAppendixA_BatchOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		check  func(t *testing.T, code int, body []byte)
		name   string
		method string
		path   string
	}{
		{
			name:   "BatchGetCodeSnippet returns results and errors",
			method: http.MethodPost,
			path:   "/codesnippet/batchget",
			body:   map[string]any{"findingArns": []string{"arn:aws:inspector2:us-east-1:123456789012:finding/abc"}},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				_, ok := resp["codeSnippetResults"]
				assert.True(t, ok)
				_, ok = resp["errors"]
				assert.True(t, ok)
			},
		},
		{
			name:   "BatchGetFindingDetails returns results and errors",
			method: http.MethodPost,
			path:   "/findings/details/batch/get",
			body: map[string]any{
				"findingArns": []any{
					map[string]any{"findingArn": "arn:aws:inspector2:us-east-1:123456789012:finding/abc"},
				},
			},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				_, ok := resp["findingDetails"]
				assert.True(t, ok)
			},
		},
		{
			name:   "BatchGetFreeTrialInfo returns accounts",
			method: http.MethodPost,
			path:   "/freetrialinfo/batchget",
			body:   map[string]any{"accountIds": []string{"123456789012"}},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				accounts, _ := resp["accounts"].([]any)
				assert.Len(t, accounts, 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := axHandler(t)
			rec := auditDo(t, h, tc.method, tc.path, tc.body)
			tc.check(t, rec.Code, rec.Body.Bytes())
		})
	}
}

// --- GetClustersForImage ---

func TestAppendixA_GetClustersForImage(t *testing.T) {
	t.Parallel()

	h := axHandler(t)
	rec := auditDo(t, h, http.MethodPost, "/cluster/get", map[string]any{
		"filterCriteria": map[string]any{},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, ok := resp["clusters"]
	assert.True(t, ok)
}
