package guardduty_test

// Appendix A: 57 new ops — member mgmt, invitations, org config,
// publishing destinations, malware, malware protection plans,
// threat entity sets, trusted entity sets, coverage, usage.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

// --- members ---

func TestAppendixA_Members(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(t *testing.T, h *guardduty.Handler)
	}{
		{
			name: "create_list_delete",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := auditCreateDetector(t, h)

				// CreateMembers
				rec := auditDo(t, h, http.MethodPost, "/detector/"+id+"/member", map[string]any{
					"accountDetails": []map[string]any{
						{"accountId": "111111111111", "email": "a@example.com"},
						{"accountId": "222222222222", "email": "b@example.com"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				assert.Empty(t, createResp["unprocessedAccounts"])

				// ListMembers
				rec = auditDo(t, h, http.MethodGet, "/detector/"+id+"/member", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				members, _ := listResp["members"].([]any)
				assert.Len(t, members, 2)

				// GetMembers
				rec = auditDo(t, h, http.MethodPost, "/detector/"+id+"/member/get", map[string]any{
					"accountIds": []string{"111111111111"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var getResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				got, _ := getResp["members"].([]any)
				assert.Len(t, got, 1)

				// DeleteMembers
				rec = auditDo(t, h, http.MethodPost, "/detector/"+id+"/member/delete", map[string]any{
					"accountIds": []string{"111111111111", "222222222222"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// ListMembers after delete
				rec = auditDo(t, h, http.MethodGet, "/detector/"+id+"/member", nil)
				var listAfter map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listAfter))
				membersAfter, _ := listAfter["members"].([]any)
				assert.Empty(t, membersAfter)
			},
		},
		{
			name: "invite_and_monitoring",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := auditCreateDetector(t, h)

				auditDo(t, h, http.MethodPost, "/detector/"+id+"/member", map[string]any{
					"accountDetails": []map[string]any{
						{"accountId": "333333333333", "email": "c@example.com"},
					},
				})

				// InviteMembers
				rec := auditDo(t, h, http.MethodPost, "/detector/"+id+"/member/invite", map[string]any{
					"accountIds": []string{"333333333333"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// StartMonitoringMembers
				rec = auditDo(t, h, http.MethodPost, "/detector/"+id+"/member/start", map[string]any{
					"accountIds": []string{"333333333333"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// StopMonitoringMembers
				rec = auditDo(t, h, http.MethodPost, "/detector/"+id+"/member/stop", map[string]any{
					"accountIds": []string{"333333333333"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "disassociate_members",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := auditCreateDetector(t, h)

				auditDo(t, h, http.MethodPost, "/detector/"+id+"/member", map[string]any{
					"accountDetails": []map[string]any{
						{"accountId": "444444444444", "email": "d@example.com"},
					},
				})

				rec := auditDo(t, h, http.MethodPost, "/detector/"+id+"/member/disassociate", map[string]any{
					"accountIds": []string{"444444444444"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "member_detectors",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := auditCreateDetector(t, h)

				auditDo(t, h, http.MethodPost, "/detector/"+id+"/member", map[string]any{
					"accountDetails": []map[string]any{
						{"accountId": "555555555555", "email": "e@example.com"},
					},
				})

				// GetMemberDetectors
				rec := auditDo(t, h, http.MethodPost, "/detector/"+id+"/member/detector/get", map[string]any{
					"accountIds": []string{"555555555555"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// UpdateMemberDetectors
				rec = auditDo(t, h, http.MethodPost, "/detector/"+id+"/member/detector/update", map[string]any{
					"accountIds": []string{"555555555555"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)
			tt.fn(t, h)
		})
	}
}

// --- invitations ---

func TestAppendixA_Invitations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(t *testing.T, h *guardduty.Handler)
	}{
		{
			name: "count_and_list",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				// GetInvitationsCount
				rec := auditDo(t, h, http.MethodGet, "/invitation/count", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var countResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &countResp))
				assert.NotNil(t, countResp["invitationsCount"])

				// ListInvitations
				rec = auditDo(t, h, http.MethodGet, "/invitation", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				assert.NotNil(t, listResp["invitations"])
			},
		},
		{
			name: "decline_and_delete",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				// DeclineInvitations
				rec := auditDo(t, h, http.MethodPost, "/invitation/decline", map[string]any{
					"accountIds": []string{"999999999999"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// DeleteInvitations
				rec = auditDo(t, h, http.MethodPost, "/invitation/delete", map[string]any{
					"accountIds": []string{"999999999999"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "administrator_account",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := auditCreateDetector(t, h)

				// AcceptAdministratorInvitation
				rec := auditDo(t, h, http.MethodPost, "/detector/"+id+"/administrator", map[string]any{
					"administratorId": "777777777777",
					"invitationId":    "inv-001",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// GetAdministratorAccount
				rec = auditDo(t, h, http.MethodGet, "/detector/"+id+"/administrator", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var adminResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &adminResp))
				assert.NotNil(t, adminResp["administrator"])

				// DisassociateFromAdministratorAccount
				rec = auditDo(t, h, http.MethodPost, "/detector/"+id+"/administrator/disassociate", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "master_account_legacy",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := auditCreateDetector(t, h)

				// AcceptInvitation (legacy master)
				rec := auditDo(t, h, http.MethodPost, "/detector/"+id+"/master", map[string]any{
					"masterId":     "888888888888",
					"invitationId": "inv-002",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// GetMasterAccount
				rec = auditDo(t, h, http.MethodGet, "/detector/"+id+"/master", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var masterResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &masterResp))
				assert.NotNil(t, masterResp["master"])

				// DisassociateFromMasterAccount
				rec = auditDo(t, h, http.MethodPost, "/detector/"+id+"/master/disassociate", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)
			tt.fn(t, h)
		})
	}
}

// --- organization ---

func TestAppendixA_Organization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(t *testing.T, h *guardduty.Handler)
	}{
		{
			name: "admin_account_lifecycle",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				// EnableOrganizationAdminAccount
				rec := auditDo(t, h, http.MethodPost, "/admin/enable", map[string]any{
					"adminAccountId": "100000000001",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// ListOrganizationAdminAccounts
				rec = auditDo(t, h, http.MethodGet, "/admin", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				accounts, _ := listResp["adminAccounts"].([]any)
				assert.Len(t, accounts, 1)

				// DisableOrganizationAdminAccount
				rec = auditDo(t, h, http.MethodPost, "/admin/disable", map[string]any{
					"adminAccountId": "100000000001",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// List after disable
				rec = auditDo(t, h, http.MethodGet, "/admin", nil)
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				accounts2, _ := listResp["adminAccounts"].([]any)
				assert.Empty(t, accounts2)
			},
		},
		{
			name: "org_config",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := auditCreateDetector(t, h)

				// DescribeOrganizationConfiguration
				rec := auditDo(t, h, http.MethodGet, "/detector/"+id+"/admin", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var descResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
				assert.NotNil(t, descResp["autoEnable"])

				// UpdateOrganizationConfiguration
				rec = auditDo(t, h, http.MethodPost, "/detector/"+id+"/admin", map[string]any{
					"autoEnable": true,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify update
				rec = auditDo(t, h, http.MethodGet, "/detector/"+id+"/admin", nil)
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
				assert.Equal(t, true, descResp["autoEnable"])
			},
		},
		{
			name: "org_statistics",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodGet, "/organization/statistics", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)
			tt.fn(t, h)
		})
	}
}

// --- publishing destinations ---

func TestAppendixA_PublishingDestinations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(t *testing.T, h *guardduty.Handler)
	}{
		{
			name: "create_describe_update_list_delete",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := auditCreateDetector(t, h)

				// CreatePublishingDestination
				rec := auditDo(t, h, http.MethodPost, "/detector/"+id+"/publishingDestination", map[string]any{
					"destinationType": "S3",
					"destinationProperties": map[string]any{
						"destinationArn": "arn:aws:s3:::my-bucket",
						"kmsKeyArn":      "arn:aws:kms:us-east-1:123456789012:key/abc",
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				destID, _ := createResp["destinationId"].(string)
				require.NotEmpty(t, destID)

				// DescribePublishingDestination
				rec = auditDo(t, h, http.MethodGet, "/detector/"+id+"/publishingDestination/"+destID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var descResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
				assert.Equal(t, "S3", descResp["destinationType"])

				// UpdatePublishingDestination
				rec = auditDo(t, h, http.MethodPost, "/detector/"+id+"/publishingDestination/"+destID, map[string]any{
					"destinationProperties": map[string]any{
						"destinationArn": "arn:aws:s3:::my-bucket-v2",
						"kmsKeyArn":      "arn:aws:kms:us-east-1:123456789012:key/def",
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// ListPublishingDestinations
				rec = auditDo(t, h, http.MethodGet, "/detector/"+id+"/publishingDestination", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				dests, _ := listResp["destinations"].([]any)
				assert.Len(t, dests, 1)

				// DeletePublishingDestination
				rec = auditDo(t, h, http.MethodDelete, "/detector/"+id+"/publishingDestination/"+destID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// List after delete
				rec = auditDo(t, h, http.MethodGet, "/detector/"+id+"/publishingDestination", nil)
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				destsAfter, _ := listResp["destinations"].([]any)
				assert.Empty(t, destsAfter)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)
			tt.fn(t, h)
		})
	}
}

// --- malware scanning ---

func TestAppendixA_MalwareScanning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(t *testing.T, h *guardduty.Handler)
	}{
		{
			name: "start_get_list",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				// StartMalwareScan
				rec := auditDo(t, h, http.MethodPost, "/malware-scan/start", map[string]any{
					"resourceArn": "arn:aws:ec2:us-east-1:123456789012:instance/i-1234",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var startResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
				scanID, _ := startResp["scanId"].(string)
				require.NotEmpty(t, scanID)

				// GetMalwareScan
				rec = auditDo(t, h, http.MethodGet, "/malware-scan/"+scanID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var getResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				assert.Equal(t, scanID, getResp["scanId"])

				// ListMalwareScans
				rec = auditDo(t, h, http.MethodPost, "/malware-scan", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				scans, _ := listResp["scans"].([]any)
				assert.Len(t, scans, 1)
			},
		},
		{
			name: "describe_malware_scans",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := auditCreateDetector(t, h)

				rec := auditDo(t, h, http.MethodPost, "/detector/"+id+"/malware-scans", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotNil(t, resp["scans"])
			},
		},
		{
			name: "scan_settings_crud",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := auditCreateDetector(t, h)

				// GetMalwareScanSettings
				rec := auditDo(t, h, http.MethodGet, "/detector/"+id+"/malware-scan-settings", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// UpdateMalwareScanSettings
				rec = auditDo(t, h, http.MethodPost, "/detector/"+id+"/malware-scan-settings", map[string]any{
					"ebsSnapshotPreservation": "RETENTION_WITH_FINDING",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify update
				rec = auditDo(t, h, http.MethodGet, "/detector/"+id+"/malware-scan-settings", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var settingsResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &settingsResp))
				assert.Equal(t, "RETENTION_WITH_FINDING", settingsResp["ebsSnapshotPreservation"])
			},
		},
		{
			name: "usage_and_free_trial",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := auditCreateDetector(t, h)

				// GetUsageStatistics
				rec := auditDo(t, h, http.MethodPost, "/detector/"+id+"/usage/statistics", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// GetRemainingFreeTrialDays
				rec = auditDo(t, h, http.MethodPost, "/detector/"+id+"/freeTrial/daysRemaining", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "send_object_malware_scan",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodPost, "/object-malware-scan/send", map[string]any{
					"s3ObjectDetails": map[string]any{
						"bucketArn": "arn:aws:s3:::my-bucket",
						"key":       "path/to/object",
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["scanId"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)
			tt.fn(t, h)
		})
	}
}

// --- coverage ---

func TestAppendixA_Coverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(t *testing.T, h *guardduty.Handler)
	}{
		{
			name: "list_and_statistics",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := auditCreateDetector(t, h)

				// ListCoverage
				rec := auditDo(t, h, http.MethodPost, "/detector/"+id+"/coverage", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				assert.NotNil(t, listResp["resources"])

				// GetCoverageStatistics
				rec = auditDo(t, h, http.MethodPost, "/detector/"+id+"/coverage/statistics", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)
			tt.fn(t, h)
		})
	}
}

// --- malware protection plans ---

func TestAppendixA_MalwareProtectionPlans(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(t *testing.T, h *guardduty.Handler)
	}{
		{
			name: "create_get_update_list_delete",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				// CreateMalwareProtectionPlan
				rec := auditDo(t, h, http.MethodPost, "/malware-protection-plan", map[string]any{
					"role": "arn:aws:iam::123456789012:role/GuardDutyS3Role",
					"protectedResource": map[string]any{
						"s3Bucket": map[string]any{
							"bucketName": "my-bucket",
						},
					},
					"actions": map[string]any{
						"tagging": map[string]any{
							"status": "ENABLED",
						},
					},
					"tags": map[string]string{"env": "test"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				planID, _ := createResp["malwareProtectionPlanId"].(string)
				require.NotEmpty(t, planID)

				// GetMalwareProtectionPlan
				rec = auditDo(t, h, http.MethodGet, "/malware-protection-plan/"+planID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var getResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				assert.Equal(t, planID, getResp["malwareProtectionPlanId"])

				// UpdateMalwareProtectionPlan
				rec = auditDo(t, h, http.MethodPost, "/malware-protection-plan/"+planID, map[string]any{
					"role": "arn:aws:iam::123456789012:role/GuardDutyS3RoleV2",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// ListMalwareProtectionPlans
				rec = auditDo(t, h, http.MethodGet, "/malware-protection-plan", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				plans, _ := listResp["malwareProtectionPlans"].([]any)
				assert.Len(t, plans, 1)

				// DeleteMalwareProtectionPlan
				rec = auditDo(t, h, http.MethodDelete, "/malware-protection-plan/"+planID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// List after delete
				rec = auditDo(t, h, http.MethodGet, "/malware-protection-plan", nil)
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				plansAfter, _ := listResp["malwareProtectionPlans"].([]any)
				assert.Empty(t, plansAfter)
			},
		},
		{
			name: "get_not_found",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				rec := auditDo(t, h, http.MethodGet, "/malware-protection-plan/no-such-plan", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)
			tt.fn(t, h)
		})
	}
}

// --- threat entity sets ---

func TestAppendixA_ThreatEntitySets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(t *testing.T, h *guardduty.Handler)
	}{
		{
			name: "create_get_update_list_delete",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := auditCreateDetector(t, h)

				// CreateThreatEntitySet
				rec := auditDo(t, h, http.MethodPost, "/detector/"+id+"/threatentityset", map[string]any{
					"name":     "my-threat-set",
					"format":   "TXT",
					"location": "s3://my-bucket/threat.txt",
					"activate": true,
					"tags":     map[string]string{"team": "security"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				setID, _ := createResp["threatEntitySetId"].(string)
				require.NotEmpty(t, setID)

				// GetThreatEntitySet
				rec = auditDo(t, h, http.MethodGet, "/detector/"+id+"/threatentityset/"+setID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var getResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				assert.Equal(t, "my-threat-set", getResp["name"])
				assert.Equal(t, "ACTIVE", getResp["status"])

				// UpdateThreatEntitySet
				activate := false
				_ = activate
				rec = auditDo(t, h, http.MethodPost, "/detector/"+id+"/threatentityset/"+setID, map[string]any{
					"name":     "updated-threat-set",
					"location": "s3://my-bucket/threat-v2.txt",
					"activate": false,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify update
				rec = auditDo(t, h, http.MethodGet, "/detector/"+id+"/threatentityset/"+setID, nil)
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				assert.Equal(t, "updated-threat-set", getResp["name"])
				assert.Equal(t, "INACTIVE", getResp["status"])

				// ListThreatEntitySets
				rec = auditDo(t, h, http.MethodGet, "/detector/"+id+"/threatentityset", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				ids, _ := listResp["threatEntitySetIds"].([]any)
				assert.Len(t, ids, 1)
				assert.Equal(t, setID, ids[0])

				// DeleteThreatEntitySet
				rec = auditDo(t, h, http.MethodDelete, "/detector/"+id+"/threatentityset/"+setID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// List after delete
				rec = auditDo(t, h, http.MethodGet, "/detector/"+id+"/threatentityset", nil)
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				idsAfter, _ := listResp["threatEntitySetIds"].([]any)
				assert.Empty(t, idsAfter)
			},
		},
		{
			name: "get_not_found",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := auditCreateDetector(t, h)

				rec := auditDo(t, h, http.MethodGet, "/detector/"+id+"/threatentityset/no-such-set", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)
			tt.fn(t, h)
		})
	}
}

// --- trusted entity sets ---

func TestAppendixA_TrustedEntitySets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(t *testing.T, h *guardduty.Handler)
	}{
		{
			name: "create_get_update_list_delete",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := auditCreateDetector(t, h)

				// CreateTrustedEntitySet
				rec := auditDo(t, h, http.MethodPost, "/detector/"+id+"/trustedentityset", map[string]any{
					"name":     "my-trusted-set",
					"format":   "TXT",
					"location": "s3://my-bucket/trusted.txt",
					"activate": true,
					"tags":     map[string]string{"team": "security"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				setID, _ := createResp["trustedEntitySetId"].(string)
				require.NotEmpty(t, setID)

				// GetTrustedEntitySet
				rec = auditDo(t, h, http.MethodGet, "/detector/"+id+"/trustedentityset/"+setID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var getResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				assert.Equal(t, "my-trusted-set", getResp["name"])
				assert.Equal(t, "ACTIVE", getResp["status"])

				// UpdateTrustedEntitySet
				rec = auditDo(t, h, http.MethodPost, "/detector/"+id+"/trustedentityset/"+setID, map[string]any{
					"name":     "updated-trusted-set",
					"location": "s3://my-bucket/trusted-v2.txt",
					"activate": false,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify update
				rec = auditDo(t, h, http.MethodGet, "/detector/"+id+"/trustedentityset/"+setID, nil)
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				assert.Equal(t, "updated-trusted-set", getResp["name"])
				assert.Equal(t, "INACTIVE", getResp["status"])

				// ListTrustedEntitySets
				rec = auditDo(t, h, http.MethodGet, "/detector/"+id+"/trustedentityset", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				ids, _ := listResp["trustedEntitySetIds"].([]any)
				assert.Len(t, ids, 1)
				assert.Equal(t, setID, ids[0])

				// DeleteTrustedEntitySet
				rec = auditDo(t, h, http.MethodDelete, "/detector/"+id+"/trustedentityset/"+setID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// List after delete
				rec = auditDo(t, h, http.MethodGet, "/detector/"+id+"/trustedentityset", nil)
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				idsAfter, _ := listResp["trustedEntitySetIds"].([]any)
				assert.Empty(t, idsAfter)
			},
		},
		{
			name: "get_not_found",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := auditCreateDetector(t, h)

				rec := auditDo(t, h, http.MethodGet, "/detector/"+id+"/trustedentityset/no-such-set", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)
			tt.fn(t, h)
		})
	}
}
