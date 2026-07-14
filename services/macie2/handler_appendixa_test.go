package macie2_test

// Appendix A: 55 new ops — classification jobs, members, invitations, admin/master,
// org config, automated discovery, buckets, batch CDI, classification export/scopes,
// findings publication, resource profiles, reveal config, sensitive data,
// sensitivity inspection templates, usage, managed data identifiers, search.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

// --- classification jobs ---

func TestAppendixA_ClassificationJobs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "create_describe_list_update",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// CreateClassificationJob
				rec := doRequest(t, h, http.MethodPost, "/jobs", map[string]any{
					"name":    "test-job",
					"jobType": "ONE_TIME",
					"s3JobDefinition": map[string]any{
						"bucketDefinitions": []any{},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var createResp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				jobID := createResp["jobId"]
				assert.NotEmpty(t, jobID)
				assert.Equal(t, "RUNNING", createResp["jobStatus"])
				// Real CreateClassificationJobOutput always includes jobArn.
				assert.Contains(t, createResp["jobArn"], "arn:aws:macie2:")

				// DescribeClassificationJob
				rec = doRequest(t, h, http.MethodGet, "/jobs/"+jobID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var descResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
				assert.Equal(t, jobID, descResp["jobId"])
				assert.Equal(t, "ONE_TIME", descResp["jobType"])
				assert.Equal(t, "test-job", descResp["name"])
				assert.Equal(t, createResp["jobArn"], descResp["jobArn"])

				// ListClassificationJobs
				rec = doRequest(t, h, http.MethodPost, "/jobs/list", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				items, _ := listResp["items"].([]any)
				assert.Len(t, items, 1)

				// UpdateClassificationJob
				rec = doRequest(t, h, http.MethodPatch, "/jobs/"+jobID, map[string]any{
					"jobStatus": "CANCELLED",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify update
				rec = doRequest(t, h, http.MethodGet, "/jobs/"+jobID, nil)
				var updated map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
				assert.Equal(t, "CANCELLED", updated["jobStatus"])
			},
		},
		{
			name: "describe_missing_returns_404",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodGet, "/jobs/nonexistent", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "scheduled_job_starts_idle",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodPost, "/jobs", map[string]any{
					"name":    "sched-job",
					"jobType": "SCHEDULED",
					"scheduleFrequency": map[string]any{
						"dailySchedule": map[string]any{},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				rec = doRequest(t, h, http.MethodGet, "/jobs/"+resp["jobId"], nil)
				var desc map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc))
				assert.Equal(t, "IDLE", desc["jobStatus"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}

// --- members ---

func TestAppendixA_Members(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "create_get_list_delete",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// CreateMember
				rec := doRequest(t, h, http.MethodPost, "/members", map[string]any{
					"account": map[string]string{
						"accountId": "111111111111",
						"email":     "member@example.com",
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// GetMember
				rec = doRequest(t, h, http.MethodGet, "/members/111111111111", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var getMem map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getMem))
				assert.Equal(t, "111111111111", getMem["accountId"])
				assert.Equal(t, "member@example.com", getMem["email"])
				assert.Equal(t, "CREATED", getMem["relationshipStatus"])
				// Real GetMemberOutput always includes arn and masterAccountId
				// (the deprecated wire name for administratorAccountId).
				assert.Contains(t, getMem["arn"], "arn:aws:macie2:")
				assert.NotEmpty(t, getMem["masterAccountId"])

				// ListMembers
				rec = doRequest(t, h, http.MethodGet, "/members", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				members, _ := listResp["members"].([]any)
				assert.Len(t, members, 1)

				// DeleteMember
				rec = doRequest(t, h, http.MethodDelete, "/members/111111111111", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// List after delete
				rec = doRequest(t, h, http.MethodGet, "/members", nil)
				var afterList map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &afterList))
				membersAfter, _ := afterList["members"].([]any)
				assert.Empty(t, membersAfter)
			},
		},
		{
			name: "disassociate_member",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				doRequest(t, h, http.MethodPost, "/members", map[string]any{
					"account": map[string]string{
						"accountId": "222222222222",
						"email":     "other@example.com",
					},
				})

				// DisassociateMember
				rec := doRequest(t, h, http.MethodPost, "/members/disassociate/222222222222", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify status changed
				rec = doRequest(t, h, http.MethodGet, "/members/222222222222", nil)
				var mem map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &mem))
				assert.Equal(t, "DISASSOCIATED", mem["relationshipStatus"])
			},
		},
		{
			name: "update_member_session",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				doRequest(t, h, http.MethodPost, "/members", map[string]any{
					"account": map[string]string{
						"accountId": "333333333333",
						"email":     "session@example.com",
					},
				})

				// UpdateMemberSession via /macie/members/{id}
				rec := doRequest(t, h, http.MethodPatch, "/macie/members/333333333333", map[string]any{
					"status": "PAUSED",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, http.MethodGet, "/members/333333333333", nil)
				var mem map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &mem))
				assert.Equal(t, "PAUSED", mem["relationshipStatus"])
			},
		},
		{
			name: "duplicate_member_returns_conflict",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				doRequest(t, h, http.MethodPost, "/members", map[string]any{
					"account": map[string]string{"accountId": "444444444444", "email": "dup@example.com"},
				})

				rec := doRequest(t, h, http.MethodPost, "/members", map[string]any{
					"account": map[string]string{"accountId": "444444444444", "email": "dup@example.com"},
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "get_missing_returns_404",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodGet, "/members/999999999999", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}

// --- invitations ---

func TestAppendixA_Invitations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "create_list_count",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// CreateInvitations
				rec := doRequest(t, h, http.MethodPost, "/invitations", map[string]any{
					"accountIds": []string{"111111111111", "222222222222"},
					"message":    "Join Macie",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// GetInvitationsCount
				rec = doRequest(t, h, http.MethodGet, "/invitations/count", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var countResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &countResp))
				count, _ := countResp["invitationsCount"].(float64)
				assert.InDelta(t, 2, count, 0.0001)

				// ListInvitations
				rec = doRequest(t, h, http.MethodGet, "/invitations", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				invitations, _ := listResp["invitations"].([]any)
				assert.Len(t, invitations, 2)
			},
		},
		{
			name: "accept_invitation",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodPost, "/invitations/accept", map[string]any{
					"administratorAccountId": "123456789012",
					"invitationId":           "inv-abc",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify administrator is set
				rec = doRequest(t, h, http.MethodGet, "/administrator", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var adminResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &adminResp))
				admin, _ := adminResp["administrator"].(map[string]any)
				assert.Equal(t, "123456789012", admin["accountId"])
			},
		},
		{
			name: "decline_and_delete_invitations",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				doRequest(t, h, http.MethodPost, "/invitations", map[string]any{
					"accountIds": []string{"111111111111", "222222222222"},
				})

				// DeclineInvitations
				rec := doRequest(t, h, http.MethodPost, "/invitations/decline", map[string]any{
					"accountIds": []string{"111111111111"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var declineResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &declineResp))
				assert.Empty(t, declineResp["unprocessedAccounts"])

				// DeleteInvitations
				rec = doRequest(t, h, http.MethodPost, "/invitations/delete", map[string]any{
					"accountIds": []string{"222222222222"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}

// --- administrator / master ---

func TestAppendixA_AdministratorMaster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "get_administrator_nil_when_none",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodGet, "/administrator", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Nil(t, resp["administrator"])
			},
		},
		{
			name: "accept_then_disassociate_administrator",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				doRequest(t, h, http.MethodPost, "/invitations/accept", map[string]any{
					"administratorAccountId": "admin-account",
					"invitationId":           "inv-1",
				})

				// GetAdministratorAccount
				rec := doRequest(t, h, http.MethodGet, "/administrator", nil)
				var adminResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &adminResp))
				assert.NotNil(t, adminResp["administrator"])

				// DisassociateFromAdministratorAccount
				rec = doRequest(t, h, http.MethodPost, "/administrator/disassociate", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify cleared
				rec = doRequest(t, h, http.MethodGet, "/administrator", nil)
				var afterResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &afterResp))
				assert.Nil(t, afterResp["administrator"])
			},
		},
		{
			name: "get_master_account_legacy",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				doRequest(t, h, http.MethodPost, "/invitations/accept", map[string]any{
					"administratorAccountId": "legacy-admin",
					"invitationId":           "inv-2",
				})

				rec := doRequest(t, h, http.MethodGet, "/master", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				master, _ := resp["master"].(map[string]any)
				assert.Equal(t, "legacy-admin", master["accountId"])
			},
		},
		{
			name: "disassociate_from_master_legacy",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				doRequest(t, h, http.MethodPost, "/invitations/accept", map[string]any{
					"administratorAccountId": "legacy-admin",
					"invitationId":           "inv-3",
				})

				rec := doRequest(t, h, http.MethodPost, "/master/disassociate", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, http.MethodGet, "/master", nil)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Nil(t, resp["master"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}

// --- org admin accounts and configuration ---

func TestAppendixA_OrgAdmin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "enable_list_disable",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// EnableOrganizationAdminAccount
				rec := doRequest(t, h, http.MethodPost, "/admin", map[string]any{
					"adminAccountId": "111111111111",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// ListOrganizationAdminAccounts
				rec = doRequest(t, h, http.MethodGet, "/admin", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				accounts, _ := listResp["adminAccounts"].([]any)
				assert.Len(t, accounts, 1)

				// DisableOrganizationAdminAccount
				rec = doRequest(t, h, http.MethodDelete, "/admin?adminAccountId=111111111111", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify removed
				rec = doRequest(t, h, http.MethodGet, "/admin", nil)
				var afterList map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &afterList))
				accountsAfter, _ := afterList["adminAccounts"].([]any)
				assert.Empty(t, accountsAfter)
			},
		},
		{
			name: "describe_and_update_org_configuration",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// DescribeOrganizationConfiguration
				rec := doRequest(t, h, http.MethodGet, "/admin/configuration", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var descResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
				autoEnable, _ := descResp["autoEnable"].(bool)
				assert.False(t, autoEnable)

				// UpdateOrganizationConfiguration
				rec = doRequest(t, h, http.MethodPatch, "/admin/configuration", map[string]any{
					"autoEnable": true,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify change
				rec = doRequest(t, h, http.MethodGet, "/admin/configuration", nil)
				var updated map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
				assert.True(t, updated["autoEnable"].(bool))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}

// --- automated discovery ---

func TestAppendixA_AutomatedDiscovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "get_update_configuration",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// GetAutomatedDiscoveryConfiguration
				rec := doRequest(t, h, http.MethodGet, "/automated-discovery/configuration", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var cfg map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cfg))
				assert.Equal(t, "DISABLED", cfg["status"])

				// UpdateAutomatedDiscoveryConfiguration
				rec = doRequest(t, h, http.MethodPut, "/automated-discovery/configuration", map[string]any{
					"status":                        "ENABLED",
					"autoEnableOrganizationMembers": "NEW",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify
				rec = doRequest(t, h, http.MethodGet, "/automated-discovery/configuration", nil)
				var updated map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
				assert.Equal(t, "ENABLED", updated["status"])
				assert.Equal(t, "NEW", updated["autoEnableOrganizationMembers"])
			},
		},
		{
			name: "list_and_batch_update_accounts",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// BatchUpdateAutomatedDiscoveryAccounts
				rec := doRequest(t, h, http.MethodPatch, "/automated-discovery/accounts", map[string]any{
					"accounts": []map[string]any{
						{"accountId": "111111111111", "status": "ENABLED"},
						{"accountId": "222222222222", "status": "DISABLED"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// ListAutomatedDiscoveryAccounts
				rec = doRequest(t, h, http.MethodGet, "/automated-discovery/accounts", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				items, _ := listResp["items"].([]any)
				assert.Len(t, items, 2)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}

// --- buckets and batch CDI ---

func TestAppendixA_BucketsAndBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "describe_buckets_returns_empty",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodPost, "/datasources/s3", map[string]any{
					"criteria": map[string]any{},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				buckets, _ := resp["buckets"].([]any)
				assert.Empty(t, buckets)
			},
		},
		{
			name: "get_bucket_statistics_returns_zeros",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodPost, "/datasources/s3/statistics", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				count, _ := resp["bucketCount"].(float64)
				assert.InDelta(t, 0, count, 0.0001)
			},
		},
		{
			name: "batch_get_custom_data_identifiers",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// Create a CDI first
				createRec := doRequest(t, h, http.MethodPost, "/custom-data-identifiers", map[string]any{
					"name":  "test-cdi",
					"regex": `\d{4}-\d{4}`,
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var createResp map[string]string
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				cdiID := createResp["customDataIdentifierId"]

				// BatchGetCustomDataIdentifiers
				rec := doRequest(t, h, http.MethodPost, "/custom-data-identifiers/get", map[string]any{
					"ids": []string{cdiID},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var batchResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &batchResp))
				items, _ := batchResp["customDataIdentifiers"].([]any)
				assert.Len(t, items, 1)
			},
		},
		{
			name: "search_resources_returns_empty",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodPost, "/datasources/search-resources", map[string]any{
					"bucketCriteria": map[string]any{},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				results, _ := resp["matchingResources"].([]any)
				assert.Empty(t, results)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}

// --- classification export config and scopes ---

func TestAppendixA_ClassificationConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "get_put_classification_export_config",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// GetClassificationExportConfiguration — empty by default
				rec := doRequest(t, h, http.MethodGet, "/classification-export-configuration", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var getResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				cfg, _ := getResp["configuration"].(map[string]any)
				assert.Nil(t, cfg["s3Destination"])

				// PutClassificationExportConfiguration
				rec = doRequest(t, h, http.MethodPut, "/classification-export-configuration", map[string]any{
					"configuration": map[string]any{
						"s3Destination": map[string]any{
							"bucketName": "my-export-bucket",
							"keyPrefix":  "macie/",
							"kmsKeyArn":  "arn:aws:kms:us-east-1:000000000000:key/abc",
						},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify
				rec = doRequest(t, h, http.MethodGet, "/classification-export-configuration", nil)
				var updated map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
				updCfg, _ := updated["configuration"].(map[string]any)
				s3Dest, _ := updCfg["s3Destination"].(map[string]any)
				assert.Equal(t, "my-export-bucket", s3Dest["bucketName"])
			},
		},
		{
			name: "list_and_get_and_update_classification_scope",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// ListClassificationScopes — auto-creates default
				rec := doRequest(t, h, http.MethodGet, "/classification-scopes", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				scopes, _ := listResp["classificationScopes"].([]any)
				require.Len(t, scopes, 1)

				scope0, _ := scopes[0].(map[string]any)
				scopeID, _ := scope0["id"].(string)
				require.NotEmpty(t, scopeID)

				// GetClassificationScope
				rec = doRequest(t, h, http.MethodGet, "/classification-scopes/"+scopeID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var scopeResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &scopeResp))
				assert.Equal(t, scopeID, scopeResp["id"])

				// UpdateClassificationScope
				rec = doRequest(t, h, http.MethodPatch, "/classification-scopes/"+scopeID, map[string]any{
					"s3": map[string]any{
						"excludes": map[string]any{
							"and": []any{},
						},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "get_missing_scope_returns_404",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodGet, "/classification-scopes/nonexistent", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}

// --- findings publication config ---

func TestAppendixA_FindingsPublicationConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "get_put_publication_config",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// GetFindingsPublicationConfiguration — defaults to zero value
				rec := doRequest(t, h, http.MethodGet, "/findings-publication-configuration", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// PutFindingsPublicationConfiguration
				rec = doRequest(t, h, http.MethodPut, "/findings-publication-configuration", map[string]any{
					"publishClassificationFindings": true,
					"publishPolicyFindings":         true,
					"securityHubConfiguration": map[string]any{
						"publishClassificationFindings": true,
						"publishPolicyFindings":         false,
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify
				rec = doRequest(t, h, http.MethodGet, "/findings-publication-configuration", nil)
				var updated map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
				assert.True(t, updated["publishClassificationFindings"].(bool))
				assert.True(t, updated["publishPolicyFindings"].(bool))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}

// --- resource profiles ---

func TestAppendixA_ResourceProfiles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "get_and_update_resource_profile",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				arn := "arn:aws:s3:::my-bucket"

				// GetResourceProfile — returns default for unknown ARN
				rec := doRequest(t, h, http.MethodGet, "/resource-profiles?resourceArn="+arn, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var profile map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &profile))
				assert.Equal(t, arn, profile["resourceArn"])

				// UpdateResourceProfile
				rec = doRequest(t, h, http.MethodPatch, "/resource-profiles?resourceArn="+arn, map[string]any{
					"sensitivityScoreOverride": 75,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify
				rec = doRequest(t, h, http.MethodGet, "/resource-profiles?resourceArn="+arn, nil)
				var updated map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
				assert.EqualValues(t, 75, updated["sensitivityScore"])
				assert.True(t, updated["sensitivityScoreOverride"].(bool))
			},
		},
		{
			name: "list_profile_artifacts_and_detections",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				arn := "arn:aws:s3:::another-bucket"

				// ListResourceProfileArtifacts
				rec := doRequest(t, h, http.MethodGet, "/resource-profiles/artifacts?resourceArn="+arn, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var artResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &artResp))
				artifacts, _ := artResp["artifacts"].([]any)
				assert.Empty(t, artifacts)

				// ListResourceProfileDetections
				rec = doRequest(t, h, http.MethodGet, "/resource-profiles/detections?resourceArn="+arn, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var detResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &detResp))
				detections, _ := detResp["detections"].([]any)
				assert.Empty(t, detections)

				// UpdateResourceProfileDetections
				rec = doRequest(
					t,
					h,
					http.MethodPatch,
					"/resource-profiles/detections?resourceArn="+arn,
					map[string]any{
						"suppressDataIdentifiers": []map[string]any{},
					},
				)
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}

// --- reveal configuration ---

func TestAppendixA_RevealConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "get_and_update_reveal_config",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// GetRevealConfiguration — default DISABLED
				rec := doRequest(t, h, http.MethodGet, "/reveal-configuration", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var getResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				cfg, _ := getResp["configuration"].(map[string]any)
				assert.Equal(t, "DISABLED", cfg["status"])

				// UpdateRevealConfiguration
				rec = doRequest(t, h, http.MethodPut, "/reveal-configuration", map[string]any{
					"configuration": map[string]any{
						"status":   "ENABLED",
						"kmsKeyId": "arn:aws:kms:us-east-1:000000000000:key/reveal-key",
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify
				rec = doRequest(t, h, http.MethodGet, "/reveal-configuration", nil)
				var updated map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
				updCfg, _ := updated["configuration"].(map[string]any)
				assert.Equal(t, "ENABLED", updCfg["status"])
				assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/reveal-key", updCfg["kmsKeyId"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}

// --- sensitive data occurrences ---

func TestAppendixA_SensitiveDataOccurrences(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "get_occurrences_and_availability_for_finding",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// Enable Macie and Reveal Config
				doRequest(t, h, http.MethodPost, "/macie", map[string]any{
					"status": "ENABLED",
				})
				doRequest(t, h, http.MethodPut, "/reveal-configuration", map[string]any{
					"configuration": map[string]any{
						"kmsKeyId": "arn:aws:kms:us-east-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab",
						"status":   "ENABLED",
					},
				})

				// Create a sample finding
				doRequest(t, h, http.MethodPost, "/findings/sample", map[string]any{
					"findingTypes": []string{"SensitiveData:S3Object/Personal"},
				})

				// Get finding IDs
				rec := doRequest(t, h, http.MethodPost, "/findings", nil)
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				findingIDs, _ := listResp["findingIds"].([]any)
				require.NotEmpty(t, findingIDs)
				findingID, _ := findingIDs[0].(string)

				// GetSensitiveDataOccurrences
				rec = doRequest(t, h, http.MethodGet, "/findings/"+findingID+"/reveal", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// GetSensitiveDataOccurrencesAvailability
				rec = doRequest(t, h, http.MethodGet, "/findings/"+findingID+"/reveal/availability", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var avail map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &avail))
				assert.Equal(t, "AVAILABLE", avail["code"])
			},
		},
		{
			name: "missing_finding_returns_404",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodGet, "/findings/nonexistent/reveal", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}

// --- sensitivity inspection templates ---

func TestAppendixA_SensitivityInspectionTemplates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "list_get_update_template",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// ListSensitivityInspectionTemplates — auto-creates default
				rec := doRequest(t, h, http.MethodGet, "/templates/sensitivity-inspections", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				templates, _ := listResp["sensitivityInspectionTemplates"].([]any)
				require.Len(t, templates, 1)

				tmpl0, _ := templates[0].(map[string]any)
				templateID, _ := tmpl0["id"].(string)
				require.NotEmpty(t, templateID)

				// GetSensitivityInspectionTemplate
				rec = doRequest(t, h, http.MethodGet, "/templates/sensitivity-inspections/"+templateID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var getResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				assert.Equal(t, templateID, getResp["id"])

				// UpdateSensitivityInspectionTemplate
				rec = doRequest(
					t,
					h,
					http.MethodPut,
					"/templates/sensitivity-inspections/"+templateID,
					map[string]any{
						"description": "Updated description",
						"includes": map[string]any{
							"managedDataIdentifierIds": []string{"EMAIL_ADDRESS"},
						},
					},
				)
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify update
				rec = doRequest(t, h, http.MethodGet, "/templates/sensitivity-inspections/"+templateID, nil)
				var updated map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
				assert.Equal(t, "Updated description", updated["description"])
			},
		},
		{
			name: "get_missing_template_returns_404",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodGet, "/templates/sensitivity-inspections/nonexistent", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}

// --- usage and managed data identifiers ---

func TestAppendixA_UsageAndManagedIdentifiers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "get_usage_statistics",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodPost, "/usage/statistics", map[string]any{
					"maxResults": 25,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				records, _ := resp["records"].([]any)
				assert.Empty(t, records)
			},
		},
		{
			name: "get_usage_totals",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodGet, "/usage", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				totals, _ := resp["usageTotals"].([]any)
				assert.NotEmpty(t, totals)
			},
		},
		{
			name: "list_managed_data_identifiers",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodGet, "/managed-data-identifiers/list", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				items, _ := resp["items"].([]any)
				assert.NotEmpty(t, items)

				// Verify structure of first item
				first, _ := items[0].(map[string]any)
				assert.NotEmpty(t, first["id"])
				assert.NotEmpty(t, first["category"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}
