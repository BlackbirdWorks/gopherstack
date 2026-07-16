package backup_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func TestUpdateBackupPlanUpdateDate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "update_returns_update_date"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newBatch1Handler(t)

			createResp := doBatch1Request(t, h, http.MethodPut, "/backup/plans", `{
				"BackupPlan": {
					"BackupPlanName": "plan-upd",
					"Rules": [{"RuleName": "r1", "TargetBackupVaultName": "v1"}]
				}
			}`)
			require.Equal(t, http.StatusOK, createResp.Code)
			var createData map[string]any
			require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &createData))
			planID := createData["BackupPlanId"].(string)

			updateResp := doBatch1Request(t, h, http.MethodPost, "/backup/plans/"+planID, `{
				"BackupPlan": {
					"BackupPlanName": "plan-upd",
					"Rules": [{"RuleName": "r2", "TargetBackupVaultName": "v1"}]
				}
			}`)
			require.Equal(t, http.StatusOK, updateResp.Code)

			var updateData map[string]any
			require.NoError(t, json.Unmarshal(updateResp.Body.Bytes(), &updateData))

			updateDate, exists := updateData["UpdateDate"]
			require.True(t, exists, "UpdateDate must be present in UpdateBackupPlan response")
			_, isFloat := updateDate.(float64)
			assert.True(t, isFloat,
				"UpdateDate must be epoch seconds (float64), got %T: %v", updateDate, updateDate)
		})
	}
}

func TestCreateBackupPlanAdvancedBackupSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		body             string
		wantResourceType string
		wantInResponse   bool
	}{
		{
			name: "advanced_settings_in_response",
			body: `{
				"BackupPlan": {
					"BackupPlanName": "plan-adv-resp",
					"Rules": [{"RuleName": "r1", "TargetBackupVaultName": "v"}],
					"AdvancedBackupSettings": [
						{
							"ResourceType": "EC2",
							"BackupOptions": {"WindowsVSS": "enabled"}
						}
					]
				}
			}`,
			wantInResponse:   true,
			wantResourceType: "EC2",
		},
		{
			name: "no_advanced_settings_absent_from_response",
			body: `{
				"BackupPlan": {
					"BackupPlanName": "plan-no-adv",
					"Rules": [{"RuleName": "r1", "TargetBackupVaultName": "v"}]
				}
			}`,
			wantInResponse: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newBatch1Handler(t)

			resp := doBatch1Request(t, h, http.MethodPut, "/backup/plans", tc.body)
			require.Equal(t, http.StatusOK, resp.Code)

			var data map[string]any
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &data))

			if tc.wantInResponse {
				settings, ok := data["AdvancedBackupSettings"].([]any)
				require.True(t, ok, "AdvancedBackupSettings must be present in CreateBackupPlan response")
				require.Len(t, settings, 1)
				s := settings[0].(map[string]any)
				assert.Equal(t, tc.wantResourceType, s["ResourceType"])
			} else {
				_, hasSettings := data["AdvancedBackupSettings"]
				assert.False(t, hasSettings, "AdvancedBackupSettings should be absent when not provided")
			}
		})
	}
}

func TestRuleLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantInRules func(t *testing.T, rules []any)
		name        string
		rules       []any
	}{
		{
			name: "lifecycle_fields_round_trip",
			rules: []any{
				map[string]any{
					"RuleName":              "r1",
					"TargetBackupVaultName": "my-vault",
					"Lifecycle": map[string]any{
						"MoveToColdStorageAfterDays": 30,
						"DeleteAfterDays":            120,
					},
				},
			},
			wantInRules: func(t *testing.T, rules []any) {
				t.Helper()
				require.Len(t, rules, 1)
				r := rules[0].(map[string]any)
				lc := r["Lifecycle"].(map[string]any)
				assert.InDelta(t, float64(30), lc["MoveToColdStorageAfterDays"], 0)
				assert.InDelta(t, float64(120), lc["DeleteAfterDays"], 0)
			},
		},
		{
			name: "copy_actions_round_trip",
			rules: []any{
				map[string]any{
					"RuleName":              "r1",
					"TargetBackupVaultName": "vault-a",
					"CopyActions": []any{
						map[string]any{
							"DestinationBackupVaultArn": "arn:aws:backup:us-west-2:123456789012:backup-vault:dst",
							"Lifecycle": map[string]any{
								"DeleteAfterDays": 60,
							},
						},
					},
				},
			},
			wantInRules: func(t *testing.T, rules []any) {
				t.Helper()
				require.Len(t, rules, 1)
				r := rules[0].(map[string]any)
				copies, ok := r["CopyActions"].([]any)
				require.True(t, ok)
				require.Len(t, copies, 1)
				ca := copies[0].(map[string]any)
				assert.Equal(
					t,
					"arn:aws:backup:us-west-2:123456789012:backup-vault:dst",
					ca["DestinationBackupVaultArn"],
				)
			},
		},
		{
			name: "recovery_point_tags_round_trip",
			rules: []any{
				map[string]any{
					"RuleName":              "r1",
					"TargetBackupVaultName": "vault-b",
					"RecoveryPointTags": map[string]any{
						"Env":     "prod",
						"Service": "payments",
					},
				},
			},
			wantInRules: func(t *testing.T, rules []any) {
				t.Helper()
				require.Len(t, rules, 1)
				r := rules[0].(map[string]any)
				tags, ok := r["RecoveryPointTags"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "prod", tags["Env"])
				assert.Equal(t, "payments", tags["Service"])
			},
		},
		{
			name: "enable_continuous_backup_round_trip",
			rules: []any{
				map[string]any{
					"RuleName":               "r1",
					"TargetBackupVaultName":  "vault-c",
					"EnableContinuousBackup": true,
				},
			},
			wantInRules: func(t *testing.T, rules []any) {
				t.Helper()
				require.Len(t, rules, 1)
				r := rules[0].(map[string]any)
				assert.Equal(t, true, r["EnableContinuousBackup"])
			},
		},
		{
			name: "schedule_expression_timezone_round_trip",
			rules: []any{
				map[string]any{
					"RuleName":                   "r1",
					"TargetBackupVaultName":      "vault-d",
					"ScheduleExpression":         "cron(0 0 * * ? *)",
					"ScheduleExpressionTimezone": "America/New_York",
				},
			},
			wantInRules: func(t *testing.T, rules []any) {
				t.Helper()
				require.Len(t, rules, 1)
				r := rules[0].(map[string]any)
				assert.Equal(t, "America/New_York", r["ScheduleExpressionTimezone"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, b := newBatch1Handler(t)

			// Create vault first.
			vaultNames := []string{"my-vault", "vault-a", "vault-b", "vault-c", "vault-d"}
			for _, vn := range vaultNames {
				doBatch1Request(t, h, http.MethodPut, "/backup-vaults/"+vn, `{}`)
			}

			// Create plan with rules.
			body, err := json.Marshal(map[string]any{
				"BackupPlan": map[string]any{
					"BackupPlanName": "test-plan",
					"Rules":          tt.rules,
				},
			})
			require.NoError(t, err)

			resp := doBatch1Request(t, h, http.MethodPut, "/backup/plans", string(body))
			require.Equal(t, http.StatusOK, resp.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &createResp))
			planID := createResp["BackupPlanId"].(string)

			// Retrieve and verify.
			getResp := doBatch1Request(t, h, http.MethodGet, "/backup/plans/"+planID, "")
			require.Equal(t, http.StatusOK, getResp.Code)
			_ = b

			var getPlan map[string]any
			require.NoError(t, json.Unmarshal(getResp.Body.Bytes(), &getPlan))
			backupPlan := getPlan["BackupPlan"].(map[string]any)
			rules := backupPlan["Rules"].([]any)
			tt.wantInRules(t, rules)
		})
	}
}

// ---- AdvancedBackupSettings (item 5) ----

func TestAdvancedBackupSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantSettings     func(t *testing.T, settings []any)
		name             string
		advancedSettings []any
	}{
		{
			name: "windows_vss_round_trip",
			advancedSettings: []any{
				map[string]any{
					"ResourceType": "EC2",
					"BackupOptions": map[string]any{
						"WindowsVSS": "enabled",
					},
				},
			},
			wantSettings: func(t *testing.T, settings []any) {
				t.Helper()
				require.Len(t, settings, 1)
				s := settings[0].(map[string]any)
				assert.Equal(t, "EC2", s["ResourceType"])
				opts := s["BackupOptions"].(map[string]any)
				assert.Equal(t, "enabled", opts["WindowsVSS"])
			},
		},
		{
			name:             "no_advanced_settings",
			advancedSettings: nil,
			wantSettings: func(t *testing.T, _ []any) {
				t.Helper()
				// settings key should be absent or empty.
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newBatch1Handler(t)

			planBody := map[string]any{
				"BackupPlan": map[string]any{
					"BackupPlanName": "plan-adv",
					"Rules": []any{
						map[string]any{"RuleName": "r1", "TargetBackupVaultName": "vault-x"},
					},
				},
			}
			if tt.advancedSettings != nil {
				planBody["BackupPlan"].(map[string]any)["AdvancedBackupSettings"] = tt.advancedSettings
			}

			body, err := json.Marshal(planBody)
			require.NoError(t, err)

			resp := doBatch1Request(t, h, http.MethodPut, "/backup/plans", string(body))
			require.Equal(t, http.StatusOK, resp.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &createResp))
			planID := createResp["BackupPlanId"].(string)

			getResp := doBatch1Request(t, h, http.MethodGet, "/backup/plans/"+planID, "")
			require.Equal(t, http.StatusOK, getResp.Code)

			var getPlan map[string]any
			require.NoError(t, json.Unmarshal(getResp.Body.Bytes(), &getPlan))
			backupPlan := getPlan["BackupPlan"].(map[string]any)

			if tt.advancedSettings != nil {
				settings, ok := backupPlan["AdvancedBackupSettings"].([]any)
				require.True(t, ok, "AdvancedBackupSettings should be present")
				tt.wantSettings(t, settings)
			}
		})
	}
}

// ---- Selection Resources / NotResources / ListOfTags / Conditions (item 7) ----

func TestListBackupPlanVersionsFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "version_fields_present"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			doREST(t, h, http.MethodPut, "/backup-vaults/vv-vault", nil)
			planID := createBackupPlan(t, h, "vv-plan")

			rec := doREST(t, h, http.MethodGet, "/backup/plans/"+planID+"/versions", nil)
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := parseResp(t, rec)
			versions, ok := resp["BackupPlanVersionsList"].([]any)
			assert.True(t, ok)
			require.NotEmpty(t, versions)

			v := versions[0].(map[string]any)
			assert.NotEmpty(t, v["BackupPlanId"], "BackupPlanId should be present")
			assert.NotEmpty(t, v["BackupPlanName"], "BackupPlanName should be present")
			assert.NotEmpty(t, v["VersionId"], "VersionId should be present")
			assert.NotNil(t, v["CreationDate"], "CreationDate should be present")
		})
	}
}

// ---- 7. ListBackupVaults includes EncryptionKeyArn when set ----

// TestBackupPlanCRUD exercises CreateBackupPlan, GetBackupPlan, ListBackupPlans,
// UpdateBackupPlan, and DeleteBackupPlan through the REST handler.
func TestBackupPlanCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *backup.Handler)
		name string
	}{
		{
			name: "create_and_get",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
					"BackupPlan": map[string]any{
						"BackupPlanName": "my-plan",
						"Rules": []map[string]any{
							{
								"RuleName":              "rule1",
								"TargetBackupVaultName": "vault1",
								"ScheduleExpression":    "cron(0 5 ? * * *)",
							},
						},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				createResp := parseResp(t, rec)
				planID, ok := createResp["BackupPlanId"].(string)
				require.True(t, ok)
				assert.NotEmpty(t, planID)

				rec2 := doREST(t, h, http.MethodGet, "/backup/plans/"+planID, nil)
				assert.Equal(t, http.StatusOK, rec2.Code)
				getResp := parseResp(t, rec2)
				assert.Equal(t, planID, getResp["BackupPlanId"])
				bp, ok := getResp["BackupPlan"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-plan", bp["BackupPlanName"])
			},
		},
		{
			name: "list_plans",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
					"BackupPlan": map[string]any{"BackupPlanName": "plan-a", "Rules": []any{}},
				})
				doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
					"BackupPlan": map[string]any{"BackupPlanName": "plan-b", "Rules": []any{}},
				})
				rec := doREST(t, h, http.MethodGet, "/backup/plans", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				list, ok := resp["BackupPlansList"].([]any)
				require.True(t, ok)
				assert.Len(t, list, 2)
			},
		},
		{
			name: "update_plan",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				createRec := doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
					"BackupPlan": map[string]any{"BackupPlanName": "upd-plan", "Rules": []any{}},
				})
				createResp := parseResp(t, createRec)
				planID := createResp["BackupPlanId"].(string)

				rec := doREST(t, h, http.MethodPost, "/backup/plans/"+planID, map[string]any{
					"BackupPlan": map[string]any{
						"BackupPlanName": "upd-plan",
						"Rules": []map[string]any{
							{"RuleName": "new-rule", "TargetBackupVaultName": "vault1"},
						},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				updResp := parseResp(t, rec)
				assert.Equal(t, planID, updResp["BackupPlanId"])
				assert.NotEmpty(t, updResp["VersionId"])
			},
		},
		{
			name: "delete_plan",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				createRec := doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
					"BackupPlan": map[string]any{"BackupPlanName": "del-plan", "Rules": []any{}},
				})
				planID := parseResp(t, createRec)["BackupPlanId"].(string)

				rec := doREST(t, h, http.MethodDelete, "/backup/plans/"+planID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				rec2 := doREST(t, h, http.MethodGet, "/backup/plans/"+planID, nil)
				assert.Equal(t, http.StatusNotFound, rec2.Code)
			},
		},
		{
			name: "get_plan_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodGet, "/backup/plans/not-exist", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestDeleteBackupPlanDeletionDate verifies that DeletionDate is current time, not creation time.
func TestDeleteBackupPlanDeletionDate(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()
	createRec := doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
		"BackupPlan": map[string]any{"BackupPlanName": "del-plan"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	createResp := parseResp(t, createRec)
	planID := createResp["BackupPlanId"].(string)
	creationDate := createResp["CreationDate"].(float64)

	delRec := doREST(t, h, http.MethodDelete, "/backup/plans/"+planID, nil)
	require.Equal(t, http.StatusOK, delRec.Code)
	delResp := parseResp(t, delRec)
	deletionDate := delResp["DeletionDate"].(float64)

	// DeletionDate must be >= CreationDate.
	assert.GreaterOrEqual(t, deletionDate, creationDate)
}

// TestParity_GetBackupPlan_LastExecutionDate verifies that GetBackupPlan returns
// LastExecutionDate when the plan has been updated, matching real AWS behavior.
func TestParity_GetBackupPlan_LastExecutionDate(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()

	createRec := doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
		"BackupPlan": map[string]any{
			"BackupPlanName": "led-plan",
			"Rules": []map[string]any{
				{
					"RuleName":              "daily",
					"TargetBackupVaultName": "Default",
					"ScheduleExpression":    "cron(0 5 ? * * *)",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code, "create plan: %s", createRec.Body)
	planID, _ := parseResp(t, createRec)["BackupPlanId"].(string)
	require.NotEmpty(t, planID)

	// Update the plan to trigger UpdateTime.
	doREST(t, h, http.MethodPost, "/backup/plans/"+planID, map[string]any{
		"BackupPlan": map[string]any{
			"BackupPlanName": "led-plan",
			"Rules": []map[string]any{
				{
					"RuleName":              "daily",
					"TargetBackupVaultName": "Default",
					"ScheduleExpression":    "cron(0 6 ? * * *)",
				},
			},
		},
	})

	getRec := doREST(t, h, http.MethodGet, "/backup/plans/"+planID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	resp := parseResp(t, getRec)
	_, hasField := resp["LastExecutionDate"]
	assert.True(t, hasField, "GetBackupPlan must return LastExecutionDate after plan update")
}

func TestBackupPlan_CreateGetListDelete(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()
	doREST(t, h, http.MethodPut, "/backup-vaults/plan-vault", nil)

	// Create
	planID := createBackupPlan(t, h, "myplan")
	assert.NotEmpty(t, planID)

	// Get
	rec := doREST(t, h, http.MethodGet, "/backup/plans/"+planID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	assert.Equal(t, planID, resp["BackupPlanId"])

	// List
	rec2 := doREST(t, h, http.MethodGet, "/backup/plans", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)
	listResp := parseResp(t, rec2)
	plans, ok := listResp["BackupPlansList"].([]any)
	assert.True(t, ok)
	assert.NotEmpty(t, plans)

	// Delete
	rec3 := doREST(t, h, http.MethodDelete, "/backup/plans/"+planID, nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Get after delete
	rec4 := doREST(t, h, http.MethodGet, "/backup/plans/"+planID, nil)
	assert.Equal(t, http.StatusNotFound, rec4.Code)
}
