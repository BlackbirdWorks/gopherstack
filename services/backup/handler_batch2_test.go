package backup_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// ---- Rule lifecycle / copy-actions / recovery-point-tags (items 1-3) ----

func TestRuleLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		rules       []any
		wantInRules func(t *testing.T, rules []any)
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
				assert.Equal(t, float64(30), lc["MoveToColdStorageAfterDays"])
				assert.Equal(t, float64(120), lc["DeleteAfterDays"])
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
				assert.Equal(t, "arn:aws:backup:us-west-2:123456789012:backup-vault:dst", ca["DestinationBackupVaultArn"])
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
					"TargetBackupVaultName":       "vault-d",
					"ScheduleExpression":          "cron(0 0 * * ? *)",
					"ScheduleExpressionTimezone":  "America/New_York",
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
					"Rules":         tt.rules,
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
		name             string
		advancedSettings []any
		wantSettings     func(t *testing.T, settings []any)
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
			wantSettings: func(t *testing.T, settings []any) {
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

func TestBackupSelectionResourceFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		selection map[string]any
		wantSel   func(t *testing.T, sel map[string]any)
	}{
		{
			name: "resources_round_trip",
			selection: map[string]any{
				"SelectionName": "sel1",
				"IamRoleArn":    "arn:aws:iam::000000000000:role/r",
				"Resources": []any{
					"arn:aws:ec2:us-east-1:000000000000:instance/i-1234",
					"arn:aws:rds:us-east-1:000000000000:db:mydb",
				},
			},
			wantSel: func(t *testing.T, sel map[string]any) {
				t.Helper()
				resources, ok := sel["Resources"].([]any)
				require.True(t, ok, "Resources should be present")
				assert.Len(t, resources, 2)
			},
		},
		{
			name: "not_resources_round_trip",
			selection: map[string]any{
				"SelectionName": "sel2",
				"IamRoleArn":    "arn:aws:iam::000000000000:role/r",
				"NotResources": []any{
					"arn:aws:s3:::my-exclude-bucket",
				},
			},
			wantSel: func(t *testing.T, sel map[string]any) {
				t.Helper()
				notRes, ok := sel["NotResources"].([]any)
				require.True(t, ok, "NotResources should be present")
				assert.Len(t, notRes, 1)
			},
		},
		{
			name: "list_of_tags_round_trip",
			selection: map[string]any{
				"SelectionName": "sel3",
				"IamRoleArn":    "arn:aws:iam::000000000000:role/r",
				"ListOfTags": []any{
					map[string]any{
						"ConditionType":  "STRINGEQUALS",
						"ConditionKey":   "backup",
						"ConditionValue": "true",
					},
				},
			},
			wantSel: func(t *testing.T, sel map[string]any) {
				t.Helper()
				tags, ok := sel["ListOfTags"].([]any)
				require.True(t, ok, "ListOfTags should be present")
				require.Len(t, tags, 1)
				tag := tags[0].(map[string]any)
				assert.Equal(t, "STRINGEQUALS", tag["ConditionType"])
				assert.Equal(t, "backup", tag["ConditionKey"])
			},
		},
		{
			name: "conditions_round_trip",
			selection: map[string]any{
				"SelectionName": "sel4",
				"IamRoleArn":    "arn:aws:iam::000000000000:role/r",
				"Conditions": map[string]any{
					"StringEquals": []any{
						map[string]any{"Key": "aws:ResourceTag/env", "Value": "prod"},
					},
					"StringLike": []any{
						map[string]any{"Key": "aws:ResourceTag/app", "Value": "pay*"},
					},
				},
			},
			wantSel: func(t *testing.T, sel map[string]any) {
				t.Helper()
				conds, ok := sel["Conditions"]
				require.True(t, ok, "Conditions should be present")
				_ = conds
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newBatch1Handler(t)

			// Create plan first.
			planResp := doBatch1Request(t, h, http.MethodPut, "/backup/plans",
				`{"BackupPlan":{"BackupPlanName":"p","Rules":[{"RuleName":"r","TargetBackupVaultName":"v"}]}}`)
			require.Equal(t, http.StatusOK, planResp.Code)
			var planData map[string]any
			require.NoError(t, json.Unmarshal(planResp.Body.Bytes(), &planData))
			planID := planData["BackupPlanId"].(string)

			// Create selection.
			body, err := json.Marshal(map[string]any{"BackupSelection": tt.selection})
			require.NoError(t, err)
			selResp := doBatch1Request(t, h, http.MethodPut,
				"/backup/plans/"+planID+"/selections", string(body))
			require.Equal(t, http.StatusOK, selResp.Code)

			var selData map[string]any
			require.NoError(t, json.Unmarshal(selResp.Body.Bytes(), &selData))
			selID := selData["SelectionId"].(string)

			// Get selection and verify fields.
			getResp := doBatch1Request(t, h, http.MethodGet,
				"/backup/plans/"+planID+"/selections/"+selID, "")
			require.Equal(t, http.StatusOK, getResp.Code)

			var getData map[string]any
			require.NoError(t, json.Unmarshal(getResp.Body.Bytes(), &getData))
			selDoc := getData["BackupSelection"].(map[string]any)
			tt.wantSel(t, selDoc)
		})
	}
}

// ---- Vault name underscore (item 20) ----

func TestVaultNameUnderscoreAllowed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		vaultName  string
		wantStatus int
	}{
		{
			name:       "underscore_valid",
			vaultName:  "my_vault",
			wantStatus: http.StatusOK,
		},
		{
			name:       "leading_underscore",
			vaultName:  "_vault",
			wantStatus: http.StatusOK,
		},
		{
			name:       "mixed_underscore_hyphen",
			vaultName:  "my-vault_01",
			wantStatus: http.StatusOK,
		},
		{
			name:       "dot_invalid",
			vaultName:  "my.vault",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newBatch1Handler(t)
			resp := doBatch1Request(t, h, http.MethodPut, "/backup-vaults/"+tt.vaultName, `{}`)
			assert.Equal(t, tt.wantStatus, resp.Code, "vault name %q", tt.vaultName)
		})
	}

	// Space and slash are invalid URL path characters and cannot be tested via HTTP path.
	// Test validation directly at the backend layer.
	invalidNames := []struct {
		name      string
		vaultName string
	}{
		{name: "space_invalid", vaultName: "my vault"},
		{name: "slash_invalid", vaultName: "my/vault"},
	}
	for _, tt := range invalidNames {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := backup.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateBackupVault(tt.vaultName, "", "", nil)
			require.Error(t, err, "vault name %q should be rejected", tt.vaultName)
		})
	}
}

// ---- CreatorRequestId idempotency (item 21) ----

func TestCreateVaultCreatorRequestIdIdempotency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		firstRequest   string
		secondRequest  string
		wantSecondCode int
		wantSameArn    bool
	}{
		{
			name:           "same_creator_request_id_returns_existing",
			firstRequest:   `{"CreatorRequestId":"req-001"}`,
			secondRequest:  `{"CreatorRequestId":"req-001"}`,
			wantSecondCode: http.StatusOK,
			wantSameArn:    true,
		},
		{
			name:           "different_creator_request_id_conflicts",
			firstRequest:   `{"CreatorRequestId":"req-aaa"}`,
			secondRequest:  `{"CreatorRequestId":"req-bbb"}`,
			wantSecondCode: http.StatusConflict,
			wantSameArn:    false,
		},
		{
			name:           "no_creator_request_id_conflicts",
			firstRequest:   `{}`,
			secondRequest:  `{}`,
			wantSecondCode: http.StatusConflict,
			wantSameArn:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newBatch1Handler(t)

			r1 := doBatch1Request(t, h, http.MethodPut, "/backup-vaults/idem-vault", tt.firstRequest)
			require.Equal(t, http.StatusOK, r1.Code)
			var d1 map[string]any
			require.NoError(t, json.Unmarshal(r1.Body.Bytes(), &d1))
			arn1 := d1["BackupVaultArn"].(string)

			r2 := doBatch1Request(t, h, http.MethodPut, "/backup-vaults/idem-vault", tt.secondRequest)
			assert.Equal(t, tt.wantSecondCode, r2.Code)
			if tt.wantSameArn {
				var d2 map[string]any
				require.NoError(t, json.Unmarshal(r2.Body.Bytes(), &d2))
				assert.Equal(t, arn1, d2["BackupVaultArn"].(string))
			}
		})
	}
}

// ---- Vault lock LockDate + enforcement (item 17) ----

func TestVaultLockLockDate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		lockBody         string
		wantStatus       int
		wantLockResponse bool
	}{
		{
			name: "changeable_for_days_computes_lock_date",
			lockBody: `{
				"MinRetentionDays": 7,
				"MaxRetentionDays": 90,
				"ChangeableForDays": 3
			}`,
			wantStatus:       http.StatusOK,
			wantLockResponse: true,
		},
		{
			name: "no_changeable_for_days",
			lockBody: `{
				"MinRetentionDays": 7,
				"MaxRetentionDays": 365
			}`,
			wantStatus:       http.StatusOK,
			wantLockResponse: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, b := newBatch1Handler(t)

			// Create vault.
			doBatch1Request(t, h, http.MethodPut, "/backup-vaults/lock-vault", `{}`)

			// Apply lock config.
			resp := doBatch1Request(t, h, http.MethodPut, "/backup-vaults/lock-vault/vault-lock", tt.lockBody)
			assert.Equal(t, tt.wantStatus, resp.Code)

			if tt.wantStatus == http.StatusOK && tt.wantLockResponse {
				// Verify LockDate is set in the backend config.
				cfg, err := b.GetBackupVaultLockConfig("lock-vault")
				require.NoError(t, err)
				assert.NotNil(t, cfg.LockDate, "LockDate should be set when ChangeableForDays > 0")
			}
		})
	}
}

// ---- Vault notification event validation (item 18) ----

func TestVaultNotificationsEventValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "valid_events",
			body: `{
				"SNSTopicArn": "arn:aws:sns:us-east-1:000000000000:my-topic",
				"BackupVaultEvents": ["BACKUP_JOB_STARTED","BACKUP_JOB_COMPLETED","RESTORE_JOB_STARTED"]
			}`,
			wantStatus: http.StatusOK,
		},
		{
			name: "invalid_event",
			body: `{
				"SNSTopicArn": "arn:aws:sns:us-east-1:000000000000:my-topic",
				"BackupVaultEvents": ["INVALID_EVENT_TYPE"]
			}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "mixed_valid_and_invalid",
			body: `{
				"SNSTopicArn": "arn:aws:sns:us-east-1:000000000000:topic",
				"BackupVaultEvents": ["BACKUP_JOB_STARTED","NOT_REAL_EVENT"]
			}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "empty_events",
			body: `{
				"SNSTopicArn": "arn:aws:sns:us-east-1:000000000000:my-topic",
				"BackupVaultEvents": []
			}`,
			wantStatus: http.StatusOK,
		},
		{
			name: "all_valid_event_types",
			body: `{
				"SNSTopicArn": "arn:aws:sns:us-east-1:000000000000:topic",
				"BackupVaultEvents": [
					"BACKUP_JOB_STARTED","BACKUP_JOB_COMPLETED","BACKUP_JOB_SUCCESSFUL",
					"BACKUP_JOB_FAILED","BACKUP_JOB_EXPIRED","RESTORE_JOB_STARTED",
					"RESTORE_JOB_COMPLETED","RESTORE_JOB_SUCCESSFUL","RESTORE_JOB_FAILED",
					"COPY_JOB_STARTED","COPY_JOB_SUCCESSFUL","COPY_JOB_FAILURE",
					"RECOVERY_POINT_MODIFIED","BACKUP_PLAN_CREATED","BACKUP_PLAN_MODIFIED",
					"S3_BACKUP_OBJECT_FAILED","S3_RESTORE_OBJECT_FAILED"
				]
			}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newBatch1Handler(t)

			doBatch1Request(t, h, http.MethodPut, "/backup-vaults/notif-vault", `{}`)

			resp := doBatch1Request(t, h, http.MethodPut,
				"/backup-vaults/notif-vault/notification-configuration", tt.body)
			assert.Equal(t, tt.wantStatus, resp.Code, "body: %s", tt.name)
		})
	}
}

// ---- Vault access policy JSON validation (item 19) ----

func TestVaultAccessPolicyJSONValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "valid_json_policy",
			body: `{
				"Policy": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"*\"},\"Action\":\"backup:StartBackupJob\",\"Resource\":\"*\"}]}"
			}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_json_policy",
			body:       `{"Policy": "this is not json"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty_policy_allowed",
			body:       `{"Policy": ""}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "no_policy_field",
			body:       `{}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newBatch1Handler(t)

			doBatch1Request(t, h, http.MethodPut, "/backup-vaults/policy-vault", `{}`)

			resp := doBatch1Request(t, h, http.MethodPut,
				"/backup-vaults/policy-vault/access-policy", tt.body)
			assert.Equal(t, tt.wantStatus, resp.Code, "test: %s", tt.name)
		})
	}
}

// ---- Framework controls (item 23) ----

func TestFrameworkControls(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		body           string
		wantStatus     int
		wantControlLen int
		wantStatus2    string
	}{
		{
			name: "framework_with_controls",
			body: `{
				"FrameworkName": "fw-with-controls",
				"FrameworkDescription": "compliance framework",
				"FrameworkControls": [
					{
						"ControlName": "BACKUP_RESOURCES_PROTECTED_BY_BACKUP_VAULT_LOCK",
						"ControlInputParameters": {"requiredRetentionDays": "30"}
					},
					{
						"ControlName": "BACKUP_PLAN_MIN_FREQUENCY_AND_MIN_RETENTION_PERIOD",
						"ControlInputParameters": {"requiredFrequencyValue": "1"}
					}
				]
			}`,
			wantStatus:     http.StatusOK,
			wantControlLen: 2,
			wantStatus2:    "ACTIVE",
		},
		{
			name: "framework_no_controls",
			body: `{
				"FrameworkName": "fw-no-controls",
				"FrameworkDescription": "empty framework"
			}`,
			wantStatus:     http.StatusOK,
			wantControlLen: 0,
			wantStatus2:    "ACTIVE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newBatch1Handler(t)

			resp := doBatch1Request(t, h, http.MethodPost, "/audit/frameworks", tt.body)
			require.Equal(t, tt.wantStatus, resp.Code)

			var createData map[string]any
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &createData))
			fwName := createData["FrameworkName"].(string)

			// Describe and verify controls.
			getResp := doBatch1Request(t, h, http.MethodGet, "/audit/frameworks/"+fwName, "")
			require.Equal(t, http.StatusOK, getResp.Code)

			var fwData map[string]any
			require.NoError(t, json.Unmarshal(getResp.Body.Bytes(), &fwData))
			assert.Equal(t, tt.wantStatus2, fwData["FrameworkStatus"])
			assert.Equal(t, "COMPLETED", fwData["DeploymentStatus"])
			if tt.wantControlLen > 0 {
				controls, ok := fwData["FrameworkControls"].([]any)
				require.True(t, ok, "FrameworkControls should be present")
				assert.Len(t, controls, tt.wantControlLen)
				ctrl := controls[0].(map[string]any)
				assert.NotEmpty(t, ctrl["ControlName"])
			}
		})
	}
}

// ---- ReportPlan sub-fields (item 24) ----

func TestReportPlanSubFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantStatus   int
		wantDelivery bool
		wantSetting  bool
	}{
		{
			name: "report_plan_with_delivery_and_setting",
			body: `{
				"ReportPlanName": "rp-full",
				"ReportPlanDescription": "full report",
				"ReportDeliveryChannel": {
					"S3BucketName": "my-reports-bucket",
					"Formats": ["CSV","JSON"]
				},
				"ReportSetting": {
					"ReportTemplate": "BACKUP_JOB_REPORT",
					"FrameworkArns": ["arn:aws:backup:us-east-1:000000000000:framework:fw-1"]
				}
			}`,
			wantStatus:   http.StatusOK,
			wantDelivery: true,
			wantSetting:  true,
		},
		{
			name: "report_plan_minimal",
			body: `{
				"ReportPlanName": "rp-minimal"
			}`,
			wantStatus:   http.StatusOK,
			wantDelivery: false,
			wantSetting:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newBatch1Handler(t)

			resp := doBatch1Request(t, h, http.MethodPost, "/audit/report-plans", tt.body)
			require.Equal(t, tt.wantStatus, resp.Code)

			var createData map[string]any
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &createData))
			rpName := createData["ReportPlanName"].(string)

			// Describe and verify sub-fields.
			getResp := doBatch1Request(t, h, http.MethodGet, "/audit/report-plans/"+rpName, "")
			require.Equal(t, http.StatusOK, getResp.Code)

			var getData map[string]any
			require.NoError(t, json.Unmarshal(getResp.Body.Bytes(), &getData))
			rpDoc := getData["ReportPlan"].(map[string]any)

			if tt.wantDelivery {
				ch, ok := rpDoc["ReportDeliveryChannel"].(map[string]any)
				require.True(t, ok, "ReportDeliveryChannel should be present")
				assert.Equal(t, "my-reports-bucket", ch["S3BucketName"])
				formats, ok2 := ch["Formats"].([]any)
				require.True(t, ok2)
				assert.Len(t, formats, 2)
			} else {
				_, hasDelivery := rpDoc["ReportDeliveryChannel"]
				assert.False(t, hasDelivery, "ReportDeliveryChannel should be absent")
			}

			if tt.wantSetting {
				rs, ok := rpDoc["ReportSetting"].(map[string]any)
				require.True(t, ok, "ReportSetting should be present")
				assert.Equal(t, "BACKUP_JOB_REPORT", rs["ReportTemplate"])
				arns, ok2 := rs["FrameworkArns"].([]any)
				require.True(t, ok2)
				assert.Len(t, arns, 1)
			} else {
				_, hasSetting := rpDoc["ReportSetting"]
				assert.False(t, hasSetting, "ReportSetting should be absent")
			}
		})
	}
}

// ---- Job progress fields (item 10) ----

func TestDescribeBackupJobProgressFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantField string
	}{
		{name: "has_job_id", wantField: "BackupJobId"},
		{name: "has_vault_name", wantField: "BackupVaultName"},
		{name: "has_state", wantField: "State"},
		{name: "has_creation_date", wantField: "CreationDate"},
	}

	h, _ := newBatch1Handler(t)
	doBatch1Request(t, h, http.MethodPut, "/backup-vaults/job-vault", `{}`)

	startResp := doBatch1Request(t, h, http.MethodPut, "/backup-jobs", `{
		"BackupVaultName": "job-vault",
		"ResourceArn": "arn:aws:ec2:us-east-1:000000000000:instance/i-abc",
		"IamRoleArn": "arn:aws:iam::000000000000:role/backup-role"
	}`)
	require.Equal(t, http.StatusOK, startResp.Code)

	var startData map[string]any
	require.NoError(t, json.Unmarshal(startResp.Body.Bytes(), &startData))
	jobID := startData["BackupJobId"].(string)

	descResp := doBatch1Request(t, h, http.MethodGet, "/backup-jobs/"+jobID, "")
	require.Equal(t, http.StatusOK, descResp.Code)

	var descData map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descData))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, ok := descData[tt.wantField]
			assert.True(t, ok, "field %q should be present in DescribeBackupJob response", tt.wantField)
		})
	}
}

// ---- RecoveryPoint struct field tests (item 14) ----

func TestRecoveryPointNewFields(t *testing.T) {
	t.Parallel()

	h, b := newBatch1Handler(t)

	// Create vault + recovery point.
	doBatch1Request(t, h, http.MethodPut, "/backup-vaults/rp-vault", `{}`)

	rp := &backup.RecoveryPoint{
		RecoveryPointArn: "arn:aws:backup:us-east-1:000000000000:recovery-point:rp-1",
		BackupVaultName:  "rp-vault",
		BackupVaultArn:   "arn:aws:backup:us-east-1:000000000000:backup-vault:rp-vault",
		Status:           "COMPLETED",
		StorageClass:     "WARM",
		IsEncrypted:      true,
		EncryptionKeyArn: "arn:aws:kms:us-east-1:000000000000:key/abc123",
		SourceBackupVaultArn: "arn:aws:backup:us-east-1:000000000000:backup-vault:src-vault",
	}
	require.NoError(t, b.AddRecoveryPoint("rp-vault", rp))

	tests := []struct {
		name      string
		checkResp func(t *testing.T, resp map[string]any)
	}{
		{
			name: "storage_class_preserved",
			checkResp: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.Equal(t, "WARM", resp["StorageClass"])
			},
		},
		{
			name: "is_encrypted_preserved",
			checkResp: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.Equal(t, true, resp["IsEncrypted"])
			},
		},
		{
			name: "encryption_key_arn_preserved",
			checkResp: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/abc123", resp["EncryptionKeyArn"])
			},
		},
		{
			name: "source_backup_vault_arn_preserved",
			checkResp: func(t *testing.T, resp map[string]any) {
				t.Helper()
				assert.Equal(t, "arn:aws:backup:us-east-1:000000000000:backup-vault:src-vault", resp["SourceBackupVaultArn"])
			},
		},
	}

	descResp := doBatch1Request(t, h, http.MethodGet,
		"/backup-vaults/rp-vault/recovery-points/"+rp.RecoveryPointArn, "")
	require.Equal(t, http.StatusOK, descResp.Code)

	var respData map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &respData))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.checkResp(t, respData)
		})
	}
}

// ---- VaultLockConfig GetBackupVaultLockConfig helper ----
// (used in TestVaultLockLockDate; exposed via export if not present)

func TestVaultLockConfigDeletion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		setup           func(t *testing.T, h *backup.Handler)
		deleteShouldOK  bool
	}{
		{
			name: "delete_existing_lock",
			setup: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doBatch1Request(t, h, http.MethodPut, "/backup-vaults/lock-v2", `{}`)
				doBatch1Request(t, h, http.MethodPut, "/backup-vaults/lock-v2/vault-lock",
					`{"MinRetentionDays":7,"MaxRetentionDays":30}`)
			},
			deleteShouldOK: true,
		},
		{
			name: "delete_nonexistent_lock_still_ok",
			setup: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doBatch1Request(t, h, http.MethodPut, "/backup-vaults/lock-v2", `{}`)
			},
			deleteShouldOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newBatch1Handler(t)
			tt.setup(t, h)
			resp := doBatch1Request(t, h, http.MethodDelete, "/backup-vaults/lock-v2/vault-lock", "")
			if tt.deleteShouldOK {
				assert.Equal(t, http.StatusOK, resp.Code)
			}
		})
	}
}
