package backup_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func TestDescribeBackupVaultLockedField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		lockBody   string
		wantMin    float64
		wantMax    float64
		wantLocked bool
		applyLock  bool
	}{
		{
			name:       "no_lock_returns_locked_false",
			applyLock:  false,
			wantLocked: false,
		},
		{
			name:      "with_lock_returns_locked_true_and_retention",
			applyLock: true,
			lockBody: `{
				"MinRetentionDays": 7,
				"MaxRetentionDays": 90
			}`,
			wantLocked: true,
			wantMin:    7,
			wantMax:    90,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newHandler(t)

			doRequest(t, h, http.MethodPut, "/backup-vaults/locked-vault", `{}`)

			if tc.applyLock {
				lockResp := doRequest(t, h, http.MethodPut,
					"/backup-vaults/locked-vault/vault-lock", tc.lockBody)
				require.Equal(t, http.StatusOK, lockResp.Code)
			}

			resp := doRequest(t, h, http.MethodGet, "/backup-vaults/locked-vault", "")
			require.Equal(t, http.StatusOK, resp.Code)

			var data map[string]any
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &data))

			locked, exists := data["Locked"]
			require.True(t, exists, "Locked must be present in DescribeBackupVault response")
			assert.Equal(t, tc.wantLocked, locked)

			if tc.applyLock {
				minDays, minOK := data["MinRetentionDays"]
				maxDays, maxOK := data["MaxRetentionDays"]
				require.True(t, minOK, "MinRetentionDays must be present when lock is applied")
				require.True(t, maxOK, "MaxRetentionDays must be present when lock is applied")
				assert.InDelta(t, tc.wantMin, minDays, 0)
				assert.InDelta(t, tc.wantMax, maxDays, 0)
			}
		})
	}
}

// ---- 4. CreateBackupPlan AdvancedBackupSettings in response ----

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
			h, b := newHandler(t)

			// Create vault.
			doRequest(t, h, http.MethodPut, "/backup-vaults/lock-vault", `{}`)

			// Apply lock config.
			resp := doRequest(t, h, http.MethodPut, "/backup-vaults/lock-vault/vault-lock", tt.lockBody)
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
			h, _ := newHandler(t)

			doRequest(t, h, http.MethodPut, "/backup-vaults/notif-vault", `{}`)

			resp := doRequest(t, h, http.MethodPut,
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
				"Policy": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",' +
					'\"Principal\":{\"AWS\":\"*\"},\"Action\":\"backup:StartBackupJob\",' +
					'\"Resource\":\"*\"}]}"
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
			h, _ := newHandler(t)

			doRequest(t, h, http.MethodPut, "/backup-vaults/policy-vault", `{}`)

			resp := doRequest(t, h, http.MethodPut,
				"/backup-vaults/policy-vault/access-policy", tt.body)
			assert.Equal(t, tt.wantStatus, resp.Code, "test: %s", tt.name)
		})
	}
}

// ---- Framework controls (item 23) ----

func TestVaultLockConfigDeletion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(t *testing.T, h *backup.Handler)
		name           string
		deleteShouldOK bool
	}{
		{
			name: "delete_existing_lock",
			setup: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doRequest(t, h, http.MethodPut, "/backup-vaults/lock-v2", `{}`)
				doRequest(t, h, http.MethodPut, "/backup-vaults/lock-v2/vault-lock",
					`{"MinRetentionDays":7,"MaxRetentionDays":30}`)
			},
			deleteShouldOK: true,
		},
		{
			name: "delete_nonexistent_lock_still_ok",
			setup: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doRequest(t, h, http.MethodPut, "/backup-vaults/lock-v2", `{}`)
			},
			deleteShouldOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newHandler(t)
			tt.setup(t, h)
			resp := doRequest(t, h, http.MethodDelete, "/backup-vaults/lock-v2/vault-lock", "")
			if tt.deleteShouldOK {
				assert.Equal(t, http.StatusOK, resp.Code)
			}
		})
	}
}

func TestGetBackupVaultAccessPolicyArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "returns_real_vault_arn"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newHandlerAndBackend()
			doREST(t, h, http.MethodPut, "/backup-vaults/arnvault", nil)
			doREST(t, h, http.MethodPut, "/backup-vaults/arnvault/access-policy", map[string]any{
				"Policy": `{"Version":"2012-10-17","Statement":[]}`,
			})

			rec := doREST(t, h, http.MethodGet, "/backup-vaults/arnvault/access-policy", nil)
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := parseResp(t, rec)
			arn, _ := resp["BackupVaultArn"].(string)
			assert.Contains(t, arn, "arnvault", "BackupVaultArn must contain vault name, got empty string")
			assert.Equal(t, "arnvault", resp["BackupVaultName"])
		})
	}
}

// ---- 3. GetBackupVaultNotifications returns real BackupVaultArn ----

func TestGetBackupVaultNotificationsArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "returns_real_vault_arn"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newHandlerAndBackend()
			doREST(t, h, http.MethodPut, "/backup-vaults/notifarnvault", nil)
			doREST(t, h, http.MethodPut, "/backup-vaults/notifarnvault/notification-configuration",
				map[string]any{
					"BackupVaultEvents": []string{"BACKUP_JOB_STARTED"},
					"SNSTopicArn":       "arn:aws:sns:us-east-1:123456789012:topic",
				})

			rec := doREST(t, h, http.MethodGet, "/backup-vaults/notifarnvault/notification-configuration", nil)
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := parseResp(t, rec)
			arn, _ := resp["BackupVaultArn"].(string)
			assert.Contains(t, arn, "notifarnvault", "BackupVaultArn must contain vault name, got empty string")
		})
	}
}

// ---- 4. GetRecoveryPointRestoreMetadata returns real BackupVaultArn and BackupVaultName ----

func TestVaultAccessPolicy_PutGetDelete(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerAndBackend()
	doREST(t, h, http.MethodPut, "/backup-vaults/policyvault", nil)

	policy := `{"Version":"2012-10-17","Statement":[]}`

	rec := doREST(t, h, http.MethodPut, "/backup-vaults/policyvault/access-policy", map[string]any{
		"Policy": policy,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doREST(t, h, http.MethodGet, "/backup-vaults/policyvault/access-policy", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)
	resp := parseResp(t, rec2)
	assert.Equal(t, "policyvault", resp["BackupVaultName"])

	rec3 := doREST(t, h, http.MethodDelete, "/backup-vaults/policyvault/access-policy", nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	rec4 := doREST(t, h, http.MethodGet, "/backup-vaults/policyvault/access-policy", nil)
	assert.Equal(t, http.StatusNotFound, rec4.Code)
}

func TestVaultLockConfiguration_PutDelete(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerAndBackend()
	doREST(t, h, http.MethodPut, "/backup-vaults/lockvault", nil)

	rec := doREST(t, h, http.MethodPut, "/backup-vaults/lockvault/vault-lock", map[string]any{
		"MinRetentionDays": 7,
		"MaxRetentionDays": 365,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doREST(t, h, http.MethodDelete, "/backup-vaults/lockvault/vault-lock", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)
}

func TestVaultNotifications_PutGetDelete(t *testing.T) {
	t.Parallel()

	h, _ := newHandlerAndBackend()
	doREST(t, h, http.MethodPut, "/backup-vaults/notifvault", nil)

	rec := doREST(t, h, http.MethodPut, "/backup-vaults/notifvault/notification-configuration", map[string]any{
		"BackupVaultEvents": []string{"BACKUP_JOB_STARTED"},
		"SNSTopicArn":       "arn:aws:sns:us-east-1:123456789012:backup-topic",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doREST(t, h, http.MethodGet, "/backup-vaults/notifvault/notification-configuration", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)
	resp := parseResp(t, rec2)
	assert.Equal(t, "notifvault", resp["BackupVaultName"])

	rec3 := doREST(t, h, http.MethodDelete, "/backup-vaults/notifvault/notification-configuration", nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	rec4 := doREST(t, h, http.MethodGet, "/backup-vaults/notifvault/notification-configuration", nil)
	assert.Equal(t, http.StatusNotFound, rec4.Code)
}
