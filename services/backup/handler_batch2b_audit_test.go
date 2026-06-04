package backup_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- 1. DescribeBackupVault VaultState ----

func TestDescribeBackupVaultVaultState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantState string
	}{
		{name: "available_state", wantState: "AVAILABLE"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newBatch1Handler(t)

			doBatch1Request(t, h, http.MethodPut, "/backup-vaults/state-vault", `{}`)

			resp := doBatch1Request(t, h, http.MethodGet, "/backup-vaults/state-vault", "")
			require.Equal(t, http.StatusOK, resp.Code)

			var data map[string]any
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &data))

			state, exists := data["VaultState"]
			require.True(t, exists, "VaultState must be present in DescribeBackupVault response")
			assert.Equal(t, tc.wantState, state)
		})
	}
}

// ---- 2. ListBackupVaults VaultState ----

func TestListBackupVaultsVaultState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
		wantState string
	}{
		{name: "list_item_has_vault_state", vaultName: "list-vault-1", wantState: "AVAILABLE"},
		{name: "second_vault_has_vault_state", vaultName: "list-vault-2", wantState: "AVAILABLE"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newBatch1Handler(t)

			doBatch1Request(t, h, http.MethodPut, "/backup-vaults/"+tc.vaultName, `{}`)

			resp := doBatch1Request(t, h, http.MethodGet, "/backup-vaults", "")
			require.Equal(t, http.StatusOK, resp.Code)

			var data map[string]any
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &data))

			list, ok := data["BackupVaultList"].([]any)
			require.True(t, ok, "BackupVaultList must be present")
			require.Len(t, list, 1)

			item := list[0].(map[string]any)
			state, exists := item["VaultState"]
			require.True(t, exists, "VaultState must be present in ListBackupVaults item")
			assert.Equal(t, tc.wantState, state)
		})
	}
}

// ---- 3. DescribeBackupVault Locked field ----

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
			h, _ := newBatch1Handler(t)

			doBatch1Request(t, h, http.MethodPut, "/backup-vaults/locked-vault", `{}`)

			if tc.applyLock {
				lockResp := doBatch1Request(t, h, http.MethodPut,
					"/backup-vaults/locked-vault/vault-lock", tc.lockBody)
				require.Equal(t, http.StatusOK, lockResp.Code)
			}

			resp := doBatch1Request(t, h, http.MethodGet, "/backup-vaults/locked-vault", "")
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
