package backup_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

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
			h, _ := newHandler(t)

			doRequest(t, h, http.MethodPut, "/backup-vaults/state-vault", `{}`)

			resp := doRequest(t, h, http.MethodGet, "/backup-vaults/state-vault", "")
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
			h, _ := newHandler(t)

			doRequest(t, h, http.MethodPut, "/backup-vaults/"+tc.vaultName, `{}`)

			resp := doRequest(t, h, http.MethodGet, "/backup-vaults", "")
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
			h, _ := newHandler(t)
			resp := doRequest(t, h, http.MethodPut, "/backup-vaults/"+tt.vaultName, `{}`)
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
			h, _ := newHandler(t)

			r1 := doRequest(t, h, http.MethodPut, "/backup-vaults/idem-vault", tt.firstRequest)
			require.Equal(t, http.StatusOK, r1.Code)
			var d1 map[string]any
			require.NoError(t, json.Unmarshal(r1.Body.Bytes(), &d1))
			arn1 := d1["BackupVaultArn"].(string)

			r2 := doRequest(t, h, http.MethodPut, "/backup-vaults/idem-vault", tt.secondRequest)
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

func TestDeleteVaultWithRecoveryPoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		seedRP     bool
		wantStatus int
	}{
		{
			name:       "empty_vault_deletes_ok",
			seedRP:     false,
			wantStatus: http.StatusOK,
		},
		{
			name:       "vault_with_recovery_point_rejected",
			seedRP:     true,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, be := newHandlerAndBackend()
			rec := doREST(t, h, http.MethodPut, "/backup-vaults/dvault", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			if tc.seedRP {
				rpArn := "arn:aws:backup:us-east-1:123456789012:recovery-point:rp-001"
				err := be.AddRecoveryPoint("dvault", &backup.RecoveryPoint{
					RecoveryPointArn: rpArn,
					BackupVaultName:  "dvault",
					Status:           "COMPLETED",
				})
				require.NoError(t, err)
			}

			rec2 := doREST(t, h, http.MethodDelete, "/backup-vaults/dvault", nil)
			assert.Equal(t, tc.wantStatus, rec2.Code)
		})
	}
}

// ---- 2. GetBackupVaultAccessPolicy returns real BackupVaultArn ----

func TestListBackupVaultsEncryptionKeyArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		encryptionKeyArn string
		expectInResponse bool
	}{
		{
			name:             "encrypted_vault_shows_key_arn",
			encryptionKeyArn: "arn:aws:kms:us-east-1:123456789012:key/test-key",
			expectInResponse: true,
		},
		{
			name:             "unencrypted_vault_omits_key_arn",
			encryptionKeyArn: "",
			expectInResponse: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			doREST(t, h, http.MethodPut, "/backup-vaults/kvault", map[string]any{
				"EncryptionKeyArn": tc.encryptionKeyArn,
			})

			rec := doREST(t, h, http.MethodGet, "/backup-vaults", nil)
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := parseResp(t, rec)
			list, _ := resp["BackupVaultList"].([]any)
			require.NotEmpty(t, list)

			var found map[string]any
			for _, item := range list {
				m := item.(map[string]any)
				if m["BackupVaultName"] == "kvault" {
					found = m

					break
				}
			}
			require.NotNil(t, found, "kvault not found in list")

			_, present := found["EncryptionKeyArn"]
			assert.Equal(t, tc.expectInResponse, present)
			if tc.expectInResponse {
				assert.Equal(t, tc.encryptionKeyArn, found["EncryptionKeyArn"])
			}
		})
	}
}

// ---- 8. RestoreTestingPlan StartWindowHours round-trip ----

// TestBackupVaultCRUD exercises CreateBackupVault, DescribeBackupVault,
// ListBackupVaults, and DeleteBackupVault through the REST handler.
func TestBackupVaultCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *backup.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "create_vault",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/backup-vaults/my-vault", map[string]any{
					"BackupVaultTags": map[string]string{"Env": "test"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				assert.Equal(t, "my-vault", resp["BackupVaultName"])
				assert.NotEmpty(t, resp["BackupVaultArn"])
				assert.NotNil(t, resp["CreationDate"])
			},
		},
		{
			name: "describe_vault",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/backup-vaults/my-vault", nil)
				rec := doREST(t, h, http.MethodGet, "/backup-vaults/my-vault", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				assert.Equal(t, "my-vault", resp["BackupVaultName"])
			},
		},
		{
			name: "describe_vault_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodGet, "/backup-vaults/missing", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "list_vaults",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/backup-vaults/vault-a", nil)
				doREST(t, h, http.MethodPut, "/backup-vaults/vault-b", nil)
				rec := doREST(t, h, http.MethodGet, "/backup-vaults", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				list, ok := resp["BackupVaultList"].([]any)
				require.True(t, ok)
				assert.Len(t, list, 2)
			},
		},
		{
			name: "delete_vault",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/backup-vaults/del-vault", nil)
				rec := doREST(t, h, http.MethodDelete, "/backup-vaults/del-vault", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				rec2 := doREST(t, h, http.MethodGet, "/backup-vaults/del-vault", nil)
				assert.Equal(t, http.StatusNotFound, rec2.Code)
			},
		},
		{
			name: "create_vault_duplicate",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/backup-vaults/dup-vault", nil)
				rec := doREST(t, h, http.MethodPut, "/backup-vaults/dup-vault", nil)
				assert.Equal(t, http.StatusConflict, rec.Code)
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

// TestAssociateBackupVaultMpaApprovalTeam exercises the MPA approval team association operation.
func TestAssociateBackupVaultMpaApprovalTeam(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *backup.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/backup-vaults/my-vault", nil)
				rec := doREST(t, h, http.MethodPut, "/backup-vaults/my-vault/mpaApprovalTeam", map[string]any{
					"MpaApprovalTeamArn": "arn:aws:mpa:us-east-1:123456789012:approval-team/my-team",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "vault_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/backup-vaults/missing/mpaApprovalTeam", map[string]any{
					"MpaApprovalTeamArn": "arn:aws:mpa:us-east-1:123:approval-team/t",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "missing_arn",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/backup-vaults/vault-x", nil)
				rec := doREST(t, h, http.MethodPut, "/backup-vaults/vault-x/mpaApprovalTeam", map[string]any{
					"MpaApprovalTeamArn": "",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
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

// TestCreateLogicallyAirGappedBackupVault exercises the logically air-gapped vault creation.
func TestCreateLogicallyAirGappedBackupVault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *backup.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/logically-air-gapped-backup-vaults/lag-vault", map[string]any{
					"MinRetentionDays": 7,
					"MaxRetentionDays": 365,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				assert.Equal(t, "lag-vault", resp["BackupVaultName"])
				assert.NotEmpty(t, resp["BackupVaultArn"])
				assert.Equal(t, "CREATING", resp["VaultState"])
			},
		},
		{
			name: "duplicate_name",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/logically-air-gapped-backup-vaults/dup-lag", map[string]any{
					"MinRetentionDays": 7,
					"MaxRetentionDays": 30,
				})
				rec := doREST(t, h, http.MethodPut, "/logically-air-gapped-backup-vaults/dup-lag", map[string]any{
					"MinRetentionDays": 7,
					"MaxRetentionDays": 30,
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
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

// TestCreateRestoreAccessBackupVault exercises the restore access vault creation.
func TestCreateRestoreAccessBackupVault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *backup.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/restore-access-backup-vaults", map[string]any{
					"SourceBackupVaultArn": "arn:aws:backup:us-east-1:123456789012:backup-vault:source-vault",
					"BackupVaultName":      "restore-access-vault",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				assert.Equal(t, "restore-access-vault", resp["RestoreAccessBackupVaultName"])
				assert.NotEmpty(t, resp["RestoreAccessBackupVaultArn"])
				assert.Equal(t, "CREATING", resp["VaultState"])
			},
		},
		{
			name: "missing_source_arn",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/restore-access-backup-vaults", map[string]any{
					"BackupVaultName": "some-vault",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
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

// TestBackupVaultValidation covers vault name validation.
func TestBackupVaultValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		vaultName  string
		wantStatus int
	}{
		{
			name:       "valid_name",
			vaultName:  "my-vault",
			wantStatus: http.StatusOK,
		},
		{
			name:       "too_short",
			vaultName:  "a",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "underscore_allowed",
			vaultName:  "my_vault_under",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_chars",
			vaultName:  "my.vault.dot",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "exactly_2_chars",
			vaultName:  "ab",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestBackupHandler()
			rec := doREST(t, h, http.MethodPut, "/backup-vaults/"+tt.vaultName, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAirGappedVaultValidation covers retention-day validation.
func TestAirGappedVaultValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "valid",
			body: map[string]any{
				"MinRetentionDays": 1,
				"MaxRetentionDays": 30,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "min_zero",
			body: map[string]any{
				"MinRetentionDays": 0,
				"MaxRetentionDays": 30,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "max_less_than_min",
			body: map[string]any{
				"MinRetentionDays": 10,
				"MaxRetentionDays": 5,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestBackupHandler()
			rec := doREST(t, h, http.MethodPut, "/logically-air-gapped-backup-vaults/air-vault", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestVaultType verifies that vault responses include VaultType with the
// correct value for regular and logically air-gapped vaults, matching real AWS
// behavior.
func TestVaultType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *backup.Handler)
		name string
	}{
		{
			name: "describe_backup_vault_vault_type",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				createRec := doREST(t, h, http.MethodPut, "/backup-vaults/parity-vault", map[string]any{})
				require.Equal(t, http.StatusOK, createRec.Code, "create vault: %s", createRec.Body)

				getRec := doREST(t, h, http.MethodGet, "/backup-vaults/parity-vault", nil)
				require.Equal(t, http.StatusOK, getRec.Code)

				resp := parseResp(t, getRec)
				vaultType, hasField := resp["VaultType"]
				assert.True(t, hasField, "DescribeBackupVault must return VaultType")
				assert.Equal(t, "BACKUP_VAULT", vaultType, "regular vault VaultType must be BACKUP_VAULT")
			},
		},
		{
			name: "list_backup_vaults_vault_type",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPut, "/backup-vaults/list-vt-vault", map[string]any{})

				listRec := doREST(t, h, http.MethodGet, "/backup-vaults", nil)
				require.Equal(t, http.StatusOK, listRec.Code)

				resp := parseResp(t, listRec)
				vaults, _ := resp["BackupVaultList"].([]any)
				require.NotEmpty(t, vaults)

				var found bool

				for _, v := range vaults {
					vm, _ := v.(map[string]any)
					if vm["BackupVaultName"] == "list-vt-vault" {
						vt, hasField := vm["VaultType"]
						assert.True(t, hasField, "ListBackupVaults item must include VaultType")
						assert.Equal(t, "BACKUP_VAULT", vt)
						found = true
					}
				}

				assert.True(t, found, "list-vt-vault must appear in BackupVaultList")
			},
		},
		{
			name: "air_gapped_vault_vault_type",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				createRec := doREST(
					t, h, http.MethodPut, "/logically-air-gapped-backup-vaults/ag-vault",
					map[string]any{
						"MinRetentionDays": 7,
						"MaxRetentionDays": 365,
					},
				)
				require.Equal(t, http.StatusOK, createRec.Code, "create air-gapped vault: %s", createRec.Body)

				resp := parseResp(t, createRec)
				vaultType, hasField := resp["VaultType"]
				assert.True(t, hasField, "CreateLogicallyAirGappedBackupVault must return VaultType")
				assert.Equal(t, "LOGICALLY_AIR_GAPPED_BACKUP_VAULT", vaultType)
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
