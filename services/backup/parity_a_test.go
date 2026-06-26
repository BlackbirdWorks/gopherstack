package backup_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestParity_ListBackupSelections_IamRoleArn verifies that ListBackupSelections
// includes IamRoleArn in each selection item, matching real AWS behavior.
func TestParity_ListBackupSelections_IamRoleArn(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()

	createPlanRec := doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
		"BackupPlan": map[string]any{
			"BackupPlanName": "sel-iam-plan",
			"Rules": []map[string]any{
				{
					"RuleName":              "daily",
					"TargetBackupVaultName": "Default",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, createPlanRec.Code)
	planID, _ := parseResp(t, createPlanRec)["BackupPlanId"].(string)
	require.NotEmpty(t, planID)

	const roleArn = "arn:aws:iam::123456789012:role/backup-role"
	createSelRec := doREST(t, h, http.MethodPut, "/backup/plans/"+planID+"/selections", map[string]any{
		"BackupSelection": map[string]any{
			"SelectionName": "my-selection",
			"IamRoleArn":    roleArn,
		},
	})
	require.Equal(t, http.StatusOK, createSelRec.Code, "create selection: %s", createSelRec.Body)

	listRec := doREST(t, h, http.MethodGet, "/backup/plans/"+planID+"/selections", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	resp := parseResp(t, listRec)
	items, _ := resp["BackupSelectionsList"].([]any)
	require.NotEmpty(t, items, "BackupSelectionsList must not be empty")

	item, _ := items[0].(map[string]any)
	arn, hasArn := item["IamRoleArn"]
	assert.True(t, hasArn, "ListBackupSelections item must include IamRoleArn")
	assert.Equal(t, roleArn, arn, "IamRoleArn must match what was set on creation")
}

// TestParity_DescribeBackupVault_VaultType verifies DescribeBackupVault returns
// VaultType as "BACKUP_VAULT" for regular vaults, matching real AWS behavior.
func TestParity_DescribeBackupVault_VaultType(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()

	createRec := doREST(t, h, http.MethodPut, "/backup-vaults/parity-vault", map[string]any{})
	require.Equal(t, http.StatusOK, createRec.Code, "create vault: %s", createRec.Body)

	getRec := doREST(t, h, http.MethodGet, "/backup-vaults/parity-vault", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	resp := parseResp(t, getRec)
	vaultType, hasField := resp["VaultType"]
	assert.True(t, hasField, "DescribeBackupVault must return VaultType")
	assert.Equal(t, "BACKUP_VAULT", vaultType, "regular vault VaultType must be BACKUP_VAULT")
}

// TestParity_ListBackupVaults_VaultType verifies ListBackupVaults items include
// VaultType, matching real AWS behavior.
func TestParity_ListBackupVaults_VaultType(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()

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
}

// TestParity_AirGappedVault_VaultType verifies CreateLogicallyAirGappedBackupVault
// returns VaultType as "LOGICALLY_AIR_GAPPED_BACKUP_VAULT", matching real AWS behavior.
func TestParity_AirGappedVault_VaultType(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()

	createRec := doREST(t, h, http.MethodPut, "/logically-air-gapped-backup-vaults/ag-vault", map[string]any{
		"MinRetentionDays": 7,
		"MaxRetentionDays": 365,
	})
	require.Equal(t, http.StatusOK, createRec.Code, "create air-gapped vault: %s", createRec.Body)

	resp := parseResp(t, createRec)
	vaultType, hasField := resp["VaultType"]
	assert.True(t, hasField, "CreateLogicallyAirGappedBackupVault must return VaultType")
	assert.Equal(t, "LOGICALLY_AIR_GAPPED_BACKUP_VAULT", vaultType)
}
