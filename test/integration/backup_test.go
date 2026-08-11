package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	backuptypes "github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_Backup_VaultAndPlanLifecycle exercises the most common AWS
// Backup workflow via the AWS SDK v2: create a backup vault, list/describe it,
// then create a backup plan referencing that vault, list/get/delete it, and
// finally delete the vault. This is the primary integration coverage for the
// AWS Backup REST handler and protects against regressions in path routing and
// JSON body shapes.
func TestIntegration_Backup_VaultAndPlanLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createBackupClient(t)
	ctx := t.Context()

	const (
		vaultName = "it-backup-vault"
		planName  = "it-backup-plan"
		ruleName  = "DailyBackups"
		schedule  = "cron(0 5 ? * * *)"
	)

	// CreateBackupVault.
	createVaultOut, err := client.CreateBackupVault(ctx, &backupsdk.CreateBackupVaultInput{
		BackupVaultName: aws.String(vaultName),
		BackupVaultTags: map[string]string{
			"env": "test",
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(createVaultOut.BackupVaultArn))
	assert.Equal(t, vaultName, aws.ToString(createVaultOut.BackupVaultName))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteBackupVault(cleanupCtx, &backupsdk.DeleteBackupVaultInput{
			BackupVaultName: aws.String(vaultName),
		})
	})

	// DescribeBackupVault.
	descVaultOut, err := client.DescribeBackupVault(ctx, &backupsdk.DescribeBackupVaultInput{
		BackupVaultName: aws.String(vaultName),
	})
	require.NoError(t, err)
	assert.Equal(t, vaultName, aws.ToString(descVaultOut.BackupVaultName))

	// ListBackupVaults - should contain the new vault.
	listVaultsOut, err := client.ListBackupVaults(ctx, &backupsdk.ListBackupVaultsInput{})
	require.NoError(t, err)

	foundVault := false

	for _, v := range listVaultsOut.BackupVaultList {
		if aws.ToString(v.BackupVaultName) == vaultName {
			foundVault = true

			break
		}
	}

	assert.True(t, foundVault, "newly created vault should be listed")

	// CreateBackupPlan referencing the vault.
	createPlanOut, err := client.CreateBackupPlan(ctx, &backupsdk.CreateBackupPlanInput{
		BackupPlan: &backuptypes.BackupPlanInput{
			BackupPlanName: aws.String(planName),
			Rules: []backuptypes.BackupRuleInput{
				{
					RuleName:                aws.String(ruleName),
					TargetBackupVaultName:   aws.String(vaultName),
					ScheduleExpression:      aws.String(schedule),
					StartWindowMinutes:      aws.Int64(60),
					CompletionWindowMinutes: aws.Int64(180),
				},
			},
		},
		BackupPlanTags: map[string]string{
			"env": "test",
		},
	})
	require.NoError(t, err)

	planID := aws.ToString(createPlanOut.BackupPlanId)
	require.NotEmpty(t, planID)
	require.NotEmpty(t, aws.ToString(createPlanOut.BackupPlanArn))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteBackupPlan(cleanupCtx, &backupsdk.DeleteBackupPlanInput{
			BackupPlanId: aws.String(planID),
		})
	})

	// GetBackupPlan.
	getPlanOut, err := client.GetBackupPlan(ctx, &backupsdk.GetBackupPlanInput{
		BackupPlanId: aws.String(planID),
	})
	require.NoError(t, err)
	require.NotNil(t, getPlanOut.BackupPlan)
	assert.Equal(t, planName, aws.ToString(getPlanOut.BackupPlan.BackupPlanName))
	require.Len(t, getPlanOut.BackupPlan.Rules, 1)
	assert.Equal(t, ruleName, aws.ToString(getPlanOut.BackupPlan.Rules[0].RuleName))
	assert.Equal(t, vaultName, aws.ToString(getPlanOut.BackupPlan.Rules[0].TargetBackupVaultName))

	// ListBackupPlans should contain the new plan.
	listPlansOut, err := client.ListBackupPlans(ctx, &backupsdk.ListBackupPlansInput{})
	require.NoError(t, err)

	foundPlan := false

	for _, p := range listPlansOut.BackupPlansList {
		if aws.ToString(p.BackupPlanId) == planID {
			foundPlan = true

			break
		}
	}

	assert.True(t, foundPlan, "newly created plan should be listed")
}
