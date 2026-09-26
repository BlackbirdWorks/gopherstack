package terraform_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsvc "github.com/aws/aws-sdk-go-v2/service/backup"
	backuptypes "github.com/aws/aws-sdk-go-v2/service/backup/types"
	ssmsvc "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mega17ProviderBlock is providerBlock with skip_requesting_account_id
// forced to false. aws_backup_global_settings/aws_backup_region_settings
// use the caller's account ID as their Terraform resource ID (like
// aws_macie2_account, see macie2ProviderBlock's comment); with
// skip_requesting_account_id=true that ID is "", which the provider reads
// back as "the object was present, but now absent" right after apply.
func mega17ProviderBlock(addr string) string {
	return strings.Replace(
		providerBlock(addr),
		"skip_requesting_account_id  = true",
		"skip_requesting_account_id  = false",
		1,
	)
}

// TestTerraform_SsmAndBackup provisions the 11 SSM resource types (activation,
// association, default patch baseline, document, maintenance window plus
// target/task, patch baseline plus patch group, resource data sync, service
// setting) and the 12 Backup resource types (framework, global settings,
// logically air-gapped vault, plan, region settings, report plan, restore
// testing plan plus selection, selection, vault lock configuration, vault
// notifications, vault policy) that had no Terraform fixture coverage, and
// verifies each via its own SDK client's Get/List path.
func TestTerraform_SsmAndBackup(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:       "success",
			fixture:    "ssm-and-backup",
			providerFn: mega17ProviderBlock,
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				cfg := megaConfig(t)

				ssmc := ssmsvc.NewFromConfig(cfg, func(o *ssmsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				bk := backupsvc.NewFromConfig(cfg, func(o *backupsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})

				verifySsmAndBackupSSM(ctx, t, ssmc)
				verifySsmAndBackupBackup(ctx, t, bk)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

func verifySsmAndBackupSSM(ctx context.Context, t *testing.T, c *ssmsvc.Client) {
	t.Helper()

	actOut, err := c.DescribeActivations(ctx, &ssmsvc.DescribeActivationsInput{})
	require.NoError(t, err, "DescribeActivations should succeed")

	var foundActivation bool

	for _, a := range actOut.ActivationList {
		if aws.ToInt32(a.RegistrationLimit) == 5 {
			foundActivation = true
		}
	}

	assert.True(t, foundActivation, "ssm activation should be listed")

	docOut, err := c.GetDocument(ctx, &ssmsvc.GetDocumentInput{
		Name: aws.String("ssbk-document"),
	})
	require.NoError(t, err, "GetDocument should succeed")
	assert.Contains(t, aws.ToString(docOut.Content), "runShellScript")

	assocOut, err := c.DescribeAssociation(ctx, &ssmsvc.DescribeAssociationInput{
		AssociationId: nil,
		Name:          aws.String("ssbk-document"),
	})
	require.NoError(t, err, "DescribeAssociation should succeed")
	require.NotNil(t, assocOut.AssociationDescription)
	assert.Equal(t, "ssbk-document", aws.ToString(assocOut.AssociationDescription.Name))

	winOut, err := c.DescribeMaintenanceWindows(ctx, &ssmsvc.DescribeMaintenanceWindowsInput{})
	require.NoError(t, err, "DescribeMaintenanceWindows should succeed")

	var windowID string

	for _, w := range winOut.WindowIdentities {
		if aws.ToString(w.Name) == "ssbk-window" {
			windowID = aws.ToString(w.WindowId)
		}
	}

	require.NotEmpty(t, windowID, "maintenance window should be listed")

	targetOut, err := c.DescribeMaintenanceWindowTargets(ctx, &ssmsvc.DescribeMaintenanceWindowTargetsInput{
		WindowId: aws.String(windowID),
	})
	require.NoError(t, err, "DescribeMaintenanceWindowTargets should succeed")

	var foundTarget bool

	for _, tg := range targetOut.Targets {
		if aws.ToString(tg.Name) == "ssbk-window-target" {
			foundTarget = true
		}
	}

	assert.True(t, foundTarget, "maintenance window target should be listed")

	taskOut, err := c.DescribeMaintenanceWindowTasks(ctx, &ssmsvc.DescribeMaintenanceWindowTasksInput{
		WindowId: aws.String(windowID),
	})
	require.NoError(t, err, "DescribeMaintenanceWindowTasks should succeed")
	require.NotEmpty(t, taskOut.Tasks, "maintenance window task should be listed")
	assert.Equal(t, "AWS-RunShellScript", aws.ToString(taskOut.Tasks[0].TaskArn))

	pbOut, err := c.DescribePatchBaselines(ctx, &ssmsvc.DescribePatchBaselinesInput{})
	require.NoError(t, err, "DescribePatchBaselines should succeed")

	var baselineID string

	for _, b := range pbOut.BaselineIdentities {
		if aws.ToString(b.BaselineName) == "ssbk-patch-baseline" {
			baselineID = aws.ToString(b.BaselineId)
		}
	}

	require.NotEmpty(t, baselineID, "patch baseline should be listed")

	defOut, err := c.GetDefaultPatchBaseline(ctx, &ssmsvc.GetDefaultPatchBaselineInput{
		OperatingSystem: "AMAZON_LINUX_2",
	})
	require.NoError(t, err, "GetDefaultPatchBaseline should succeed")
	assert.Equal(t, baselineID, aws.ToString(defOut.BaselineId))

	pgOut, err := c.DescribePatchGroups(ctx, &ssmsvc.DescribePatchGroupsInput{})
	require.NoError(t, err, "DescribePatchGroups should succeed")

	var foundPatchGroup bool

	for _, m := range pgOut.Mappings {
		if aws.ToString(m.PatchGroup) == "ssbk-patch-group" {
			foundPatchGroup = true
		}
	}

	assert.True(t, foundPatchGroup, "patch group mapping should be listed")

	syncOut, err := c.ListResourceDataSync(ctx, &ssmsvc.ListResourceDataSyncInput{})
	require.NoError(t, err, "ListResourceDataSync should succeed")

	var foundSync bool

	for _, s := range syncOut.ResourceDataSyncItems {
		if aws.ToString(s.SyncName) == "ssbk-sync" {
			foundSync = true
		}
	}

	assert.True(t, foundSync, "resource data sync should be listed")

	settingID := "arn:aws:ssm:us-east-1:000000000000:servicesetting/ssm/parameter-store/high-throughput-enabled"
	settingOut, err := c.GetServiceSetting(ctx, &ssmsvc.GetServiceSettingInput{
		SettingId: aws.String(settingID),
	})
	require.NoError(t, err, "GetServiceSetting should succeed")
	assert.Equal(t, "true", aws.ToString(settingOut.ServiceSetting.SettingValue))
}

func verifySsmAndBackupBackup(ctx context.Context, t *testing.T, c *backupsvc.Client) {
	t.Helper()

	frameworkOut, err := c.DescribeFramework(ctx, &backupsvc.DescribeFrameworkInput{
		FrameworkName: aws.String("ssbk_framework"),
	})
	require.NoError(t, err, "DescribeFramework should succeed")
	require.NotEmpty(t, frameworkOut.FrameworkControls)
	assert.Equal(
		t,
		"BACKUP_RECOVERY_POINT_MINIMUM_RETENTION_CHECK",
		aws.ToString(frameworkOut.FrameworkControls[0].ControlName),
	)

	gsOut, err := c.DescribeGlobalSettings(ctx, &backupsvc.DescribeGlobalSettingsInput{})
	require.NoError(t, err, "DescribeGlobalSettings should succeed")
	assert.Equal(t, "true", gsOut.GlobalSettings["isCrossAccountBackupEnabled"])

	rsOut, err := c.DescribeRegionSettings(ctx, &backupsvc.DescribeRegionSettingsInput{})
	require.NoError(t, err, "DescribeRegionSettings should succeed")
	assert.False(t, rsOut.ResourceTypeManagementPreference["DynamoDB"])

	vaultsOut, err := c.ListBackupVaults(ctx, &backupsvc.ListBackupVaultsInput{})
	require.NoError(t, err, "ListBackupVaults should succeed")

	var foundVault, foundLagVault bool

	for _, v := range vaultsOut.BackupVaultList {
		switch aws.ToString(v.BackupVaultName) {
		case "ssbk-vault":
			foundVault = true
		case "ssbk-lag-vault":
			foundLagVault = true
		}
	}

	assert.True(t, foundVault, "backup vault should be listed")
	assert.True(t, foundLagVault, "logically air-gapped vault should be listed")

	planOut, err := c.ListBackupPlans(ctx, &backupsvc.ListBackupPlansInput{})
	require.NoError(t, err, "ListBackupPlans should succeed")

	var planID string

	for _, p := range planOut.BackupPlansList {
		if aws.ToString(p.BackupPlanName) == "ssbk-plan" {
			planID = aws.ToString(p.BackupPlanId)
		}
	}

	require.NotEmpty(t, planID, "backup plan should be listed")

	selOut, err := c.ListBackupSelections(ctx, &backupsvc.ListBackupSelectionsInput{
		BackupPlanId: aws.String(planID),
	})
	require.NoError(t, err, "ListBackupSelections should succeed")

	var foundSelection bool

	for _, s := range selOut.BackupSelectionsList {
		if aws.ToString(s.SelectionName) == "ssbk-selection" {
			foundSelection = true
		}
	}

	assert.True(t, foundSelection, "backup selection should be listed")

	reportOut, err := c.DescribeReportPlan(ctx, &backupsvc.DescribeReportPlanInput{
		ReportPlanName: aws.String("ssbk_report_plan"),
	})
	require.NoError(t, err, "DescribeReportPlan should succeed")
	require.NotNil(t, reportOut.ReportPlan.ReportDeliveryChannel)
	assert.Equal(
		t,
		"ssbk-backup-reports",
		aws.ToString(reportOut.ReportPlan.ReportDeliveryChannel.S3BucketName),
	)

	rtPlanOut, err := c.GetRestoreTestingPlan(ctx, &backupsvc.GetRestoreTestingPlanInput{
		RestoreTestingPlanName: aws.String("ssbk_restore_testing_plan"),
	})
	require.NoError(t, err, "GetRestoreTestingPlan should succeed")
	require.NotNil(t, rtPlanOut.RestoreTestingPlan.RecoveryPointSelection)
	assert.Equal(t, "LATEST_WITHIN_WINDOW", string(rtPlanOut.RestoreTestingPlan.RecoveryPointSelection.Algorithm))

	rtSelOut, err := c.GetRestoreTestingSelection(ctx, &backupsvc.GetRestoreTestingSelectionInput{
		RestoreTestingPlanName:      aws.String("ssbk_restore_testing_plan"),
		RestoreTestingSelectionName: aws.String("ssbk_restore_testing_selection"),
	})
	require.NoError(t, err, "GetRestoreTestingSelection should succeed")
	assert.Equal(t, "EC2", aws.ToString(rtSelOut.RestoreTestingSelection.ProtectedResourceType))

	lockOut, err := c.DescribeBackupVault(ctx, &backupsvc.DescribeBackupVaultInput{
		BackupVaultName: aws.String("ssbk-vault"),
	})
	require.NoError(t, err, "DescribeBackupVault should succeed")
	assert.EqualValues(t, 365, aws.ToInt64(lockOut.MaxRetentionDays))
	assert.EqualValues(t, 7, aws.ToInt64(lockOut.MinRetentionDays))

	notifOut, err := c.GetBackupVaultNotifications(ctx, &backupsvc.GetBackupVaultNotificationsInput{
		BackupVaultName: aws.String("ssbk-vault"),
	})
	require.NoError(t, err, "GetBackupVaultNotifications should succeed")
	assert.Contains(t, notifOut.BackupVaultEvents, backuptypes.BackupVaultEvent("BACKUP_JOB_STARTED"))

	policyOut, err := c.GetBackupVaultAccessPolicy(ctx, &backupsvc.GetBackupVaultAccessPolicyInput{
		BackupVaultName: aws.String("ssbk-vault"),
	})
	require.NoError(t, err, "GetBackupVaultAccessPolicy should succeed")
	assert.Contains(t, aws.ToString(policyOut.Policy), "backup:DescribeBackupVault")
}
