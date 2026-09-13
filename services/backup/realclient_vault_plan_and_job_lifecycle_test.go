package backup_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// newRealClient returns a fresh backend/handler/typed-client trio for one
// subtest.
func newRealClient(t *testing.T) (*backup.InMemoryBackend, *backupsdk.Client) {
	t.Helper()

	b := newTestBackend(t)
	h := backup.NewHandler(b)

	return b, newTestBackupClient(t, h)
}

func TestRealClient_VaultPlanAndJobLifecycle(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "logically_air_gapped_vault_and_mpa_and_restore_access", run: func(t *testing.T) {
			t.Helper()

			_, client := newRealClient(t)
			ctx := t.Context()

			lagOut, err := client.CreateLogicallyAirGappedBackupVault(
				ctx, &backupsdk.CreateLogicallyAirGappedBackupVaultInput{
					BackupVaultName:  aws.String("lag-vault"),
					MinRetentionDays: aws.Int64(7),
					MaxRetentionDays: aws.Int64(365),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, lagOut.BackupVaultArn)
			assert.Equal(t, types.VaultStateCreating, lagOut.VaultState)

			_, err = client.AssociateBackupVaultMpaApprovalTeam(
				ctx, &backupsdk.AssociateBackupVaultMpaApprovalTeamInput{
					BackupVaultName:    aws.String("lag-vault"),
					MpaApprovalTeamArn: aws.String("arn:aws:mpa:us-east-1:123456789012:approval-team/team-1"),
				},
			)
			require.NoError(t, err)

			descOut, err := client.DescribeBackupVault(ctx, &backupsdk.DescribeBackupVaultInput{
				BackupVaultName: aws.String("lag-vault"),
			})
			require.NoError(t, err)
			assert.Equal(
				t,
				"arn:aws:mpa:us-east-1:123456789012:approval-team/team-1",
				aws.ToString(descOut.MpaApprovalTeamArn),
			)

			_, err = client.DisassociateBackupVaultMpaApprovalTeam(
				ctx, &backupsdk.DisassociateBackupVaultMpaApprovalTeamInput{
					BackupVaultName: aws.String("lag-vault"),
				},
			)
			require.NoError(t, err)

			createRAV, err := client.CreateRestoreAccessBackupVault(
				ctx, &backupsdk.CreateRestoreAccessBackupVaultInput{
					SourceBackupVaultArn: lagOut.BackupVaultArn,
					RequesterComment:     aws.String("test restore access"),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, createRAV.RestoreAccessBackupVaultArn)

			listRAV, err := client.ListRestoreAccessBackupVaults(
				ctx, &backupsdk.ListRestoreAccessBackupVaultsInput{
					BackupVaultName: aws.String("lag-vault"),
				},
			)
			require.NoError(t, err)
			require.Len(t, listRAV.RestoreAccessBackupVaults, 1)
			assert.Equal(t,
				aws.ToString(createRAV.RestoreAccessBackupVaultArn),
				aws.ToString(listRAV.RestoreAccessBackupVaults[0].RestoreAccessBackupVaultArn),
			)

			_, err = client.RevokeRestoreAccessBackupVault(
				ctx, &backupsdk.RevokeRestoreAccessBackupVaultInput{
					BackupVaultName:             aws.String("lag-vault"),
					RestoreAccessBackupVaultArn: createRAV.RestoreAccessBackupVaultArn,
				},
			)
			require.NoError(t, err)

			listRAV2, err := client.ListRestoreAccessBackupVaults(
				ctx, &backupsdk.ListRestoreAccessBackupVaultsInput{
					BackupVaultName: aws.String("lag-vault"),
				},
			)
			require.NoError(t, err)
			assert.Empty(t, listRAV2.RestoreAccessBackupVaults)
		}},
		{name: "vault_access_policy_lifecycle", run: func(t *testing.T) {
			t.Helper()

			_, client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateBackupVault(ctx, &backupsdk.CreateBackupVaultInput{
				BackupVaultName: aws.String("policy-vault"),
			})
			require.NoError(t, err)

			policyDoc := `{"Version":"2012-10-17","Statement":[` +
				`{"Effect":"Allow","Principal":"*","Action":"backup:CopyIntoBackupVault","Resource":"*"}]}`
			_, err = client.PutBackupVaultAccessPolicy(ctx, &backupsdk.PutBackupVaultAccessPolicyInput{
				BackupVaultName: aws.String("policy-vault"),
				Policy:          aws.String(policyDoc),
			})
			require.NoError(t, err)

			getOut, err := client.GetBackupVaultAccessPolicy(ctx, &backupsdk.GetBackupVaultAccessPolicyInput{
				BackupVaultName: aws.String("policy-vault"),
			})
			require.NoError(t, err)
			assert.JSONEq(t, policyDoc, aws.ToString(getOut.Policy))
			assert.NotEmpty(t, aws.ToString(getOut.BackupVaultArn))

			_, err = client.DeleteBackupVaultAccessPolicy(ctx, &backupsdk.DeleteBackupVaultAccessPolicyInput{
				BackupVaultName: aws.String("policy-vault"),
			})
			require.NoError(t, err)

			_, err = client.GetBackupVaultAccessPolicy(ctx, &backupsdk.GetBackupVaultAccessPolicyInput{
				BackupVaultName: aws.String("policy-vault"),
			})
			require.Error(t, err)
		}},
		{name: "vault_lock_configuration_lifecycle", run: func(t *testing.T) {
			t.Helper()

			_, client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateBackupVault(ctx, &backupsdk.CreateBackupVaultInput{
				BackupVaultName: aws.String("lock-vault-12"),
			})
			require.NoError(t, err)

			_, err = client.PutBackupVaultLockConfiguration(
				ctx, &backupsdk.PutBackupVaultLockConfigurationInput{
					BackupVaultName:  aws.String("lock-vault-12"),
					MinRetentionDays: aws.Int64(7),
					MaxRetentionDays: aws.Int64(90),
				},
			)
			require.NoError(t, err)

			descOut, err := client.DescribeBackupVault(ctx, &backupsdk.DescribeBackupVaultInput{
				BackupVaultName: aws.String("lock-vault-12"),
			})
			require.NoError(t, err)
			require.NotNil(t, descOut.Locked)
			assert.True(t, *descOut.Locked)

			_, err = client.DeleteBackupVaultLockConfiguration(
				ctx, &backupsdk.DeleteBackupVaultLockConfigurationInput{
					BackupVaultName: aws.String("lock-vault-12"),
				},
			)
			require.NoError(t, err)

			descOut2, err := client.DescribeBackupVault(ctx, &backupsdk.DescribeBackupVaultInput{
				BackupVaultName: aws.String("lock-vault-12"),
			})
			require.NoError(t, err)
			require.NotNil(t, descOut2.Locked)
			assert.False(t, *descOut2.Locked)
		}},
		{name: "vault_notifications_lifecycle", run: func(t *testing.T) {
			t.Helper()

			_, client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateBackupVault(ctx, &backupsdk.CreateBackupVaultInput{
				BackupVaultName: aws.String("notif-vault"),
			})
			require.NoError(t, err)

			_, err = client.PutBackupVaultNotifications(ctx, &backupsdk.PutBackupVaultNotificationsInput{
				BackupVaultName: aws.String("notif-vault"),
				SNSTopicArn:     aws.String("arn:aws:sns:us-east-1:123456789012:backup-notify"),
				BackupVaultEvents: []types.BackupVaultEvent{
					types.BackupVaultEventBackupJobCompleted,
					types.BackupVaultEventBackupJobFailed,
				},
			})
			require.NoError(t, err)

			getOut, err := client.GetBackupVaultNotifications(ctx, &backupsdk.GetBackupVaultNotificationsInput{
				BackupVaultName: aws.String("notif-vault"),
			})
			require.NoError(t, err)
			assert.Equal(t, "arn:aws:sns:us-east-1:123456789012:backup-notify", aws.ToString(getOut.SNSTopicArn))
			assert.ElementsMatch(
				t,
				[]types.BackupVaultEvent{
					types.BackupVaultEventBackupJobCompleted,
					types.BackupVaultEventBackupJobFailed,
				},
				getOut.BackupVaultEvents,
			)

			_, err = client.DeleteBackupVaultNotifications(ctx, &backupsdk.DeleteBackupVaultNotificationsInput{
				BackupVaultName: aws.String("notif-vault"),
			})
			require.NoError(t, err)

			_, err = client.GetBackupVaultNotifications(ctx, &backupsdk.GetBackupVaultNotificationsInput{
				BackupVaultName: aws.String("notif-vault"),
			})
			require.Error(t, err)
		}},
		{name: "legal_hold_lifecycle", run: func(t *testing.T) {
			t.Helper()

			_, client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateBackupVault(ctx, &backupsdk.CreateBackupVaultInput{
				BackupVaultName: aws.String("lh-vault"),
			})
			require.NoError(t, err)

			createOut, err := client.CreateLegalHold(ctx, &backupsdk.CreateLegalHoldInput{
				Title:       aws.String("litigation-hold"),
				Description: aws.String("court order 2026"),
				RecoveryPointSelection: &types.RecoveryPointSelection{
					VaultNames: []string{"lh-vault"},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, createOut.LegalHoldId)

			getOut, err := client.GetLegalHold(ctx, &backupsdk.GetLegalHoldInput{
				LegalHoldId: createOut.LegalHoldId,
			})
			require.NoError(t, err)
			assert.Equal(t, "litigation-hold", aws.ToString(getOut.Title))
			assert.Equal(t, "court order 2026", aws.ToString(getOut.Description))
			require.NotNil(t, getOut.RecoveryPointSelection)
			assert.Equal(t, []string{"lh-vault"}, getOut.RecoveryPointSelection.VaultNames)

			listRP, err := client.ListRecoveryPointsByLegalHold(ctx, &backupsdk.ListRecoveryPointsByLegalHoldInput{
				LegalHoldId: createOut.LegalHoldId,
			})
			require.NoError(t, err)
			assert.NotNil(t, listRP.RecoveryPoints)

			_, err = client.CancelLegalHold(ctx, &backupsdk.CancelLegalHoldInput{
				LegalHoldId:       createOut.LegalHoldId,
				CancelDescription: aws.String("order lifted"),
			})
			require.NoError(t, err)

			_, err = client.GetLegalHold(ctx, &backupsdk.GetLegalHoldInput{
				LegalHoldId: createOut.LegalHoldId,
			})
			require.Error(t, err)
		}},
		{name: "tiering_configuration_lifecycle", run: func(t *testing.T) {
			t.Helper()

			_, client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateBackupVault(ctx, &backupsdk.CreateBackupVaultInput{
				BackupVaultName: aws.String("tier-vault"),
			})
			require.NoError(t, err)

			createOut, err := client.CreateTieringConfiguration(ctx, &backupsdk.CreateTieringConfigurationInput{
				TieringConfiguration: &types.TieringConfigurationInputForCreate{
					TieringConfigurationName: aws.String("my_tiering_config"),
					BackupVaultName:          aws.String("tier-vault"),
					ResourceSelection: []types.ResourceSelection{
						{
							ResourceType:              aws.String("S3"),
							Resources:                 []string{"*"},
							TieringDownSettingsInDays: aws.Int32(90),
						},
					},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, createOut.TieringConfigurationArn)

			getOut, err := client.GetTieringConfiguration(ctx, &backupsdk.GetTieringConfigurationInput{
				TieringConfigurationName: aws.String("my_tiering_config"),
			})
			require.NoError(t, err)
			require.NotNil(t, getOut.TieringConfiguration)
			assert.Equal(t, "tier-vault", aws.ToString(getOut.TieringConfiguration.BackupVaultName))
			require.Len(t, getOut.TieringConfiguration.ResourceSelection, 1)
			assert.EqualValues(
				t,
				90,
				aws.ToInt32(getOut.TieringConfiguration.ResourceSelection[0].TieringDownSettingsInDays),
			)

			listOut, err := client.ListTieringConfigurations(ctx, &backupsdk.ListTieringConfigurationsInput{})
			require.NoError(t, err)
			require.Len(t, listOut.TieringConfigurations, 1)

			updOut, err := client.UpdateTieringConfiguration(ctx, &backupsdk.UpdateTieringConfigurationInput{
				TieringConfigurationName: aws.String("my_tiering_config"),
				TieringConfiguration: &types.TieringConfigurationInputForUpdate{
					BackupVaultName: aws.String("tier-vault"),
					ResourceSelection: []types.ResourceSelection{
						{
							ResourceType:              aws.String("S3"),
							Resources:                 []string{"*"},
							TieringDownSettingsInDays: aws.Int32(120),
						},
					},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, updOut.LastUpdatedTime)

			_, err = client.DeleteTieringConfiguration(ctx, &backupsdk.DeleteTieringConfigurationInput{
				TieringConfigurationName: aws.String("my_tiering_config"),
			})
			require.NoError(t, err)

			_, err = client.GetTieringConfiguration(ctx, &backupsdk.GetTieringConfigurationInput{
				TieringConfigurationName: aws.String("my_tiering_config"),
			})
			require.Error(t, err)
		}},
		{name: "framework_lifecycle", run: func(t *testing.T) {
			t.Helper()

			_, client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateFramework(ctx, &backupsdk.CreateFrameworkInput{
				FrameworkName:        aws.String("my_framework"),
				FrameworkDescription: aws.String("initial description"),
				FrameworkControls: []types.FrameworkControl{
					{ControlName: aws.String("BACKUP_RECOVERY_POINT_ENCRYPTED")},
				},
			})
			require.NoError(t, err)

			descOut, err := client.DescribeFramework(ctx, &backupsdk.DescribeFrameworkInput{
				FrameworkName: aws.String("my_framework"),
			})
			require.NoError(t, err)
			assert.Equal(t, "initial description", aws.ToString(descOut.FrameworkDescription))
			require.Len(t, descOut.FrameworkControls, 1)
			assert.Equal(t, "BACKUP_RECOVERY_POINT_ENCRYPTED", aws.ToString(descOut.FrameworkControls[0].ControlName))

			_, err = client.UpdateFramework(ctx, &backupsdk.UpdateFrameworkInput{
				FrameworkName:        aws.String("my_framework"),
				FrameworkDescription: aws.String("updated description"),
			})
			require.NoError(t, err)

			descOut2, err := client.DescribeFramework(ctx, &backupsdk.DescribeFrameworkInput{
				FrameworkName: aws.String("my_framework"),
			})
			require.NoError(t, err)
			assert.Equal(t, "updated description", aws.ToString(descOut2.FrameworkDescription))

			require.Len(t, descOut2.FrameworkControls, 1)

			_, err = client.DeleteFramework(ctx, &backupsdk.DeleteFrameworkInput{
				FrameworkName: aws.String("my_framework"),
			})
			require.NoError(t, err)

			_, err = client.DescribeFramework(ctx, &backupsdk.DescribeFrameworkInput{
				FrameworkName: aws.String("my_framework"),
			})
			require.Error(t, err)
		}},
		{name: "report_plan_lifecycle", run: func(t *testing.T) {
			t.Helper()

			_, client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateReportPlan(ctx, &backupsdk.CreateReportPlanInput{
				ReportPlanName:        aws.String("my_report_plan"),
				ReportPlanDescription: aws.String("compliance reports"),
				ReportDeliveryChannel: &types.ReportDeliveryChannel{
					S3BucketName: aws.String("my-report-bucket"),
				},
				ReportSetting: &types.ReportSetting{
					ReportTemplate: aws.String("RESOURCE_COMPLIANCE_REPORT"),
				},
			})
			require.NoError(t, err)

			descOut, err := client.DescribeReportPlan(ctx, &backupsdk.DescribeReportPlanInput{
				ReportPlanName: aws.String("my_report_plan"),
			})
			require.NoError(t, err)
			require.NotNil(t, descOut.ReportPlan)
			assert.Equal(t, "compliance reports", aws.ToString(descOut.ReportPlan.ReportPlanDescription))
			require.NotNil(t, descOut.ReportPlan.ReportDeliveryChannel)
			assert.Equal(t, "my-report-bucket", aws.ToString(descOut.ReportPlan.ReportDeliveryChannel.S3BucketName))
			require.NotNil(t, descOut.ReportPlan.ReportSetting)
			assert.Equal(t, "RESOURCE_COMPLIANCE_REPORT", aws.ToString(descOut.ReportPlan.ReportSetting.ReportTemplate))

			_, err = client.UpdateReportPlan(ctx, &backupsdk.UpdateReportPlanInput{
				ReportPlanName:        aws.String("my_report_plan"),
				ReportPlanDescription: aws.String("updated compliance reports"),
				ReportDeliveryChannel: &types.ReportDeliveryChannel{
					S3BucketName: aws.String("my-report-bucket"),
				},
				ReportSetting: &types.ReportSetting{
					ReportTemplate: aws.String("CONTROL_COMPLIANCE_REPORT"),
				},
			})
			require.NoError(t, err)

			descOut2, err := client.DescribeReportPlan(ctx, &backupsdk.DescribeReportPlanInput{
				ReportPlanName: aws.String("my_report_plan"),
			})
			require.NoError(t, err)
			assert.Equal(t, "updated compliance reports", aws.ToString(descOut2.ReportPlan.ReportPlanDescription))
			assert.Equal(t, "CONTROL_COMPLIANCE_REPORT", aws.ToString(descOut2.ReportPlan.ReportSetting.ReportTemplate))

			_, err = client.DeleteReportPlan(ctx, &backupsdk.DeleteReportPlanInput{
				ReportPlanName: aws.String("my_report_plan"),
			})
			require.NoError(t, err)

			_, err = client.DescribeReportPlan(ctx, &backupsdk.DescribeReportPlanInput{
				ReportPlanName: aws.String("my_report_plan"),
			})
			require.Error(t, err)
		}},
		{name: "backup_plan_extras", run: func(t *testing.T) {
			t.Helper()

			_, client := newRealClient(t)
			ctx := t.Context()

			listTmpl, err := client.ListBackupPlanTemplates(ctx, &backupsdk.ListBackupPlanTemplatesInput{})
			require.NoError(t, err)
			require.NotEmpty(t, listTmpl.BackupPlanTemplatesList)
			tmplID := aws.ToString(listTmpl.BackupPlanTemplatesList[0].BackupPlanTemplateId)
			require.NotEmpty(t, tmplID)

			fromTmpl, err := client.GetBackupPlanFromTemplate(ctx, &backupsdk.GetBackupPlanFromTemplateInput{
				BackupPlanTemplateId: aws.String(tmplID),
			})
			require.NoError(t, err)
			require.NotNil(t, fromTmpl.BackupPlanDocument)
			assert.NotEmpty(t, aws.ToString(fromTmpl.BackupPlanDocument.BackupPlanName))

			planJSON := `{"BackupPlanName":"json-plan","Rules":[{"RuleName":"r1","TargetBackupVaultName":"json-vault"}]}`
			fromJSON, err := client.GetBackupPlanFromJSON(ctx, &backupsdk.GetBackupPlanFromJSONInput{
				BackupPlanTemplateJson: aws.String(planJSON),
			})
			require.NoError(t, err)
			require.NotNil(t, fromJSON.BackupPlan)
			assert.Equal(t, "json-plan", aws.ToString(fromJSON.BackupPlan.BackupPlanName))
			require.Len(t, fromJSON.BackupPlan.Rules, 1)
			assert.Equal(t, "json-vault", aws.ToString(fromJSON.BackupPlan.Rules[0].TargetBackupVaultName))

			createOut, err := client.CreateBackupPlan(ctx, &backupsdk.CreateBackupPlanInput{
				BackupPlan: &types.BackupPlanInput{
					BackupPlanName: aws.String("update-me-plan"),
					Rules: []types.BackupRuleInput{
						{RuleName: aws.String("r1"), TargetBackupVaultName: aws.String("update-me-vault")},
					},
				},
			})
			require.NoError(t, err)

			updOut, err := client.UpdateBackupPlan(ctx, &backupsdk.UpdateBackupPlanInput{
				BackupPlanId: createOut.BackupPlanId,
				BackupPlan: &types.BackupPlanInput{
					BackupPlanName: aws.String("update-me-plan"),
					Rules: []types.BackupRuleInput{
						{
							RuleName:              aws.String("r2"),
							TargetBackupVaultName: aws.String("update-me-vault-2"),
							ScheduleExpression:    aws.String("cron(0 6 ? * * *)"),
						},
					},
				},
			})
			require.NoError(t, err)
			assert.NotEqual(t, aws.ToString(createOut.VersionId), aws.ToString(updOut.VersionId))

			getOut, err := client.GetBackupPlan(ctx, &backupsdk.GetBackupPlanInput{
				BackupPlanId: createOut.BackupPlanId,
			})
			require.NoError(t, err)
			require.Len(t, getOut.BackupPlan.Rules, 1)
			assert.Equal(t, "r2", aws.ToString(getOut.BackupPlan.Rules[0].RuleName))
			assert.Equal(t, "update-me-vault-2", aws.ToString(getOut.BackupPlan.Rules[0].TargetBackupVaultName))
		}},
		{name: "backup_selection_conditions_roundtrip", run: func(t *testing.T) {
			t.Helper()

			_, client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateBackupVault(ctx, &backupsdk.CreateBackupVaultInput{
				BackupVaultName: aws.String("sel-vault"),
			})
			require.NoError(t, err)

			planOut, err := client.CreateBackupPlan(ctx, &backupsdk.CreateBackupPlanInput{
				BackupPlan: &types.BackupPlanInput{
					BackupPlanName: aws.String("sel-plan"),
					Rules: []types.BackupRuleInput{
						{RuleName: aws.String("r1"), TargetBackupVaultName: aws.String("sel-vault")},
					},
				},
			})
			require.NoError(t, err)

			selOut, err := client.CreateBackupSelection(ctx, &backupsdk.CreateBackupSelectionInput{
				BackupPlanId: planOut.BackupPlanId,
				BackupSelection: &types.BackupSelection{
					SelectionName: aws.String("prod-only"),
					IamRoleArn:    aws.String("arn:aws:iam::123456789012:role/backup-role"),
					Conditions: &types.Conditions{
						StringEquals: []types.ConditionParameter{
							{ConditionKey: aws.String("aws:ResourceTag/env"), ConditionValue: aws.String("prod")},
						},
					},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, selOut.SelectionId)

			getOut, err := client.GetBackupSelection(ctx, &backupsdk.GetBackupSelectionInput{
				BackupPlanId: planOut.BackupPlanId,
				SelectionId:  selOut.SelectionId,
			})
			require.NoError(t, err)
			require.NotNil(t, getOut.BackupSelection)
			assert.Equal(t, "prod-only", aws.ToString(getOut.BackupSelection.SelectionName))
			require.NotNil(t, getOut.BackupSelection.Conditions,
				"real GetBackupSelectionOutput.BackupSelection.Conditions must decode -- "+
					"previously always nil due to lowercase/Key-Value wire tag mismatch")
			require.Len(t, getOut.BackupSelection.Conditions.StringEquals, 1)
			assert.Equal(
				t,
				"aws:ResourceTag/env",
				aws.ToString(getOut.BackupSelection.Conditions.StringEquals[0].ConditionKey),
			)
			assert.Equal(t, "prod", aws.ToString(getOut.BackupSelection.Conditions.StringEquals[0].ConditionValue))

			_, err = client.DeleteBackupSelection(ctx, &backupsdk.DeleteBackupSelectionInput{
				BackupPlanId: planOut.BackupPlanId,
				SelectionId:  selOut.SelectionId,
			})
			require.NoError(t, err)

			_, err = client.GetBackupSelection(ctx, &backupsdk.GetBackupSelectionInput{
				BackupPlanId: planOut.BackupPlanId,
				SelectionId:  selOut.SelectionId,
			})
			require.Error(t, err)
		}},
		{name: "backup_job_and_copy_job_extras", run: func(t *testing.T) {
			t.Helper()

			b, client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateBackupVault(ctx, &backupsdk.CreateBackupVaultInput{
				BackupVaultName: aws.String("job-vault"),
			})
			require.NoError(t, err)
			_, err = client.CreateBackupVault(ctx, &backupsdk.CreateBackupVaultInput{
				BackupVaultName: aws.String("job-dest-vault"),
			})
			require.NoError(t, err)

			startOut, err := client.StartBackupJob(ctx, &backupsdk.StartBackupJobInput{
				BackupVaultName: aws.String("job-vault"),
				ResourceArn:     aws.String("arn:aws:ec2:us-east-1:123456789012:instance/i-slice12"),
				IamRoleArn:      aws.String("arn:aws:iam::123456789012:role/backup-role"),
			})
			require.NoError(t, err)
			require.NotNil(t, startOut.BackupJobId)

			descJob, err := client.DescribeBackupJob(ctx, &backupsdk.DescribeBackupJobInput{
				BackupJobId: startOut.BackupJobId,
			})
			require.NoError(t, err)
			assert.Equal(t, "arn:aws:ec2:us-east-1:123456789012:instance/i-slice12", aws.ToString(descJob.ResourceArn))
			assert.Equal(t, "job-vault", aws.ToString(descJob.BackupVaultName))

			summaries, err := client.ListBackupJobSummaries(ctx, &backupsdk.ListBackupJobSummariesInput{})
			require.NoError(t, err)
			require.NotEmpty(t, summaries.BackupJobSummaries)

			startOut2, err := client.StartBackupJob(ctx, &backupsdk.StartBackupJobInput{
				BackupVaultName: aws.String("job-vault"),
				ResourceArn:     aws.String("arn:aws:ec2:us-east-1:123456789012:instance/i-slice12-b"),
				IamRoleArn:      aws.String("arn:aws:iam::123456789012:role/backup-role"),
			})
			require.NoError(t, err)

			_, err = client.StopBackupJob(ctx, &backupsdk.StopBackupJobInput{
				BackupJobId: startOut2.BackupJobId,
			})
			require.NoError(t, err)

			descJob2, err := client.DescribeBackupJob(ctx, &backupsdk.DescribeBackupJobInput{
				BackupJobId: startOut2.BackupJobId,
			})
			require.NoError(t, err)
			assert.Equal(t, types.BackupJobStateAborted, descJob2.State)

			janitor := backup.NewJanitor(b, time.Hour, time.Hour)
			janitor.SweepOnce(ctx)

			descJobDone, err := client.DescribeBackupJob(ctx, &backupsdk.DescribeBackupJobInput{
				BackupJobId: startOut.BackupJobId,
			})
			require.NoError(t, err)
			require.NotNil(t, descJobDone.RecoveryPointArn)

			destVault, err := client.DescribeBackupVault(ctx, &backupsdk.DescribeBackupVaultInput{
				BackupVaultName: aws.String("job-dest-vault"),
			})
			require.NoError(t, err)

			copyOut, err := client.StartCopyJob(ctx, &backupsdk.StartCopyJobInput{
				RecoveryPointArn:          descJobDone.RecoveryPointArn,
				SourceBackupVaultName:     aws.String("job-vault"),
				DestinationBackupVaultArn: destVault.BackupVaultArn,
				IamRoleArn:                aws.String("arn:aws:iam::123456789012:role/backup-role"),
			})
			require.NoError(t, err)
			require.NotNil(t, copyOut.CopyJobId)

			descCopy, err := client.DescribeCopyJob(ctx, &backupsdk.DescribeCopyJobInput{
				CopyJobId: copyOut.CopyJobId,
			})
			require.NoError(t, err)
			require.NotNil(t, descCopy.CopyJob)
			assert.Equal(
				t,
				"arn:aws:ec2:us-east-1:123456789012:instance/i-slice12",
				aws.ToString(descCopy.CopyJob.ResourceArn),
			)

			copySummaries, err := client.ListCopyJobSummaries(ctx, &backupsdk.ListCopyJobSummariesInput{})
			require.NoError(t, err)
			require.NotEmpty(t, copySummaries.CopyJobSummaries)

			descRes, err := client.DescribeProtectedResource(ctx, &backupsdk.DescribeProtectedResourceInput{
				ResourceArn: aws.String("arn:aws:ec2:us-east-1:123456789012:instance/i-slice12"),
			})
			require.NoError(t, err)
			assert.Equal(t, "arn:aws:ec2:us-east-1:123456789012:instance/i-slice12", aws.ToString(descRes.ResourceArn))
		}},
		{name: "recovery_point_extras", run: func(t *testing.T) {
			t.Helper()

			b, client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateBackupVault(ctx, &backupsdk.CreateBackupVaultInput{
				BackupVaultName: aws.String("rp-vault"),
			})
			require.NoError(t, err)

			startOut, err := client.StartBackupJob(ctx, &backupsdk.StartBackupJobInput{
				BackupVaultName: aws.String("rp-vault"),
				ResourceArn:     aws.String("arn:aws:ec2:us-east-1:123456789012:instance/i-rp-slice12"),
				IamRoleArn:      aws.String("arn:aws:iam::123456789012:role/backup-role"),
			})
			require.NoError(t, err)

			janitor := backup.NewJanitor(b, time.Hour, time.Hour)
			janitor.SweepOnce(ctx)

			descJob, err := client.DescribeBackupJob(ctx, &backupsdk.DescribeBackupJobInput{
				BackupJobId: startOut.BackupJobId,
			})
			require.NoError(t, err)
			rpArn := descJob.RecoveryPointArn
			require.NotNil(t, rpArn)

			descRP, err := client.DescribeRecoveryPoint(ctx, &backupsdk.DescribeRecoveryPointInput{
				BackupVaultName:  aws.String("rp-vault"),
				RecoveryPointArn: rpArn,
			})
			require.NoError(t, err)
			assert.Equal(
				t,
				"arn:aws:ec2:us-east-1:123456789012:instance/i-rp-slice12",
				aws.ToString(descRP.ResourceArn),
			)

			metaOut, err := client.GetRecoveryPointRestoreMetadata(ctx, &backupsdk.GetRecoveryPointRestoreMetadataInput{
				BackupVaultName:  aws.String("rp-vault"),
				RecoveryPointArn: rpArn,
			})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(rpArn), aws.ToString(metaOut.RecoveryPointArn))

			idxOut, err := client.GetRecoveryPointIndexDetails(ctx, &backupsdk.GetRecoveryPointIndexDetailsInput{
				BackupVaultName:  aws.String("rp-vault"),
				RecoveryPointArn: rpArn,
			})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(rpArn), aws.ToString(idxOut.RecoveryPointArn))

			updIdxOut, err := client.UpdateRecoveryPointIndexSettings(
				ctx,
				&backupsdk.UpdateRecoveryPointIndexSettingsInput{
					BackupVaultName:  aws.String("rp-vault"),
					RecoveryPointArn: rpArn,
					Index:            types.IndexDisabled,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, types.IndexDisabled, updIdxOut.Index)

			_, err = client.DisassociateRecoveryPointFromParent(
				ctx,
				&backupsdk.DisassociateRecoveryPointFromParentInput{
					BackupVaultName:  aws.String("rp-vault"),
					RecoveryPointArn: rpArn,
				},
			)
			require.NoError(t, err)

			_, err = client.DisassociateRecoveryPoint(ctx, &backupsdk.DisassociateRecoveryPointInput{
				BackupVaultName:  aws.String("rp-vault"),
				RecoveryPointArn: rpArn,
			})
			require.NoError(t, err)

			_, err = client.DescribeRecoveryPoint(ctx, &backupsdk.DescribeRecoveryPointInput{
				BackupVaultName:  aws.String("rp-vault"),
				RecoveryPointArn: rpArn,
			})
			require.Error(t, err)

			secondRPArn := "arn:aws:backup:us-east-1:123456789012:recovery-point:rp-slice12-delete"
			require.NoError(t, b.AddRecoveryPoint("rp-vault", &backup.RecoveryPoint{
				RecoveryPointArn: secondRPArn,
				ResourceArn:      "arn:aws:ec2:us-east-1:123456789012:instance/i-rp-slice12-2",
				Status:           "COMPLETED",
				CreationDate:     time.Now().UTC(),
			}))

			_, err = client.DeleteRecoveryPoint(ctx, &backupsdk.DeleteRecoveryPointInput{
				BackupVaultName:  aws.String("rp-vault"),
				RecoveryPointArn: aws.String(secondRPArn),
			})
			require.NoError(t, err)

			_, err = client.DescribeRecoveryPoint(ctx, &backupsdk.DescribeRecoveryPointInput{
				BackupVaultName:  aws.String("rp-vault"),
				RecoveryPointArn: aws.String(secondRPArn),
			})
			require.Error(t, err)
		}},
		{name: "restore_job_extras", run: func(t *testing.T) {
			t.Helper()

			b, client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateBackupVault(ctx, &backupsdk.CreateBackupVaultInput{
				BackupVaultName: aws.String("restore-vault"),
			})
			require.NoError(t, err)

			startOut, err := client.StartBackupJob(ctx, &backupsdk.StartBackupJobInput{
				BackupVaultName: aws.String("restore-vault"),
				ResourceArn:     aws.String("arn:aws:ec2:us-east-1:123456789012:instance/i-restore-slice12"),
				IamRoleArn:      aws.String("arn:aws:iam::123456789012:role/backup-role"),
			})
			require.NoError(t, err)

			janitor := backup.NewJanitor(b, time.Hour, time.Hour)
			janitor.SweepOnce(ctx)

			descJob, err := client.DescribeBackupJob(ctx, &backupsdk.DescribeBackupJobInput{
				BackupJobId: startOut.BackupJobId,
			})
			require.NoError(t, err)
			rpArn := descJob.RecoveryPointArn
			require.NotNil(t, rpArn)

			restoreOut, err := client.StartRestoreJob(ctx, &backupsdk.StartRestoreJobInput{
				RecoveryPointArn: rpArn,
				IamRoleArn:       aws.String("arn:aws:iam::123456789012:role/backup-role"),
				Metadata:         map[string]string{"instanceType": "t3.micro"},
			})
			require.NoError(t, err)
			require.NotNil(t, restoreOut.RestoreJobId)

			metaOut, err := client.GetRestoreJobMetadata(ctx, &backupsdk.GetRestoreJobMetadataInput{
				RestoreJobId: restoreOut.RestoreJobId,
			})
			require.NoError(t, err)
			assert.Equal(t, "t3.micro", metaOut.Metadata["instanceType"])
			assert.Equal(t, aws.ToString(restoreOut.RestoreJobId), aws.ToString(metaOut.RestoreJobId))

			_, err = client.PutRestoreValidationResult(ctx, &backupsdk.PutRestoreValidationResultInput{
				RestoreJobId:            restoreOut.RestoreJobId,
				ValidationStatus:        types.RestoreValidationStatusSuccessful,
				ValidationStatusMessage: aws.String("validated by integration test"),
			})
			require.NoError(t, err)

			descRestore, err := client.DescribeRestoreJob(ctx, &backupsdk.DescribeRestoreJobInput{
				RestoreJobId: restoreOut.RestoreJobId,
			})
			require.NoError(t, err)
			assert.Equal(t, types.RestoreValidationStatusSuccessful, descRestore.ValidationStatus)

			byResource, err := client.ListRestoreJobsByProtectedResource(
				ctx, &backupsdk.ListRestoreJobsByProtectedResourceInput{
					ResourceArn: aws.String("arn:aws:ec2:us-east-1:123456789012:instance/i-restore-slice12"),
				},
			)
			require.NoError(t, err)
			require.Len(t, byResource.RestoreJobs, 1)
			assert.Equal(t, aws.ToString(restoreOut.RestoreJobId), aws.ToString(byResource.RestoreJobs[0].RestoreJobId))
		}},
		{name: "global_and_region_settings", run: func(t *testing.T) {
			t.Helper()

			_, client := newRealClient(t)
			ctx := t.Context()

			_, err := client.UpdateGlobalSettings(ctx, &backupsdk.UpdateGlobalSettingsInput{
				GlobalSettings: map[string]string{"isCrossAccountBackupEnabled": "true"},
			})
			require.NoError(t, err)

			gsOut, err := client.DescribeGlobalSettings(ctx, &backupsdk.DescribeGlobalSettingsInput{})
			require.NoError(t, err)
			assert.Equal(t, "true", gsOut.GlobalSettings["isCrossAccountBackupEnabled"])

			_, err = client.UpdateRegionSettings(ctx, &backupsdk.UpdateRegionSettingsInput{
				ResourceTypeOptInPreference: map[string]bool{"DynamoDB": true},
			})
			require.NoError(t, err)

			rsOut, err := client.DescribeRegionSettings(ctx, &backupsdk.DescribeRegionSettingsInput{})
			require.NoError(t, err)
			assert.True(t, rsOut.ResourceTypeOptInPreference["DynamoDB"])

			typesOut, err := client.GetSupportedResourceTypes(ctx, &backupsdk.GetSupportedResourceTypesInput{})
			require.NoError(t, err)
			assert.Contains(t, typesOut.ResourceTypes, "EC2")
		}},
		{name: "pitr_malware_scan_results", run: func(t *testing.T) {
			t.Helper()

			b, client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateBackupVault(ctx, &backupsdk.CreateBackupVaultInput{
				BackupVaultName: aws.String("pitr-vault"),
			})
			require.NoError(t, err)

			rpArn := "arn:aws:backup:us-east-1:123456789012:recovery-point:pitr-rp-1"
			require.NoError(t, b.AddRecoveryPoint("pitr-vault", &backup.RecoveryPoint{
				RecoveryPointArn: rpArn,
				ResourceArn:      "arn:aws:ec2:us-east-1:123456789012:instance/i-pitr-slice12",
				Status:           "COMPLETED",
				CreationDate:     time.Now().UTC(),
			}))

			scanEndTime := time.Now().UTC().Truncate(time.Second)
			scanOut, err := client.GetPITRMalwareScanResults(ctx, &backupsdk.GetPITRMalwareScanResultsInput{
				BackupVaultName:  aws.String("pitr-vault"),
				MalwareScanner:   types.MalwareScannerGuardduty,
				RecoveryPointArn: aws.String(rpArn),
				ScanEndTime:      aws.Time(scanEndTime),
			})
			require.NoError(t, err)
			require.NotNil(t, scanOut.ScanResult)
			assert.Equal(t, types.ScanResultStatusUnknown, scanOut.ScanResult.ScanResultStatus)
			require.NotNil(t, scanOut.ScanEndTime)
			assert.WithinDuration(t, scanEndTime, *scanOut.ScanEndTime, time.Second)
		}},
		{name: "tags_lifecycle", run: func(t *testing.T) {
			t.Helper()

			_, client := newRealClient(t)
			ctx := t.Context()

			createOut, err := client.CreateBackupVault(ctx, &backupsdk.CreateBackupVaultInput{
				BackupVaultName: aws.String("tag-vault-12"),
			})
			require.NoError(t, err)

			_, err = client.TagResource(ctx, &backupsdk.TagResourceInput{
				ResourceArn: createOut.BackupVaultArn,
				Tags:        map[string]string{"team": "platform"},
			})
			require.NoError(t, err)

			listOut, err := client.ListTags(ctx, &backupsdk.ListTagsInput{
				ResourceArn: createOut.BackupVaultArn,
			})
			require.NoError(t, err)
			assert.Equal(t, "platform", listOut.Tags["team"])

			_, err = client.UntagResource(ctx, &backupsdk.UntagResourceInput{
				ResourceArn: createOut.BackupVaultArn,
				TagKeyList:  []string{"team"},
			})
			require.NoError(t, err)

			listOut2, err := client.ListTags(ctx, &backupsdk.ListTagsInput{
				ResourceArn: createOut.BackupVaultArn,
			})
			require.NoError(t, err)
			assert.NotContains(t, listOut2.Tags, "team")
		}},
		{name: "delete_restore_testing_selection", run: func(t *testing.T) {
			t.Helper()

			_, client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateBackupVault(ctx, &backupsdk.CreateBackupVaultInput{
				BackupVaultName: aws.String("rt-vault"),
			})
			require.NoError(t, err)

			_, err = client.CreateRestoreTestingPlan(ctx, &backupsdk.CreateRestoreTestingPlanInput{
				RestoreTestingPlan: &types.RestoreTestingPlanForCreate{
					RestoreTestingPlanName: aws.String("rt_plan"),
					ScheduleExpression:     aws.String("cron(0 5 ? * * *)"),
					RecoveryPointSelection: &types.RestoreTestingRecoveryPointSelection{
						Algorithm: types.RestoreTestingRecoveryPointSelectionAlgorithmLatestWithinWindow,
						RecoveryPointTypes: []types.RestoreTestingRecoveryPointType{
							types.RestoreTestingRecoveryPointTypeSnapshot,
						},
						IncludeVaults: []string{"*"},
					},
				},
			})
			require.NoError(t, err)

			_, err = client.CreateRestoreTestingSelection(ctx, &backupsdk.CreateRestoreTestingSelectionInput{
				RestoreTestingPlanName: aws.String("rt_plan"),
				RestoreTestingSelection: &types.RestoreTestingSelectionForCreate{
					RestoreTestingSelectionName: aws.String("rt_selection"),
					ProtectedResourceType:       aws.String("EC2"),
					IamRoleArn:                  aws.String("arn:aws:iam::123456789012:role/backup-role"),
					ProtectedResourceArns:       []string{"*"},
				},
			})
			require.NoError(t, err)

			_, err = client.GetRestoreTestingSelection(ctx, &backupsdk.GetRestoreTestingSelectionInput{
				RestoreTestingPlanName:      aws.String("rt_plan"),
				RestoreTestingSelectionName: aws.String("rt_selection"),
			})
			require.NoError(t, err)

			_, err = client.DeleteRestoreTestingSelection(ctx, &backupsdk.DeleteRestoreTestingSelectionInput{
				RestoreTestingPlanName:      aws.String("rt_plan"),
				RestoreTestingSelectionName: aws.String("rt_selection"),
			})
			require.NoError(t, err)

			_, err = client.GetRestoreTestingSelection(ctx, &backupsdk.GetRestoreTestingSelectionInput{
				RestoreTestingPlanName:      aws.String("rt_plan"),
				RestoreTestingSelectionName: aws.String("rt_selection"),
			})
			require.Error(t, err)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
