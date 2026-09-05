package backup_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// TestSDKRoundTrip_BackupPlanRulesAndCopyActions is a typed round-trip test
// (gopherstack-21my) for the deepest nested list in this service's wire
// shape: Plan.Rules[].CopyActions[], two layers below the top-level
// GetBackupPlanOutput. Seeds two rules with distinguishable, non-zero values
// -- one carrying two CopyActions each with its own Lifecycle -- and asserts
// every nested field decodes to the exact seeded value via the real
// aws-sdk-go-v2 client, not a raw-body assertion.
func TestSDKRoundTrip_BackupPlanRulesAndCopyActions(t *testing.T) {
	t.Parallel()

	h := backup.NewHandler(backup.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestBackupClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateBackupPlan(ctx, &backupsdk.CreateBackupPlanInput{
		BackupPlan: &types.BackupPlanInput{
			BackupPlanName: aws.String("nested-plan"),
			Rules: []types.BackupRuleInput{
				{
					RuleName:                aws.String("rule-daily"),
					TargetBackupVaultName:   aws.String("vault-daily"),
					ScheduleExpression:      aws.String("cron(0 5 ? * * *)"),
					StartWindowMinutes:      aws.Int64(60),
					CompletionWindowMinutes: aws.Int64(180),
					RecoveryPointTags:       map[string]string{"env": "prod"},
					Lifecycle: &types.Lifecycle{
						MoveToColdStorageAfterDays: aws.Int64(30),
						DeleteAfterDays:            aws.Int64(365),
					},
					CopyActions: []types.CopyAction{
						{
							DestinationBackupVaultArn: aws.String(
								"arn:aws:backup:us-west-2:000000000000:backup-vault:copy-a",
							),
							Lifecycle: &types.Lifecycle{
								MoveToColdStorageAfterDays: aws.Int64(7),
								DeleteAfterDays:            aws.Int64(90),
							},
						},
						{
							DestinationBackupVaultArn: aws.String(
								"arn:aws:backup:eu-west-1:000000000000:backup-vault:copy-b",
							),
							Lifecycle: &types.Lifecycle{
								DeleteAfterDays: aws.Int64(2555),
							},
						},
					},
				},
				{
					RuleName:              aws.String("rule-weekly"),
					TargetBackupVaultName: aws.String("vault-weekly"),
					ScheduleExpression:    aws.String("cron(0 5 ? * 1 *)"),
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.BackupPlanId)

	getOut, err := client.GetBackupPlan(ctx, &backupsdk.GetBackupPlanInput{
		BackupPlanId: createOut.BackupPlanId,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.BackupPlan)
	require.Len(t, getOut.BackupPlan.Rules, 2, "GetBackupPlan must return both seeded rules")

	rules := getOut.BackupPlan.Rules

	var daily, weekly *types.BackupRule
	for i := range rules {
		switch aws.ToString(rules[i].RuleName) {
		case "rule-daily":
			daily = &rules[i]
		case "rule-weekly":
			weekly = &rules[i]
		}
	}
	require.NotNil(t, daily, "rule-daily must round-trip")
	require.NotNil(t, weekly, "rule-weekly must round-trip")

	require.Equal(t, "vault-daily", aws.ToString(daily.TargetBackupVaultName))
	require.Equal(t, int64(60), aws.ToInt64(daily.StartWindowMinutes))
	require.Equal(t, int64(180), aws.ToInt64(daily.CompletionWindowMinutes))
	require.Equal(t, map[string]string{"env": "prod"}, daily.RecoveryPointTags)
	require.NotNil(t, daily.Lifecycle)
	require.Equal(t, int64(30), aws.ToInt64(daily.Lifecycle.MoveToColdStorageAfterDays))
	require.Equal(t, int64(365), aws.ToInt64(daily.Lifecycle.DeleteAfterDays))

	require.Len(t, daily.CopyActions, 2, "both CopyActions must round-trip")

	var copyA, copyB *types.CopyAction
	for i := range daily.CopyActions {
		switch aws.ToString(daily.CopyActions[i].DestinationBackupVaultArn) {
		case "arn:aws:backup:us-west-2:000000000000:backup-vault:copy-a":
			copyA = &daily.CopyActions[i]
		case "arn:aws:backup:eu-west-1:000000000000:backup-vault:copy-b":
			copyB = &daily.CopyActions[i]
		}
	}
	require.NotNil(t, copyA, "copy-a CopyAction must round-trip")
	require.NotNil(t, copyB, "copy-b CopyAction must round-trip")
	require.NotNil(t, copyA.Lifecycle)
	require.Equal(t, int64(7), aws.ToInt64(copyA.Lifecycle.MoveToColdStorageAfterDays))
	require.Equal(t, int64(90), aws.ToInt64(copyA.Lifecycle.DeleteAfterDays))
	require.NotNil(t, copyB.Lifecycle)
	require.Equal(t, int64(2555), aws.ToInt64(copyB.Lifecycle.DeleteAfterDays))

	require.Equal(t, "vault-weekly", aws.ToString(weekly.TargetBackupVaultName))
	require.Empty(t, weekly.CopyActions)

	listOut, err := client.ListBackupPlans(ctx, &backupsdk.ListBackupPlansInput{})
	require.NoError(t, err)
	require.Len(t, listOut.BackupPlansList, 1)
	require.Equal(t, "nested-plan", aws.ToString(listOut.BackupPlansList[0].BackupPlanName))
}
