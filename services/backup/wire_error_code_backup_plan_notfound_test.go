package backup_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// Test_ListBackupPlanVersions_ExportBackupPlanTemplate_UnknownPlan_TypedClient
// proves that both ops, driven through the real aws-sdk-go-v2 typed client,
// return a typed ResourceNotFoundException for an unknown BackupPlanId
// rather than an empty 200 (gopherstack-i8p8): backup@v1.64.0
// deserializers.go's awsRestjson1_deserializeOpErrorListBackupPlanVersions
// (line 12621) and awsRestjson1_deserializeOpErrorExportBackupPlanTemplate
// (line 8182) both model ResourceNotFoundException.
func Test_ListBackupPlanVersions_ExportBackupPlanTemplate_UnknownPlan_TypedClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(t *testing.T, client *backupsdk.Client) error
		name string
	}{
		{
			name: "list_versions_unknown_plan",
			call: func(t *testing.T, client *backupsdk.Client) error {
				t.Helper()
				_, err := client.ListBackupPlanVersions(t.Context(), &backupsdk.ListBackupPlanVersionsInput{
					BackupPlanId: aws.String("no-such-plan"),
				})

				return err
			},
		},
		{
			name: "export_template_unknown_plan",
			call: func(t *testing.T, client *backupsdk.Client) error {
				t.Helper()
				_, err := client.ExportBackupPlanTemplate(t.Context(), &backupsdk.ExportBackupPlanTemplateInput{
					BackupPlanId: aws.String("no-such-plan"),
				})

				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
			h := backup.NewHandler(backend)
			client := newTestBackupClient(t, h)

			err := tc.call(t, client)
			require.Error(t, err)

			var rnf *types.ResourceNotFoundException
			require.ErrorAs(t, err, &rnf, "expected a typed ResourceNotFoundException, got: %v", err)
		})
	}
}

// Test_ListBackupPlanVersions_ExportBackupPlanTemplate_RealPlan_TypedClient
// proves the two ops still work for a plan that exists, through the real
// typed client.
func Test_ListBackupPlanVersions_ExportBackupPlanTemplate_RealPlan_TypedClient(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	createOut, err := client.CreateBackupPlan(t.Context(), &backupsdk.CreateBackupPlanInput{
		BackupPlan: &types.BackupPlanInput{
			BackupPlanName: aws.String("real-plan"),
			Rules: []types.BackupRuleInput{
				{
					RuleName:              aws.String("r1"),
					TargetBackupVaultName: aws.String("v"),
					ScheduleExpression:    aws.String("cron(0 5 ? * * *)"),
				},
			},
		},
	})
	require.NoError(t, err)
	planID := aws.ToString(createOut.BackupPlanId)

	versionsOut, err := client.ListBackupPlanVersions(t.Context(), &backupsdk.ListBackupPlanVersionsInput{
		BackupPlanId: aws.String(planID),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, versionsOut.BackupPlanVersionsList)

	tmplOut, err := client.ExportBackupPlanTemplate(t.Context(), &backupsdk.ExportBackupPlanTemplateInput{
		BackupPlanId: aws.String(planID),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(tmplOut.BackupPlanTemplateJson), "real-plan")
}
