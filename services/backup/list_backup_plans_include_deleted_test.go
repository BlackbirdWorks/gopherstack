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

func TestListBackupPlans_IncludeDeleted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantNames      []string
		includeDeleted bool
	}{
		{name: "default hides deleted", wantNames: []string{"live-plan"}},
		{name: "include deleted lists both", includeDeleted: true, wantNames: []string{"dead-plan", "live-plan"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestBackupClient(t, backup.NewHandler(backup.NewInMemoryBackend("000000000000", "us-east-1")))
			ctx := t.Context()

			_, err := client.CreateBackupVault(ctx, &backupsdk.CreateBackupVaultInput{
				BackupVaultName: aws.String("vault-a"),
			})
			require.NoError(t, err)

			ids := map[string]string{}

			for _, name := range []string{"live-plan", "dead-plan"} {
				out, createErr := client.CreateBackupPlan(ctx, &backupsdk.CreateBackupPlanInput{
					BackupPlan: &types.BackupPlanInput{
						BackupPlanName: aws.String(name),
						Rules: []types.BackupRuleInput{
							{RuleName: aws.String("r"), TargetBackupVaultName: aws.String("vault-a")},
						},
					},
				})
				require.NoError(t, createErr)

				ids[name] = aws.ToString(out.BackupPlanId)
			}

			del, err := client.DeleteBackupPlan(ctx, &backupsdk.DeleteBackupPlanInput{
				BackupPlanId: aws.String(ids["dead-plan"]),
			})
			require.NoError(t, err)
			require.NotNil(t, del.DeletionDate)

			out, err := client.ListBackupPlans(ctx, &backupsdk.ListBackupPlansInput{
				IncludeDeleted: aws.Bool(tc.includeDeleted),
			})
			require.NoError(t, err)

			var names []string

			for _, p := range out.BackupPlansList {
				names = append(names, aws.ToString(p.BackupPlanName))

				if aws.ToString(p.BackupPlanName) == "dead-plan" {
					require.NotNil(t, p.DeletionDate)
					assert.WithinDuration(t, aws.ToTime(del.DeletionDate), aws.ToTime(p.DeletionDate), 2e9)
				} else {
					assert.Nil(t, p.DeletionDate)
				}
			}

			assert.Equal(t, tc.wantNames, names)
		})
	}
}
