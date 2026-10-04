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

func TestListBackupJobSummaries_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input backupsdk.ListBackupJobSummariesInput
		want  int
	}{
		{"no filter counts all", backupsdk.ListBackupJobSummariesInput{}, 3},
		{"ANY counts all", backupsdk.ListBackupJobSummariesInput{ResourceType: aws.String("ANY")}, 3},
		{"resource type", backupsdk.ListBackupJobSummariesInput{ResourceType: aws.String("S3")}, 2},
		{"other resource type", backupsdk.ListBackupJobSummariesInput{ResourceType: aws.String("EBS")}, 1},
		{"unknown resource type", backupsdk.ListBackupJobSummariesInput{ResourceType: aws.String("RDS")}, 0},
		{"state matches", backupsdk.ListBackupJobSummariesInput{State: types.BackupJobStatusCreated}, 3},
		{"state mismatch", backupsdk.ListBackupJobSummariesInput{State: types.BackupJobStatusCompleted}, 0},
		{"own account", backupsdk.ListBackupJobSummariesInput{AccountId: aws.String("000000000000")}, 3},
		{"other account", backupsdk.ListBackupJobSummariesInput{AccountId: aws.String("111111111111")}, 0},
		{
			"message category mismatch",
			backupsdk.ListBackupJobSummariesInput{MessageCategory: aws.String("AccessDenied")}, 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
			client := newTestBackupClient(t, backup.NewHandler(backend))

			_, err := backend.CreateBackupVault("sum-vault", "", "", nil)
			require.NoError(t, err)

			for _, j := range []struct{ arn, typ string }{
				{"arn:aws:s3:::bucket-a", "S3"},
				{"arn:aws:s3:::bucket-b", "S3"},
				{"arn:aws:ec2:us-east-1:000000000000:volume/vol-1", "EBS"},
			} {
				_, err = backend.StartBackupJob("sum-vault", j.arn, "arn:aws:iam::000000000000:role/r", j.typ, nil, 0)
				require.NoError(t, err)
			}

			out, err := client.ListBackupJobSummaries(t.Context(), &tc.input)
			require.NoError(t, err)

			total := 0
			for _, s := range out.BackupJobSummaries {
				total += int(s.Count)
			}

			assert.Equal(t, tc.want, total)
		})
	}
}
