package backup_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

const pagedResourceArn = "arn:aws:ec2:us-east-1:123456789012:instance/i-paged"

// TestRealClient_ListOpsHonourMaxResults pages ops whose maxResults/nextToken
// query members were previously ignored (serializers.go, backup@v1.59.4).
func TestRealClient_ListOpsHonourMaxResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fetch func(t *testing.T, c *backupsdk.Client, token *string) (int, *string)
		seed  func(t *testing.T, b *backup.InMemoryBackend)
		name  string
		total int
	}{
		{
			name: "list_tags", total: 3,
			seed: func(t *testing.T, b *backup.InMemoryBackend) {
				t.Helper()

				v := mustVault(t, b, "paged-vault")
				require.NoError(t, b.TagResource(v.BackupVaultArn, map[string]string{"a": "1", "b": "2", "c": "3"}))
			},
			fetch: func(t *testing.T, c *backupsdk.Client, token *string) (int, *string) {
				t.Helper()

				out, err := c.ListTags(t.Context(), &backupsdk.ListTagsInput{
					ResourceArn: aws.String("arn:aws:backup:us-east-1:123456789012:backup-vault:paged-vault"),
					MaxResults:  aws.Int32(1), NextToken: token,
				})
				require.NoError(t, err)

				return len(out.Tags), out.NextToken
			},
		},
		{
			name: "recovery_points_by_resource", total: 3,
			seed: seedPagedRecoveryPoints,
			fetch: func(t *testing.T, c *backupsdk.Client, token *string) (int, *string) {
				t.Helper()

				out, err := c.ListRecoveryPointsByResource(t.Context(), &backupsdk.ListRecoveryPointsByResourceInput{
					ResourceArn: aws.String(pagedResourceArn), MaxResults: aws.Int32(1), NextToken: token,
				})
				require.NoError(t, err)

				return len(out.RecoveryPoints), out.NextToken
			},
		},
		{
			name: "indexed_recovery_points", total: 3,
			seed: seedPagedRecoveryPoints,
			fetch: func(t *testing.T, c *backupsdk.Client, token *string) (int, *string) {
				t.Helper()

				out, err := c.ListIndexedRecoveryPoints(t.Context(), &backupsdk.ListIndexedRecoveryPointsInput{
					MaxResults: aws.Int32(1), NextToken: token,
				})
				require.NoError(t, err)

				return len(out.IndexedRecoveryPoints), out.NextToken
			},
		},
		{
			name: "report_jobs", total: 3,
			seed: func(t *testing.T, b *backup.InMemoryBackend) {
				t.Helper()

				for range 3 {
					b.StartReportJob("rp")
				}
			},
			fetch: func(t *testing.T, c *backupsdk.Client, token *string) (int, *string) {
				t.Helper()

				out, err := c.ListReportJobs(t.Context(), &backupsdk.ListReportJobsInput{
					MaxResults: aws.Int32(1), NextToken: token,
				})
				require.NoError(t, err)

				return len(out.ReportJobs), out.NextToken
			},
		},
		{
			name: "backup_job_summaries", total: 2,
			seed: func(t *testing.T, b *backup.InMemoryBackend) {
				t.Helper()

				mustVault(t, b, "paged-vault")
				mustJob(t, b, "paged-vault", pagedResourceArn, "EC2")
				stopped := mustJob(t, b, "paged-vault", pagedResourceArn, "EC2")
				require.NoError(t, b.StopBackupJob(stopped.BackupJobID))
			},
			fetch: func(t *testing.T, c *backupsdk.Client, token *string) (int, *string) {
				t.Helper()

				out, err := c.ListBackupJobSummaries(t.Context(), &backupsdk.ListBackupJobSummariesInput{
					MaxResults: aws.Int32(1), NextToken: token,
				})
				require.NoError(t, err)

				return len(out.BackupJobSummaries), out.NextToken
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newRealClient(t)
			tt.seed(t, b)

			var token *string

			seen, pages := 0, 0

			for {
				n, next := tt.fetch(t, client, token)
				require.LessOrEqual(t, n, 1)

				seen += n
				pages++

				if next == nil {
					break
				}

				token = next

				require.Less(t, pages, 10)
			}

			require.Equal(t, tt.total, seen)
			require.Equal(t, tt.total, pages)
		})
	}
}

func seedPagedRecoveryPoints(t *testing.T, b *backup.InMemoryBackend) {
	t.Helper()

	mustVault(t, b, "paged-vault")

	for _, id := range []string{"1", "2", "3"} {
		mustRP(t, b, "paged-vault", "arn:aws:backup:us-east-1:123456789012:recovery-point:paged-"+id,
			pagedResourceArn, "EC2")
	}
}

func TestRealClient_ListIndexedRecoveryPointsCreatedRange(t *testing.T) {
	t.Parallel()

	now := time.Now()
	tests := []struct {
		after  *time.Time
		before *time.Time
		name   string
		want   int
	}{
		{name: "none", want: 3},
		{name: "after_past", after: aws.Time(now.Add(-time.Hour)), want: 3},
		{name: "after_future", after: aws.Time(now.Add(time.Hour)), want: 0},
		{name: "before_past", before: aws.Time(now.Add(-time.Hour)), want: 0},
		{name: "before_future", before: aws.Time(now.Add(time.Hour)), want: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newRealClient(t)
			seedPagedRecoveryPoints(t, b)

			out, err := client.ListIndexedRecoveryPoints(t.Context(), &backupsdk.ListIndexedRecoveryPointsInput{
				CreatedAfter: tt.after, CreatedBefore: tt.before,
			})
			require.NoError(t, err)
			require.Len(t, out.IndexedRecoveryPoints, tt.want)
		})
	}
}
