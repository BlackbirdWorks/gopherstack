package backup_test

import (
	"bytes"
	"regexp"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

var jobCreationTimeRe = regexp.MustCompile(`"creationTime":"[^"]+"`)

// backdateJobs moves every job created in the last hour to ago in the past.
func backdateJobs(t *testing.T, b *backup.InMemoryBackend, ago time.Duration) {
	t.Helper()

	snap := b.Snapshot(t.Context())
	stamp := []byte(`"creationTime":"` + time.Now().UTC().Add(-ago).Format(time.RFC3339Nano) + `"`)
	patched := jobCreationTimeRe.ReplaceAllFunc(snap, func(m []byte) []byte {
		ts, err := time.Parse(time.RFC3339Nano, string(m[len(`"creationTime":"`):len(m)-1]))
		if err != nil || ts.Before(time.Now().Add(-time.Hour)) {
			return m
		}

		return stamp
	})
	require.False(t, bytes.Equal(snap, patched), "snapshot must contain a creationTime")
	require.NoError(t, b.Restore(t.Context(), patched))
}

func startJob(t *testing.T, b *backup.InMemoryBackend, arn, resourceType string) {
	t.Helper()

	_, err := b.StartBackupJob("agg-vault", arn, "arn:aws:iam::000000000000:role/r", resourceType, nil, 0)
	require.NoError(t, err)
}

func TestListBackupJobSummaries_Aggregation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		input        backupsdk.ListBackupJobSummariesInput
		wantCounts   []int32
		wantResource []string
		wantErr      bool
	}{
		{
			name:       "one day buckets",
			input:      backupsdk.ListBackupJobSummariesInput{AggregationPeriod: types.AggregationPeriodOneDay},
			wantCounts: []int32{1, 1, 1},
		},
		{
			name:       "seven days aggregates",
			input:      backupsdk.ListBackupJobSummariesInput{AggregationPeriod: types.AggregationPeriodSevenDays},
			wantCounts: []int32{1, 2},
		},
		{
			name:       "fourteen days excludes older",
			input:      backupsdk.ListBackupJobSummariesInput{AggregationPeriod: types.AggregationPeriodFourteenDays},
			wantCounts: []int32{1, 2},
		},
		{
			name: "twenty days not in window",
			input: backupsdk.ListBackupJobSummariesInput{
				AggregationPeriod: types.AggregationPeriodFourteenDays, ResourceType: aws.String("AGGREGATE_ALL"),
			},
			wantCounts: []int32{3},
		},
		{
			name: "resource type any splits rows",
			input: backupsdk.ListBackupJobSummariesInput{
				AggregationPeriod: types.AggregationPeriodSevenDays, ResourceType: aws.String("ANY"),
			},
			wantCounts:   []int32{1, 2},
			wantResource: []string{"EBS", "S3"},
		},
		{
			name: "resource type aggregate all sums",
			input: backupsdk.ListBackupJobSummariesInput{
				AggregationPeriod: types.AggregationPeriodSevenDays, ResourceType: aws.String("AGGREGATE_ALL"),
			},
			wantCounts:   []int32{3},
			wantResource: []string{"AGGREGATE_ALL"},
		},
		{
			name:    "invalid period",
			input:   backupsdk.ListBackupJobSummariesInput{AggregationPeriod: "ONE_WEEK"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := backend.CreateBackupVault("agg-vault", "", "", nil)
			require.NoError(t, err)

			startJob(t, backend, "arn:aws:s3:::ancient-bucket", "S3")
			backdateJobs(t, backend, 20*24*time.Hour)
			startJob(t, backend, "arn:aws:s3:::old-bucket", "S3")
			backdateJobs(t, backend, 3*24*time.Hour)
			startJob(t, backend, "arn:aws:s3:::bucket-a", "S3")
			startJob(t, backend, "arn:aws:ec2:us-east-1:000000000000:volume/vol-1", "EBS")

			client := newTestBackupClient(t, backup.NewHandler(backend))

			out, err := client.ListBackupJobSummaries(t.Context(), &tc.input)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			counts := make([]int32, 0, len(out.BackupJobSummaries))
			resources := make([]string, 0, len(out.BackupJobSummaries))

			for _, s := range out.BackupJobSummaries {
				counts = append(counts, s.Count)
				resources = append(resources, aws.ToString(s.ResourceType))

				require.NotNil(t, s.StartTime)
				require.NotNil(t, s.EndTime)
				assert.True(t, s.EndTime.After(*s.StartTime))
			}

			assert.ElementsMatch(t, tc.wantCounts, counts)

			if tc.wantResource != nil {
				assert.ElementsMatch(t, tc.wantResource, resources)
			}
		})
	}
}

func TestCopyAndScanJobSummaries_BackedFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		count func(t *testing.T, c *backupsdk.Client) int32
		name  string
		want  int32
	}{
		{
			name: "copy success category",
			count: func(t *testing.T, c *backupsdk.Client) int32 {
				t.Helper()
				out, err := c.ListCopyJobSummaries(t.Context(), &backupsdk.ListCopyJobSummariesInput{
					MessageCategory: aws.String("SUCCESS"),
				})
				require.NoError(t, err)

				return sumCopy(out.CopyJobSummaries)
			},
			want: 1,
		},
		{
			name: "copy other category",
			count: func(t *testing.T, c *backupsdk.Client) int32 {
				t.Helper()
				out, err := c.ListCopyJobSummaries(t.Context(), &backupsdk.ListCopyJobSummariesInput{
					MessageCategory: aws.String("AccessDenied"),
				})
				require.NoError(t, err)

				return sumCopy(out.CopyJobSummaries)
			},
			want: 0,
		},
		{
			name: "scan no threats",
			count: func(t *testing.T, c *backupsdk.Client) int32 {
				t.Helper()
				out, err := c.ListScanJobSummaries(t.Context(), &backupsdk.ListScanJobSummariesInput{
					ScanResultStatus: types.ScanResultStatusNoThreatsFound,
				})
				require.NoError(t, err)

				var n int32
				for _, s := range out.ScanJobSummaries {
					n += s.Count
				}

				return n
			},
			want: 1,
		},
		{
			name: "scan threats found",
			count: func(t *testing.T, c *backupsdk.Client) int32 {
				t.Helper()
				out, err := c.ListScanJobSummaries(t.Context(), &backupsdk.ListScanJobSummariesInput{
					ScanResultStatus: types.ScanResultStatusThreatsFound,
				})
				require.NoError(t, err)

				return int32(len(out.ScanJobSummaries))
			},
			want: 0,
		},
		{
			name: "list scan jobs by result status",
			count: func(t *testing.T, c *backupsdk.Client) int32 {
				t.Helper()
				out, err := c.ListScanJobs(t.Context(), &backupsdk.ListScanJobsInput{
					ByScanResultStatus: types.ScanResultStatusNoThreatsFound,
				})
				require.NoError(t, err)

				return int32(len(out.ScanJobs))
			},
			want: 1,
		},
		{
			name: "list scan jobs threats",
			count: func(t *testing.T, c *backupsdk.Client) int32 {
				t.Helper()
				out, err := c.ListScanJobs(t.Context(), &backupsdk.ListScanJobsInput{
					ByScanResultStatus: types.ScanResultStatusThreatsFound,
				})
				require.NoError(t, err)

				return int32(len(out.ScanJobs))
			},
			want: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
			client := newTestBackupClient(t, backup.NewHandler(backend))

			_, err := backend.CreateBackupVault("src-vault", "", "", nil)
			require.NoError(t, err)
			dst, err := backend.CreateBackupVault("dst-vault", "", "", nil)
			require.NoError(t, err)
			_, err = backend.StartCopyJob("arn:rp", "src-vault", dst.BackupVaultArn, "arn:role")
			require.NoError(t, err)

			_, err = client.StartScanJob(t.Context(), &backupsdk.StartScanJobInput{
				BackupVaultName:  aws.String("src-vault"),
				IamRoleArn:       aws.String("arn:aws:iam::000000000000:role/ScanRole"),
				MalwareScanner:   types.MalwareScannerGuardduty,
				RecoveryPointArn: aws.String("arn:aws:backup:us-east-1:000000000000:recovery-point:rp-1"),
				ScanMode:         types.ScanModeFullScan,
				ScannerRoleArn:   aws.String("arn:aws:iam::000000000000:role/ScannerRole"),
			})
			require.NoError(t, err)

			assert.Equal(t, tc.want, tc.count(t, client))
		})
	}
}

func sumCopy(s []types.CopyJobSummary) int32 {
	var n int32
	for _, x := range s {
		n += x.Count
	}

	return n
}
