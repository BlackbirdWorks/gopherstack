package backup_test

import (
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// TestListBackupJobs_WireFilters proves ListBackupJobs' query filters use
// the real wire keys (backup@v1.59.4 serializers.go:4629-4677) rather than
// the "by"-prefixed Go field names -- gopherstack-i25e. Each case asserts a
// record the filter should EXCLUDE is actually absent, not just that a
// matching record comes back (the unfiltered list would pass that alone).
func TestListBackupJobs_WireFilters(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	mustVault(t, backend, "bj-vault-a")
	mustVault(t, backend, "bj-vault-b")

	keep := mustJob(t, backend, "bj-vault-a", "arn:aws:ec2:us-east-1:000000000000:instance/i-bj-keep", "EC2")
	drop := mustJob(t, backend, "bj-vault-b", "arn:aws:ec2:us-east-1:000000000000:instance/i-bj-drop", "RDS")
	require.NoError(t, backend.StopBackupJob(drop.BackupJobID))

	now := time.Now().UTC()

	tests := []struct {
		mutate   func(*backupsdk.ListBackupJobsInput)
		name     string
		wantKeep bool
		wantDrop bool
	}{
		{name: "byResourceArn", wantKeep: true, wantDrop: false, mutate: func(in *backupsdk.ListBackupJobsInput) {
			in.ByResourceArn = aws.String(keep.ResourceArn)
		}},
		{name: "byResourceType", wantKeep: true, wantDrop: false, mutate: func(in *backupsdk.ListBackupJobsInput) {
			in.ByResourceType = aws.String(keep.ResourceType)
		}},
		{name: "byState", wantKeep: true, wantDrop: false, mutate: func(in *backupsdk.ListBackupJobsInput) {
			in.ByState = types.BackupJobStateCreated
		}},
		{name: "byAccountId matches", wantKeep: true, wantDrop: true, mutate: func(in *backupsdk.ListBackupJobsInput) {
			in.ByAccountId = aws.String("000000000000")
		}},
		{
			name:     "byAccountId wrong excludes all",
			wantKeep: false,
			wantDrop: false,
			mutate: func(in *backupsdk.ListBackupJobsInput) {
				in.ByAccountId = aws.String("999999999999")
			},
		},
		{
			name:     "byParentJobId wrong excludes all",
			wantKeep: false,
			wantDrop: false,
			mutate: func(in *backupsdk.ListBackupJobsInput) {
				in.ByParentJobId = aws.String("no-such-parent")
			},
		},
		{
			name:     "byCreatedAfter future excludes all",
			wantKeep: false,
			wantDrop: false,
			mutate: func(in *backupsdk.ListBackupJobsInput) {
				in.ByCreatedAfter = aws.Time(now.Add(time.Hour))
			},
		},
		{
			name:     "byCreatedBefore past excludes all",
			wantKeep: false,
			wantDrop: false,
			mutate: func(in *backupsdk.ListBackupJobsInput) {
				in.ByCreatedBefore = aws.Time(now.Add(-time.Hour))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := &backupsdk.ListBackupJobsInput{}
			tc.mutate(in)

			out, err := client.ListBackupJobs(t.Context(), in)
			require.NoError(t, err)

			ids := make([]string, 0, len(out.BackupJobs))
			for _, j := range out.BackupJobs {
				ids = append(ids, aws.ToString(j.BackupJobId))
			}

			require.Equal(t, tc.wantKeep, slices.Contains(ids, keep.BackupJobID), "keep presence")
			require.Equal(t, tc.wantDrop, slices.Contains(ids, drop.BackupJobID), "drop presence")
		})
	}
}

// TestListCopyJobs_WireFilters covers ListCopyJobs (serializers.go:5211-5259)
// plus the SourceRecoveryPointArn defect: gopherstack previously filtered on
// a "bySourceBackupVaultArn" key that has no wire equivalent at all -- the
// real filter is BySourceRecoveryPointArn -> "sourceRecoveryPointArn".
func TestListCopyJobs_WireFilters(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	mustVault(t, backend, "cj-src-a")
	mustVault(t, backend, "cj-src-b")
	destA := mustVault(t, backend, "cj-dst-a")
	destB := mustVault(t, backend, "cj-dst-b")

	rpKeepArn := "arn:aws:backup:us-east-1:000000000000:recovery-point:cj-rp-keep"
	rpDropArn := "arn:aws:backup:us-east-1:000000000000:recovery-point:cj-rp-drop"
	mustRP(t, backend, "cj-src-a", rpKeepArn, "arn:aws:ec2:us-east-1:000000000000:instance/i-cj-keep", "EC2")
	mustRP(t, backend, "cj-src-b", rpDropArn, "arn:aws:ec2:us-east-1:000000000000:instance/i-cj-drop", "RDS")

	iamRoleArn := "arn:aws:iam::000000000000:role/r"

	keep, startErr := backend.StartCopyJob(rpKeepArn, "cj-src-a", destA.BackupVaultArn, iamRoleArn)
	require.NoError(t, startErr)
	drop, startErr := backend.StartCopyJob(rpDropArn, "cj-src-b", destB.BackupVaultArn, iamRoleArn)
	require.NoError(t, startErr)

	now := time.Now().UTC()

	tests := []struct {
		mutate   func(*backupsdk.ListCopyJobsInput)
		name     string
		wantKeep bool
		wantDrop bool
	}{
		{name: "byResourceArn", wantKeep: true, wantDrop: false, mutate: func(in *backupsdk.ListCopyJobsInput) {
			in.ByResourceArn = aws.String(keep.ResourceArn)
		}},
		{name: "byResourceType", wantKeep: true, wantDrop: false, mutate: func(in *backupsdk.ListCopyJobsInput) {
			in.ByResourceType = aws.String(keep.ResourceType)
		}},
		{name: "byDestinationVaultArn", wantKeep: true, wantDrop: false, mutate: func(in *backupsdk.ListCopyJobsInput) {
			in.ByDestinationVaultArn = aws.String(destA.BackupVaultArn)
		}},
		{
			name: "bySourceRecoveryPointArn", wantKeep: true, wantDrop: false,
			mutate: func(in *backupsdk.ListCopyJobsInput) { in.BySourceRecoveryPointArn = aws.String(rpKeepArn) },
		},
		{
			name:     "byState wrong excludes all",
			wantKeep: false,
			wantDrop: false,
			mutate: func(in *backupsdk.ListCopyJobsInput) {
				in.ByState = types.CopyJobStateFailed
			},
		},
		{name: "byAccountId matches", wantKeep: true, wantDrop: true, mutate: func(in *backupsdk.ListCopyJobsInput) {
			in.ByAccountId = aws.String("000000000000")
		}},
		{
			name:     "byAccountId wrong excludes all",
			wantKeep: false,
			wantDrop: false,
			mutate: func(in *backupsdk.ListCopyJobsInput) {
				in.ByAccountId = aws.String("999999999999")
			},
		},
		{
			name:     "byCreatedAfter future excludes all",
			wantKeep: false,
			wantDrop: false,
			mutate: func(in *backupsdk.ListCopyJobsInput) {
				in.ByCreatedAfter = aws.Time(now.Add(time.Hour))
			},
		},
		{
			name:     "byCreatedBefore past excludes all",
			wantKeep: false,
			wantDrop: false,
			mutate: func(in *backupsdk.ListCopyJobsInput) {
				in.ByCreatedBefore = aws.Time(now.Add(-time.Hour))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := &backupsdk.ListCopyJobsInput{}
			tc.mutate(in)

			out, err := client.ListCopyJobs(t.Context(), in)
			require.NoError(t, err)

			ids := make([]string, 0, len(out.CopyJobs))
			for _, j := range out.CopyJobs {
				ids = append(ids, aws.ToString(j.CopyJobId))
			}

			require.Equal(t, tc.wantKeep, slices.Contains(ids, keep.CopyJobID), "keep presence")
			require.Equal(t, tc.wantDrop, slices.Contains(ids, drop.CopyJobID), "drop presence")
		})
	}
}

// TestListRecoveryPointsByBackupVault_WireFilters covers serializers.go
// (ListRecoveryPointsByBackupVault query bindings).
func TestListRecoveryPointsByBackupVault_WireFilters(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	mustVault(t, backend, "rp-vault")

	keepArn := "arn:aws:backup:us-east-1:000000000000:recovery-point:rp-keep"
	dropArn := "arn:aws:backup:us-east-1:000000000000:recovery-point:rp-drop"
	now := time.Now().UTC()

	require.NoError(t, backend.AddRecoveryPoint("rp-vault", &backup.RecoveryPoint{
		RecoveryPointArn:       keepArn,
		ResourceArn:            "arn:aws:ec2:us-east-1:000000000000:instance/i-rp-keep",
		ResourceType:           "EC2",
		ParentRecoveryPointArn: "arn:aws:backup:us-east-1:000000000000:recovery-point:rp-parent-keep",
		Status:                 "COMPLETED",
		CreationDate:           now,
	}))
	require.NoError(t, backend.AddRecoveryPoint("rp-vault", &backup.RecoveryPoint{
		RecoveryPointArn:       dropArn,
		ResourceArn:            "arn:aws:ec2:us-east-1:000000000000:instance/i-rp-drop",
		ResourceType:           "RDS",
		ParentRecoveryPointArn: "arn:aws:backup:us-east-1:000000000000:recovery-point:rp-parent-drop",
		Status:                 "COMPLETED",
		CreationDate:           now,
	}))

	tests := []struct {
		mutate   func(*backupsdk.ListRecoveryPointsByBackupVaultInput)
		name     string
		wantKeep bool
		wantDrop bool
	}{
		{
			name: "byResourceArn", wantKeep: true, wantDrop: false,
			mutate: func(in *backupsdk.ListRecoveryPointsByBackupVaultInput) {
				in.ByResourceArn = aws.String("arn:aws:ec2:us-east-1:000000000000:instance/i-rp-keep")
			},
		},
		{
			name: "byResourceType", wantKeep: true, wantDrop: false,
			mutate: func(in *backupsdk.ListRecoveryPointsByBackupVaultInput) { in.ByResourceType = aws.String("EC2") },
		},
		{
			name: "byParentRecoveryPointArn", wantKeep: true, wantDrop: false,
			mutate: func(in *backupsdk.ListRecoveryPointsByBackupVaultInput) {
				in.ByParentRecoveryPointArn = aws.String(
					"arn:aws:backup:us-east-1:000000000000:recovery-point:rp-parent-keep",
				)
			},
		},
		{
			name: "byCreatedAfter future excludes all", wantKeep: false, wantDrop: false,
			mutate: func(in *backupsdk.ListRecoveryPointsByBackupVaultInput) {
				in.ByCreatedAfter = aws.Time(now.Add(time.Hour))
			},
		},
		{
			name: "byCreatedBefore past excludes all", wantKeep: false, wantDrop: false,
			mutate: func(in *backupsdk.ListRecoveryPointsByBackupVaultInput) {
				in.ByCreatedBefore = aws.Time(now.Add(-time.Hour))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := &backupsdk.ListRecoveryPointsByBackupVaultInput{BackupVaultName: aws.String("rp-vault")}
			tc.mutate(in)

			out, err := client.ListRecoveryPointsByBackupVault(t.Context(), in)
			require.NoError(t, err)

			ids := make([]string, 0, len(out.RecoveryPoints))
			for _, rp := range out.RecoveryPoints {
				ids = append(ids, aws.ToString(rp.RecoveryPointArn))
			}

			require.Equal(t, tc.wantKeep, slices.Contains(ids, keepArn), "keep presence")
			require.Equal(t, tc.wantDrop, slices.Contains(ids, dropArn), "drop presence")
		})
	}
}

// TestListBackupVaults_WireFilters covers ByVaultType -> "vaultType"
// (serializers.go ListBackupVaults query bindings).
func TestListBackupVaults_WireFilters(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	mustVault(t, backend, "bv-regular")
	_, err := backend.CreateLogicallyAirGappedBackupVault("bv-airgapped", "", 7, 30, nil)
	require.NoError(t, err)

	out, err := client.ListBackupVaults(t.Context(), &backupsdk.ListBackupVaultsInput{
		ByVaultType: types.VaultTypeBackupVault,
	})
	require.NoError(t, err)

	names := make([]string, 0, len(out.BackupVaultList))
	for _, v := range out.BackupVaultList {
		names = append(names, aws.ToString(v.BackupVaultName))
	}

	require.True(t, slices.Contains(names, "bv-regular"), "regular vault should be present")
	require.False(t, slices.Contains(names, "bv-airgapped"), "air-gapped vault should be excluded")

	out, err = client.ListBackupVaults(t.Context(), &backupsdk.ListBackupVaultsInput{
		ByVaultType: types.VaultTypeLogicallyAirGappedBackupVault,
	})
	require.NoError(t, err)

	names = make([]string, 0, len(out.BackupVaultList))
	for _, v := range out.BackupVaultList {
		names = append(names, aws.ToString(v.BackupVaultName))
	}

	require.False(t, slices.Contains(names, "bv-regular"), "regular vault should be excluded")
	require.True(t, slices.Contains(names, "bv-airgapped"), "air-gapped vault should be present")
}

// TestListRestoreJobs_WireFilters covers ListRestoreJobs (serializers.go
// ~5450-5510), which previously read no query filters at all -- every call
// silently returned every restore job regardless of the filter set on the
// real typed client (same user-visible symptom as a wrong wire key).
func TestListRestoreJobs_WireFilters(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	mustVault(t, backend, "rj-vault")
	rpArn := "arn:aws:backup:us-east-1:000000000000:recovery-point:rj-rp"
	mustRP(t, backend, "rj-vault", rpArn, "arn:aws:ec2:us-east-1:000000000000:instance/i-rj", "EC2")

	iamRoleArn := "arn:aws:iam::000000000000:role/r"
	metadata := map[string]string{"k": "v"}

	keep, startErr := backend.StartRestoreJob(rpArn, iamRoleArn, "EC2", metadata)
	require.NoError(t, startErr)
	drop, startErr := backend.StartRestoreJob(rpArn, iamRoleArn, "RDS", metadata)
	require.NoError(t, startErr)

	now := time.Now().UTC()

	tests := []struct {
		mutate   func(*backupsdk.ListRestoreJobsInput)
		name     string
		wantKeep bool
		wantDrop bool
	}{
		{name: "byResourceType", wantKeep: true, wantDrop: false, mutate: func(in *backupsdk.ListRestoreJobsInput) {
			in.ByResourceType = aws.String("EC2")
		}},
		{name: "byStatus matches", wantKeep: true, wantDrop: true, mutate: func(in *backupsdk.ListRestoreJobsInput) {
			in.ByStatus = types.RestoreJobStatusCompleted
		}},
		{
			name:     "byStatus wrong excludes all",
			wantKeep: false,
			wantDrop: false,
			mutate: func(in *backupsdk.ListRestoreJobsInput) {
				in.ByStatus = types.RestoreJobStatusFailed
			},
		},
		{name: "byAccountId matches", wantKeep: true, wantDrop: true, mutate: func(in *backupsdk.ListRestoreJobsInput) {
			in.ByAccountId = aws.String("000000000000")
		}},
		{
			name:     "byAccountId wrong excludes all",
			wantKeep: false,
			wantDrop: false,
			mutate: func(in *backupsdk.ListRestoreJobsInput) {
				in.ByAccountId = aws.String("999999999999")
			},
		},
		{
			name:     "byCreatedAfter future excludes all",
			wantKeep: false,
			wantDrop: false,
			mutate: func(in *backupsdk.ListRestoreJobsInput) {
				in.ByCreatedAfter = aws.Time(now.Add(time.Hour))
			},
		},
		{
			name:     "byCreatedBefore past excludes all",
			wantKeep: false,
			wantDrop: false,
			mutate: func(in *backupsdk.ListRestoreJobsInput) {
				in.ByCreatedBefore = aws.Time(now.Add(-time.Hour))
			},
		},
		{
			name:     "byCompleteAfter future excludes all",
			wantKeep: false,
			wantDrop: false,
			mutate: func(in *backupsdk.ListRestoreJobsInput) {
				in.ByCompleteAfter = aws.Time(now.Add(time.Hour))
			},
		},
		{
			name:     "byCompleteBefore past excludes all",
			wantKeep: false,
			wantDrop: false,
			mutate: func(in *backupsdk.ListRestoreJobsInput) {
				in.ByCompleteBefore = aws.Time(now.Add(-time.Hour))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := &backupsdk.ListRestoreJobsInput{}
			tc.mutate(in)

			out, err := client.ListRestoreJobs(t.Context(), in)
			require.NoError(t, err)

			ids := make([]string, 0, len(out.RestoreJobs))
			for _, j := range out.RestoreJobs {
				ids = append(ids, aws.ToString(j.RestoreJobId))
			}

			require.Equal(t, tc.wantKeep, slices.Contains(ids, keep.RestoreJobID), "keep presence")
			require.Equal(t, tc.wantDrop, slices.Contains(ids, drop.RestoreJobID), "drop presence")
		})
	}
}

// TestListScanJobs_WireFilters covers ListScanJobs, which is the single
// exception to the "by"-prefix-stripping pattern in this sweep: its wire
// keys keep the full PascalCase Go field name ("ByAccountId", not
// "accountId" -- serializers.go ListScanJobs query bindings). Before this
// fix gopherstack read no query filters at all for this op either.
func TestListScanJobs_WireFilters(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	vaultA := mustVault(t, backend, "sj-vault-a")
	mustVault(t, backend, "sj-vault-b")

	rpKeepArn := "arn:aws:backup:us-east-1:000000000000:recovery-point:sj-rp-keep"
	rpDropArn := "arn:aws:backup:us-east-1:000000000000:recovery-point:sj-rp-drop"
	mustRP(t, backend, "sj-vault-a", rpKeepArn, "arn:aws:ec2:us-east-1:000000000000:instance/i-sj-keep", "EC2")
	mustRP(t, backend, "sj-vault-b", rpDropArn, "arn:aws:ec2:us-east-1:000000000000:instance/i-sj-drop", "RDS")

	keep := backend.StartScanJob(vaultA.BackupVaultArn, backup.StartScanJobInput{
		BackupVaultName:  "sj-vault-a",
		IamRoleArn:       "arn:aws:iam::000000000000:role/r",
		MalwareScanner:   "GUARDDUTY",
		RecoveryPointArn: rpKeepArn,
		ScanMode:         "FULL_SCAN",
		ScannerRoleArn:   "arn:aws:iam::000000000000:role/scanner",
	})
	drop := backend.StartScanJob(
		"arn:aws:backup:us-east-1:000000000000:backup-vault:sj-vault-b",
		backup.StartScanJobInput{
			BackupVaultName:  "sj-vault-b",
			IamRoleArn:       "arn:aws:iam::000000000000:role/r",
			MalwareScanner:   "GUARDDUTY",
			RecoveryPointArn: rpDropArn,
			ScanMode:         "FULL_SCAN",
			ScannerRoleArn:   "arn:aws:iam::000000000000:role/scanner",
		},
	)

	tests := []struct {
		mutate   func(*backupsdk.ListScanJobsInput)
		name     string
		wantKeep bool
		wantDrop bool
	}{
		{name: "byBackupVaultName", wantKeep: true, wantDrop: false, mutate: func(in *backupsdk.ListScanJobsInput) {
			in.ByBackupVaultName = aws.String("sj-vault-a")
		}},
		{name: "byRecoveryPointArn", wantKeep: true, wantDrop: false, mutate: func(in *backupsdk.ListScanJobsInput) {
			in.ByRecoveryPointArn = aws.String(rpKeepArn)
		}},
		{name: "byResourceArn", wantKeep: true, wantDrop: false, mutate: func(in *backupsdk.ListScanJobsInput) {
			in.ByResourceArn = aws.String(keep.ResourceArn)
		}},
		{name: "byResourceType", wantKeep: true, wantDrop: false, mutate: func(in *backupsdk.ListScanJobsInput) {
			in.ByResourceType = types.ScanResourceTypeEc2
		}},
		{name: "byAccountId matches", wantKeep: true, wantDrop: true, mutate: func(in *backupsdk.ListScanJobsInput) {
			in.ByAccountId = aws.String("000000000000")
		}},
		{
			name:     "byAccountId wrong excludes all",
			wantKeep: false,
			wantDrop: false,
			mutate: func(in *backupsdk.ListScanJobsInput) {
				in.ByAccountId = aws.String("999999999999")
			},
		},
		{
			name:     "byState wrong excludes all",
			wantKeep: false,
			wantDrop: false,
			mutate: func(in *backupsdk.ListScanJobsInput) {
				in.ByState = types.ScanStateFailed
			},
		},
		{
			name:     "byMalwareScanner wrong excludes all",
			wantKeep: false,
			wantDrop: false,
			mutate: func(in *backupsdk.ListScanJobsInput) {
				in.ByMalwareScanner = "OTHER"
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := &backupsdk.ListScanJobsInput{}
			tc.mutate(in)

			out, err := client.ListScanJobs(t.Context(), in)
			require.NoError(t, err)

			ids := make([]string, 0, len(out.ScanJobs))
			for _, j := range out.ScanJobs {
				ids = append(ids, aws.ToString(j.ScanJobId))
			}

			require.Equal(t, tc.wantKeep, slices.Contains(ids, keep.ScanJobID), "keep presence")
			require.Equal(t, tc.wantDrop, slices.Contains(ids, drop.ScanJobID), "drop presence")
		})
	}
}

// TestListRestoreJobSummaries_State proves ListRestoreJobSummaries never
// read State/AccountId at all (real RestoreJobSummary, backup@v1.59.4
// api_op_ListRestoreJobSummaries.go, deserializers.go's per-field case
// switch: AccountId/Count/EndTime/Region/ResourceType/StartTime/State) --
// the handler returned a single fabricated {Count, Region} entry regardless
// of how many jobs existed or what state they were in, so a real client's
// State/AccountId fields were always empty/zero.
func TestListRestoreJobSummaries_State(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	mustVault(t, backend, "rjs-vault")
	rpArn := "arn:aws:backup:us-east-1:000000000000:recovery-point:rjs-rp"
	mustRP(t, backend, "rjs-vault", rpArn, "arn:aws:ec2:us-east-1:000000000000:instance/i-rjs", "EC2")

	iamRoleArn := "arn:aws:iam::000000000000:role/r"

	_, err := backend.StartRestoreJob(rpArn, iamRoleArn, "EC2", map[string]string{"k": "v"})
	require.NoError(t, err)
	_, err = backend.StartRestoreJob(rpArn, iamRoleArn, "EC2", map[string]string{"k": "v"})
	require.NoError(t, err)

	out, err := client.ListRestoreJobSummaries(t.Context(), &backupsdk.ListRestoreJobSummariesInput{})
	require.NoError(t, err)
	require.Len(t, out.RestoreJobSummaries, 1)

	summary := out.RestoreJobSummaries[0]
	assert.Equal(t, types.RestoreJobState("COMPLETED"), summary.State, "State must be populated, not dropped")
	assert.EqualValues(t, 2, summary.Count)
	assert.Equal(t, "000000000000", aws.ToString(summary.AccountId), "AccountId must be populated, not dropped")
	assert.Equal(t, "us-east-1", aws.ToString(summary.Region))
}

// TestListScanJobSummaries_State proves ListScanJobSummaries never read
// State/AccountId either (real ScanJobSummary, backup@v1.59.4
// api_op_ListScanJobSummaries.go): the handler returned a single fabricated
// {Count} entry with nothing else, regardless of how many scan jobs existed
// or what state they were in.
func TestListScanJobSummaries_State(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	vault := mustVault(t, backend, "sjs-vault")

	backend.StartScanJob(vault.BackupVaultArn, backup.StartScanJobInput{
		BackupVaultName:  "sjs-vault",
		IamRoleArn:       "arn:aws:iam::000000000000:role/r",
		MalwareScanner:   "GUARDDUTY",
		RecoveryPointArn: "arn:aws:backup:us-east-1:000000000000:recovery-point:sjs-rp-1",
		ScanMode:         "SNAPSHOT",
		ScannerRoleArn:   "arn:aws:iam::000000000000:role/scanner",
	})
	backend.StartScanJob(vault.BackupVaultArn, backup.StartScanJobInput{
		BackupVaultName:  "sjs-vault",
		IamRoleArn:       "arn:aws:iam::000000000000:role/r",
		MalwareScanner:   "GUARDDUTY",
		RecoveryPointArn: "arn:aws:backup:us-east-1:000000000000:recovery-point:sjs-rp-2",
		ScanMode:         "SNAPSHOT",
		ScannerRoleArn:   "arn:aws:iam::000000000000:role/scanner",
	})

	out, err := client.ListScanJobSummaries(t.Context(), &backupsdk.ListScanJobSummariesInput{})
	require.NoError(t, err)
	require.Len(t, out.ScanJobSummaries, 1)

	summary := out.ScanJobSummaries[0]
	assert.Equal(t, types.ScanJobStatus("COMPLETED"), summary.State, "State must be populated, not dropped")
	assert.EqualValues(t, 2, summary.Count)
	assert.Equal(t, "000000000000", aws.ToString(summary.AccountId), "AccountId must be populated, not dropped")
	assert.Equal(t, "us-east-1", aws.ToString(summary.Region))
}

// TestListRestoreJobs_Pagination proves ListRestoreJobsInput's MaxResults/
// NextToken (real query params -- backup@v1.59.4 serializers.go
// awsRestjson1_serializeOpHttpBindingsListRestoreJobsInput's encoder.SetQuery
// calls) were never read at all: RestoreJobsFilterFromQuery built a
// ListRestoreJobsFilter with no MaxResults/NextToken fields, so every real
// client's page size request was silently ignored and the full unpaginated
// set came back in one response every time.
func TestListRestoreJobs_Pagination(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	mustVault(t, backend, "rjp-vault")
	rpArn := "arn:aws:backup:us-east-1:000000000000:recovery-point:rjp-rp"
	mustRP(t, backend, "rjp-vault", rpArn, "arn:aws:ec2:us-east-1:000000000000:instance/i-rjp", "EC2")

	iamRoleArn := "arn:aws:iam::000000000000:role/r"

	job1, err := backend.StartRestoreJob(rpArn, iamRoleArn, "EC2", map[string]string{"k": "v"})
	require.NoError(t, err)
	job2, err := backend.StartRestoreJob(rpArn, iamRoleArn, "EC2", map[string]string{"k": "v"})
	require.NoError(t, err)

	page1, err := client.ListRestoreJobs(t.Context(), &backupsdk.ListRestoreJobsInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, page1.RestoreJobs, 1)
	require.NotNil(t, page1.NextToken, "a second page must exist")

	page2, err := client.ListRestoreJobs(t.Context(), &backupsdk.ListRestoreJobsInput{
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.RestoreJobs, 1)
	assert.Nil(t, page2.NextToken, "no third page")

	seen := map[string]bool{
		aws.ToString(page1.RestoreJobs[0].RestoreJobId): true,
		aws.ToString(page2.RestoreJobs[0].RestoreJobId): true,
	}
	assert.True(t, seen[job1.RestoreJobID])
	assert.True(t, seen[job2.RestoreJobID])
}

// TestListScanJobs_Pagination mirrors TestListRestoreJobs_Pagination for
// ListScanJobs, whose MaxResults/NextToken are query-bound under their
// PascalCase Go field names (backup@v1.59.4 serializers.go
// awsRestjson1_serializeOpHttpBindingsListScanJobsInput -- the one op in
// this service that keeps PascalCase on the wire, see ListScanJobs' own
// PARITY.md note).
func TestListScanJobs_Pagination(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	vault := mustVault(t, backend, "sjp-vault")

	job1 := backend.StartScanJob(vault.BackupVaultArn, backup.StartScanJobInput{
		BackupVaultName:  "sjp-vault",
		IamRoleArn:       "arn:aws:iam::000000000000:role/r",
		MalwareScanner:   "GUARDDUTY",
		RecoveryPointArn: "arn:aws:backup:us-east-1:000000000000:recovery-point:sjp-rp-1",
		ScanMode:         "SNAPSHOT",
		ScannerRoleArn:   "arn:aws:iam::000000000000:role/scanner",
	})
	job2 := backend.StartScanJob(vault.BackupVaultArn, backup.StartScanJobInput{
		BackupVaultName:  "sjp-vault",
		IamRoleArn:       "arn:aws:iam::000000000000:role/r",
		MalwareScanner:   "GUARDDUTY",
		RecoveryPointArn: "arn:aws:backup:us-east-1:000000000000:recovery-point:sjp-rp-2",
		ScanMode:         "SNAPSHOT",
		ScannerRoleArn:   "arn:aws:iam::000000000000:role/scanner",
	})

	page1, err := client.ListScanJobs(t.Context(), &backupsdk.ListScanJobsInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, page1.ScanJobs, 1)
	require.NotNil(t, page1.NextToken, "a second page must exist")

	page2, err := client.ListScanJobs(t.Context(), &backupsdk.ListScanJobsInput{
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.ScanJobs, 1)
	assert.Nil(t, page2.NextToken, "no third page")

	seen := map[string]bool{
		aws.ToString(page1.ScanJobs[0].ScanJobId): true,
		aws.ToString(page2.ScanJobs[0].ScanJobId): true,
	}
	assert.True(t, seen[job1.ScanJobID])
	assert.True(t, seen[job2.ScanJobID])
}

// TestListProtectedResources_Pagination proves ListProtectedResources honors
// MaxResults/NextToken (real query params, backup@v1.59.4 serializers.go
// awsRestjson1_serializeOpHttpBindingsListProtectedResourcesInput) -- prior
// code ignored both and always returned every record in one response.
func TestListProtectedResources_Pagination(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	mustVault(t, backend, "prp-vault")
	backend.PutProtectedResource("arn:aws:ec2:us-east-1:000000000000:instance/i-prp-1", "EC2", "prp-vault")
	backend.PutProtectedResource("arn:aws:ec2:us-east-1:000000000000:instance/i-prp-2", "EC2", "prp-vault")

	page1, err := client.ListProtectedResources(
		t.Context(), &backupsdk.ListProtectedResourcesInput{MaxResults: aws.Int32(1)},
	)
	require.NoError(t, err)
	require.Len(t, page1.Results, 1)
	require.NotNil(t, page1.NextToken, "a second page must exist")

	page2, err := client.ListProtectedResources(t.Context(), &backupsdk.ListProtectedResourcesInput{
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Results, 1)
	assert.Nil(t, page2.NextToken, "no third page")

	seen := map[string]bool{
		aws.ToString(page1.Results[0].ResourceArn): true,
		aws.ToString(page2.Results[0].ResourceArn): true,
	}
	assert.True(t, seen["arn:aws:ec2:us-east-1:000000000000:instance/i-prp-1"])
	assert.True(t, seen["arn:aws:ec2:us-east-1:000000000000:instance/i-prp-2"])
}

// TestListProtectedResourcesByBackupVault_Pagination mirrors
// TestListProtectedResources_Pagination for the vault-scoped variant (same
// serializer, plus a required BackupVaultName URI member).
func TestListProtectedResourcesByBackupVault_Pagination(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	mustVault(t, backend, "prpv-vault")
	backend.PutProtectedResource("arn:aws:ec2:us-east-1:000000000000:instance/i-prpv-1", "EC2", "prpv-vault")
	backend.PutProtectedResource("arn:aws:ec2:us-east-1:000000000000:instance/i-prpv-2", "EC2", "prpv-vault")

	page1, err := client.ListProtectedResourcesByBackupVault(
		t.Context(),
		&backupsdk.ListProtectedResourcesByBackupVaultInput{
			BackupVaultName: aws.String("prpv-vault"),
			MaxResults:      aws.Int32(1),
		},
	)
	require.NoError(t, err)
	require.Len(t, page1.Results, 1)
	require.NotNil(t, page1.NextToken, "a second page must exist")

	page2, err := client.ListProtectedResourcesByBackupVault(
		t.Context(),
		&backupsdk.ListProtectedResourcesByBackupVaultInput{
			BackupVaultName: aws.String("prpv-vault"),
			MaxResults:      aws.Int32(1),
			NextToken:       page1.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, page2.Results, 1)
	assert.Nil(t, page2.NextToken, "no third page")

	seen := map[string]bool{
		aws.ToString(page1.Results[0].ResourceArn): true,
		aws.ToString(page2.Results[0].ResourceArn): true,
	}
	assert.True(t, seen["arn:aws:ec2:us-east-1:000000000000:instance/i-prpv-1"])
	assert.True(t, seen["arn:aws:ec2:us-east-1:000000000000:instance/i-prpv-2"])
}
