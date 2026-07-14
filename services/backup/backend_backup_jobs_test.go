package backup_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func TestCompleteBackupJob(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	mustVault(t, b, "vault-complete")
	j := mustJob(t, b, "vault-complete", "arn:aws:ec2:::instance/i-1", "EC2")

	if j.State != "CREATED" {
		t.Errorf("initial state want CREATED got %s", j.State)
	}

	if err := b.CompleteBackupJob(j.BackupJobID); err != nil {
		t.Fatalf("CompleteBackupJob: %v", err)
	}

	// Job state should be COMPLETED.
	got, err := b.DescribeBackupJob(j.BackupJobID)
	if err != nil {
		t.Fatalf("DescribeBackupJob: %v", err)
	}
	if got.State != "COMPLETED" {
		t.Errorf("state want COMPLETED got %s", got.State)
	}
	if got.RecoveryPointArn == "" {
		t.Error("RecoveryPointArn should be set after completion")
	}

	// A recovery point should now exist in the vault.
	rps, rpErr := b.ListRecoveryPointsByBackupVault("vault-complete")
	if rpErr != nil {
		t.Fatalf("ListRecoveryPointsByBackupVault: %v", rpErr)
	}
	if len(rps) != 1 {
		t.Errorf("want 1 recovery point, got %d", len(rps))
	}

	// NumberOfRecoveryPoints should be incremented.
	v, vErr := b.DescribeBackupVault("vault-complete")
	if vErr != nil {
		t.Fatalf("DescribeBackupVault: %v", vErr)
	}
	if v.NumberOfRecoveryPoints != 1 {
		t.Errorf("NumberOfRecoveryPoints want 1 got %d", v.NumberOfRecoveryPoints)
	}

	t.Run("complete nonexistent job returns error", func(t *testing.T) {
		t.Parallel()
		if completeErr := b.CompleteBackupJob("no-such-job"); completeErr == nil {
			t.Error("expected error, got nil")
		}
	})
}

// ---- ListBackupJobsFiltered ----

func TestListBackupJobsFiltered(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	mustVault(t, b, "vault-jobs")
	mustVault(t, b, "vault-other")

	j1 := mustJob(t, b, "vault-jobs", "arn:aws:ec2:::instance/i-1", "EC2")
	j2 := mustJob(t, b, "vault-jobs", "arn:aws:rds:::db/db-1", "RDS")
	_ = mustJob(t, b, "vault-other", "arn:aws:ec2:::instance/i-2", "EC2")

	futureTime := time.Now().Add(time.Hour)
	pastTime := time.Now().Add(-time.Hour)

	cases := []struct {
		name      string
		filter    backup.ListBackupJobsFilter
		wantIDs   []string
		wantCount int
	}{
		{
			name:      "no filter returns all",
			filter:    backup.ListBackupJobsFilter{},
			wantCount: 3,
		},
		{
			name:      "filter by vault name",
			filter:    backup.ListBackupJobsFilter{VaultName: "vault-jobs"},
			wantCount: 2,
		},
		{
			name:      "filter by resourceType EC2",
			filter:    backup.ListBackupJobsFilter{ResourceType: "EC2"},
			wantCount: 2,
		},
		{
			name:      "filter by resourceType RDS",
			filter:    backup.ListBackupJobsFilter{ResourceType: "RDS"},
			wantCount: 1,
			wantIDs:   []string{j2.BackupJobID},
		},
		{
			name:      "filter by resourceArn",
			filter:    backup.ListBackupJobsFilter{ResourceArn: "arn:aws:ec2:::instance/i-1"},
			wantCount: 1,
			wantIDs:   []string{j1.BackupJobID},
		},
		{
			name:      "filter by state CREATED",
			filter:    backup.ListBackupJobsFilter{State: "CREATED"},
			wantCount: 3,
		},
		{
			name:      "filter by state COMPLETED returns none",
			filter:    backup.ListBackupJobsFilter{State: "COMPLETED"},
			wantCount: 0,
		},
		{
			name:      "filter by createdAfter far future returns none",
			filter:    backup.ListBackupJobsFilter{CreatedAfter: &futureTime},
			wantCount: 0,
		},
		{
			name:      "filter by createdBefore far past returns none",
			filter:    backup.ListBackupJobsFilter{CreatedBefore: &pastTime},
			wantCount: 0,
		},
		{
			name:      "accountID filter matches all",
			filter:    backup.ListBackupJobsFilter{AccountID: "123456789012"},
			wantCount: 3,
		},
		{
			name:      "accountID filter no match",
			filter:    backup.ListBackupJobsFilter{AccountID: "999999999999"},
			wantCount: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, _ := b.ListBackupJobsFiltered(tc.filter)
			if len(got) != tc.wantCount {
				t.Errorf("count: want %d got %d", tc.wantCount, len(got))
			}
			for _, wantID := range tc.wantIDs {
				found := false
				for _, jj := range got {
					if jj.BackupJobID == wantID {
						found = true

						break
					}
				}
				if !found {
					t.Errorf("expected job %s in results", wantID)
				}
			}
		})
	}
}

// ---- ListBackupJobs pagination ----

func TestListBackupJobsPagination(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	mustVault(t, b, "pg-vault")

	const total = 10
	for i := range total {
		mustJob(t, b, "pg-vault", fmt.Sprintf("arn:aws:ec2:::instance/i-%d", i), "EC2")
	}

	t.Run("maxResults limits page size", func(t *testing.T) {
		t.Parallel()
		got, next := b.ListBackupJobsFiltered(backup.ListBackupJobsFilter{MaxResults: 3})
		if len(got) != 3 {
			t.Errorf("want 3 got %d", len(got))
		}
		if next == "" {
			t.Error("expected NextToken for subsequent page")
		}
	})

	t.Run("full pagination collects all items", func(t *testing.T) {
		t.Parallel()
		var all []*backup.Job
		nextToken := ""
		for {
			got, next := b.ListBackupJobsFiltered(
				backup.ListBackupJobsFilter{MaxResults: 3, NextToken: nextToken},
			)
			all = append(all, got...)
			if next == "" {
				break
			}
			nextToken = next
		}
		if len(all) != total {
			t.Errorf("pagination: want %d total got %d", total, len(all))
		}
	})

	t.Run("invalid next token returns empty", func(t *testing.T) {
		t.Parallel()
		got, _ := b.ListBackupJobsFiltered(
			backup.ListBackupJobsFilter{MaxResults: 3, NextToken: "nonexistent-token"},
		)
		if len(got) != 0 {
			t.Errorf("invalid token: want empty, got %d", len(got))
		}
	})
}

// ---- ListRecoveryPointsFiltered ----
