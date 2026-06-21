package backup_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func newTestBackend(t *testing.T) *backup.InMemoryBackend {
	t.Helper()

	return backup.NewInMemoryBackend("123456789012", "us-east-1")
}

// mustVault creates a vault or fatals.
func mustVault(t *testing.T, b *backup.InMemoryBackend, name string) *backup.Vault {
	t.Helper()
	v, err := b.CreateBackupVault(name, "", "", nil)
	if err != nil {
		t.Fatalf("CreateBackupVault(%q): %v", name, err)
	}

	return v
}

// mustPlan creates a plan with one valid rule or fatals.
func mustPlan(t *testing.T, b *backup.InMemoryBackend, name, vaultName string) *backup.Plan {
	t.Helper()
	rules := []backup.Rule{{RuleName: "daily", TargetVaultName: vaultName}}
	p, err := b.CreateBackupPlanValidated(name, rules, nil, nil)
	if err != nil {
		t.Fatalf("CreateBackupPlanValidated(%q): %v", name, err)
	}

	return p
}

// mustJob creates a backup job or fatals.
func mustJob(
	t *testing.T,
	b *backup.InMemoryBackend,
	vaultName, resourceArn, resourceType string,
) *backup.Job {
	t.Helper()
	j, err := b.StartBackupJob(vaultName, resourceArn, "arn:aws:iam::123:role/r", resourceType)
	if err != nil {
		t.Fatalf("StartBackupJob: %v", err)
	}

	return j
}

// mustRP adds a recovery point to a vault or fatals.
func mustRP(
	t *testing.T,
	b *backup.InMemoryBackend,
	vaultName, rpArn, resourceArn, resourceType string,
) {
	t.Helper()
	now := time.Now().UTC()
	rp := &backup.RecoveryPoint{
		RecoveryPointArn: rpArn,
		BackupVaultName:  vaultName,
		ResourceArn:      resourceArn,
		ResourceType:     resourceType,
		Status:           "COMPLETED",
		CreationDate:     now,
	}
	if err := b.AddRecoveryPoint(vaultName, rp); err != nil {
		t.Fatalf("AddRecoveryPoint: %v", err)
	}
}

// ---- Rule validation ----

func TestValidateRules(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	mustVault(t, b, "vault-a")

	cases := []struct {
		name    string
		rules   []backup.Rule
		wantErr bool
	}{
		{
			name:    "empty rules ok",
			rules:   nil,
			wantErr: false,
		},
		{
			name:    "valid single rule",
			rules:   []backup.Rule{{RuleName: "daily", TargetVaultName: "vault-a"}},
			wantErr: false,
		},
		{
			name:    "missing RuleName",
			rules:   []backup.Rule{{TargetVaultName: "vault-a"}},
			wantErr: true,
		},
		{
			name:    "missing TargetVaultName",
			rules:   []backup.Rule{{RuleName: "daily"}},
			wantErr: true,
		},
		{
			name: "duplicate rule name",
			rules: []backup.Rule{
				{RuleName: "daily", TargetVaultName: "vault-a"},
				{RuleName: "daily", TargetVaultName: "vault-a"},
			},
			wantErr: true,
		},
		{
			name: "two valid rules",
			rules: []backup.Rule{
				{RuleName: "daily", TargetVaultName: "vault-a"},
				{RuleName: "weekly", TargetVaultName: "vault-a"},
			},
			wantErr: false,
		},
		{
			name: "lifecycle delete before cold storage",
			rules: []backup.Rule{
				{
					RuleName:        "bad-lifecycle",
					TargetVaultName: "vault-a",
					Lifecycle: &backup.Lifecycle{
						MoveToColdStorageAfterDays: 30,
						DeleteAfterDays:            20,
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := b.CreateBackupPlanValidated("plan-"+tc.name, tc.rules, nil, nil)
			if (err != nil) != tc.wantErr {
				t.Errorf("wantErr=%v got=%v err=%v", tc.wantErr, err != nil, err)
			}
		})
	}
}

// ---- DeleteBackupPlan with selections ----

func TestDeleteBackupPlanChecked(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	mustVault(t, b, "v1")

	t.Run("delete plan without selections succeeds", func(t *testing.T) {
		t.Parallel()
		p := mustPlan(t, b, "plan-empty", "v1")
		_, err := b.DeleteBackupPlanChecked(p.BackupPlanID)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("delete plan with selections fails", func(t *testing.T) {
		t.Parallel()
		p := mustPlan(t, b, "plan-with-sel", "v1")
		_, selErr := b.CreateBackupSelection(
			p.BackupPlanID, "sel1", "arn:aws:iam::123:role/r", nil, nil, nil, nil,
		)
		if selErr != nil {
			t.Fatalf("CreateBackupSelection: %v", selErr)
		}
		_, err := b.DeleteBackupPlanChecked(p.BackupPlanID)
		if err == nil {
			t.Error("expected error deleting plan with selections, got nil")
		}
	})

	t.Run("delete nonexistent plan returns not-found", func(t *testing.T) {
		t.Parallel()
		_, err := b.DeleteBackupPlanChecked("no-such-id")
		if err == nil {
			t.Error("expected error, got nil")
		}
	})
}

// ---- DeleteBackupVault with lock enforcement ----

func TestDeleteBackupVaultChecked(t *testing.T) {
	t.Parallel()

	t.Run("delete unlocked empty vault succeeds", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		mustVault(t, b, "unlocked")
		if err := b.DeleteBackupVaultChecked("unlocked"); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("delete vault with recovery points fails", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		mustVault(t, b, "has-rp")
		mustRP(t, b, "has-rp", "arn:aws:backup:::rp/1", "arn:aws:ec2:::instance/i-1", "EC2")
		if err := b.DeleteBackupVaultChecked("has-rp"); err == nil {
			t.Error("expected error deleting vault with recovery points")
		}
	})

	t.Run("delete locked vault fails", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		mustVault(t, b, "locked")
		// Pass a LockDate in the past directly — PutBackupVaultLockConfiguration stores it as-is
		// when ChangeableForDays == 0, so the vault is immediately in locked state.
		past := time.Now().Add(-1 * time.Hour)
		if lockErr := b.PutBackupVaultLockConfiguration("locked", &backup.VaultLockConfig{
			MinRetentionDays: 1,
			MaxRetentionDays: 365,
			LockDate:         &past,
		}); lockErr != nil {
			t.Fatalf("PutBackupVaultLockConfiguration: %v", lockErr)
		}
		if delErr := b.DeleteBackupVaultChecked("locked"); delErr == nil {
			t.Error("expected error deleting locked vault")
		}
	})

	t.Run("delete nonexistent vault returns not-found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		if err := b.DeleteBackupVaultChecked("ghost"); err == nil {
			t.Error("expected error, got nil")
		}
	})
}

// ---- CompleteBackupJob ----

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

func TestListRecoveryPointsFiltered(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	mustVault(t, b, "rp-vault")

	now := time.Now().UTC()
	rps := []*backup.RecoveryPoint{
		{
			RecoveryPointArn: "arn:aws:backup:::rp/rp-1",
			BackupVaultName:  "rp-vault",
			ResourceArn:      "arn:aws:ec2:::instance/i-1",
			ResourceType:     "EC2",
			Status:           "COMPLETED",
			CreationDate:     now,
		},
		{
			RecoveryPointArn: "arn:aws:backup:::rp/rp-2",
			BackupVaultName:  "rp-vault",
			ResourceArn:      "arn:aws:rds:::db/db-1",
			ResourceType:     "RDS",
			Status:           "COMPLETED",
			CreationDate:     now.Add(-2 * time.Hour),
		},
		{
			RecoveryPointArn:       "arn:aws:backup:::rp/rp-3",
			BackupVaultName:        "rp-vault",
			ResourceArn:            "arn:aws:ec2:::instance/i-2",
			ResourceType:           "EC2",
			Status:                 "COMPLETED",
			CreationDate:           now.Add(-1 * time.Hour),
			ParentRecoveryPointArn: "arn:aws:backup:::rp/rp-parent",
		},
	}
	for _, rp := range rps {
		if err := b.AddRecoveryPoint(rp.BackupVaultName, rp); err != nil {
			t.Fatalf("AddRecoveryPoint: %v", err)
		}
	}

	createdAfter30m := now.Add(-30 * time.Minute)
	createdBefore30m := now.Add(-30 * time.Minute)

	cases := []struct {
		name      string
		vaultName string
		filter    backup.ListRPFilter
		wantArns  []string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "no filter returns all",
			vaultName: "rp-vault",
			filter:    backup.ListRPFilter{},
			wantCount: 3,
		},
		{
			name:      "filter by EC2",
			vaultName: "rp-vault",
			filter:    backup.ListRPFilter{ResourceType: "EC2"},
			wantCount: 2,
		},
		{
			name:      "filter by RDS",
			vaultName: "rp-vault",
			filter:    backup.ListRPFilter{ResourceType: "RDS"},
			wantCount: 1,
			wantArns:  []string{"arn:aws:backup:::rp/rp-2"},
		},
		{
			name:      "filter by resourceArn",
			vaultName: "rp-vault",
			filter:    backup.ListRPFilter{ResourceArn: "arn:aws:ec2:::instance/i-1"},
			wantCount: 1,
			wantArns:  []string{"arn:aws:backup:::rp/rp-1"},
		},
		{
			name:      "filter by parentRecoveryPointArn",
			vaultName: "rp-vault",
			filter:    backup.ListRPFilter{ParentRecoveryPointArn: "arn:aws:backup:::rp/rp-parent"},
			wantCount: 1,
			wantArns:  []string{"arn:aws:backup:::rp/rp-3"},
		},
		{
			name:      "filter by createdAfter 30m ago returns recent ones",
			vaultName: "rp-vault",
			filter:    backup.ListRPFilter{CreatedAfter: &createdAfter30m},
			wantCount: 1,
			wantArns:  []string{"arn:aws:backup:::rp/rp-1"},
		},
		{
			name:      "filter by createdBefore 30m ago",
			vaultName: "rp-vault",
			filter:    backup.ListRPFilter{CreatedBefore: &createdBefore30m},
			wantCount: 2,
		},
		{
			name:      "nonexistent vault returns not-found error",
			vaultName: "ghost-vault",
			filter:    backup.ListRPFilter{},
			wantErr:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, _, err := b.ListRecoveryPointsFiltered(tc.vaultName, tc.filter)
			if tc.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}

				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != tc.wantCount {
				t.Errorf("count: want %d got %d", tc.wantCount, len(got))
			}
			for _, wantArn := range tc.wantArns {
				found := false
				for _, rp := range got {
					if rp.RecoveryPointArn == wantArn {
						found = true

						break
					}
				}
				if !found {
					t.Errorf("expected rp %s in results", wantArn)
				}
			}
		})
	}
}

// ---- ListRecoveryPoints pagination ----

func TestListRecoveryPointsPagination(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	mustVault(t, b, "rp-pg-vault")

	const total = 8
	for i := range total {
		rp := &backup.RecoveryPoint{
			RecoveryPointArn: fmt.Sprintf("arn:aws:backup:::rp/rp-%d", i),
			BackupVaultName:  "rp-pg-vault",
			ResourceArn:      fmt.Sprintf("arn:aws:ec2:::instance/i-%d", i),
			ResourceType:     "EC2",
			Status:           "COMPLETED",
			CreationDate:     time.Now().UTC(),
		}
		if err := b.AddRecoveryPoint("rp-pg-vault", rp); err != nil {
			t.Fatalf("AddRecoveryPoint: %v", err)
		}
	}

	t.Run("paginate all items", func(t *testing.T) {
		t.Parallel()
		var all []*backup.RecoveryPoint
		nextToken := ""
		for {
			got, next, err := b.ListRecoveryPointsFiltered(
				"rp-pg-vault",
				backup.ListRPFilter{MaxResults: 3, NextToken: nextToken},
			)
			if err != nil {
				t.Fatalf("ListRecoveryPointsFiltered: %v", err)
			}
			all = append(all, got...)
			if next == "" {
				break
			}
			nextToken = next
		}
		if len(all) != total {
			t.Errorf("pagination: want %d got %d", total, len(all))
		}
	})
}

// ---- ListCopyJobsFiltered ----

func TestListCopyJobsFiltered(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	mustVault(t, b, "src-vault")
	mustVault(t, b, "dst-vault")
	mustVault(t, b, "dst-vault2")

	j1 := b.StartCopyJob(
		"arn:aws:backup:::rp/rp-1",
		"arn:aws:backup:::vault/src-vault",
		"arn:aws:backup:::vault/dst-vault",
		"arn:aws:iam::123:role/r",
	)
	_ = b.StartCopyJob(
		"arn:aws:backup:::rp/rp-2",
		"arn:aws:backup:::vault/src-vault",
		"arn:aws:backup:::vault/dst-vault2",
		"arn:aws:iam::123:role/r",
	)

	futureTime := time.Now().Add(time.Hour)

	cases := []struct {
		name      string
		filter    backup.ListCopyJobsFilter
		wantIDs   []string
		wantCount int
	}{
		{
			name:      "no filter returns all",
			filter:    backup.ListCopyJobsFilter{},
			wantCount: 2,
		},
		{
			name: "filter by destination vault",
			filter: backup.ListCopyJobsFilter{
				DestinationBackupVaultArn: "arn:aws:backup:::vault/dst-vault",
			},
			wantCount: 1,
			wantIDs:   []string{j1.CopyJobID},
		},
		{
			name: "filter by source vault",
			filter: backup.ListCopyJobsFilter{
				SourceBackupVaultArn: "arn:aws:backup:::vault/src-vault",
			},
			wantCount: 2,
		},
		{
			name:      "filter by state COMPLETED",
			filter:    backup.ListCopyJobsFilter{State: "COMPLETED"},
			wantCount: 2,
		},
		{
			name:      "filter by state RUNNING returns none",
			filter:    backup.ListCopyJobsFilter{State: "RUNNING"},
			wantCount: 0,
		},
		{
			name:      "filter by account ID matches",
			filter:    backup.ListCopyJobsFilter{AccountID: "123456789012"},
			wantCount: 2,
		},
		{
			name:      "filter by createdAfter far future",
			filter:    backup.ListCopyJobsFilter{CreatedAfter: &futureTime},
			wantCount: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, _ := b.ListCopyJobsFiltered(tc.filter)
			if len(got) != tc.wantCount {
				t.Errorf("count: want %d got %d", tc.wantCount, len(got))
			}
			for _, wantID := range tc.wantIDs {
				found := false
				for _, jj := range got {
					if jj.CopyJobID == wantID {
						found = true

						break
					}
				}
				if !found {
					t.Errorf("expected copy job %s in results", wantID)
				}
			}
		})
	}
}

// ---- ListBackupVaultsFiltered ----

func TestListBackupVaultsFiltered(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	mustVault(t, b, "plain-vault")
	mustVault(t, b, "plain-vault2")

	// Create a logically air-gapped vault by setting lock with MinRetentionDays.
	mustVault(t, b, "locked-vault")
	if err := b.PutBackupVaultLockConfiguration("locked-vault", &backup.VaultLockConfig{
		MinRetentionDays: 30,
		MaxRetentionDays: 365,
	}); err != nil {
		t.Fatalf("PutBackupVaultLockConfiguration: %v", err)
	}

	cases := []struct {
		name      string
		filter    backup.ListVaultsFilter
		wantCount int
	}{
		{
			name:      "no filter returns all",
			filter:    backup.ListVaultsFilter{},
			wantCount: 3,
		},
		{
			name:      "maxResults=1 limits page",
			filter:    backup.ListVaultsFilter{MaxResults: 1},
			wantCount: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, _ := b.ListBackupVaultsFiltered(tc.filter)
			if len(got) != tc.wantCount {
				t.Errorf("count: want %d got %d", tc.wantCount, len(got))
			}
		})
	}
}

// ---- ListBackupVaults pagination ----

func TestListBackupVaultsPagination(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	const total = 6
	for i := range total {
		mustVault(t, b, fmt.Sprintf("pg-vault-%d", i))
	}

	t.Run("paginate all vaults", func(t *testing.T) {
		t.Parallel()
		var all []*backup.Vault
		nextToken := ""
		for {
			got, next := b.ListBackupVaultsFiltered(
				backup.ListVaultsFilter{MaxResults: 2, NextToken: nextToken},
			)
			all = append(all, got...)
			if next == "" {
				break
			}
			nextToken = next
		}
		if len(all) != total {
			t.Errorf("want %d got %d", total, len(all))
		}
	})
}

// ---- ListBackupPlansPaged pagination ----

func TestListBackupPlansPagination(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	mustVault(t, b, "plan-vault")
	const total = 7
	for i := range total {
		mustPlan(t, b, fmt.Sprintf("plan-%d", i), "plan-vault")
	}

	t.Run("paginate all plans", func(t *testing.T) {
		t.Parallel()
		var all []*backup.Plan
		nextToken := ""
		for {
			got, next := b.ListBackupPlansPaged(
				backup.ListPlansFilter{MaxResults: 3, NextToken: nextToken},
			)
			all = append(all, got...)
			if next == "" {
				break
			}
			nextToken = next
		}
		if len(all) != total {
			t.Errorf("want %d got %d", total, len(all))
		}
	})
}

// ---- CreateBackupPlanValidated ----

func TestCreateBackupPlanValidated(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	mustVault(t, b, "vv")

	rules := []backup.Rule{{RuleName: "r1", TargetVaultName: "vv"}}
	p1, err := b.CreateBackupPlanValidated("my-plan-a", rules, nil, nil)
	if err != nil {
		t.Fatalf("first create: %v", err)
	}

	// Creating a second plan with a different name succeeds with a distinct ID.
	p2, err := b.CreateBackupPlanValidated("my-plan-b", rules, nil, nil)
	if err != nil {
		t.Fatalf("second create: %v", err)
	}
	if p1.BackupPlanID == p2.BackupPlanID {
		t.Error("expected distinct IDs for different-name plans")
	}

	// Duplicate name returns an error.
	_, err = b.CreateBackupPlanValidated("my-plan-a", rules, nil, nil)
	if err == nil {
		t.Error("expected error for duplicate plan name, got nil")
	}
}

// ---- UpdateBackupPlanValidated ----

func TestUpdateBackupPlanValidated(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	mustVault(t, b, "uv-vault")

	p := mustPlan(t, b, "up-plan", "uv-vault")

	t.Run("update with valid rules succeeds", func(t *testing.T) {
		t.Parallel()
		newRules := []backup.Rule{{RuleName: "weekly", TargetVaultName: "uv-vault"}}
		updated, err := b.UpdateBackupPlanValidated(p.BackupPlanID, newRules, nil)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if updated.BackupPlanID != p.BackupPlanID {
			t.Errorf("plan ID changed unexpectedly")
		}
	})

	t.Run("update with invalid rules returns validation error", func(t *testing.T) {
		t.Parallel()
		badRules := []backup.Rule{{RuleName: "", TargetVaultName: "uv-vault"}}
		_, err := b.UpdateBackupPlanValidated(p.BackupPlanID, badRules, nil)
		if err == nil {
			t.Error("expected validation error, got nil")
		}
	})
}

// ---- parseTimeFilter ----

func TestParseTimeFilter(t *testing.T) {
	t.Parallel()
	cases := []struct {
		input   string
		wantNil bool
	}{
		{input: "", wantNil: true},
		{input: "not-a-date", wantNil: true},
		{input: "2024-01-15T12:00:00Z", wantNil: false},
		{input: "2024-01-15T12:00:00+05:30", wantNil: false},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			got := backup.ParseTimeFilter(tc.input)
			if tc.wantNil && got != nil {
				t.Errorf("expected nil, got %v", got)
			}
			if !tc.wantNil && got == nil {
				t.Error("expected non-nil time")
			}
		})
	}
}
