package backup_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

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

// ---- RestoreAccessVault ----

func TestRestoreAccessVaultCreate(t *testing.T) {
	t.Parallel()

	t.Run("resolves SourceBackupVaultArn to a real vault", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		src := mustVault(t, b, "src-vault")
		rav, err := b.CreateRestoreAccessBackupVault(src.BackupVaultArn, "rav1", "", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if rav.SourceBackupVaultArn != src.BackupVaultArn {
			t.Errorf("SourceBackupVaultArn: want %s got %s", src.BackupVaultArn, rav.SourceBackupVaultArn)
		}
		if rav.RestoreAccessBackupVaultName != "rav1" {
			t.Errorf("RestoreAccessBackupVaultName: want rav1 got %s", rav.RestoreAccessBackupVaultName)
		}
	})

	t.Run("unresolvable SourceBackupVaultArn is not-found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateRestoreAccessBackupVault(
			"arn:aws:backup:us-east-1:000000000000:backup-vault:ghost", "rav-ghost", "", nil,
		)
		if err == nil {
			t.Fatal("expected error for unresolvable source vault ARN")
		}
	})

	t.Run("missing SourceBackupVaultArn is a validation error", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateRestoreAccessBackupVault("", "rav-noarn", "", nil)
		if err == nil {
			t.Fatal("expected error for missing SourceBackupVaultArn")
		}
	})
}

func TestRestoreAccessVaultList(t *testing.T) {
	t.Parallel()

	t.Run("scoped to the source vault name, sorted by name", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		srcA := mustVault(t, b, "src-a")
		srcB := mustVault(t, b, "src-b")

		if _, err := b.CreateRestoreAccessBackupVault(srcA.BackupVaultArn, "rav-a2", "", nil); err != nil {
			t.Fatalf("CreateRestoreAccessBackupVault: %v", err)
		}
		if _, err := b.CreateRestoreAccessBackupVault(srcA.BackupVaultArn, "rav-a1", "", nil); err != nil {
			t.Fatalf("CreateRestoreAccessBackupVault: %v", err)
		}
		if _, err := b.CreateRestoreAccessBackupVault(srcB.BackupVaultArn, "rav-b1", "", nil); err != nil {
			t.Fatalf("CreateRestoreAccessBackupVault: %v", err)
		}

		got, err := b.ListRestoreAccessBackupVaults("src-a")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("want 2 restore access vaults for src-a, got %d", len(got))
		}
		if got[0].RestoreAccessBackupVaultName != "rav-a1" || got[1].RestoreAccessBackupVaultName != "rav-a2" {
			t.Errorf(
				"unexpected order: %s, %s",
				got[0].RestoreAccessBackupVaultName,
				got[1].RestoreAccessBackupVaultName,
			)
		}
	})

	t.Run("unknown source vault name is not-found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		if _, err := b.ListRestoreAccessBackupVaults("ghost"); err == nil {
			t.Fatal("expected error for unknown source vault")
		}
	})
}

func TestRestoreAccessVaultRevoke(t *testing.T) {
	t.Parallel()

	t.Run("removes a vault sourced from the given name", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		src := mustVault(t, b, "revoke-src")
		rav, err := b.CreateRestoreAccessBackupVault(src.BackupVaultArn, "revoke-rav", "", nil)
		if err != nil {
			t.Fatalf("CreateRestoreAccessBackupVault: %v", err)
		}

		if revokeErr := b.RevokeRestoreAccessBackupVault(
			"revoke-src",
			rav.RestoreAccessBackupVaultArn,
		); revokeErr != nil {
			t.Fatalf("unexpected error: %v", revokeErr)
		}

		remaining, err := b.ListRestoreAccessBackupVaults("revoke-src")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(remaining) != 0 {
			t.Errorf("want 0 remaining restore access vaults, got %d", len(remaining))
		}
	})

	t.Run("mismatched source vault name is not-found (no cross-vault revoke)", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		srcA := mustVault(t, b, "revoke-a")
		mustVault(t, b, "revoke-b")
		rav, err := b.CreateRestoreAccessBackupVault(srcA.BackupVaultArn, "revoke-cross", "", nil)
		if err != nil {
			t.Fatalf("CreateRestoreAccessBackupVault: %v", err)
		}

		if revokeErr := b.RevokeRestoreAccessBackupVault(
			"revoke-b",
			rav.RestoreAccessBackupVaultArn,
		); revokeErr == nil {
			t.Fatal("expected error revoking a restore access vault scoped to a different source vault")
		}
	})

	t.Run("unknown ARN is not-found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		mustVault(t, b, "revoke-none")
		if err := b.RevokeRestoreAccessBackupVault(
			"revoke-none",
			"arn:aws:backup:::restore-access-backup-vault:ghost",
		); err == nil {
			t.Fatal("expected error for unknown restore access vault ARN")
		}
	})
}
