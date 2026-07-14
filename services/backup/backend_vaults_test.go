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
