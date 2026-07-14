package backup_test

import (
	"fmt"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

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
