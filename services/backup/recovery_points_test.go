package backup_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

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
