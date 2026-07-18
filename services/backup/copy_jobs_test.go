package backup_test

import (
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

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
