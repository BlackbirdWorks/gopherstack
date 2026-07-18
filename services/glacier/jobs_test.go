package glacier_test

import (
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/glacier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		jobType     string
		archiveID   string
		expectJobID bool
		wantErr     bool
	}{
		{
			name:        "inventory_retrieval",
			jobType:     "InventoryRetrieval",
			expectJobID: true,
		},
		{
			name:        "archive_retrieval",
			jobType:     "ArchiveRetrieval",
			archiveID:   "test-archive-id",
			expectJobID: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			// This case exercises CRUD, not the async window: complete jobs immediately.
			glacier.SetRetrievalDelay(bk, 0)
			_, err := bk.CreateVault(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			if tt.archiveID != "" {
				bk.AddArchiveInternal(testAccountID, testRegion, "vault", &glacier.Archive{
					ArchiveID: tt.archiveID,
					Size:      1024,
				})
			}

			req := &glacier.ExportedInitiateJobRequest{
				Type:      tt.jobType,
				ArchiveID: tt.archiveID,
			}

			j, err := bk.InitiateJob(testAccountID, testRegion, "vault", req)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, j.JobID)
			assert.Equal(t, tt.jobType, j.Action)
			assert.True(t, j.Completed)

			got, err := bk.DescribeJob(testAccountID, testRegion, "vault", j.JobID)
			require.NoError(t, err)
			assert.Equal(t, j.JobID, got.JobID)

			jobs, listErr := bk.ListJobs(testAccountID, testRegion, "vault")
			require.NoError(t, listErr)
			assert.Len(t, jobs, 1)
		})
	}
}

// TestRetrievalJobAsyncLifecycle verifies that a freshly initiated retrieval job starts
// InProgress and is only promoted to Succeeded once the simulated retrieval window has
// elapsed, matching AWS's asynchronous retrieval semantics.
func TestRetrievalJobAsyncLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantStatus    string
		delay         time.Duration
		waitForReady  bool
		wantCompleted bool
	}{
		{
			name:          "in_progress_within_window",
			delay:         time.Hour,
			waitForReady:  false,
			wantCompleted: false,
			wantStatus:    "InProgress",
		},
		{
			name:          "succeeded_after_window",
			delay:         5 * time.Millisecond,
			waitForReady:  true,
			wantCompleted: true,
			wantStatus:    "Succeeded",
		},
		{
			name:          "immediate_when_zero_delay",
			delay:         0,
			waitForReady:  false,
			wantCompleted: true,
			wantStatus:    "Succeeded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			glacier.SetRetrievalDelay(bk, tt.delay)

			_, err := bk.CreateVault(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			j, err := bk.InitiateJob(testAccountID, testRegion, "vault",
				&glacier.ExportedInitiateJobRequest{Type: "InventoryRetrieval"})
			require.NoError(t, err)

			if tt.waitForReady {
				require.Eventually(t, func() bool {
					got, descErr := bk.DescribeJob(testAccountID, testRegion, "vault", j.JobID)

					return descErr == nil && got.Completed
				}, time.Second, 2*time.Millisecond)
			}

			got, err := bk.DescribeJob(testAccountID, testRegion, "vault", j.JobID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCompleted, got.Completed)
			assert.Equal(t, tt.wantStatus, got.StatusCode)
		})
	}
}

// TestSortedListJobs verifies ListJobs returns jobs sorted by JobID.
func TestSortedListJobs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		jobCount   int
		wantSorted bool
	}{
		{name: "jobs_sorted_by_id", jobCount: 3, wantSorted: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			_, err := b.CreateVault(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			for range tt.jobCount {
				_, err = b.InitiateJob(testAccountID, testRegion, "vault", &glacier.ExportedInitiateJobRequest{
					Type: "InventoryRetrieval",
				})
				require.NoError(t, err)
			}

			jobs, listErr := b.ListJobs(testAccountID, testRegion, "vault")
			require.NoError(t, listErr)
			require.Len(t, jobs, tt.jobCount)

			for i := 1; i < len(jobs); i++ {
				assert.LessOrEqual(t, jobs[i-1].JobID, jobs[i].JobID)
			}
		})
	}
}

// TestNonNilListJobs verifies ListJobs returns a non-nil empty slice.
func TestNonNilListJobs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
	}{
		{name: "empty_vault_non_nil_jobs", vaultName: "empty-vault"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			_, err := b.CreateVault(testAccountID, testRegion, tt.vaultName)
			require.NoError(t, err)

			jobs, listErr := b.ListJobs(testAccountID, testRegion, tt.vaultName)
			require.NoError(t, listErr)

			assert.NotNil(t, jobs)
			assert.Empty(t, jobs)
		})
	}
}
