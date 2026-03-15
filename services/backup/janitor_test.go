package backup_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func newBackupBackend(t *testing.T) *backup.InMemoryBackend {
	t.Helper()

	return backup.NewInMemoryBackend("123456789012", "us-east-1")
}

// TestJanitor_SweepCompletedJobs verifies that the janitor removes backup jobs
// in terminal states whose CompletionTime is past the configured TTL.
func TestBackupJanitor_SweepCompletedJobs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		state           string
		completionDelay time.Duration // negative = in the past
		ttl             time.Duration
		wantEvicted     bool
	}{
		{
			name:            "evict_completed_past_ttl",
			state:           "COMPLETED",
			completionDelay: -25 * time.Hour,
			ttl:             24 * time.Hour,
			wantEvicted:     true,
		},
		{
			name:            "evict_failed_past_ttl",
			state:           "FAILED",
			completionDelay: -25 * time.Hour,
			ttl:             24 * time.Hour,
			wantEvicted:     true,
		},
		{
			name:            "evict_expired_past_ttl",
			state:           "EXPIRED",
			completionDelay: -25 * time.Hour,
			ttl:             24 * time.Hour,
			wantEvicted:     true,
		},
		{
			name:            "evict_aborted_past_ttl",
			state:           "ABORTED",
			completionDelay: -25 * time.Hour,
			ttl:             24 * time.Hour,
			wantEvicted:     true,
		},
		{
			name:            "keep_completed_within_ttl",
			state:           "COMPLETED",
			completionDelay: -1 * time.Hour,
			ttl:             24 * time.Hour,
			wantEvicted:     false,
		},
		{
			name:            "keep_created_no_completion",
			state:           "CREATED",
			completionDelay: 0,
			ttl:             24 * time.Hour,
			wantEvicted:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newBackupBackend(t)

			_, err := backend.CreateBackupVault("test-vault", "", "", nil)
			require.NoError(t, err)

			job, err := backend.StartBackupJob(
				"test-vault",
				"arn:aws:ec2:us-east-1:123456789012:instance/i-1234",
				"",
				"EC2",
			)
			require.NoError(t, err)

			if tt.completionDelay != 0 {
				ct := time.Now().Add(tt.completionDelay)
				backend.SetJobState(job.BackupJobID, tt.state, &ct)
			} else {
				backend.SetJobState(job.BackupJobID, tt.state, nil)
			}

			janitor := backup.NewJanitor(backend, time.Hour, tt.ttl)
			janitor.SweepOnce(t.Context())

			_, err = backend.DescribeBackupJob(job.BackupJobID)

			if tt.wantEvicted {
				assert.ErrorIs(t, err, backup.ErrNotFound)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestBackupJanitor_RunContext verifies that the janitor stops when context is cancelled.
func TestBackupJanitor_RunContext(t *testing.T) {
	t.Parallel()

	backend := newBackupBackend(t)
	janitor := backup.NewJanitor(backend, 10*time.Millisecond, time.Hour)

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})

	go func() {
		janitor.Run(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "janitor did not stop after context cancellation")
	}
}

// TestBackupPlanIDIndex verifies that plan operations by plan ID are O(1)
// via the planIDIndex and that the index is cleaned up on delete.
func TestBackupPlanIDIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *backup.InMemoryBackend)
		name string
	}{
		{
			name: "get_by_id",
			run: func(t *testing.T, b *backup.InMemoryBackend) {
				t.Helper()

				plan, err := b.CreateBackupPlan("idx-plan", nil, nil)
				require.NoError(t, err)

				got, err := b.GetBackupPlan(plan.BackupPlanID)
				require.NoError(t, err)
				assert.Equal(t, "idx-plan", got.BackupPlanName)
			},
		},
		{
			name: "update_by_id",
			run: func(t *testing.T, b *backup.InMemoryBackend) {
				t.Helper()

				plan, err := b.CreateBackupPlan("upd-plan", nil, nil)
				require.NoError(t, err)

				rules := []backup.Rule{{RuleName: "r1", TargetVaultName: "vault1"}}
				updated, err := b.UpdateBackupPlan(plan.BackupPlanID, rules)
				require.NoError(t, err)
				require.Len(t, updated.Rules, 1)
				assert.Equal(t, "r1", updated.Rules[0].RuleName)
			},
		},
		{
			name: "delete_by_id_cleans_indexes",
			run: func(t *testing.T, b *backup.InMemoryBackend) {
				t.Helper()

				plan, err := b.CreateBackupPlan("del-plan", nil, nil)
				require.NoError(t, err)

				err = b.DeleteBackupPlan(plan.BackupPlanID)
				require.NoError(t, err)

				// Plan should not be accessible by name or ID.
				_, err = b.GetBackupPlan(plan.BackupPlanName)
				require.Error(t, err)

				_, err = b.GetBackupPlan(plan.BackupPlanID)
				require.Error(t, err)

				// Tags by ARN should return ErrNotFound too.
				err = b.TagResource(plan.BackupPlanArn, map[string]string{"k": "v"})
				require.ErrorIs(t, err, backup.ErrNotFound)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackupBackend(t)
			tt.run(t, b)
		})
	}
}
