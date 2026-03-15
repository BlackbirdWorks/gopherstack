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
