package fis_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fis"
)

// newFakeExperiment creates a minimal Experiment for injection in janitor tests.
// No background goroutine is launched, avoiding lifecycle races.
func newFakeExperiment(id, status string, endTime *time.Time) *fis.Experiment {
	exp := &fis.Experiment{
		ID:        id,
		Arn:       "arn:aws:fis:us-east-1:000000000000:experiment/" + id,
		Status:    fis.ExperimentStatus{Status: status},
		StartTime: time.Now().Add(-time.Hour),
		EndTime:   endTime,
	}

	return exp
}

// TestFISJanitor_SweepCompletedExperiments verifies that experiments in terminal states
// whose EndTime is past the configured TTL are removed.
func TestFISJanitor_SweepCompletedExperiments(t *testing.T) {
	t.Parallel()

	past := time.Now().Add(-25 * time.Hour)
	recent := time.Now().Add(-1 * time.Hour)

	tests := []struct {
		name        string
		endTime     *time.Time
		status      string
		ttl         time.Duration
		wantEvicted bool
	}{
		{
			name:        "evict_completed_past_ttl",
			status:      "completed",
			endTime:     &past,
			ttl:         24 * time.Hour,
			wantEvicted: true,
		},
		{
			name:        "evict_stopped_past_ttl",
			status:      "stopped",
			endTime:     &past,
			ttl:         24 * time.Hour,
			wantEvicted: true,
		},
		{
			name:        "evict_failed_past_ttl",
			status:      "failed",
			endTime:     &past,
			ttl:         24 * time.Hour,
			wantEvicted: true,
		},
		{
			name:        "keep_completed_within_ttl",
			status:      "completed",
			endTime:     &recent,
			ttl:         24 * time.Hour,
			wantEvicted: false,
		},
		{
			name:        "keep_running_no_endtime",
			status:      "running",
			endTime:     nil,
			ttl:         24 * time.Hour,
			wantEvicted: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := fis.NewTestBackend()
			expID := "EXP" + tt.name

			backend.InjectExperiment(newFakeExperiment(expID, tt.status, tt.endTime))

			janitor := fis.NewJanitor(backend, time.Hour, tt.ttl)
			janitor.SweepOnce(t.Context())

			_, err := backend.GetExperiment(expID)

			if tt.wantEvicted {
				assert.ErrorIs(t, err, fis.ErrExperimentNotFound)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestFISJanitor_RunContext verifies that the janitor stops when context is cancelled.
func TestFISJanitor_RunContext(t *testing.T) {
	t.Parallel()

	backend := fis.NewTestBackend()
	janitor := fis.NewJanitor(backend, 10*time.Millisecond, time.Hour)

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
