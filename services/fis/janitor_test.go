package fis_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fis"
)

// createExperimentForJanitorTest starts an experiment via the backend and returns its ID.
// It also immediately cancels the async goroutine by stopping the experiment.
func createExperimentForJanitorTest(t *testing.T, backend *fis.ExportedInMemoryBackend, templateID string) string {
	t.Helper()

	exp, err := backend.StartExperiment(t.Context(),
		&fis.ExportedStartExperimentRequest{ExperimentTemplateID: templateID},
		"000000000000", "us-east-1")
	require.NoError(t, err)

	return exp.ID
}

// createMinimalTemplate creates a template with no actions/targets for janitor tests.
func createMinimalTemplate(t *testing.T, backend *fis.ExportedInMemoryBackend) string {
	t.Helper()

	tpl, err := backend.CreateExperimentTemplate(
		&fis.ExportedCreateTemplateRequest{
			Description:    "test",
			RoleArn:        "arn:aws:iam::000000000000:role/test",
			Tags:           map[string]string{},
			StopConditions: []fis.ExportedStopConditionDTO{{Source: "none"}},
		},
		"000000000000",
		"us-east-1",
	)
	require.NoError(t, err)

	return tpl.ID
}

// TestJanitor_SweepCompletedExperiments verifies that experiments in terminal states
// whose EndTime is past the configured TTL are removed.
func TestFISJanitor_SweepCompletedExperiments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		status      string
		endOffset   time.Duration
		ttl         time.Duration
		wantEvicted bool
	}{
		{
			name:        "evict_completed_past_ttl",
			status:      "completed",
			endOffset:   -25 * time.Hour,
			ttl:         24 * time.Hour,
			wantEvicted: true,
		},
		{
			name:        "evict_stopped_past_ttl",
			status:      "stopped",
			endOffset:   -25 * time.Hour,
			ttl:         24 * time.Hour,
			wantEvicted: true,
		},
		{
			name:        "evict_failed_past_ttl",
			status:      "failed",
			endOffset:   -25 * time.Hour,
			ttl:         24 * time.Hour,
			wantEvicted: true,
		},
		{
			name:        "keep_completed_within_ttl",
			status:      "completed",
			endOffset:   -1 * time.Hour,
			ttl:         24 * time.Hour,
			wantEvicted: false,
		},
		{
			name:        "keep_running_no_endtime",
			status:      "running",
			endOffset:   0,
			ttl:         24 * time.Hour,
			wantEvicted: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := fis.NewTestBackend()
			templateID := createMinimalTemplate(t, backend)

			expID := createExperimentForJanitorTest(t, backend, templateID)

			if tt.endOffset != 0 {
				endTime := time.Now().Add(tt.endOffset)
				backend.SetExperimentTerminal(expID, tt.status, endTime)
			} else {
				// running state: stop the experiment to cancel goroutine but leave endTime nil
				_, _ = backend.StopExperiment(expID)
			}

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
		t.Fatal("janitor did not stop after context cancellation")
	}
}
