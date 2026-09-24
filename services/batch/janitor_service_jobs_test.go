package batch_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

// TestBatchJanitor_SweepCompletedServiceJobs locks in the fix for the
// unbounded-memory-growth leak in b.serviceJobs: sweepCompletedJobs evicted
// regular Jobs past CompletedJobTTL but never swept SubmitServiceJob entries,
// so SUCCEEDED/FAILED service jobs accumulated forever. AWS Batch documents
// job history (including service jobs) as visible for a limited retention
// window after completion; this mirrors that for regular jobs.
func TestBatchJanitor_SweepCompletedServiceJobs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		ttl         time.Duration
		sleep       time.Duration
		wantEvicted bool
	}{
		{
			name:        "evicted_past_ttl",
			ttl:         time.Hour,
			sleep:       time.Hour + time.Second,
			wantEvicted: true,
		},
		{
			name:        "kept_within_ttl",
			ttl:         time.Hour,
			sleep:       time.Minute,
			wantEvicted: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := batch.NewInMemoryBackend("000000000000", "us-east-1")

				queue, err := b.CreateJobQueue(t.Context(), "sj-queue", 1, "ENABLED", nil, nil, "", nil, "", nil)
				require.NoError(t, err)

				_, err = b.SubmitServiceJob(
					t.Context(), "sj1", queue.JobQueueName, "SAGEMAKER_TRAINING", `{"foo":"bar"}`,
					nil, nil, nil, 0, "", "", nil,
				)
				require.NoError(t, err)

				j := batch.NewJanitor(b, time.Minute, 24*time.Hour, tt.ttl)
				j.SweepOnce(t.Context()) // SUBMITTED -> RUNNING
				j.SweepOnce(t.Context()) // RUNNING -> SUCCEEDED

				time.Sleep(tt.sleep)

				j.SweepOnce(t.Context())

				jobs, _, err := b.ListServiceJobs(t.Context(), queue.JobQueueName, "SUCCEEDED", "", 0, nil)
				require.NoError(t, err)

				if tt.wantEvicted {
					assert.Empty(t, jobs, "service job should have been evicted")
				} else {
					assert.NotEmpty(t, jobs, "service job should not have been evicted")
				}
			})
		})
	}
}
