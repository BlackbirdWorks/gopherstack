// White-box: needs autoMLJobStoppingDelay directly, mirroring
// pipeline_execution_start_test.go.
package sagemaker //nolint:testpackage // see comment above

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

// TestStopAutoMLJob_TransitionsThroughStopping verifies StopAutoMLJob moves a
// job to Stopping immediately and only to Stopped after
// autoMLJobStoppingDelay, matching the real AutoMLJobStatus enum
// (types/enums.go:1190-1198) instead of jumping straight to Stopped.
func TestStopAutoMLJob_TransitionsThroughStopping(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := NewInMemoryBackend("000000000000", "us-east-1")
		defer b.Shutdown(context.Background())

		ctx := context.Background()
		name := "stop-transition-job"

		_, err := b.CreateAutoMLJob(ctx, name, "arn:aws:iam::000000000000:role/sagemaker", nil)
		if err != nil {
			t.Fatalf("CreateAutoMLJob: %v", err)
		}

		if stopErr := b.StopAutoMLJob(ctx, name); stopErr != nil {
			t.Fatalf("StopAutoMLJob: %v", stopErr)
		}

		got, describeErr := b.DescribeAutoMLJob(ctx, name)
		if describeErr != nil {
			t.Fatalf("DescribeAutoMLJob: %v", describeErr)
		}
		if got.AutoMLJobStatus != pipelineStatusStopping {
			t.Fatalf("status before delay = %q, want %q (must not report Stopped early)",
				got.AutoMLJobStatus, pipelineStatusStopping)
		}

		time.Sleep(autoMLJobStoppingDelay + time.Millisecond)
		synctest.Wait()

		got, describeErr = b.DescribeAutoMLJob(ctx, name)
		if describeErr != nil {
			t.Fatalf("DescribeAutoMLJob after delay: %v", describeErr)
		}
		if got.AutoMLJobStatus != pipelineStatusStopped {
			t.Fatalf("status after delay = %q, want %q", got.AutoMLJobStatus, pipelineStatusStopped)
		}
	})
}
