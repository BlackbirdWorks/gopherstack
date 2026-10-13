package emr

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

func TestJanitor_IdleTimeoutSweep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		idleFor        time.Duration
		timeout        int64
		noPolicy       bool
		withSession    bool
		withStep       bool
		wantTerminated bool
	}{
		{name: "elapsed", idleFor: 2 * time.Minute, timeout: 60, wantTerminated: true},
		{name: "not elapsed", idleFor: 30 * time.Second, timeout: 60},
		{name: "no policy", idleFor: 2 * time.Minute, noPolicy: true},
		{name: "live session blocks", idleFor: 2 * time.Minute, timeout: 60, withSession: true},
		{name: "pending step blocks", idleFor: 2 * time.Minute, timeout: 60, withStep: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := NewInMemoryBackend("123456789012", "us-east-1")

			params := RunJobFlowParams{
				Name:           "idle",
				ReleaseLabel:   testReleaseLabel6,
				Instances:      RunJobFlowInstances{KeepJobFlowAliveWhenNoSteps: true},
				SessionEnabled: tt.withSession,
			}
			if !tt.noPolicy {
				params.AutoTerminationPolicy = &AutoTerminationPolicy{IdleTimeout: tt.timeout}
			}

			if tt.withStep {
				params.Steps = []StepSpec{{Name: "s", HadoopJarStep: StepHadoopJarStepInput{Jar: "s3://b/j"}}}
			}

			cluster, err := b.RunJobFlow(ctx, params)
			require.NoError(t, err)

			if tt.withSession {
				_, err = b.StartSession(ctx, StartSessionParams{ClusterID: cluster.ID})
				require.NoError(t, err)
			}

			stored, ok := b.clusterGet("us-east-1", cluster.ID)
			require.True(t, ok)
			stored.Status.Timeline[timelineKeyReady] = awstime.Epoch(time.Now().Add(-tt.idleFor))

			NewJanitor(b, time.Minute, time.Hour).SweepOnce(ctx)

			got, err := b.DescribeCluster(ctx, cluster.ID)
			require.NoError(t, err)

			if tt.wantTerminated {
				assert.Equal(t, StateTerminated, got.Status.State)
				assert.Equal(t, "ALL_STEPS_COMPLETED", got.Status.StateChangeReason["Code"])

				return
			}

			assert.Equal(t, StateWaiting, got.Status.State)
		})
	}
}
