package emr

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

func TestStepLifecycle_SequentialRunning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantStep1   string
		wantStep2   string
		wantCluster string
		age         time.Duration
	}{
		{name: "fresh", age: 0, wantStep1: "PENDING", wantStep2: "PENDING", wantCluster: "WAITING"},
		{
			name:        "first running",
			age:         1500 * time.Millisecond,
			wantStep1:   "RUNNING",
			wantStep2:   "PENDING",
			wantCluster: "RUNNING",
		},
		{
			name:        "second running",
			age:         2500 * time.Millisecond,
			wantStep1:   "COMPLETED",
			wantStep2:   "RUNNING",
			wantCluster: "RUNNING",
		},
		{
			name:        "all done",
			age:         5 * time.Second,
			wantStep1:   "COMPLETED",
			wantStep2:   "COMPLETED",
			wantCluster: "WAITING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1")
			ctx := context.Background()

			c, err := b.RunJobFlow(ctx, RunJobFlowParams{
				Name:         "lifecycle",
				ReleaseLabel: testReleaseLabel6,
				Steps: []StepSpec{
					{Name: "one", HadoopJarStep: StepHadoopJarStepInput{Jar: "a.jar"}},
					{Name: "two", HadoopJarStep: StepHadoopJarStepInput{Jar: "b.jar"}},
				},
				Instances: RunJobFlowInstances{KeepJobFlowAliveWhenNoSteps: true},
			})
			require.NoError(t, err)

			stored, ok := b.clusterGet("us-east-1", c.ID)
			require.True(t, ok)

			b.mu.Lock("test")
			for i := range stored.steps {
				stored.steps[i].Status.Timeline.CreationDateTime = awstime.Epoch(time.Now().Add(-tt.age))
			}
			b.mu.Unlock()

			steps, _, err := b.ListSteps(ctx, c.ID, nil, nil, "")
			require.NoError(t, err)

			byName := map[string]string{}
			for _, s := range steps {
				byName[s.Name] = s.Status.State
			}

			assert.Equal(t, tt.wantStep1, byName["one"])
			assert.Equal(t, tt.wantStep2, byName["two"])

			got, err := b.DescribeCluster(ctx, c.ID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCluster, got.Status.State)
		})
	}
}

func TestCancelStep_RunningStep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		wantState  string
		age        time.Duration
		wantStart  bool
	}{
		{name: "pending", age: 0, wantStatus: cancelStepsStatusSubmitted, wantState: StepStateCancelled},
		{
			name:       "running",
			age:        1500 * time.Millisecond,
			wantStatus: cancelStepsStatusSubmitted,
			wantState:  StepStateCancelled,
			wantStart:  true,
		},
		{name: "completed", age: 5 * time.Second, wantStatus: cancelStepsStatusFailed, wantState: StepStateCompleted},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1")
			ctx := context.Background()

			c, err := b.RunJobFlow(ctx, RunJobFlowParams{
				Name:         "cancel",
				ReleaseLabel: testReleaseLabel6,
				Steps:        []StepSpec{{Name: "one", HadoopJarStep: StepHadoopJarStepInput{Jar: "a.jar"}}},
				Instances:    RunJobFlowInstances{KeepJobFlowAliveWhenNoSteps: true},
			})
			require.NoError(t, err)

			stored, ok := b.clusterGet("us-east-1", c.ID)
			require.True(t, ok)

			b.mu.Lock("test")
			stored.steps[0].Status.Timeline.CreationDateTime = awstime.Epoch(time.Now().Add(-tt.age))
			stepID := stored.steps[0].ID
			b.mu.Unlock()

			res, err := b.CancelSteps(ctx, c.ID, []string{stepID})
			require.NoError(t, err)
			require.Len(t, res, 1)
			assert.Equal(t, tt.wantStatus, res[0].Status)

			step, err := b.DescribeStep(ctx, c.ID, stepID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantState, step.Status.State)
			assert.Equal(
				t,
				tt.wantStart,
				step.Status.Timeline.StartDateTime > 0 && step.Status.State == StepStateCancelled,
			)
		})
	}
}

func TestTerminate_SettlesStepsAndAutoTerminatesOnRead(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantStep      string
		wantCluster   string
		age           time.Duration
		autoTerminate bool
		terminate     bool
	}{
		{name: "terminate cancels pending", terminate: true, wantStep: "CANCELLED", wantCluster: "TERMINATED"},
		{
			name:        "terminate interrupts running",
			terminate:   true,
			age:         1500 * time.Millisecond,
			wantStep:    "INTERRUPTED",
			wantCluster: "TERMINATED",
		},
		{
			name:        "terminate keeps completed",
			terminate:   true,
			age:         5 * time.Second,
			wantStep:    "COMPLETED",
			wantCluster: "TERMINATED",
		},
		{
			name:          "auto terminate on describe",
			autoTerminate: true,
			age:           5 * time.Second,
			wantStep:      "COMPLETED",
			wantCluster:   "TERMINATED",
		},
		{
			name:          "auto terminate waits for steps",
			autoTerminate: true,
			age:           0,
			wantStep:      "PENDING",
			wantCluster:   "WAITING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1")
			ctx := context.Background()

			c, err := b.RunJobFlow(ctx, RunJobFlowParams{
				Name:         "term",
				ReleaseLabel: testReleaseLabel6,
				Steps:        []StepSpec{{Name: "one", HadoopJarStep: StepHadoopJarStepInput{Jar: "a.jar"}}},
				Instances:    RunJobFlowInstances{KeepJobFlowAliveWhenNoSteps: !tt.autoTerminate},
			})
			require.NoError(t, err)

			stored, ok := b.clusterGet("us-east-1", c.ID)
			require.True(t, ok)

			b.mu.Lock("test")
			stored.steps[0].Status.Timeline.CreationDateTime = awstime.Epoch(time.Now().Add(-tt.age))
			stepID := stored.steps[0].ID
			b.mu.Unlock()

			if tt.terminate {
				require.NoError(t, b.TerminateJobFlows(ctx, []string{c.ID}))
			}

			got, err := b.DescribeCluster(ctx, c.ID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCluster, got.Status.State)

			step, err := b.DescribeStep(ctx, c.ID, stepID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStep, step.Status.State)
		})
	}
}
