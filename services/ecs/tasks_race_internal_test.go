package ecs

import (
	"sync"
	"testing"
)

// TestTaskConcurrentWithStopTask proves DescribeTasks/RunTask must not hand
// back a Task whose Containers slice StopTask mutates in place.
func TestTaskConcurrentWithStopTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reader func(b *InMemoryBackend, taskArn string, runTaskSnapshot Task) func()
		name   string
	}{
		{
			name: "DescribeTasks races StopTask",
			reader: func(b *InMemoryBackend, taskArn string, _ Task) func() {
				return func() {
					out, _, err := b.DescribeTasks("race-cluster", []string{taskArn})
					if err != nil || len(out) == 0 {
						return
					}

					for _, c := range out[0].Containers {
						_ = c.LastStatus
						_ = c.ExitCode
					}
				}
			},
		},
		{
			// Reads the single snapshot RunTask handed back once, mirroring a
			// caller that keeps its own RunTask response around.
			name: "RunTask snapshot races StopTask",
			reader: func(_ *InMemoryBackend, _ string, runTaskSnapshot Task) func() {
				return func() {
					for _, c := range runTaskSnapshot.Containers {
						_ = c.LastStatus
						_ = c.ExitCode
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, taskArn, snapshot := newRaceTestTask(t)
			reader := tt.reader(b, taskArn, snapshot)

			const iterations = 500

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()

				for range iterations {
					reader()
				}
			}()

			go func() {
				defer wg.Done()

				for range iterations {
					_, _ = b.StopTask("race-cluster", taskArn, "race")
				}
			}()

			wg.Wait()
		})
	}
}

// newRaceTestTask creates a cluster with one running task and returns the
// backend, the task's ARN, and RunTask's own returned snapshot of it.
func newRaceTestTask(t *testing.T) (*InMemoryBackend, string, Task) {
	t.Helper()

	b := newTestBackend()

	tdArn := registerSimpleTaskDef(t, b, "race-app", "nginx")
	if _, err := b.CreateCluster(CreateClusterInput{ClusterName: "race-cluster"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	tasks, _, err := b.RunTask(RunTaskInput{
		Cluster:        "race-cluster",
		TaskDefinition: tdArn,
		Count:          1,
	})
	if err != nil {
		t.Fatalf("RunTask: %v", err)
	}

	return b, tasks[0].TaskArn, tasks[0]
}
