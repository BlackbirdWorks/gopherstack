package ecs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func snapshotMutated(t *testing.T, mutate func(task *Task)) ([]byte, string) {
	t.Helper()

	b1 := NewInMemoryBackend("000000000000", "us-east-1", nil)
	_, err := b1.CreateCluster(CreateClusterInput{ClusterName: "c"})
	require.NoError(t, err)

	_, err = b1.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family: "fam", ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "busybox"}},
	})
	require.NoError(t, err)

	tasks, _, err := b1.RunTask(RunTaskInput{Cluster: "c", TaskDefinition: "fam"})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	b1.mu.Lock("test-mutate")

	task, _ := b1.tasks.Get(tasks[0].TaskArn)
	mutate(task)

	if c, _ := b1.clusters.Get("c"); c != nil {
		c.RunningTasksCount = 0
		c.PendingTasksCount = 1
	}

	b1.mu.Unlock()

	return b1.Snapshot(t.Context()), tasks[0].TaskArn
}

func TestSecondRestoreRelaunchesWhileFirstIsInFlight(t *testing.T) {
	t.Parallel()

	snap, arn := snapshotMutated(t, func(task *Task) { task.LastStatus = statusProvisioning })

	runner := newResumeRunner()
	runner.gate = make(chan struct{})

	b := NewInMemoryBackend("000000000000", "us-east-1", runner)
	t.Cleanup(b.Close)

	require.NoError(t, b.Restore(t.Context(), snap))
	b.adoptRestoredTasks(t.Context())

	require.NoError(t, b.Restore(t.Context(), snap))
	b.adoptRestoredTasks(t.Context())

	close(runner.gate)
	b.awaitLaunches()

	got, _, err := b.DescribeTasks("c", []string{arn})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, statusRunning, got[0].LastStatus)
}

func TestRestoreStopsLaunchingTaskWithStoppedDesiredStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status string
	}{
		{name: "provisioning", status: statusProvisioning},
		{name: "pending", status: statusPending},
		{name: "activating", status: statusActivating},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snap, arn := snapshotMutated(t, func(task *Task) {
				task.LastStatus = tt.status
				task.DesiredStatus = statusStopped
				task.StoppedReason = "Stopped by user"
			})

			runner := newResumeRunner()
			runner.adopted[arn] = true

			b := NewInMemoryBackend("000000000000", "us-east-1", runner)
			require.NoError(t, b.Restore(t.Context(), snap))
			b.adoptRestoredTasks(t.Context())
			b.awaitLaunches()

			got, _, err := b.DescribeTasks("c", []string{arn})
			require.NoError(t, err)
			require.Len(t, got, 1)

			assert.Equal(t, statusStopped, got[0].LastStatus)
			assert.NotNil(t, got[0].StoppedAt)
			assert.Equal(t, "Stopped by user", got[0].StoppedReason)
			assert.Zero(t, runner.callCount(arn))

			c, _ := b.clusters.Get("c")
			require.NotNil(t, c)
			assert.Zero(t, c.PendingTasksCount)
		})
	}
}

func TestRestoredServiceTasksAreNotDuplicatedByTheReconciler(t *testing.T) {
	t.Parallel()

	const desired = 3

	gated := newResumeRunner()
	gated.gate = make(chan struct{})

	b1 := NewInMemoryBackend("000000000000", "us-east-1", gated)
	_, err := b1.CreateCluster(CreateClusterInput{ClusterName: "c"})
	require.NoError(t, err)

	_, err = b1.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family: "fam", ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "busybox"}},
	})
	require.NoError(t, err)

	_, err = b1.CreateService(CreateServiceInput{
		ServiceName: "svc", Cluster: "c", TaskDefinition: "fam", DesiredCount: desired,
	})
	require.NoError(t, err)

	NewReconciler(b1).RunOnce(t.Context())

	snap := b1.Snapshot(t.Context())

	close(gated.gate)
	b1.awaitLaunches()

	runner := newResumeRunner()
	b2 := NewInMemoryBackend("000000000000", "us-east-1", runner)
	require.NoError(t, b2.Restore(t.Context(), snap))

	b2.mu.Lock("test-rewind")

	launching := 0

	for _, task := range b2.tasks.All() {
		task.LastStatus = statusProvisioning
		launching++
	}

	b2.mu.Unlock()

	require.Equal(t, desired, launching)

	b2.adoptRestoredTasks(t.Context())
	NewReconciler(b2).RunOnce(t.Context())
	b2.awaitLaunches()
	NewReconciler(b2).RunOnce(t.Context())
	b2.awaitLaunches()

	live := 0

	for _, task := range b2.tasks.All() {
		if task.LastStatus != statusStopped {
			live++

			assert.Equal(t, statusRunning, task.LastStatus)
		}
	}

	assert.Equal(t, desired, live)
}

type healthResumeRunner struct{ *resumeRunner }

func (healthResumeRunner) SetContainerHealthHandler(func(taskArn, containerName, status string)) {}
func (healthResumeRunner) SetContainerReasonHandler(func(taskArn, containerName, reason string)) {}

func TestAdoptedLaunchKeepsRuntimeReportedHealth(t *testing.T) {
	t.Parallel()

	snap, arn := snapshotMutated(t, func(task *Task) {
		task.LastStatus = statusActivating

		for i := range task.Containers {
			task.Containers[i].HealthStatus = containerHealthStatusUnknown
		}
	})

	runner := healthResumeRunner{newResumeRunner()}
	runner.adopted[arn] = true

	b := NewInMemoryBackend("000000000000", "us-east-1", runner)
	require.NoError(t, b.Restore(t.Context(), snap))
	b.adoptRestoredTasks(t.Context())
	b.awaitLaunches()

	got, _, err := b.DescribeTasks("c", []string{arn})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, statusRunning, got[0].LastStatus)
	require.NotEmpty(t, got[0].Containers)

	for _, c := range got[0].Containers {
		assert.Equal(t, containerHealthStatusUnknown, c.HealthStatus)
	}
}
