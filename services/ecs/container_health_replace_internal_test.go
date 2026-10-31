package ecs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStopUnhealthyServiceTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		health      string
		grace       int
		elapsed     time.Duration
		standalone  bool
		wantStopped bool
	}{
		{name: "unhealthy_no_grace", health: healthECSUnhealthy, wantStopped: true},
		{name: "unhealthy_inside_grace", health: healthECSUnhealthy, grace: 60, elapsed: 30 * time.Second},
		{
			name:        "unhealthy_after_grace",
			health:      healthECSUnhealthy,
			grace:       60,
			elapsed:     61 * time.Second,
			wantStopped: true,
		},
		{name: "healthy", health: healthECSHealthy},
		{name: "standalone_not_replaced", health: healthECSUnhealthy, standalone: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			now := start
			b.nowFn = func() time.Time { return now }

			_, err := b.CreateCluster(CreateClusterInput{ClusterName: "c"})
			require.NoError(t, err)

			td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
				Family:               "hc",
				ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx", Essential: true}},
			})
			require.NoError(t, err)

			grace := tt.grace
			_, err = b.CreateService(CreateServiceInput{
				Cluster: "c", ServiceName: "svc", TaskDefinition: td.TaskDefinitionArn, DesiredCount: 0,
				HealthCheckGracePeriodSeconds: &grace,
			})
			require.NoError(t, err)

			var taskArn string

			if tt.standalone {
				tasks, _, rerr := b.RunTask(RunTaskInput{Cluster: "c", TaskDefinition: td.TaskDefinitionArn})
				require.NoError(t, rerr)

				taskArn = tasks[0].TaskArn
			} else {
				require.NoError(t, b.StartTaskForService("c", "svc", td.TaskDefinitionArn))

				tasks, _, lerr := b.DescribeTasks("c", nil)
				require.NoError(t, lerr)
				require.Len(t, tasks, 1)

				taskArn = tasks[0].TaskArn
			}

			b.updateContainerHealth(taskArn, "app", tt.health)

			now = start.Add(tt.elapsed)
			b.stopUnhealthyServiceTasks()

			got, _, err := b.DescribeTasks("c", []string{taskArn})
			require.NoError(t, err)
			require.Len(t, got, 1)

			if !tt.wantStopped {
				assert.Equal(t, statusRunning, got[0].DesiredStatus)

				return
			}

			assert.Equal(t, statusStopped, got[0].DesiredStatus)
			assert.Equal(t, containerHealthStopReason, got[0].StoppedReason)
			assert.Equal(t, stopCodeServiceSchedulerInitiated, got[0].StopCode)
		})
	}
}

func TestReconciler_ReplacesUnhealthyServiceTask(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())

	_, err := b.CreateCluster(CreateClusterInput{ClusterName: "c"})
	require.NoError(t, err)

	td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family: "hc", ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx", Essential: true}},
	})
	require.NoError(t, err)

	_, err = b.CreateService(CreateServiceInput{
		Cluster: "c", ServiceName: "svc", TaskDefinition: td.TaskDefinitionArn, DesiredCount: 1,
	})
	require.NoError(t, err)

	rec := NewReconciler(b)
	rec.RunOnce(t.Context())
	b.awaitLaunches()

	running, err := b.ListTasksFiltered(ListTasksInput{Cluster: "c", DesiredStatus: statusRunning})
	require.NoError(t, err)
	require.Len(t, running, 1)

	first := running[0]

	b.updateContainerHealth(first, "app", healthECSUnhealthy)

	rec.RunOnce(t.Context())
	b.awaitLaunches()

	running, err = b.ListTasksFiltered(ListTasksInput{Cluster: "c", DesiredStatus: statusRunning})
	require.NoError(t, err)
	require.Len(t, running, 1)
	assert.NotEqual(t, first, running[0], "unhealthy task replaced by a new one")
}
