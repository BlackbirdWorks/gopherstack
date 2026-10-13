package ecs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func TestReconciler_RollingDeploymentReplacesTasks(t *testing.T) {
	t.Parallel()

	maxHundred, minFifty := 100, 50

	tests := []struct {
		dc        *ecs.DeploymentConfiguration
		name      string
		wantTicks int
		sameTD    bool
	}{
		{name: "default percents", wantTicks: 3},
		{name: "force same task definition", sameTD: true, wantTicks: 3},
		{
			name:      "no surge",
			dc:        &ecs.DeploymentConfiguration{MaximumPercent: &maxHundred, MinimumHealthyPercent: &minFifty},
			wantTicks: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
			reg := func(image string) string {
				td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
					Family:               "roll",
					ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: image}},
				})
				require.NoError(t, err)

				return td.TaskDefinitionArn
			}

			td1 := reg("nginx:1")
			_, err := backend.CreateCluster(ecs.CreateClusterInput{ClusterName: "c"})
			require.NoError(t, err)
			_, err = backend.CreateService(ecs.CreateServiceInput{
				ServiceName: "svc", Cluster: "c", TaskDefinition: td1, DesiredCount: 2, DeploymentConfiguration: tt.dc,
			})
			require.NoError(t, err)

			rec := ecs.NewReconciler(backend)
			rec.RunOnce(t.Context())
			require.Equal(t, 2, backend.CountRunningTasksForService("c", "svc"))

			in := ecs.UpdateServiceInput{Cluster: "c", Service: "svc", ForceNewDeployment: true}
			if !tt.sameTD {
				in.TaskDefinition = new(reg("nginx:2"))
			}

			_, err = backend.UpdateService(in)
			require.NoError(t, err)

			for range tt.wantTicks {
				rec.RunOnce(t.Context())
			}

			out, _, err := backend.DescribeServices("c", []string{"svc"})
			require.NoError(t, err)
			require.Len(t, out, 1)
			svc := out[0]

			assert.Len(t, svc.Deployments, 1, "superseded deployment must be dropped")
			assert.Equal(t, 2, svc.RunningCount)
			assert.Equal(t, "COMPLETED", svc.Deployments[0].RolloutState)
			assert.Equal(t, 2, svc.Deployments[0].RunningCount)
			assert.Equal(t, svc.Deployments[0].ID, runningStartedBy(t, backend, "c"))

			msgs := make([]string, 0, len(svc.Events))
			for _, ev := range svc.Events {
				msgs = append(msgs, ev.Message)
			}

			assert.Contains(t, msgs, "(service svc) has reached a steady state.")
			assert.Contains(t, msgs, "(service svc) ("+"deployment "+svc.Deployments[0].ID+") deployment completed.")
		})
	}
}

func runningStartedBy(t *testing.T, backend *ecs.InMemoryBackend, cluster string) string {
	t.Helper()

	arns, err := backend.ListTasks(cluster)
	require.NoError(t, err)

	tasks, _, err := backend.DescribeTasks(cluster, arns)
	require.NoError(t, err)
	require.NotEmpty(t, tasks)

	for _, task := range tasks {
		require.Equal(t, tasks[0].StartedBy, task.StartedBy)
	}

	return tasks[0].StartedBy
}
