package ecs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func TestRunningTaskIPv4s(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filters map[string]string
		name    string
		stop    int
		want    int
	}{
		{name: "all running", want: 3},
		{name: "stopped excluded", stop: 1, want: 2},
		{
			name:    "attribute filter excludes fargate",
			filters: map[string]string{"ecs.instance-type": "c5.xlarge"},
			want:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			be := ecs.NewInMemoryBackend("000000000000", "us-east-1", nil)
			_, err := be.CreateCluster(ecs.CreateClusterInput{ClusterName: "c"})
			require.NoError(t, err)

			td, err := be.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
				Family:               "f",
				ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx"}},
			})
			require.NoError(t, err)

			tasks, _, err := be.RunTask(ecs.RunTaskInput{
				Cluster: "c", TaskDefinition: td.TaskDefinitionArn, Count: 3, LaunchType: "FARGATE",
			})
			require.NoError(t, err)
			require.Len(t, tasks, 3)

			for _, task := range tasks[:tt.stop] {
				_, err = be.StopTask("c", task.TaskArn, "test")
				require.NoError(t, err)
			}

			ips, err := be.RunningTaskIPv4s("c", tt.filters)
			require.NoError(t, err)
			assert.Len(t, ips, tt.want)
		})
	}
}

func TestRunTaskUniquePrivateIPs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		count int
	}{
		{name: "three", count: 3},
		{name: "hundred", count: 100},
		{name: "five hundred", count: 500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			be := ecs.NewInMemoryBackend("000000000000", "us-east-1", nil)
			_, err := be.CreateCluster(ecs.CreateClusterInput{ClusterName: "c"})
			require.NoError(t, err)

			td, err := be.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
				Family:               "f",
				ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx"}},
			})
			require.NoError(t, err)

			tasks, _, err := be.RunTask(ecs.RunTaskInput{
				Cluster: "c", TaskDefinition: td.TaskDefinitionArn, Count: tt.count, LaunchType: "FARGATE",
			})
			require.NoError(t, err)
			require.Len(t, tasks, tt.count)

			ips, err := be.RunningTaskIPv4s("c", nil)
			require.NoError(t, err)
			assert.Len(t, ips, tt.count)
		})
	}
}
