package ecs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func TestListTasks_DaemonNameFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		daemonName string
		wantGroup  string
	}{
		{name: "matching daemon", daemonName: "log-agent", wantGroup: "daemon:log-agent"},
		{name: "unknown daemon", daemonName: "nope"},
		{name: "unfiltered", wantGroup: "all"},
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

			daemonTasks, _, err := be.RunTask(ecs.RunTaskInput{
				Cluster: "c", TaskDefinition: td.TaskDefinitionArn, Count: 1, Group: "daemon:log-agent",
			})
			require.NoError(t, err)

			_, _, err = be.RunTask(ecs.RunTaskInput{Cluster: "c", TaskDefinition: td.TaskDefinitionArn, Count: 1})
			require.NoError(t, err)

			client := newTestECSClient(t, ecs.NewHandler(be))

			in := &ecssdk.ListTasksInput{Cluster: aws.String("c")}
			if tt.daemonName != "" {
				in.DaemonName = aws.String(tt.daemonName)
			}

			out, err := client.ListTasks(t.Context(), in)
			require.NoError(t, err)

			switch tt.wantGroup {
			case "":
				assert.Empty(t, out.TaskArns)
			case "all":
				assert.Len(t, out.TaskArns, 2)
			default:
				assert.Equal(t, []string{daemonTasks[0].TaskArn}, out.TaskArns)
			}
		})
	}
}
