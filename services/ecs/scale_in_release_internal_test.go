package ecs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStopOldestServiceTask_ReleasesTaskResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "scale-in frees host port and protection"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			ci, err := b.RegisterContainerInstance("default", "i-scalein")
			require.NoError(t, err)

			td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
				Family:      "scalein",
				NetworkMode: networkModeBridge,
				ContainerDefinitions: []ContainerDefinition{
					{Name: "app", Image: "nginx", PortMappings: []PortMapping{{ContainerPort: 8080}}},
				},
			})
			require.NoError(t, err)

			tasks, _, err := b.RunTask(RunTaskInput{
				TaskDefinition: td.TaskDefinitionArn,
				LaunchType:     "EC2",
				Count:          1,
				Group:          "service:web",
			})
			require.NoError(t, err)
			require.Len(t, tasks, 1)

			b.taskProtections.Put(&TaskProtection{TaskArn: tasks[0].TaskArn, ProtectionEnabled: true})

			require.NoError(t, b.StopOldestServiceTask("default", "web"))

			stored, found := b.containerInstances.Get(scopedKey("default", ci.ContainerInstanceArn))
			require.True(t, found)
			assert.Empty(t, stored.AllocatedPorts)
			assert.False(t, b.taskProtections.Has(tasks[0].TaskArn))
		})
	}
}
