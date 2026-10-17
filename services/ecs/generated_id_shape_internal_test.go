package ecs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskSetIDShape(t *testing.T) {
	t.Parallel()

	tests := []struct{ name string }{{name: "first"}, {name: "second"}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
			tdArn := registerSimpleTaskDef(t, b, "app", "nginx:good")
			_, err := b.CreateCluster(CreateClusterInput{ClusterName: "c"})
			require.NoError(t, err)
			_, err = b.CreateService(CreateServiceInput{
				ServiceName: "s", Cluster: "c", TaskDefinition: tdArn, DesiredCount: 1,
			})
			require.NoError(t, err)

			ts, err := b.CreateTaskSet(CreateTaskSetInput{Cluster: "c", Service: "s", TaskDefinition: tdArn})
			require.NoError(t, err)

			assert.Regexp(t, `^ecs-svc/\d{19}$`, ts.ID)
			assert.Regexp(t, `^arn:aws:ecs:us-east-1:123456789012:task-set/c/s/ecs-svc/\d{19}$`, ts.TaskSetArn)

			got, ok := func() (*TaskSet, bool) {
				b.mu.RLock("test")
				defer b.mu.RUnlock()

				return b.findTaskSetRefLocked(ts.ServiceArn, ts.ID)
			}()
			require.True(t, ok)
			assert.Equal(t, ts.TaskSetArn, got.TaskSetArn)
		})
	}
}

func TestContainerRuntimeIDShape(t *testing.T) {
	t.Parallel()

	tests := []struct{ name string }{{name: "running"}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			task := &Task{LastStatus: statusRunning, Containers: []Container{{Name: "a"}}}
			syncContainerStatuses(task, nil)

			assert.Regexp(t, `^[0-9a-f]{64}$`, task.Containers[0].RuntimeID)
		})
	}
}
