package ecs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func newFISECSHandler() *ecs.Handler {
	backend := ecs.NewInMemoryBackend("000000000000", "us-east-1", ecs.NewNoopRunner())

	return ecs.NewHandler(backend)
}

func TestECS_FISActions(t *testing.T) {
	t.Parallel()

	h := newFISECSHandler()
	actions := h.FISActions()

	ids := make([]string, len(actions))
	for i, a := range actions {
		ids[i] = a.ActionID
	}

	assert.Contains(t, ids, "aws:ecs:stop-task")
}

func TestECS_FISActions_TargetType(t *testing.T) {
	t.Parallel()

	h := newFISECSHandler()

	actions := h.FISActions()
	require.Len(t, actions, 1)
	assert.Equal(t, "aws:ecs:task", actions[0].TargetType)
}

func TestECS_ExecuteFISAction_StopTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		targets       []string
		createCluster bool
		createTask    bool
		wantErr       bool
	}{
		{
			name:          "stop_known_task",
			createCluster: true,
			createTask:    true,
			wantErr:       false,
		},
		{
			name:    "no_targets",
			targets: []string{},
			wantErr: false,
		},
		{
			name:    "unknown_task_ignored",
			targets: []string{"arn:aws:ecs:us-east-1:000000000000:task/default/nonexistent"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newFISECSHandler()

			var targets []string

			if tt.createCluster {
				_, err := h.Backend.CreateCluster(ecs.CreateClusterInput{ClusterName: "default"})
				require.NoError(t, err)
			}

			if tt.createTask {
				_, err := h.Backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
					Family: "test-family",
					ContainerDefinitions: []ecs.ContainerDefinition{
						{Name: "test", Image: "nginx"},
					},
				})
				require.NoError(t, err)

				tasks, err := h.Backend.RunTask(ecs.RunTaskInput{
					Cluster:        "default",
					TaskDefinition: "test-family",
					Count:          1,
				})
				require.NoError(t, err)
				require.NotEmpty(t, tasks)

				targets = []string{tasks[0].TaskArn}
			}

			if tt.targets != nil {
				targets = tt.targets
			}

			err := h.ExecuteFISAction(context.Background(), service.FISActionExecution{
				ActionID: "aws:ecs:stop-task",
				Targets:  targets,
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			// Verify task is stopped.
			if tt.createTask && len(targets) > 0 {
				taskArn := targets[0]
				describedTasks, descErr := h.Backend.DescribeTasks("default", []string{taskArn})
				require.NoError(t, descErr)
				require.Len(t, describedTasks, 1)
				assert.Equal(t, "STOPPED", describedTasks[0].LastStatus,
					"task should be in STOPPED state after FIS action")
			}
		})
	}
}

func TestECS_ExecuteFISAction_Unknown(t *testing.T) {
	t.Parallel()

	h := newFISECSHandler()

	err := h.ExecuteFISAction(context.Background(), service.FISActionExecution{
		ActionID: "aws:ecs:unknown-action",
		Targets:  []string{"some-task"},
	})

	require.NoError(t, err)
}
