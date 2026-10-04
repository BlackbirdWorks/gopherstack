package ecs_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

type recordingRunner struct {
	stopped []string
	mu      sync.Mutex
}

func (r *recordingRunner) RunTask(*ecs.Task, *ecs.TaskDefinition) error { return nil }

func (r *recordingRunner) StopTask(t *ecs.Task) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.stopped = append(r.stopped, t.TaskArn)

	return nil
}

func TestBackendResetStopsRunnerTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		tasks int
	}{
		{name: "none"},
		{name: "two", tasks: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := &recordingRunner{}
			b := ecs.NewInMemoryBackend(testAccountID, testRegion, r)

			td, err := b.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
				Family:               "reset-task",
				ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx:latest"}},
			})
			require.NoError(t, err)

			var want []string

			if tc.tasks > 0 {
				tasks, _, runErr := b.RunTask(ecs.RunTaskInput{TaskDefinition: td.TaskDefinitionArn, Count: tc.tasks})
				require.NoError(t, runErr)

				for _, task := range tasks {
					want = append(want, task.TaskArn)
				}
			}

			b.Reset()

			r.mu.Lock()
			defer r.mu.Unlock()

			assert.ElementsMatch(t, want, r.stopped)
		})
	}
}
