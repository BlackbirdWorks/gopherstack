package ecs

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// settleTasks waits for every launch to finish and returns the tasks' current state.
func settleTasks(t *testing.T, b *InMemoryBackend, tasks []Task) []Task {
	t.Helper()
	b.awaitLaunches()

	out := make([]Task, 0, len(tasks))

	for _, task := range tasks {
		got, _, err := b.DescribeTasks(clusterKey(clusterFromTaskARN(task.TaskArn)), []string{task.TaskArn})
		require.NoError(t, err)
		require.Len(t, got, 1)

		out = append(out, got[0])
	}

	return out
}

// gateRunner is a TaskRunner whose launch blocks on gate, so tests drive the async lifecycle deterministically.
type gateRunner struct {
	err     error
	gate    chan struct{}
	entered chan struct{}
	stopped []string
	mu      sync.Mutex
}

func newGateRunner(err error) *gateRunner {
	return &gateRunner{err: err, gate: make(chan struct{}), entered: make(chan struct{}, 16)}
}

func (g *gateRunner) RunTask(task *Task, td *TaskDefinition) error {
	return g.RunTaskContext(context.Background(), task, td)
}

func (g *gateRunner) RunTaskContext(ctx context.Context, _ *Task, _ *TaskDefinition) error {
	g.entered <- struct{}{}

	select {
	case <-g.gate:
		return g.err
	case <-ctx.Done():
		return fmt.Errorf("launch cancelled: %w", ctx.Err())
	}
}

func (g *gateRunner) StopTask(task *Task) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.stopped = append(g.stopped, task.TaskArn)

	return nil
}

func (g *gateRunner) open() { close(g.gate) }

func (g *gateRunner) awaitEntered(t *testing.T) {
	t.Helper()

	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("launch never reached the runner")
	}
}

// runTaskPromptly fails the test when RunTask blocks on the launch instead of returning.
func runTaskPromptly(t *testing.T, b *InMemoryBackend, in RunTaskInput) []Task {
	t.Helper()

	var (
		tasks []Task
		err   error
	)

	done := make(chan struct{})

	go func() {
		defer close(done)

		tasks, _, err = b.RunTask(in)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunTask blocked on the launch")
	}

	require.NoError(t, err)
	require.Len(t, tasks, 1)

	return tasks
}

func describeTask(t *testing.T, b *InMemoryBackend, arn string) Task {
	t.Helper()

	got, _, err := b.DescribeTasks("ev", []string{arn})
	require.NoError(t, err)
	require.Len(t, got, 1)

	return got[0]
}

func TestAsyncLaunch_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		runErr     error
		name       string
		wantStatus string
		wantStop   string
		wantReason string
	}{
		{name: "gates_pass", wantStatus: statusRunning},
		{
			name:       "gate_failure",
			runErr:     &taskLaunchError{Reason: "Task failed to start: dependency not satisfied"},
			wantStatus: statusStopped,
			wantStop:   stopCodeTaskFailedToStart,
			wantReason: "Task failed to start: dependency not satisfied",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := newGateRunner(tt.runErr)
			b, _, tdArn := newEventBackend(t, g)
			t.Cleanup(b.Close)
			t.Cleanup(func() {
				select {
				case <-g.gate:
				default:
					g.open()
				}
			})

			tasks := runTaskPromptly(t, b, RunTaskInput{Cluster: "ev", TaskDefinition: tdArn})
			arn := tasks[0].TaskArn

			assert.Equal(t, statusProvisioning, tasks[0].LastStatus)
			assert.Nil(t, tasks[0].StartedAt)

			g.awaitEntered(t)

			mid := describeTask(t, b, arn)
			assert.Equal(t, statusActivating, mid.LastStatus)
			assert.NotNil(t, mid.CreatedAt)
			assert.NotNil(t, mid.PullStartedAt)
			assert.Nil(t, mid.PullStoppedAt)
			assert.Nil(t, mid.StartedAt)

			g.open()
			b.awaitLaunches()

			end := describeTask(t, b, arn)
			assert.Equal(t, tt.wantStatus, end.LastStatus)
			assert.Equal(t, tt.wantStop, end.StopCode)
			assert.Equal(t, tt.wantReason, end.StoppedReason)
			assert.NotNil(t, end.PullStoppedAt)

			if tt.wantStatus == statusRunning {
				assert.NotNil(t, end.StartedAt)
				assert.NotNil(t, end.ConnectivityAt)
				assert.Equal(t, connectivityConnected, end.Connectivity)

				return
			}

			assert.Nil(t, end.StartedAt)
			assert.NotNil(t, end.StoppedAt)
		})
	}
}

func TestAsyncLaunch_Cancellation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cancel func(t *testing.T, b *InMemoryBackend, arn string)
		name   string
	}{
		{
			name: "stop_task",
			cancel: func(t *testing.T, b *InMemoryBackend, arn string) {
				t.Helper()

				stopped, err := b.StopTask("ev", arn, "changed my mind")
				require.NoError(t, err)
				assert.Equal(t, stopCodeUserInitiated, stopped.StopCode)
			},
		},
		{name: "close", cancel: func(t *testing.T, b *InMemoryBackend, _ string) { t.Helper(); b.Close() }},
		{name: "reset", cancel: func(t *testing.T, b *InMemoryBackend, _ string) { t.Helper(); b.Reset() }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := newGateRunner(nil)
			b, _, tdArn := newEventBackend(t, g)
			t.Cleanup(g.open)

			arn := runTaskPromptly(t, b, RunTaskInput{Cluster: "ev", TaskDefinition: tdArn})[0].TaskArn
			g.awaitEntered(t)

			tt.cancel(t, b, arn)
			b.awaitLaunches()

			b.mu.RLock("test")
			defer b.mu.RUnlock()

			assert.Zero(t, b.launches.n)

			if task, ok := b.tasks.Get(arn); ok {
				assert.Equal(t, statusStopped, task.LastStatus)
				assert.Nil(t, task.StartedAt)
			}
		})
	}
}

func TestAsyncLaunch_ServiceDoesNotDoubleLaunch(t *testing.T) {
	t.Parallel()

	g := newGateRunner(nil)
	b, _, tdArn := newEventBackend(t, g)
	t.Cleanup(g.open)
	t.Cleanup(b.Close)

	_, err := b.CreateService(CreateServiceInput{
		ServiceName: "svc", Cluster: "ev", TaskDefinition: tdArn, DesiredCount: 1,
	})
	require.NoError(t, err)

	rec := NewReconciler(b)

	for range 3 {
		rec.RunOnce(context.Background())
	}

	arns, err := b.ListTasks("ev")
	require.NoError(t, err)
	assert.Len(t, arns, 1, "a PROVISIONING task already satisfies the desired count")
}
