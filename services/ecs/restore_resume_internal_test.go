package ecs

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	errResumeBoom     = errors.New("image pull failed")
	errResumeDescribe = errors.New("describe during launch failed")
)

type resumeRunner struct {
	err     error
	gate    chan struct{}
	calls   map[string]int
	adopted map[string]bool
	be      *InMemoryBackend
	seen    map[string]string
	mu      sync.Mutex
}

func newResumeRunner() *resumeRunner {
	return &resumeRunner{calls: map[string]int{}, adopted: map[string]bool{}, seen: map[string]string{}}
}

func (r *resumeRunner) RunTask(task *Task, _ *TaskDefinition) error {
	if r.gate != nil {
		<-r.gate
	}

	r.mu.Lock()
	r.calls[task.TaskArn]++
	be := r.be
	r.mu.Unlock()

	if be != nil {
		got, _, err := be.DescribeTasks("c", []string{task.TaskArn})
		if err != nil || len(got) != 1 {
			return errResumeDescribe
		}

		r.mu.Lock()
		r.seen[task.TaskArn] = got[0].LastStatus
		r.mu.Unlock()
	}

	return r.err
}

func (*resumeRunner) StopTask(*Task) error { return nil }

func (r *resumeRunner) AdoptTask(_ context.Context, task *Task, _ *TaskDefinition) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.adopted[task.TaskArn]
}

func (r *resumeRunner) callCount(arn string) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.calls[arn]
}

func snapshotWithStatus(t *testing.T, status string, awaiting bool) ([]byte, string) {
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

	arn := tasks[0].TaskArn

	b1.mu.Lock("test-set-status")

	task, _ := b1.tasks.Get(arn)
	task.LastStatus = status
	task.AwaitingCapacity = awaiting

	if status == statusStopped {
		task.DesiredStatus = statusStopped
	}

	if c, _ := b1.clusters.Get("c"); c != nil && isLaunchingStatus(status) {
		c.RunningTasksCount = 0
		c.PendingTasksCount = 1
	}

	b1.mu.Unlock()

	return b1.Snapshot(t.Context()), arn
}

func TestRestoreResumesLaunches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		runErr     error
		name       string
		status     string
		wantStatus string
		wantReason string
		wantCalls  int
		adopt      bool
		awaiting   bool
	}{
		{name: "provisioning", status: statusProvisioning, wantStatus: statusRunning, wantCalls: 1},
		{name: "pending", status: statusPending, wantStatus: statusRunning, wantCalls: 1},
		{name: "activating adopted", status: statusActivating, adopt: true, wantStatus: statusRunning},
		{name: "activating lost", status: statusActivating, wantStatus: statusRunning, wantCalls: 1},
		{
			name: "launch failure stops task", status: statusProvisioning, runErr: errResumeBoom, wantCalls: 1,
			wantStatus: statusStopped, wantReason: "container start failed: image pull failed",
		},
		{
			name: "awaiting capacity left alone", status: statusProvisioning, awaiting: true,
			wantStatus: statusProvisioning,
		},
		{name: "running left alone", status: statusRunning, adopt: true, wantStatus: statusRunning},
		{name: "stopped left alone", status: statusStopped, wantStatus: statusStopped},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snap, arn := snapshotWithStatus(t, tt.status, tt.awaiting)

			runner := newResumeRunner()
			runner.err = tt.runErr
			runner.adopted[arn] = tt.adopt

			b2 := NewInMemoryBackend("000000000000", "us-east-1", runner)
			runner.be = b2

			require.NoError(t, b2.Restore(t.Context(), snap))
			b2.adoptRestoredTasks(t.Context())
			b2.awaitLaunches()

			got, _, err := b2.DescribeTasks("c", []string{arn})
			require.NoError(t, err)
			require.Len(t, got, 1)

			assert.Equal(t, tt.wantStatus, got[0].LastStatus)
			assert.Equal(t, tt.wantCalls, runner.callCount(arn))
			assert.Equal(t, tt.wantReason, got[0].StoppedReason)

			if tt.wantCalls > 0 {
				assert.Equal(t, statusActivating, runner.seen[arn])
			}

			if tt.wantStatus == statusStopped && tt.wantReason != "" {
				assert.Equal(t, stopCodeTaskFailedToStart, got[0].StopCode)
			}

			c, _ := b2.clusters.Get("c")
			require.NotNil(t, c)

			if isLaunchingStatus(tt.status) && !tt.awaiting {
				assert.Zero(t, c.PendingTasksCount)
			}
		})
	}
}

func TestRestoreResumesLaunchOnce(t *testing.T) {
	t.Parallel()

	snap, arn := snapshotWithStatus(t, statusProvisioning, false)

	runner := newResumeRunner()
	runner.gate = make(chan struct{})

	b2 := NewInMemoryBackend("000000000000", "us-east-1", runner)
	require.NoError(t, b2.Restore(t.Context(), snap))

	b2.adoptRestoredTasks(t.Context())
	b2.adoptRestoredTasks(t.Context())
	close(runner.gate)
	b2.awaitLaunches()

	assert.Equal(t, 1, runner.callCount(arn))
}

func TestRestoreWithoutRunnerLeavesLaunchingTasks(t *testing.T) {
	t.Parallel()

	snap, arn := snapshotWithStatus(t, statusProvisioning, false)

	b2 := NewInMemoryBackend("000000000000", "us-east-1", nil)
	require.NoError(t, b2.Restore(t.Context(), snap))
	b2.adoptRestoredTasks(t.Context())
	b2.awaitLaunches()

	got, _, err := b2.DescribeTasks("c", []string{arn})
	require.NoError(t, err)
	assert.Equal(t, statusProvisioning, got[0].LastStatus)
}

func TestRestoreFailsLaunchWhenTaskDefinitionGone(t *testing.T) {
	t.Parallel()

	snap, arn := snapshotWithStatus(t, statusProvisioning, false)

	runner := newResumeRunner()
	b2 := NewInMemoryBackend("000000000000", "us-east-1", runner)
	require.NoError(t, b2.Restore(t.Context(), snap))

	b2.mu.Lock("test-drop-def")
	b2.taskDefByArn.Reset()
	b2.mu.Unlock()

	b2.adoptRestoredTasks(t.Context())
	b2.awaitLaunches()

	got, _, err := b2.DescribeTasks("c", []string{arn})
	require.NoError(t, err)
	assert.Equal(t, statusStopped, got[0].LastStatus)
	assert.Contains(t, got[0].StoppedReason, errRestoredTaskDefinition.Error())
	assert.Zero(t, runner.callCount(arn))
}
