package ecs

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	mobyclient "github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/dockercompat/client"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

// onlyResource lists just the managed containers whose resource key starts with prefix, so a live test never
// reconciles containers it did not create.
type onlyResource struct {
	container.ManagedLister
	prefix string
}

func (o onlyResource) ListManaged(ctx context.Context) ([]container.ManagedContainer, error) {
	all, err := o.ManagedLister.ListManaged(ctx)
	if err != nil {
		return nil, err
	}

	var out []container.ManagedContainer

	for _, c := range all {
		if strings.HasPrefix(c.Labels[container.LabelResource], o.prefix) {
			out = append(out, c)
		}
	}

	return out, nil
}

func TestDockerRunnerLiveCrashedTaskAdopted(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not found")
	}

	probe, err := mobyclient.New(mobyclient.FromEnv)
	require.NoError(t, err)

	t.Cleanup(func() { _ = probe.Close() })

	if _, imgErr := probe.ImageInspect(t.Context(), liveImage); imgErr != nil {
		t.Skipf("%s not present: %v", liveImage, imgErr)
	}

	rt, err := container.NewRuntime(container.Config{})
	require.NoError(t, err)

	t.Cleanup(func() { _ = rt.Close() })

	lister, ok := rt.(container.ManagedLister)
	require.True(t, ok)

	cli, err := client.NewClientWithOpts(client.FromEnv(), client.WithAPIVersionNegotiation())
	require.NoError(t, err)

	t.Cleanup(func() { _ = cli.Close() })

	crashCtx, crash := context.WithCancel(context.Background())
	r1 := newDockerRunnerWithClient(crashCtx, sleepingClient{cli})
	b1 := NewInMemoryBackend("123456789012", "us-east-1", r1)
	r1.SetTaskCompletionHandler(b1.markTaskStoppedByContainerExit)

	_, err = b1.CreateCluster(CreateClusterInput{ClusterName: "adopt"})
	require.NoError(t, err)

	stopTimeout := 1

	td, err := b1.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family: "adopt-live",
		ContainerDefinitions: []ContainerDefinition{
			{Name: "app", Image: liveImage, Essential: true, StopTimeout: &stopTimeout},
		},
	})
	require.NoError(t, err)

	tasks, _, err := b1.RunTask(RunTaskInput{Cluster: "adopt", TaskDefinition: td.TaskDefinitionArn})
	require.NoError(t, err)

	arn := tasks[0].TaskArn
	b1.awaitLaunches()

	ids := r1.ContainerIDs(arn)
	require.Len(t, ids, 1)

	t.Cleanup(func() {
		_, _ = probe.ContainerRemove(
			context.Background(), ids[0], mobyclient.ContainerRemoveOptions{Force: true, RemoveVolumes: true},
		)
	})

	snap := b1.Snapshot(t.Context())

	crash()

	orphans, err := container.Reconcile(
		t.Context(), onlyResource{lister, arn}, container.CurrentIdentity(), nil,
	)
	require.NoError(t, err)

	r2 := newDockerRunnerWithClient(t.Context(), sleepingClient{cli})
	r2.claim = orphans.Claim
	b2 := NewInMemoryBackend("123456789012", "us-east-1", r2)
	r2.SetTaskCompletionHandler(b2.markTaskStoppedByContainerExit)

	require.NoError(t, b2.Restore(t.Context(), snap))
	b2.adoptRestoredTasks(t.Context())

	got, _, err := b2.DescribeTasks("adopt", []string{arn})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, statusRunning, got[0].LastStatus)
	assert.Equal(t, ids, r2.ContainerIDs(arn), "the surviving container must be adopted, not replaced")

	insp, err := probe.ContainerInspect(t.Context(), ids[0], mobyclient.ContainerInspectOptions{})
	require.NoError(t, err)
	require.NotNil(t, insp.Container.State)
	assert.True(t, insp.Container.State.Running)

	assert.Zero(t, orphans.SweepUnclaimed(t.Context()), "nothing may be left for the sweep")

	require.NoError(t, r2.StopTask(&Task{TaskArn: arn}))

	insp, err = probe.ContainerInspect(t.Context(), ids[0], mobyclient.ContainerInspectOptions{})
	require.NoError(t, err)
	assert.False(t, insp.Container.State.Running, "StopTask must reach the adopted container")
}
