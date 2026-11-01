package ecs

import (
	"context"
	"os/exec"
	"testing"
	"time"

	mobyclient "github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/dockercompat/client"
)

func TestDockerRunnerLiveUnhealthyServiceTaskReplaced(t *testing.T) {
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

	cli, err := client.NewClientWithOpts(client.FromEnv(), client.WithAPIVersionNegotiation())
	require.NoError(t, err)

	t.Cleanup(func() { _ = cli.Close() })

	runner := newDockerRunnerWithClient(context.Background(), sleepingClient{cli})
	runner.pollInterval = 200 * time.Millisecond
	b := NewInMemoryBackend("123456789012", "us-east-1", runner)
	b.wireRunnerHooks(runner)

	_, err = b.CreateCluster(CreateClusterInput{ClusterName: "live-unhealthy"})
	require.NoError(t, err)

	td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family: "live-unhealthy",
		ContainerDefinitions: []ContainerDefinition{{
			Name: "app", Image: liveImage, Essential: true,
			HealthCheck: &HealthCheck{Command: []string{"CMD-SHELL", "false"}, Interval: 1, Timeout: 2, Retries: 1},
		}},
	})
	require.NoError(t, err)

	_, err = b.CreateService(CreateServiceInput{
		Cluster: "live-unhealthy", ServiceName: "svc", TaskDefinition: td.TaskDefinitionArn, DesiredCount: 1,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		all, _, _ := b.DescribeTasks("live-unhealthy", nil)

		ids := make([]string, 0, len(all))
		for _, tk := range all {
			ids = append(ids, runner.ContainerIDs(tk.TaskArn)...)
		}

		b.Close()

		for _, id := range ids {
			_, _ = probe.ContainerRemove(
				context.Background(), id, mobyclient.ContainerRemoveOptions{Force: true, RemoveVolumes: true},
			)
		}
	})

	rec := NewReconciler(b)

	stoppedUnhealthy := func() []Task {
		all, _, descErr := b.DescribeTasks("live-unhealthy", nil)
		require.NoError(t, descErr)

		var out []Task

		for _, tk := range all {
			if tk.StoppedReason == containerHealthStopReason {
				out = append(out, tk)
			}
		}

		return out
	}

	require.Eventually(t, func() bool {
		rec.RunOnce(t.Context())
		b.awaitLaunches()

		return len(stoppedUnhealthy()) > 0
	}, liveTimeout*2, liveTick, "unhealthy task stopped by the service scheduler")

	stopped := stoppedUnhealthy()[0]
	assert.Equal(t, stopCodeServiceSchedulerInitiated, stopped.StopCode)

	require.Eventually(t, func() bool {
		rec.RunOnce(t.Context())
		b.awaitLaunches()

		all, _, descErr := b.DescribeTasks("live-unhealthy", nil)
		if descErr != nil {
			return false
		}

		for _, tk := range all {
			if tk.TaskArn != stopped.TaskArn && tk.DesiredStatus == statusRunning {
				return true
			}
		}

		return false
	}, liveTimeout*2, liveTick, "replacement task launched")
}
