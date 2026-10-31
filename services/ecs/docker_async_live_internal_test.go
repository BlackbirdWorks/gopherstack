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

func TestDockerRunnerLiveAsyncHealthyDependency(t *testing.T) {
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

	_, err = b.CreateCluster(CreateClusterInput{ClusterName: "live"})
	require.NoError(t, err)

	td, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family: "live-health",
		ContainerDefinitions: []ContainerDefinition{
			{
				Name: "db", Image: liveImage, Essential: true,
				HealthCheck: &HealthCheck{Command: []string{"CMD-SHELL", "true"}, Interval: 5, Timeout: 2, Retries: 1},
			},
			{
				Name: "app", Image: liveImage, Essential: true,
				DependsOn: []ContainerDependency{{ContainerName: "db", Condition: dependsHealthy}},
			},
		},
	})
	require.NoError(t, err)

	started := time.Now()

	tasks, _, err := b.RunTask(RunTaskInput{Cluster: "live", TaskDefinition: td.TaskDefinitionArn})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	t.Cleanup(func() {
		ids := runner.ContainerIDs(tasks[0].TaskArn)

		b.Close()

		for _, id := range ids {
			_, _ = probe.ContainerRemove(
				context.Background(), id, mobyclient.ContainerRemoveOptions{Force: true, RemoveVolumes: true},
			)
		}
	})

	assert.Equal(t, statusProvisioning, tasks[0].LastStatus)
	assert.Less(t, time.Since(started), 3*time.Second, "RunTask must not wait for the HEALTHY gate")

	require.Eventually(t, func() bool {
		got, _, descErr := b.DescribeTasks("live", []string{tasks[0].TaskArn})

		return descErr == nil && len(got) == 1 && got[0].LastStatus == statusRunning
	}, liveTimeout*2, liveTick)

	got, _, err := b.DescribeTasks("live", []string{tasks[0].TaskArn})
	require.NoError(t, err)
	assert.NotNil(t, got[0].StartedAt)
	assert.NotNil(t, got[0].PullStoppedAt)
	assert.Len(t, runner.ContainerIDs(tasks[0].TaskArn), 2)
}
