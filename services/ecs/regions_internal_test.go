package ecs

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dockertypes "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
)

const peerRegion = "eu-west-1"

func newDockerRegionHandler(t *testing.T, fake *fakeDockerClient) (*Handler, *InMemoryBackend) {
	t.Helper()

	runner := newDockerRunnerWithClient(t.Context(), fake)
	home := NewInMemoryBackend("000000000000", "us-east-1", runner)
	runner.SetTaskCompletionHandler(home.markTaskStoppedByContainerExit)

	h := NewHandler(home)
	h.EnableRegions(t.Context())

	peer, ok := h.BackendFor(peerRegion).(*InMemoryBackend)
	require.True(t, ok)

	return h, peer
}

func runPeerTask(t *testing.T, peer *InMemoryBackend, def ContainerDefinition) Task {
	t.Helper()

	_, err := peer.CreateCluster(CreateClusterInput{ClusterName: "c"})
	require.NoError(t, err)

	_, err = peer.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family: "job", ContainerDefinitions: []ContainerDefinition{def},
	})
	require.NoError(t, err)

	tasks, _, err := peer.RunTask(RunTaskInput{Cluster: "c", TaskDefinition: "job"})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	return tasks[0]
}

func TestHandler_PeerDockerRunner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *Handler, peer *InMemoryBackend, fake *fakeDockerClient)
		name  string
	}{
		{
			name: "own-runner-per-region",
			check: func(t *testing.T, h *Handler, peer *InMemoryBackend, _ *fakeDockerClient) {
				t.Helper()

				assert.NotSame(t, h.Backend.(*InMemoryBackend).runner, peer.runner)
			},
		},
		{
			name: "container-exit-stops-peer-task",
			check: func(t *testing.T, _ *Handler, peer *InMemoryBackend, fake *fakeDockerClient) {
				t.Helper()

				task := runPeerTask(t, peer, ContainerDefinition{Name: "app", Image: "busybox"})
				assert.Contains(t, task.TaskArn, ":ecs:"+peerRegion+":")

				require.Eventually(t, func() bool {
					fake.mu.Lock()
					defer fake.mu.Unlock()

					return len(fake.waitRequestedOn) > 0
				}, 2*time.Second, 10*time.Millisecond)

				fake.waitResult <- dockertypes.WaitResponse{StatusCode: 1}

				require.Eventually(t, func() bool {
					got, _, err := peer.DescribeTasks("c", []string{task.TaskArn})
					require.NoError(t, err)

					return len(got) == 1 && got[0].LastStatus == statusStopped
				}, 2*time.Second, 10*time.Millisecond)
			},
		},
		{
			name: "awslogs-use-region-sink",
			check: func(t *testing.T, h *Handler, _ *InMemoryBackend, _ *fakeDockerClient) {
				t.Helper()

				sinks := map[string]*mockECSCWLogsBackend{}

				var mu sync.Mutex

				h.SetCWLogsFactory(func(region string) CWLogsBackend {
					mu.Lock()
					defer mu.Unlock()

					sinks[region] = &mockECSCWLogsBackend{}

					return sinks[region]
				})

				fresh := h.BackendFor("ap-south-1").(*InMemoryBackend)
				runPeerTask(t, fresh, awslogsContainerDef("/ecs/app", ""))

				mu.Lock()
				defer mu.Unlock()

				require.Contains(t, sinks, "ap-south-1")
				assert.NotEmpty(t, sinks["ap-south-1"].calls())
				assert.NotContains(t, sinks, peerRegion)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeDockerClient{waitResult: make(chan dockertypes.WaitResponse, 1)}
			h, peer := newDockerRegionHandler(t, fake)
			tc.check(t, h, peer, fake)
		})
	}
}
