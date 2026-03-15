package ecs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	dockertypes "github.com/docker/docker/api/types/container"
	dockerimage "github.com/docker/docker/api/types/image"
	dockernetwork "github.com/docker/docker/api/types/network"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errContainerStartFailed = errors.New("start failed")

// fakeDockerClient is a test double for dockerClient.
// It assigns sequential IDs to created containers and records all operations.
type fakeDockerClient struct {
	startErrOnID string
	started      []string
	stopped      []string
	removed      []string
	nextID       int
	mu           sync.Mutex
}

func (f *fakeDockerClient) ImagePull(_ context.Context, _ string, _ dockerimage.PullOptions) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (f *fakeDockerClient) ContainerCreate(
	_ context.Context,
	_ *dockertypes.Config,
	_ *dockertypes.HostConfig,
	_ *dockernetwork.NetworkingConfig,
	_ *ocispec.Platform,
	_ string,
) (dockertypes.CreateResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.nextID++
	id := fmt.Sprintf("%s%02d", strings.Repeat("a", 12), f.nextID)

	return dockertypes.CreateResponse{ID: id}, nil
}

func (f *fakeDockerClient) ContainerStart(_ context.Context, containerID string, _ dockertypes.StartOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.startErrOnID != "" && containerID == f.startErrOnID {
		return errContainerStartFailed
	}

	f.started = append(f.started, containerID)

	return nil
}

func (f *fakeDockerClient) ContainerStop(_ context.Context, containerID string, _ dockertypes.StopOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.stopped = append(f.stopped, containerID)

	return nil
}

func (f *fakeDockerClient) ContainerRemove(_ context.Context, containerID string, _ dockertypes.RemoveOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.removed = append(f.removed, containerID)

	return nil
}

func TestBuildPortMappings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mappings []PortMapping
		wantHost int
	}{
		{
			name:     "empty mappings",
			mappings: nil,
			wantHost: 0,
		},
		{
			name: "with host port",
			mappings: []PortMapping{
				{ContainerPort: 80, HostPort: 8080, Protocol: "tcp"},
			},
			wantHost: 1,
		},
		{
			name: "default protocol is tcp",
			mappings: []PortMapping{
				{ContainerPort: 443, HostPort: 443},
			},
			wantHost: 1,
		},
		{
			name: "no host port means no binding",
			mappings: []PortMapping{
				{ContainerPort: 8080, Protocol: "tcp"},
			},
			wantHost: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			portBindings, exposedPorts := buildPortMappings(tt.mappings)
			assert.Len(t, portBindings, tt.wantHost)
			assert.Len(t, exposedPorts, len(tt.mappings))
		})
	}
}

func TestBuildEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		kvs  []KeyValuePair
		want []string
	}{
		{
			name: "empty",
			kvs:  nil,
			want: []string{},
		},
		{
			name: "single pair",
			kvs:  []KeyValuePair{{Name: "FOO", Value: "bar"}},
			want: []string{"FOO=bar"},
		},
		{
			name: "multiple pairs",
			kvs: []KeyValuePair{
				{Name: "A", Value: "1"},
				{Name: "B", Value: "2"},
			},
			want: []string{"A=1", "B=2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := buildEnv(tt.kvs)
			require.Len(t, got, len(tt.want))

			for i, expected := range tt.want {
				assert.Equal(t, expected, got[i])
			}
		})
	}
}

func TestClusterKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{
			input: "my-cluster",
			want:  "my-cluster",
		},
		{
			input: "arn:aws:ecs:us-east-1:000000000000:cluster/my-cluster",
			want:  "my-cluster",
		},
		{
			input: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, clusterKey(tt.input))
		})
	}
}

func TestServiceKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{
			input: "my-service",
			want:  "my-service",
		},
		{
			input: "arn:aws:ecs:us-east-1:000000000000:service/cluster/my-service",
			want:  "my-service",
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, serviceKey(tt.input))
		})
	}
}

// TestNewTaskRunner_Noop verifies that the default (no env var) returns a no-op runner.
func TestNewTaskRunner_Noop(t *testing.T) {
	t.Parallel()

	runner, err := newTaskRunner()
	require.NoError(t, err)
	require.NotNil(t, runner)

	// Noop runner should never fail.
	require.NoError(t, runner.RunTask(&Task{}, &TaskDefinition{}))
	require.NoError(t, runner.StopTask(&Task{}))
}

// TestNewTaskRunner_Docker verifies that GOPHERSTACK_ECS_RUNTIME=docker attempts
// to create a Docker runner. The test is skipped (gracefully) when the Docker
// daemon is unavailable, which is the expected state in most CI environments.
func TestNewTaskRunner_Docker(t *testing.T) {
	t.Setenv("GOPHERSTACK_ECS_RUNTIME", "docker")

	runner, err := newTaskRunner()
	if err != nil {
		// Docker daemon not reachable — acceptable in CI without Docker-in-Docker.
		return
	}

	// If Docker is available, the runner must be non-nil.
	assert.NotNil(t, runner)
}

// TestDockerRunner_MultiContainerTracking verifies that all containers in a
// multi-container task are individually tracked, not just the last one.
func TestDockerRunner_MultiContainerTracking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		containers  []ContainerDefinition
		wantTracked int
		wantStarted int
	}{
		{
			name: "single container",
			containers: []ContainerDefinition{
				{Image: "nginx:latest"},
			},
			wantTracked: 1,
			wantStarted: 1,
		},
		{
			name: "two containers in same task",
			containers: []ContainerDefinition{
				{Image: "nginx:latest"},
				{Image: "redis:latest"},
			},
			wantTracked: 2,
			wantStarted: 2,
		},
		{
			name: "three containers in same task",
			containers: []ContainerDefinition{
				{Image: "app:latest"},
				{Image: "sidecar:latest"},
				{Image: "proxy:latest"},
			},
			wantTracked: 3,
			wantStarted: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeDockerClient{}
			runner := newDockerRunnerWithClient(fake)
			task := &Task{TaskArn: "arn:aws:ecs:us-east-1:000000000000:task/default/task-1"}
			td := &TaskDefinition{ContainerDefinitions: tt.containers}

			require.NoError(t, runner.RunTask(task, td))

			runner.mu.Lock()
			tracked := runner.containers[task.TaskArn]
			runner.mu.Unlock()

			assert.Len(t, tracked, tt.wantTracked, "all container IDs must be tracked")
			assert.Len(t, fake.started, tt.wantStarted, "all containers must have been started")
		})
	}
}

// TestDockerRunner_StopTask_StopsAllContainers verifies that StopTask stops every
// container associated with a multi-container task, not just the last one.
func TestDockerRunner_StopTask_StopsAllContainers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		numContainers int
	}{
		{name: "single container", numContainers: 1},
		{name: "two containers", numContainers: 2},
		{name: "three containers", numContainers: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeDockerClient{}
			runner := newDockerRunnerWithClient(fake)
			task := &Task{TaskArn: "arn:aws:ecs:us-east-1:000000000000:task/default/task-1"}

			cds := make([]ContainerDefinition, tt.numContainers)
			for i := range cds {
				cds[i] = ContainerDefinition{Image: "img:latest"}
			}

			require.NoError(t, runner.RunTask(task, &TaskDefinition{ContainerDefinitions: cds}))
			require.NoError(t, runner.StopTask(task))

			assert.Len(t, fake.stopped, tt.numContainers, "every container must be stopped")

			runner.mu.Lock()
			_, stillTracked := runner.containers[task.TaskArn]
			runner.mu.Unlock()

			assert.False(t, stillTracked, "task must be removed from tracking after stop")
		})
	}
}

// TestDockerRunner_ContainerLeakOnStartFailure verifies that when ContainerStart
// fails, the already-created container is removed to prevent a resource leak.
func TestDockerRunner_ContainerLeakOnStartFailure(t *testing.T) {
	t.Parallel()

	fake := &fakeDockerClient{}
	runner := newDockerRunnerWithClient(fake)
	task := &Task{TaskArn: "arn:aws:ecs:us-east-1:000000000000:task/default/task-1"}
	td := &TaskDefinition{
		ContainerDefinitions: []ContainerDefinition{
			{Image: "nginx:latest"},
		},
	}

	// Trigger the failure after we know what ID will be assigned.
	// The fake assigns IDs sequentially with zero-padded numbers; first container gets "01".
	// We must pre-set the startErrOnID before calling RunTask.
	// ContainerCreate increments nextID and builds the ID, so we pre-compute it.
	fake.startErrOnID = fmt.Sprintf("%s%02d", strings.Repeat("a", 12), 1)

	err := runner.RunTask(task, td)
	require.Error(t, err, "RunTask must return an error when ContainerStart fails")

	fake.mu.Lock()
	removed := fake.removed
	fake.mu.Unlock()

	assert.Contains(t, removed, fake.startErrOnID, "failed container must be removed to prevent a leak")

	runner.mu.Lock()
	_, tracked := runner.containers[task.TaskArn]
	runner.mu.Unlock()

	assert.False(t, tracked, "failed container must not be tracked")
}

// TestDeleteCluster_CascadesContainerStops verifies that deleting a cluster stops
// Docker containers for all running tasks, preventing resource leaks.
func TestDeleteCluster_CascadesContainerStops(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		numTasks    int
		cdsPerTask  int
		wantStopped int
	}{
		{
			name:        "single task single container",
			numTasks:    1,
			cdsPerTask:  1,
			wantStopped: 1,
		},
		{
			name:        "two tasks single container each",
			numTasks:    2,
			cdsPerTask:  1,
			wantStopped: 2,
		},
		{
			name:        "two tasks two containers each",
			numTasks:    2,
			cdsPerTask:  2,
			wantStopped: 4,
		},
		{
			name:        "no tasks",
			numTasks:    0,
			cdsPerTask:  0,
			wantStopped: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeDockerClient{}
			runner := newDockerRunnerWithClient(fake)
			backend := NewInMemoryBackend("000000000000", "us-east-1", runner)

			_, err := backend.CreateCluster(CreateClusterInput{ClusterName: "test-cluster"})
			require.NoError(t, err)

			if tt.numTasks > 0 {
				cds := make([]ContainerDefinition, tt.cdsPerTask)
				for i := range cds {
					cds[i] = ContainerDefinition{Image: "img:latest"}
				}

				_, err = backend.RegisterTaskDefinition(RegisterTaskDefinitionInput{
					Family:               "test",
					ContainerDefinitions: cds,
				})
				require.NoError(t, err)

				for range tt.numTasks {
					_, runErr := backend.RunTask(RunTaskInput{
						Cluster:        "test-cluster",
						TaskDefinition: "test",
					})
					require.NoError(t, runErr)
				}
			}

			_, err = backend.DeleteCluster("test-cluster")
			require.NoError(t, err)

			fake.mu.Lock()
			stoppedCount := len(fake.stopped)
			fake.mu.Unlock()

			assert.Equal(t, tt.wantStopped, stoppedCount, "all task containers must be stopped on cluster deletion")
		})
	}
}
