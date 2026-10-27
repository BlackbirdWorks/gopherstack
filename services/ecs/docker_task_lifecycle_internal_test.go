package ecs

import (
	"testing"
	"time"

	mobycontainer "github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dockertypes "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
)

func newLifecycleBackend(t *testing.T, fake *fakeDockerClient, defs []ContainerDefinition) (*InMemoryBackend, []Task) {
	t.Helper()

	runner := newLaunchRunner(t, fake)
	b := NewInMemoryBackend("000000000000", "us-east-1", runner)
	b.wireRunnerHooks(runner)

	_, err := b.CreateCluster(CreateClusterInput{ClusterName: "c"})
	require.NoError(t, err)

	_, err = b.RegisterTaskDefinition(RegisterTaskDefinitionInput{Family: "job", ContainerDefinitions: defs})
	require.NoError(t, err)

	tasks, failures, err := b.RunTask(RunTaskInput{Cluster: "c", TaskDefinition: "job"})
	tasks = settleTasks(t, b, tasks)
	require.NoError(t, err)
	require.Empty(t, failures)
	require.Len(t, tasks, 1)

	return b, tasks
}

func describeOne(t *testing.T, b *InMemoryBackend, arn string) Task {
	t.Helper()

	got, _, err := b.DescribeTasks("c", []string{arn})
	require.NoError(t, err)
	require.Len(t, got, 1)

	return got[0]
}

func containerByName(t *testing.T, task Task, name string) Container {
	t.Helper()

	for _, c := range task.Containers {
		if c.Name == name {
			return c
		}
	}

	require.Failf(t, "container not found", name)

	return Container{}
}

func TestDockerTask_EssentialExit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantTask      string
		wantStopCode  string
		wantOtherLast string
		exitIdx       int
		exitCode      int64
		wantStopped   bool
	}{
		{
			name:          "non_essential_exit_keeps_task",
			exitIdx:       2,
			exitCode:      4,
			wantTask:      statusRunning,
			wantOtherLast: statusRunning,
		},
		{
			name: "essential_exit_stops_task", exitIdx: 1, exitCode: 9, wantTask: statusStopped,
			wantStopCode: stopCodeEssentialContainerExited, wantStopped: true, wantOtherLast: statusStopped,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			exits := map[string]chan dockertypes.WaitResponse{
				fakeID(1): make(chan dockertypes.WaitResponse, 1),
				fakeID(2): make(chan dockertypes.WaitResponse, 1),
			}
			fake := &fakeDockerClient{waitByID: exits}
			b, tasks := newLifecycleBackend(t, fake, []ContainerDefinition{
				{Name: "app", Image: "busybox", Essential: true},
				{Name: "side", Image: "busybox", Essential: false},
			})
			arn := tasks[0].TaskArn

			require.Equal(t, statusRunning, describeOne(t, b, arn).LastStatus)

			exits[fakeID(tt.exitIdx)] <- dockertypes.WaitResponse{StatusCode: tt.exitCode}

			require.Eventually(t, func() bool {
				c := containerByName(t, describeOne(t, b, arn), map[int]string{1: "app", 2: "side"}[tt.exitIdx])

				return c.ExitCode != nil
			}, 2*time.Second, time.Millisecond)

			got := describeOne(t, b, arn)
			exited := containerByName(t, got, map[int]string{1: "app", 2: "side"}[tt.exitIdx])
			assert.Equal(t, int(tt.exitCode), *exited.ExitCode)
			assert.Equal(t, tt.wantTask, got.LastStatus)
			assert.Equal(t, tt.wantStopCode, got.StopCode)

			if tt.wantStopped {
				assert.Equal(t, "Essential container in task exited", got.StoppedReason)
			}

			other := map[int]string{1: "side", 2: "app"}[tt.exitIdx]
			assert.Equal(t, tt.wantOtherLast, containerByName(t, got, other).LastStatus)
		})
	}
}

func TestDockerTask_HealthStatus(t *testing.T) {
	t.Parallel()

	state := func(s string) *dockertypes.State {
		return &dockertypes.State{Running: true, Health: &mobycontainer.Health{Status: mobycontainer.HealthStatus(s)}}
	}

	tests := []struct {
		name          string
		dockerStatus  string
		wantContainer string
		wantTask      string
	}{
		{name: "healthy", dockerStatus: "healthy", wantContainer: "HEALTHY", wantTask: "HEALTHY"},
		{name: "unhealthy", dockerStatus: "unhealthy", wantContainer: "UNHEALTHY", wantTask: "UNHEALTHY"},
		{
			name:          "starting_is_not_fabricated_healthy",
			dockerStatus:  "starting",
			wantContainer: "UNKNOWN",
			wantTask:      "UNKNOWN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeDockerClient{states: map[string]*dockertypes.State{fakeID(1): state(tt.dockerStatus)}}
			b, tasks := newLifecycleBackend(t, fake, []ContainerDefinition{
				{
					Name: "app", Image: "busybox", Essential: true,
					HealthCheck: &HealthCheck{Command: []string{"CMD-SHELL", "true"}},
				},
				{Name: "side", Image: "busybox", Essential: false},
			})
			arn := tasks[0].TaskArn

			require.Eventually(t, func() bool {
				return describeOne(t, b, arn).HealthStatus == tt.wantTask
			}, 2*time.Second, time.Millisecond)

			got := describeOne(t, b, arn)
			assert.Equal(t, tt.wantContainer, containerByName(t, got, "app").HealthStatus)
			assert.Empty(t, containerByName(t, got, "side").HealthStatus)
		})
	}
}

func TestDockerTask_SecretFailureStopsTask(t *testing.T) {
	t.Parallel()

	fake := &fakeDockerClient{}
	runner := newLaunchRunner(t, fake)
	b := NewInMemoryBackend("000000000000", "us-east-1", runner)
	b.wireRunnerHooks(runner)
	b.SetSecretsResolver(&fakeSecrets{smErr: assert.AnError})

	_, err := b.CreateCluster(CreateClusterInput{ClusterName: "c"})
	require.NoError(t, err)

	_, err = b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
		Family: "job",
		ContainerDefinitions: []ContainerDefinition{{
			Name: "app", Image: "busybox", Essential: true,
			Secrets: []SecretReference{{
				Name: "S", ValueFrom: "arn:aws:secretsmanager:us-east-1:000000000000:secret:missing-AbCdEf",
			}},
		}},
	})
	require.NoError(t, err)

	tasks, _, err := b.RunTask(RunTaskInput{Cluster: "c", TaskDefinition: "job"})
	tasks = settleTasks(t, b, tasks)
	require.NoError(t, err)

	got := describeOne(t, b, tasks[0].TaskArn)
	assert.Equal(t, statusStopped, got.LastStatus)
	assert.Equal(t, stopCodeTaskFailedToStart, got.StopCode)
	assert.Contains(t, got.StoppedReason, "ResourceInitializationError: unable to pull secrets or registry auth")
	assert.Empty(t, fake.createdImages())
}

func TestDockerTask_UserStopCode(t *testing.T) {
	t.Parallel()

	b, tasks := newLifecycleBackend(t, &fakeDockerClient{}, []ContainerDefinition{
		{Name: "app", Image: "busybox", Essential: true},
	})

	_, err := b.StopTask("c", tasks[0].TaskArn, "bye")
	require.NoError(t, err)

	got := describeOne(t, b, tasks[0].TaskArn)
	assert.Equal(t, stopCodeUserInitiated, got.StopCode)
	assert.Equal(t, "bye", got.StoppedReason)
}

func TestRegisterTaskDefinition_ContainerRuntimeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		defs    []ContainerDefinition
		volumes []Volume
		wantErr bool
	}{
		{
			name: "valid_healthy_dependency",
			defs: []ContainerDefinition{
				{Name: "a", Image: "x", Essential: true, DependsOn: dep("b", dependsHealthy)},
				{Name: "b", Image: "x", Essential: true, HealthCheck: &HealthCheck{Command: []string{"CMD", "x"}}},
			},
		},
		{
			name: "healthy_without_healthcheck",
			defs: []ContainerDefinition{
				{Name: "a", Image: "x", Essential: true, DependsOn: dep("b", dependsHealthy)},
				{Name: "b", Image: "x", Essential: true},
			},
			wantErr: true,
		},
		{
			name: "unknown_dependency",
			defs: []ContainerDefinition{
				{Name: "a", Image: "x", Essential: true, DependsOn: dep("ghost", dependsStart)},
			},
			wantErr: true,
		},
		{
			name:    "bad_condition",
			defs:    []ContainerDefinition{{Name: "a", Image: "x", Essential: true, DependsOn: dep("a", "SOON")}},
			wantErr: true,
		},
		{
			name: "mount_of_undefined_volume",
			defs: []ContainerDefinition{
				{
					Name:        "a",
					Image:       "x",
					Essential:   true,
					MountPoints: []MountPoint{{SourceVolume: "v", ContainerPath: "/v"}},
				},
			},
			wantErr: true,
		},
		{
			name:    "autoprovision_needs_shared_scope",
			defs:    []ContainerDefinition{{Name: "a", Image: "x", Essential: true}},
			volumes: []Volume{{Name: "v", DockerVolumeConfig: &DockerVolumeConfig{Autoprovision: new(true)}}},
			wantErr: true,
		},
		{
			name: "bad_healthcheck_command",
			defs: []ContainerDefinition{
				{Name: "a", Image: "x", Essential: true, HealthCheck: &HealthCheck{Command: []string{"true"}}},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1", NewNoopRunner())
			_, err := b.RegisterTaskDefinition(RegisterTaskDefinitionInput{
				Family: "f", ContainerDefinitions: tt.defs, Volumes: tt.volumes,
			})

			if tt.wantErr {
				require.ErrorIs(t, err, ErrClient)

				return
			}

			require.NoError(t, err)
		})
	}
}
