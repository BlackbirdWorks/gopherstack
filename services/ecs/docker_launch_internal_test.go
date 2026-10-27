package ecs

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mobycontainer "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dockertypes "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
)

const launchTaskArn = "arn:aws:ecs:us-east-1:000000000000:task/default/task-1"

var (
	errTestMissing        = errors.New("missing")
	errTestParamNotFound  = errors.New("ParameterNotFound")
	errTestSecretNotFound = errors.New("ResourceNotFoundException")
)

func fakeID(n int) string {
	return "aaaaaaaaaaaa0" + string(rune('0'+n))
}

func newLaunchRunner(t *testing.T, fake *fakeDockerClient) *realDockerRunner {
	t.Helper()

	r := newDockerRunnerWithClient(t.Context(), fake)
	r.pollInterval = time.Millisecond

	return r
}

func launchTask() *Task { return &Task{TaskArn: launchTaskArn, ClusterArn: "arn:cluster"} }

func (f *fakeDockerClient) createdImages() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]string, 0, len(f.creates))
	for _, c := range f.creates {
		out = append(out, c.cfg.Image)
	}

	return out
}

func (f *fakeDockerClient) createAt(i int) createCall {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.creates[i]
}

func TestRunTask_Secrets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lookup    func(ctx context.Context, valueFrom string) (string, error)
		name      string
		wantEnv   []string
		wantErr   bool
		wantNoRun bool
	}{
		{
			name: "secret_appended_after_environment",
			lookup: func(_ context.Context, v string) (string, error) {
				return "val-of-" + v, nil
			},
			wantEnv: []string{"PLAIN=1", "DB_PASS=val-of-arn:x"},
		},
		{
			name: "lookup_failure_stops_before_any_container",
			lookup: func(context.Context, string) (string, error) {
				return "", &taskLaunchError{Reason: "ResourceInitializationError: boom", Err: errTestMissing}
			},
			wantErr:   true,
			wantNoRun: true,
		},
		{name: "no_resolver_wired", lookup: nil, wantErr: true, wantNoRun: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeDockerClient{}
			r := newLaunchRunner(t, fake)
			r.SetSecretResolver(tt.lookup)

			td := &TaskDefinition{ContainerDefinitions: []ContainerDefinition{{
				Name: "app", Image: "busybox", Essential: true,
				Environment: []KeyValuePair{{Name: "PLAIN", Value: "1"}},
				Secrets:     []SecretReference{{Name: "DB_PASS", ValueFrom: "arn:x"}},
			}}}

			err := r.RunTask(launchTask(), td)
			if tt.wantErr {
				var le *taskLaunchError

				require.ErrorAs(t, err, &le)
				assert.Contains(t, le.Reason, "ResourceInitializationError")
				assert.Empty(t, fake.createdImages())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantEnv, fake.createAt(0).cfg.Env)
		})
	}
}

type fakeSecrets struct {
	smErr     error
	ssmValues map[string]string
	smValue   string
	smCalls   []string
	mu        sync.Mutex
}

func (f *fakeSecrets) SecretString(_ context.Context, region, id, stage, version string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.smCalls = append(f.smCalls, region+"|"+id+"|"+stage+"|"+version)

	return f.smValue, f.smErr
}

func (f *fakeSecrets) ParameterValue(_ context.Context, region, name string) (string, error) {
	if v, ok := f.ssmValues[region+"|"+name]; ok {
		return v, nil
	}

	return "", errTestParamNotFound
}

func TestResolveSecret(t *testing.T) {
	t.Parallel()

	const sm = "arn:aws:secretsmanager:us-east-1:000000000000:secret:db-AbCdEf"

	tests := []struct {
		resolver  *fakeSecrets
		name      string
		valueFrom string
		want      string
		wantCall  string
		wantErr   string
	}{
		{
			name: "whole_secret", valueFrom: sm,
			resolver: &fakeSecrets{smValue: "plain"}, want: "plain", wantCall: "us-east-1|" + sm + "||",
		},
		{
			name: "json_key_stage_version", valueFrom: sm + ":password:AWSPREVIOUS:v1",
			resolver: &fakeSecrets{smValue: `{"password":"s3cret","n":5}`}, want: "s3cret",
			wantCall: "us-east-1|" + sm + "|AWSPREVIOUS|v1",
		},
		{
			name: "json_key_non_string", valueFrom: sm + ":n::",
			resolver: &fakeSecrets{smValue: `{"n":5}`}, want: "5",
		},
		{
			name: "json_key_missing", valueFrom: sm + ":nope::",
			resolver: &fakeSecrets{smValue: `{"n":5}`}, wantErr: "secrets manager",
		},
		{
			name: "secret_not_found", valueFrom: sm,
			resolver: &fakeSecrets{smErr: errTestSecretNotFound}, wantErr: "secrets manager",
		},
		{
			name: "ssm_name", valueFrom: "/app/db",
			resolver: &fakeSecrets{ssmValues: map[string]string{"us-east-1|/app/db": "p"}}, want: "p",
		},
		{
			name: "ssm_arn_without_slash_fallback", valueFrom: "arn:aws:ssm:us-east-1:000000000000:parameter/plain",
			resolver: &fakeSecrets{ssmValues: map[string]string{"us-east-1|plain": "q"}}, want: "q",
		},
		{
			name: "ssm_missing", valueFrom: "/nope",
			resolver: &fakeSecrets{}, wantErr: "ssm",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1", NewNoopRunner())
			b.SetSecretsResolver(tt.resolver)

			got, err := b.resolveSecret(t.Context(), tt.valueFrom)
			if tt.wantErr != "" {
				var le *taskLaunchError

				require.ErrorAs(t, err, &le)
				assert.Contains(t, le.Reason, "ResourceInitializationError: unable to pull secrets or registry auth")
				assert.Contains(t, le.Reason, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)

			if tt.wantCall != "" {
				assert.Equal(t, []string{tt.wantCall}, tt.resolver.smCalls)
			}
		})
	}
}

func TestResolveSecret_NoResolver(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1", NewNoopRunner())
	_, err := b.resolveSecret(t.Context(), "/x")

	var le *taskLaunchError

	require.ErrorAs(t, err, &le)
}

func TestRunTask_Volumes(t *testing.T) {
	t.Parallel()

	taskVol := "gopherstack-ecs-task-1-"

	tests := []struct {
		fake      *fakeDockerClient
		check     func(t *testing.T, fake *fakeDockerClient)
		name      string
		volumes   []Volume
		mounts    []MountPoint
		wantError bool
	}{
		{
			name:    "bind_host_path_readonly",
			volumes: []Volume{{Name: "data", Host: &HostVolumeProperties{SourcePath: "/srv/data"}}},
			mounts:  []MountPoint{{SourceVolume: "data", ContainerPath: "/data", ReadOnly: true}},
			check: func(t *testing.T, fake *fakeDockerClient) {
				t.Helper()

				assert.Equal(t, []mount.Mount{{
					Type: mount.TypeBind, Source: "/srv/data", Target: "/data", ReadOnly: true,
					BindOptions: &mount.BindOptions{CreateMountpoint: true},
				}}, fake.createAt(0).host.Mounts)
			},
		},
		{
			name:    "empty_host_is_task_scoped_ephemeral",
			volumes: []Volume{{Name: "scratch"}},
			mounts:  []MountPoint{{SourceVolume: "scratch", ContainerPath: "/tmp/s"}},
			check: func(t *testing.T, fake *fakeDockerClient) {
				t.Helper()

				assert.Equal(t, []mount.Mount{{
					Type: mount.TypeVolume, Source: taskVol + "scratch", Target: "/tmp/s",
				}}, fake.createAt(0).host.Mounts)
			},
		},
		{
			name: "docker_task_scope_driver_and_labels",
			volumes: []Volume{{Name: "d", DockerVolumeConfig: &DockerVolumeConfig{
				Scope: "task", Driver: "local", DriverOpts: map[string]string{"type": "tmpfs"},
				Labels: map[string]string{"k": "v"},
			}}},
			mounts: []MountPoint{{SourceVolume: "d", ContainerPath: "/d"}},
			check: func(t *testing.T, fake *fakeDockerClient) {
				t.Helper()

				assert.Equal(t, []mount.Mount{{
					Type: mount.TypeVolume, Source: taskVol + "d", Target: "/d",
					VolumeOptions: &mount.VolumeOptions{
						Labels:       map[string]string{"k": "v"},
						DriverConfig: &mount.Driver{Name: "local", Options: map[string]string{"type": "tmpfs"}},
					},
				}}, fake.createAt(0).host.Mounts)
			},
		},
		{
			name: "shared_autoprovision",
			volumes: []Volume{{Name: "shared", DockerVolumeConfig: &DockerVolumeConfig{
				Scope: "shared", Autoprovision: new(true),
			}}},
			mounts: []MountPoint{{SourceVolume: "shared", ContainerPath: "/s"}},
			check: func(t *testing.T, fake *fakeDockerClient) {
				t.Helper()

				assert.Equal(t, "shared", fake.createAt(0).host.Mounts[0].Source)
				assert.Empty(t, fake.volumesRemoved)
			},
		},
		{
			name:      "shared_without_autoprovision_must_exist",
			volumes:   []Volume{{Name: "shared", DockerVolumeConfig: &DockerVolumeConfig{Scope: "shared"}}},
			mounts:    []MountPoint{{SourceVolume: "shared", ContainerPath: "/s"}},
			wantError: true,
		},
		{
			name:    "shared_without_autoprovision_existing",
			fake:    &fakeDockerClient{volumes: map[string]bool{"shared": true}},
			volumes: []Volume{{Name: "shared", DockerVolumeConfig: &DockerVolumeConfig{Scope: "shared"}}},
			mounts:  []MountPoint{{SourceVolume: "shared", ContainerPath: "/s"}},
			check: func(t *testing.T, fake *fakeDockerClient) {
				t.Helper()

				assert.Equal(t, "shared", fake.createAt(0).host.Mounts[0].Source)
			},
		},
		{
			name:      "efs_rejected",
			volumes:   []Volume{{Name: "efs", EFSVolumeConfiguration: &EFSVolumeConfiguration{FileSystemID: "fs-1"}}},
			mounts:    []MountPoint{{SourceVolume: "efs", ContainerPath: "/e"}},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := tt.fake
			if fake == nil {
				fake = &fakeDockerClient{}
			}

			r := newLaunchRunner(t, fake)
			td := &TaskDefinition{
				Volumes: tt.volumes,
				ContainerDefinitions: []ContainerDefinition{
					{Name: "app", Image: "busybox", Essential: true, MountPoints: tt.mounts},
				},
			}

			err := r.RunTask(launchTask(), td)
			if tt.wantError {
				require.Error(t, err)
				assert.Empty(t, fake.createdImages())

				return
			}

			require.NoError(t, err)
			tt.check(t, fake)
		})
	}
}

func TestRunTask_VolumesFromOrdersSourceFirst(t *testing.T) {
	t.Parallel()

	fake := &fakeDockerClient{}
	r := newLaunchRunner(t, fake)
	td := &TaskDefinition{ContainerDefinitions: []ContainerDefinition{
		{
			Name:        "consumer",
			Image:       "consumer",
			Essential:   true,
			VolumesFrom: []VolumeFrom{{SourceContainer: "owner", ReadOnly: true}},
		},
		{Name: "owner", Image: "owner", Essential: true},
	}}

	require.NoError(t, r.RunTask(launchTask(), td))
	assert.Equal(t, []string{"owner", "consumer"}, fake.createdImages())
	assert.Equal(t, []string{fakeID(1) + ":ro"}, fake.createAt(1).host.VolumesFrom)

	ids := r.ContainerIDs(launchTaskArn)
	assert.Equal(t, []string{fakeID(2), fakeID(1)}, ids, "ContainerIDs keeps task-definition order")
}

func TestOrderContainers_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		defs []ContainerDefinition
	}{
		{
			name: "unknown_dependency",
			defs: []ContainerDefinition{
				{Name: "a", DependsOn: []ContainerDependency{{ContainerName: "ghost", Condition: "START"}}},
			},
		},
		{
			name: "cycle",
			defs: []ContainerDefinition{
				{Name: "a", DependsOn: []ContainerDependency{{ContainerName: "b", Condition: "START"}}},
				{Name: "b", DependsOn: []ContainerDependency{{ContainerName: "a", Condition: "START"}}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := orderContainers(tt.defs)
			require.Error(t, err)
		})
	}
}

func dep(name, cond string) []ContainerDependency {
	return []ContainerDependency{{ContainerName: name, Condition: cond}}
}

func TestRunTask_DependsOnStart(t *testing.T) {
	t.Parallel()

	fake := &fakeDockerClient{}
	r := newLaunchRunner(t, fake)
	td := &TaskDefinition{ContainerDefinitions: []ContainerDefinition{
		{Name: "web", Image: "web", Essential: true, DependsOn: dep("db", dependsStart)},
		{Name: "db", Image: "db", Essential: true},
	}}

	require.NoError(t, r.RunTask(launchTask(), td))
	assert.Equal(t, []string{"db", "web"}, fake.createdImages())
}

func TestRunTask_DependsOnSuccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cond     string
		exitCode int64
		wantErr  bool
	}{
		{name: "success_zero", cond: dependsSuccess, exitCode: 0},
		{name: "success_nonzero_fails_task", cond: dependsSuccess, exitCode: 3, wantErr: true},
		{name: "complete_nonzero_ok", cond: dependsComplete, exitCode: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			initExit := make(chan dockertypes.WaitResponse, 1)
			fake := &fakeDockerClient{waitByID: map[string]chan dockertypes.WaitResponse{fakeID(1): initExit}}
			r := newLaunchRunner(t, fake)
			td := &TaskDefinition{ContainerDefinitions: []ContainerDefinition{
				{Name: "main", Image: "main", Essential: true, DependsOn: dep("init", tt.cond)},
				{Name: "init", Image: "init", Essential: false},
			}}

			done := make(chan error, 1)

			go func() { done <- r.RunTask(launchTask(), td) }()

			require.Eventually(
				t,
				func() bool { return len(fake.createdImages()) == 1 },
				2*time.Second,
				time.Millisecond,
			)

			select {
			case <-done:
				t.Fatal("RunTask returned before the init container exited")
			default:
			}

			initExit <- dockertypes.WaitResponse{StatusCode: tt.exitCode}

			err := <-done
			if tt.wantErr {
				var le *taskLaunchError

				require.ErrorAs(t, err, &le)
				assert.Contains(t, le.Reason, "exited with code 3")
				assert.Equal(t, []string{"init"}, fake.createdImages(), "dependent container never created")
				assert.Empty(t, r.ContainerIDs(launchTaskArn), "failed launch is rolled back")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, []string{"init", "main"}, fake.createdImages())
		})
	}
}

func TestRunTask_DependsOnHealthy(t *testing.T) {
	t.Parallel()

	healthy := func(status string) *dockertypes.State {
		return &dockertypes.State{
			Running: true,
			Health:  &mobycontainer.Health{Status: mobycontainer.HealthStatus(status)},
		}
	}

	tests := []struct {
		final   *dockertypes.State
		name    string
		wantErr string
	}{
		{name: "becomes_healthy", final: healthy("healthy")},
		{name: "becomes_unhealthy", final: healthy("unhealthy"), wantErr: "became unhealthy"},
		{name: "exits", final: &dockertypes.State{Running: false, ExitCode: 2}, wantErr: "exited with code 2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake := &fakeDockerClient{states: map[string]*dockertypes.State{fakeID(1): healthy("starting")}}
			r := newLaunchRunner(t, fake)
			td := &TaskDefinition{ContainerDefinitions: []ContainerDefinition{
				{Name: "app", Image: "app", Essential: true, DependsOn: dep("db", dependsHealthy)},
				{
					Name: "db", Image: "db", Essential: true,
					HealthCheck: &HealthCheck{Command: []string{"CMD-SHELL", "true"}},
				},
			}}

			done := make(chan error, 1)

			go func() { done <- r.RunTask(launchTask(), td) }()

			require.Eventually(
				t,
				func() bool { return len(fake.createdImages()) == 1 },
				2*time.Second,
				time.Millisecond,
			)
			assert.Len(t, fake.createdImages(), 1, "app waits while db is still starting")

			fake.mu.Lock()
			fake.states[fakeID(1)] = tt.final
			fake.mu.Unlock()

			err := <-done
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, []string{"db", "app"}, fake.createdImages())
		})
	}
}

func TestRunTask_DependencyTimeout(t *testing.T) {
	t.Parallel()

	fake := &fakeDockerClient{states: map[string]*dockertypes.State{
		fakeID(1): {Running: true, Health: &mobycontainer.Health{Status: "starting"}},
	}}
	r := newLaunchRunner(t, fake)
	r.dependencyTimeout = 20 * time.Millisecond

	td := &TaskDefinition{ContainerDefinitions: []ContainerDefinition{
		{Name: "app", Image: "app", Essential: true, DependsOn: dep("db", dependsHealthy)},
		{Name: "db", Image: "db", Essential: true, HealthCheck: &HealthCheck{Command: []string{"CMD", "x"}}},
	}}

	err := r.RunTask(launchTask(), td)

	var le *taskLaunchError

	require.ErrorAs(t, err, &le)
	assert.Contains(t, le.Reason, "timed out")
	assert.Empty(t, r.ContainerIDs(launchTaskArn))
}

func TestStopTask_ReverseOrderAndStopTimeout(t *testing.T) {
	t.Parallel()

	fake := &fakeDockerClient{}
	r := newLaunchRunner(t, fake)
	td := &TaskDefinition{
		Volumes: []Volume{{Name: "scratch"}},
		ContainerDefinitions: []ContainerDefinition{
			{
				Name:        "a",
				Image:       "a",
				Essential:   true,
				MountPoints: []MountPoint{{SourceVolume: "scratch", ContainerPath: "/s"}},
			},
			{Name: "b", Image: "b", Essential: true, StopTimeout: new(7), DependsOn: dep("a", dependsStart)},
			{Name: "c", Image: "c", Essential: true, DependsOn: dep("b", dependsStart)},
		},
	}

	require.NoError(t, r.RunTask(launchTask(), td))
	require.NoError(t, r.StopTask(launchTask()))

	fake.mu.Lock()
	defer fake.mu.Unlock()

	assert.Equal(t, []string{fakeID(3), fakeID(2), fakeID(1)}, fake.stopped, "reverse of start order")
	assert.Equal(t, map[string]int{fakeID(1): 30, fakeID(2): 7, fakeID(3): 30}, fake.stopTimeouts)
	assert.ElementsMatch(
		t,
		[]string{fakeID(1), fakeID(2), fakeID(3)},
		fake.removed,
		"containers removed with task volumes",
	)
	assert.Equal(t, []string{"gopherstack-ecs-task-1-scratch"}, fake.volumesRemoved)
}

func TestRunTask_FailureRemovesTaskVolumes(t *testing.T) {
	t.Parallel()

	fake := &fakeDockerClient{startErrOnID: fakeID(2)}
	r := newLaunchRunner(t, fake)
	td := &TaskDefinition{
		Volumes: []Volume{{Name: "scratch"}},
		ContainerDefinitions: []ContainerDefinition{
			{Name: "a", Image: "a", Essential: true},
			{Name: "b", Image: "b", Essential: true},
		},
	}

	require.Error(t, r.RunTask(launchTask(), td))

	fake.mu.Lock()
	defer fake.mu.Unlock()

	assert.Equal(t, []string{"gopherstack-ecs-task-1-scratch"}, fake.volumesRemoved)
}

func TestWatchContainerExit_RestartPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy      *ContainerRestartPolicy
		name        string
		exitCode    int64
		ranFor      time.Duration
		wantStarts  int
		wantHandler bool
	}{
		{
			name: "restarts_after_period", policy: &ContainerRestartPolicy{Enabled: true, RestartAttemptPeriod: 60},
			exitCode: 1, ranFor: 61 * time.Second, wantStarts: 2,
		},
		{
			name: "restarts_on_zero_exit_too", policy: &ContainerRestartPolicy{Enabled: true, RestartAttemptPeriod: 60},
			exitCode: 0, ranFor: 2 * time.Minute, wantStarts: 2,
		},
		{
			name: "ignored_exit_code", policy: &ContainerRestartPolicy{Enabled: true, IgnoredExitCodes: []int{7}},
			exitCode: 7, ranFor: time.Hour, wantStarts: 1, wantHandler: true,
		},
		{
			name: "exited_too_early", policy: &ContainerRestartPolicy{Enabled: true, RestartAttemptPeriod: 60},
			exitCode: 1, ranFor: 10 * time.Second, wantStarts: 1, wantHandler: true,
		},
		{
			name: "disabled", policy: &ContainerRestartPolicy{Enabled: false},
			exitCode: 1, ranFor: time.Hour, wantStarts: 1, wantHandler: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			exit := make(chan dockertypes.WaitResponse, 1)
			fake := &fakeDockerClient{waitByID: map[string]chan dockertypes.WaitResponse{fakeID(1): exit}}
			r := newLaunchRunner(t, fake)

			var (
				cur     atomic.Int64
				handled = make(chan int, 1)
			)

			base := time.Unix(1_700_000_000, 0)
			r.now = func() time.Time { return base.Add(time.Duration(cur.Load())) }
			r.SetTaskCompletionHandler(func(_, _ string, code int) { handled <- code })

			td := &TaskDefinition{ContainerDefinitions: []ContainerDefinition{
				{Name: "app", Image: "app", Essential: true, RestartPolicy: tt.policy},
			}}

			require.NoError(t, r.RunTask(launchTask(), td))

			cur.Store(int64(tt.ranFor))
			exit <- dockertypes.WaitResponse{StatusCode: tt.exitCode}

			if tt.wantHandler {
				assert.Equal(t, int(tt.exitCode), <-handled)
			} else {
				require.Eventually(t, func() bool {
					fake.mu.Lock()
					defer fake.mu.Unlock()

					return len(fake.started) == tt.wantStarts
				}, 2*time.Second, time.Millisecond)
				assert.Empty(t, handled, "a restart is not a task-level exit")
			}

			fake.mu.Lock()
			defer fake.mu.Unlock()

			assert.Len(t, fake.started, tt.wantStarts)
		})
	}
}
