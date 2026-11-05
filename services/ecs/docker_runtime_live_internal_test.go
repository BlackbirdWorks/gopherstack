package ecs

import (
	"context"
	"net"
	"net/http/httptest"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/labstack/echo/v5"
	mobyclient "github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/dockercompat/client"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// liveEnv is an ECS backend running tasks on the local Docker daemon, driven through the real SDK client.
type liveEnv struct {
	b      *InMemoryBackend
	runner *realDockerRunner
	sdk    *ecssdk.Client
	probe  *mobyclient.Client
}

func newLiveEnv(t *testing.T) *liveEnv {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not found")
	}

	probe, err := mobyclient.New(mobyclient.FromEnv)
	require.NoError(t, err)

	t.Cleanup(func() { _ = probe.Close() })

	for _, img := range []string{liveImage, pauseImage()} {
		if _, imgErr := probe.ImageInspect(t.Context(), img); imgErr != nil {
			t.Skipf("%s not present: %v", img, imgErr)
		}
	}

	cli, err := client.NewClientWithOpts(client.FromEnv(), client.WithAPIVersionNegotiation())
	require.NoError(t, err)

	t.Cleanup(func() { _ = cli.Close() })

	runner := newDockerRunnerWithClient(context.Background(), noPullClient{cli})
	runner.pollInterval = 200 * time.Millisecond
	b := NewInMemoryBackend("123456789012", "us-east-1", runner)
	b.wireRunnerHooks(runner)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(NewHandler(b)))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	env := &liveEnv{
		b: b, runner: runner, probe: probe,
		sdk: ecssdk.NewFromConfig(cfg, func(o *ecssdk.Options) { o.BaseEndpoint = aws.String(srv.URL) }),
	}

	t.Cleanup(func() { env.cleanup(t) })

	return env
}

// cleanup stops every task and removes whatever containers the test left behind.
func (e *liveEnv) cleanup(t *testing.T) {
	t.Helper()

	ctx := context.Background()
	arns, _ := e.b.ListTasks("default")

	for _, arn := range arns {
		ids := e.runner.ContainerIDs(arn)
		_, _ = e.b.StopTask("default", arn, "test cleanup")
		_ = e.runner.StopTask(&Task{TaskArn: arn})

		for _, id := range ids {
			_, _ = e.probe.ContainerRemove(ctx, id, mobyclient.ContainerRemoveOptions{Force: true, RemoveVolumes: true})
		}
	}

	e.b.Close()
}

func (e *liveEnv) runTask(
	t *testing.T, family string, mode ecstypes.NetworkMode, launch ecstypes.LaunchType,
	defs []ecstypes.ContainerDefinition,
) string {
	t.Helper()

	in := &ecssdk.RegisterTaskDefinitionInput{
		Family: aws.String(family), NetworkMode: mode, ContainerDefinitions: defs,
	}

	run := &ecssdk.RunTaskInput{TaskDefinition: aws.String(family), LaunchType: launch}

	if launch == ecstypes.LaunchTypeFargate {
		in.RequiresCompatibilities = []ecstypes.Compatibility{ecstypes.CompatibilityFargate}
		in.Cpu, in.Memory = aws.String("512"), aws.String("1024")
		run.NetworkConfiguration = &ecstypes.NetworkConfiguration{AwsvpcConfiguration: &ecstypes.AwsVpcConfiguration{
			Subnets: []string{"subnet-live"},
		}}
	}

	_, err := e.sdk.RegisterTaskDefinition(t.Context(), in)
	require.NoError(t, err)

	out, err := e.sdk.RunTask(t.Context(), run)
	require.NoError(t, err)
	require.Empty(t, out.Failures)
	require.Len(t, out.Tasks, 1)

	return aws.ToString(out.Tasks[0].TaskArn)
}

func (e *liveEnv) describe(t *testing.T, arn string) ecstypes.Task {
	t.Helper()

	out, err := e.sdk.DescribeTasks(t.Context(), &ecssdk.DescribeTasksInput{Tasks: []string{arn}})
	require.NoError(t, err)
	require.Len(t, out.Tasks, 1)

	return out.Tasks[0]
}

func (e *liveEnv) awaitRunning(t *testing.T, arn string) ecstypes.Task {
	t.Helper()

	require.Eventually(t, func() bool {
		st := aws.ToString(e.describe(t, arn).LastStatus)

		return st == statusRunning || st == statusStopped
	}, liveTimeout*2, liveTick)

	task := e.describe(t, arn)
	require.Equal(
		t,
		statusRunning,
		aws.ToString(task.LastStatus),
		"stopped reason: %s",
		aws.ToString(task.StoppedReason),
	)

	return task
}

func dockerLogs(ctx context.Context, id string) string {
	out, _ := exec.CommandContext(ctx, "docker", "logs", id).CombinedOutput()

	return string(out)
}

func containerID(t *testing.T, r *realDockerRunner, arn string, index int) string {
	t.Helper()

	ids := r.ContainerIDs(arn)
	require.Greater(t, len(ids), index)

	return ids[index]
}

func TestLiveAwsvpcContainersShareLocalhost(t *testing.T) {
	t.Parallel()

	env := newLiveEnv(t)

	arn := env.runTask(t, "live-awsvpc", ecstypes.NetworkModeAwsvpc, ecstypes.LaunchTypeFargate,
		[]ecstypes.ContainerDefinition{
			{
				Name: aws.String("server"), Image: aws.String(liveImage), Essential: aws.Bool(true),
				Command:      []string{"sh", "-c", "while true; do echo hello-localhost | nc -l -p 8080; done"},
				PortMappings: []ecstypes.PortMapping{{ContainerPort: aws.Int32(8080)}},
			},
			{
				Name: aws.String("client"), Image: aws.String(liveImage), Essential: aws.Bool(false),
				Command: []string{
					"sh", "-c",
					"for i in $(seq 1 40); do nc -w 2 127.0.0.1 8080 && exit 0; sleep 0.5; done; exit 3",
				},
			},
		})

	task := env.awaitRunning(t, arn)

	var ip string

	for _, a := range task.Attachments {
		for _, d := range a.Details {
			if aws.ToString(d.Name) == detailPrivateIPv4 {
				ip = aws.ToString(d.Value)
			}
		}
	}

	require.NotEmpty(t, ip)
	assert.Equal(t, ip, env.runner.TaskAddress(arn), "the task ENI address is the pause container's address")

	client := containerID(t, env.runner, arn, 1)

	require.Eventually(t, func() bool {
		return strings.Contains(dockerLogs(t.Context(), client), "hello-localhost")
	}, liveTimeout, liveTick, "client reached the server over 127.0.0.1")

	require.Eventually(t, func() bool {
		conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), "tcp", net.JoinHostPort(ip, "8080"))
		if err != nil {
			return false
		}

		_ = conn.Close()

		return true
	}, liveTimeout, liveTick, "the task address is reachable from the host")

	pause := env.runner.taskNets[arn].pauseID

	_, err := env.b.StopTask("default", arn, "done")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, inspectErr := env.probe.ContainerInspect(t.Context(), pause, mobyclient.ContainerInspectOptions{})

		return inspectErr != nil
	}, liveTimeout, liveTick, "pause container removed with the task")

	_, err = env.probe.NetworkInspect(
		t.Context(),
		taskNetworkName(&Task{TaskArn: arn}),
		mobyclient.NetworkInspectOptions{},
	)
	require.Error(t, err, "task network removed with the task")
}

func TestLiveBridgeDynamicHostPort(t *testing.T) {
	t.Parallel()

	env := newLiveEnv(t)

	_, err := env.b.RegisterContainerInstance("default", "i-live-bridge")
	require.NoError(t, err)

	arn := env.runTask(t, "live-bridge", ecstypes.NetworkModeBridge, ecstypes.LaunchTypeEc2,
		[]ecstypes.ContainerDefinition{{
			Name: aws.String("server"), Image: aws.String(liveImage), Essential: aws.Bool(true),
			Command:      []string{"sh", "-c", "while true; do echo hello-bridge | nc -l -p 80; done"},
			PortMappings: []ecstypes.PortMapping{{ContainerPort: aws.Int32(80)}},
		}})

	task := env.awaitRunning(t, arn)
	require.Len(t, task.Containers, 1)
	require.Len(t, task.Containers[0].NetworkBindings, 1)

	hostPort := aws.ToInt32(task.Containers[0].NetworkBindings[0].HostPort)
	assert.GreaterOrEqual(t, hostPort, int32(ephemeralPortRangeMin))

	require.Eventually(t, func() bool {
		conn, dialErr := (&net.Dialer{Timeout: time.Second}).DialContext(
			t.Context(), "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(int(hostPort))))
		if dialErr != nil {
			return false
		}

		defer conn.Close()

		buf := make([]byte, 32)
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, _ := conn.Read(buf)

		return strings.Contains(string(buf[:n]), "hello-bridge")
	}, liveTimeout, liveTick, "the allocated host port reaches the container")
}

func TestLiveNetworkModesHostAndNone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mode ecstypes.NetworkMode
		want string
	}{
		{name: "host", mode: ecstypes.NetworkModeHost, want: "host"},
		{name: "none", mode: ecstypes.NetworkModeNone, want: "none"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newLiveEnv(t)
			_, err := env.b.RegisterContainerInstance("default", "i-live-"+tt.name)
			require.NoError(t, err)

			arn := env.runTask(t, "live-"+tt.name, tt.mode, ecstypes.LaunchTypeEc2, []ecstypes.ContainerDefinition{{
				Name: aws.String("app"), Image: aws.String(liveImage), Essential: aws.Bool(true),
				Command: []string{"sleep", "60"},
			}})
			env.awaitRunning(t, arn)

			res, err := env.probe.ContainerInspect(t.Context(), containerID(t, env.runner, arn, 0),
				mobyclient.ContainerInspectOptions{})
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(res.Container.HostConfig.NetworkMode))
		})
	}
}

func TestLiveRestartPolicy(t *testing.T) {
	t.Parallel()

	env := newLiveEnv(t)

	_, err := env.b.RegisterContainerInstance("default", "i-live-restart")
	require.NoError(t, err)

	var tick atomic.Int64

	base := time.Unix(1_700_000_000, 0)
	env.runner.now = func() time.Time { return base.Add(time.Duration(tick.Add(1)) * 2 * time.Minute) }

	policy := &ecstypes.ContainerRestartPolicy{Enabled: aws.Bool(true), RestartAttemptPeriod: aws.Int32(60)}
	ignoring := &ecstypes.ContainerRestartPolicy{
		Enabled: aws.Bool(true), RestartAttemptPeriod: aws.Int32(60), IgnoredExitCodes: []int32{7},
	}

	arn := env.runTask(t, "live-restart", ecstypes.NetworkModeBridge, ecstypes.LaunchTypeEc2,
		[]ecstypes.ContainerDefinition{
			{
				Name: aws.String("keep"), Image: aws.String(liveImage), Essential: aws.Bool(true),
				Command: []string{"sleep", "120"},
			},
			{
				Name: aws.String("flaky"), Image: aws.String(liveImage), Essential: aws.Bool(false),
				Command:       []string{"sh", "-c", "echo flaky-run; exit 1"},
				RestartPolicy: policy,
			},
			{
				Name: aws.String("ignored"), Image: aws.String(liveImage), Essential: aws.Bool(false),
				Command:       []string{"sh", "-c", "echo ignored-run; exit 7"},
				RestartPolicy: ignoring,
			},
		})

	env.awaitRunning(t, arn)

	flaky, ignored := containerID(t, env.runner, arn, 1), containerID(t, env.runner, arn, 2)

	require.Eventually(t, func() bool {
		return strings.Count(dockerLogs(t.Context(), flaky), "flaky-run") >= 3
	}, liveTimeout, liveTick, "a container exiting 1 is restarted")

	assert.Equal(t, 1, strings.Count(dockerLogs(t.Context(), ignored), "ignored-run"),
		"an ignored exit code is not restarted")
}

func TestLiveLogDrivers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		driver  string
		options map[string]string
		wantMax string
	}{
		{
			name:    "json-file",
			driver:  "json-file",
			options: map[string]string{"max-size": "1m", "max-file": "2"},
			wantMax: "1m",
		},
		{name: "awslogs", driver: "awslogs", options: map[string]string{"awslogs-group": "g", "mode": "non-blocking"}},
		{name: "syslog", driver: "syslog", options: map[string]string{"syslog-address": "udp://127.0.0.1:65000"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newLiveEnv(t)
			_, err := env.b.RegisterContainerInstance("default", "i-live-log-"+tt.name)
			require.NoError(t, err)

			arn := env.runTask(t, "live-log-"+tt.name, ecstypes.NetworkModeBridge, ecstypes.LaunchTypeEc2,
				[]ecstypes.ContainerDefinition{{
					Name: aws.String("app"), Image: aws.String(liveImage), Essential: aws.Bool(true),
					Command: []string{"sh", "-c", "echo logged-line; sleep 60"},
					LogConfiguration: &ecstypes.LogConfiguration{
						LogDriver: ecstypes.LogDriver(tt.driver), Options: tt.options,
					},
				}})
			env.awaitRunning(t, arn)

			res, err := env.probe.ContainerInspect(t.Context(), containerID(t, env.runner, arn, 0),
				mobyclient.ContainerInspectOptions{})
			require.NoError(t, err)

			want := tt.driver
			if tt.driver == "awslogs" {
				want = "json-file"
			}

			lc := res.Container.HostConfig.LogConfig
			assert.Equal(t, want, lc.Type)
			assert.Equal(t, "non-blocking", lc.Config["mode"])

			if tt.wantMax != "" {
				assert.Equal(t, tt.wantMax, lc.Config["max-size"])
			}
		})
	}
}

func TestLiveSharedDockerVolume(t *testing.T) {
	t.Parallel()

	volName := "gopherstack-live-shared-" + strconv.FormatInt(time.Now().UnixNano(), 36)

	// Registered before newLiveEnv so it runs after the tasks' containers are gone.
	t.Cleanup(func() { _ = exec.CommandContext(context.Background(), "docker", "volume", "rm", "-f", volName).Run() })

	env := newLiveEnv(t)
	_, err := env.b.RegisterContainerInstance("default", "i-live-vol")
	require.NoError(t, err)

	_, err = env.sdk.RegisterTaskDefinition(t.Context(), &ecssdk.RegisterTaskDefinitionInput{
		Family: aws.String("live-vol"), NetworkMode: ecstypes.NetworkModeBridge,
		Volumes: []ecstypes.Volume{{
			Name: aws.String(volName),
			DockerVolumeConfiguration: &ecstypes.DockerVolumeConfiguration{
				Scope: ecstypes.ScopeShared, Autoprovision: aws.Bool(true), Driver: aws.String("local"),
				Labels: map[string]string{"gopherstack-live": "1"},
			},
		}},
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{
				Name:      aws.String("writer"),
				Image:     aws.String(liveImage),
				Essential: aws.Bool(true),
				Command:   []string{"sh", "-c", "echo shared-data > /data/f; sleep 60"},
				MountPoints: []ecstypes.MountPoint{
					{SourceVolume: aws.String(volName), ContainerPath: aws.String("/data")},
				},
			},
			{
				Name: aws.String("reader"), Image: aws.String(liveImage), Essential: aws.Bool(false),
				Command: []string{
					"sh", "-c", "for i in $(seq 1 40); do cat /data/f 2>/dev/null && exit 0; sleep 0.5; done; exit 1",
				},
				MountPoints: []ecstypes.MountPoint{{
					SourceVolume: aws.String(volName), ContainerPath: aws.String("/data"), ReadOnly: aws.Bool(true),
				}},
			},
		},
	})
	require.NoError(t, err)

	out, err := env.sdk.RunTask(t.Context(), &ecssdk.RunTaskInput{
		TaskDefinition: aws.String("live-vol"), LaunchType: ecstypes.LaunchTypeEc2,
	})
	require.NoError(t, err)
	require.Len(t, out.Tasks, 1)

	arn := aws.ToString(out.Tasks[0].TaskArn)
	env.awaitRunning(t, arn)

	reader := containerID(t, env.runner, arn, 1)

	require.Eventually(t, func() bool {
		return strings.Contains(dockerLogs(t.Context(), reader), "shared-data")
	}, liveTimeout, liveTick, "reader sees the file the writer put on the shared volume")

	vol, err := env.probe.VolumeInspect(t.Context(), volName, mobyclient.VolumeInspectOptions{})
	require.NoError(t, err)
	assert.Equal(t, "1", vol.Volume.Labels["gopherstack-live"])
}

type liveEBS struct {
	created []EBSVolumeSpec
	deleted []string
	mu      sync.Mutex
}

func (l *liveEBS) CreateTaskVolume(spec EBSVolumeSpec) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.created = append(l.created, spec)

	return "vol-live" + strconv.FormatInt(time.Now().UnixNano(), 36), nil
}

func (l *liveEBS) DeleteTaskVolume(_, id string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.deleted = append(l.deleted, id)

	return nil
}

func TestLiveManagedEBSVolume(t *testing.T) {
	t.Parallel()

	env := newLiveEnv(t)
	ebs := &liveEBS{}
	env.b.SetEBSProvisioner(ebs)

	_, err := env.b.RegisterContainerInstance("default", "i-live-ebs")
	require.NoError(t, err)

	_, err = env.sdk.RegisterTaskDefinition(t.Context(), &ecssdk.RegisterTaskDefinitionInput{
		Family: aws.String("live-ebs"), NetworkMode: ecstypes.NetworkModeBridge,
		Volumes: []ecstypes.Volume{{Name: aws.String("data"), ConfiguredAtLaunch: aws.Bool(true)}},
		ContainerDefinitions: []ecstypes.ContainerDefinition{{
			Name: aws.String("app"), Image: aws.String(liveImage), Essential: aws.Bool(true),
			Command:     []string{"sh", "-c", "echo ebs-data > /data/f; sleep 60"},
			MountPoints: []ecstypes.MountPoint{{SourceVolume: aws.String("data"), ContainerPath: aws.String("/data")}},
		}},
	})
	require.NoError(t, err)

	out, err := env.sdk.RunTask(t.Context(), &ecssdk.RunTaskInput{
		TaskDefinition: aws.String("live-ebs"), LaunchType: ecstypes.LaunchTypeEc2,
		VolumeConfigurations: []ecstypes.TaskVolumeConfiguration{{
			Name: aws.String("data"),
			ManagedEBSVolume: &ecstypes.TaskManagedEBSVolumeConfiguration{
				RoleArn: aws.String("arn:aws:iam::123456789012:role/infra"), SizeInGiB: aws.Int32(4),
			},
		}},
	})
	require.NoError(t, err)
	require.Len(t, out.Tasks, 1)

	arn := aws.ToString(out.Tasks[0].TaskArn)
	task := env.awaitRunning(t, arn)

	volumeID := ""

	for _, a := range task.Attachments {
		if aws.ToString(a.Type) == attachmentTypeEBS {
			assert.Equal(t, attachmentStatusAttached, aws.ToString(a.Status))

			for _, d := range a.Details {
				if aws.ToString(d.Name) == detailVolumeID {
					volumeID = aws.ToString(d.Value)
				}
			}
		}
	}

	require.NotEmpty(t, volumeID)

	id := containerID(t, env.runner, arn, 0)

	require.Eventually(t, func() bool {
		o, execErr := exec.CommandContext(t.Context(), "docker", "exec", id, "cat", "/data/f").Output()

		return execErr == nil && strings.Contains(string(o), "ebs-data")
	}, liveTimeout, liveTick, "the EBS volume is mounted in the container")

	dockerVolume := ebsDockerVolumeName(volumeID)
	_, err = env.probe.VolumeInspect(t.Context(), dockerVolume, mobyclient.VolumeInspectOptions{})
	require.NoError(t, err)

	_, err = env.b.StopTask("default", arn, "done")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, inspectErr := env.probe.VolumeInspect(t.Context(), dockerVolume, mobyclient.VolumeInspectOptions{})

		return inspectErr != nil
	}, liveTimeout, liveTick, "the docker volume is removed with the task")

	ebs.mu.Lock()
	defer ebs.mu.Unlock()

	assert.Equal(t, []string{volumeID}, ebs.deleted)
}
