package ocicontainerinstances_test

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	ci "github.com/oracle/oci-go-sdk/v65/containerinstances"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/services/ociartifacts"
	svc "github.com/blackbirdworks/gopherstack/services/ocicontainerinstances"
)

const (
	dockerWait  = 2 * time.Minute
	testImage   = "alpine:latest"
	registryNS  = "gopherstack"
	registryTok = "r3gistry-t0ken"
)

// newRealEnv wires the service to the local container runtime; it skips when none is reachable.
func newRealEnv(t *testing.T) *env {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	lr := gcpserve.NewLazyRuntime(nil, "ocicontainerinstances.test.runtime")
	t.Cleanup(func() { _ = lr.Close() })

	if _, err := lr.Get(); err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	if out, err := exec.CommandContext(t.Context(), "docker", "image", "inspect", testImage).CombinedOutput(); err != nil {
		t.Skipf("%s is not present locally: %s", testImage, out)
	}

	eng := svc.NewEngine(func() (svc.Runtime, error) {
		r, err := lr.Get()
		if err != nil {
			return nil, err
		}

		return svc.NewDockerRuntime(r), nil
	})
	eng.SetTiming(nil, func(int) time.Duration { return 200 * time.Millisecond }, nil)

	e := newEnv(t, envOpts{configure: func(b *svc.InMemoryBackend) { b.SetEngine(eng) }})
	t.Cleanup(e.backend.Close)

	return e
}

func (e *env) awaitState(t *testing.T, id *string, want ci.ContainerInstanceLifecycleStateEnum) ci.ContainerInstance {
	t.Helper()

	var got ci.ContainerInstance

	require.Eventually(t, func() bool {
		got = e.get(t, id)

		return got.LifecycleState == want
	}, dockerWait, 100*time.Millisecond, "want %s, last %s (%v)", want, got.LifecycleState, got.LifecycleDetails)

	return got
}

func (e *env) firstContainer(t *testing.T, instID *string) ci.Container {
	t.Helper()

	list, err := e.client.ListContainers(t.Context(), ci.ListContainersRequest{
		CompartmentId: common.String(testCompartment), ContainerInstanceId: instID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, list.Items)

	got, err := e.client.GetContainer(t.Context(), ci.GetContainerRequest{ContainerId: list.Items[0].Id})
	require.NoError(t, err)

	return got.Container
}

func (e *env) logsOf(t *testing.T, ctrID *string) string {
	t.Helper()

	resp, err := e.client.RetrieveLogs(t.Context(), ci.RetrieveLogsRequest{ContainerId: ctrID})
	require.NoError(t, err)

	defer resp.Content.Close()

	b, err := io.ReadAll(resp.Content)
	require.NoError(t, err)

	return string(b)
}

func shell(script string) func(*ci.CreateContainerInstanceDetails) {
	return func(d *ci.CreateContainerInstanceDetails) {
		d.Containers[0].ImageUrl = common.String(testImage)
		d.Containers[0].Command = []string{"sh", "-c", script}
	}
}

func TestDockerRunToCompletion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		script   string
		policy   ci.ContainerInstanceContainerRestartPolicyEnum
		wantExit int
		wantLogs []string
	}{
		{
			name: "never-success", script: "echo hello-$GREETING; echo oops >&2", wantExit: 0,
			policy: ci.ContainerInstanceContainerRestartPolicyNever, wantLogs: []string{"hello-world", "oops"},
		},
		{
			name: "never-failure", script: "echo failing; exit 3", wantExit: 3,
			policy: ci.ContainerInstanceContainerRestartPolicyNever, wantLogs: []string{"failing"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newRealEnv(t)
			resp := e.mustCreate(t, createOpts{mutate: func(d *ci.CreateContainerInstanceDetails) {
				shell(tc.script)(d)
				d.ContainerRestartPolicy = tc.policy
				d.Containers[0].EnvironmentVariables = map[string]string{"GREETING": "world"}
			}})

			e.awaitState(t, resp.Id, ci.ContainerInstanceLifecycleStateInactive)

			ctr := e.firstContainer(t, resp.Id)
			assert.Equal(t, ci.ContainerLifecycleStateInactive, ctr.LifecycleState)
			require.NotNil(t, ctr.ExitCode)
			assert.Equal(t, tc.wantExit, *ctr.ExitCode)
			assert.Zero(t, *ctr.ContainerRestartAttemptCount)

			logs := e.logsOf(t, ctr.Id)
			for _, want := range tc.wantLogs {
				assert.Contains(t, logs, want)
			}
		})
	}
}

func TestDockerOnFailureRestarts(t *testing.T) {
	t.Parallel()

	e := newRealEnv(t)
	resp := e.mustCreate(t, createOpts{mutate: func(d *ci.CreateContainerInstanceDetails) {
		shell("echo attempt; exit 1")(d)
		d.ContainerRestartPolicy = ci.ContainerInstanceContainerRestartPolicyOnFailure
	}})

	require.Eventually(t, func() bool {
		n := e.firstContainer(t, resp.Id).ContainerRestartAttemptCount

		return n != nil && *n >= 2
	}, dockerWait, 100*time.Millisecond)

	assert.Equal(t, ci.ContainerInstanceLifecycleStateActive, e.get(t, resp.Id).LifecycleState)

	_, err := e.client.DeleteContainerInstance(t.Context(), ci.DeleteContainerInstanceRequest{ContainerInstanceId: resp.Id})
	require.NoError(t, err)
	e.awaitState(t, resp.Id, ci.ContainerInstanceLifecycleStateDeleted)
}

func TestDockerVolumesAndLimits(t *testing.T) {
	t.Parallel()

	e := newRealEnv(t)
	resp := e.mustCreate(t, createOpts{mutate: func(d *ci.CreateContainerInstanceDetails) {
		shell("cat /etc/app/app.conf; cat /etc/app/sub/n.conf; echo scratch > /scratch/f && cat /scratch/f; " +
			"(cat /sys/fs/cgroup/memory.max || cat /sys/fs/cgroup/memory/memory.limit_in_bytes) 2>/dev/null; " +
			"cat /etc/resolv.conf; echo done")(d)
		d.ContainerRestartPolicy = ci.ContainerInstanceContainerRestartPolicyNever
		d.Containers[0].ResourceConfig = &ci.CreateContainerResourceConfigDetails{VcpusLimit: f32(0.5), MemoryLimitInGBs: f32(0.25)}
		d.Containers[0].VolumeMounts = []ci.CreateVolumeMountDetails{
			{MountPath: common.String("/etc/app"), VolumeName: common.String("cfg"), IsReadOnly: common.Bool(true)},
			{MountPath: common.String("/scratch"), VolumeName: common.String("tmp")},
		}
		d.Volumes = []ci.CreateContainerVolumeDetails{
			ci.CreateContainerConfigFileVolumeDetails{Name: common.String("cfg"), Configs: []ci.ContainerConfigFile{
				{FileName: common.String("app.conf"), Data: []byte("mode=docker-test\n")},
				{FileName: common.String("n.conf"), Path: common.String("sub"), Data: []byte("nested-ok\n")},
			}},
			ci.CreateContainerEmptyDirVolumeDetails{Name: common.String("tmp")},
		}
		d.DnsConfig = &ci.CreateContainerDnsConfigDetails{Nameservers: []string{"10.20.30.40"}, Searches: []string{"ci.example"}}
	}})

	e.awaitState(t, resp.Id, ci.ContainerInstanceLifecycleStateInactive)

	ctr := e.firstContainer(t, resp.Id)
	require.NotNil(t, ctr.ExitCode)
	assert.Zero(t, *ctr.ExitCode)

	logs := e.logsOf(t, ctr.Id)
	for _, want := range []string{
		"mode=docker-test", "nested-ok", "scratch", "268435456", "nameserver 10.20.30.40", "search ci.example", "done",
	} {
		assert.Contains(t, logs, want)
	}
}

const httpServer = `while true; do printf "HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\nok\n" | nc -l -p 8080; done`

func TestDockerHealthChecks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		check  ci.CreateContainerHealthCheckDetails
		want   ci.ContainerHealthCheckStatusEnum
	}{
		{
			name: "http-healthy", script: httpServer, want: ci.ContainerHealthCheckStatusHealthy,
			check: ci.CreateContainerHttpHealthCheckDetails{
				Port: common.Int(8080), Path: common.String("/"), IntervalInSeconds: common.Int(1),
				TimeoutInSeconds: common.Int(2),
			},
		},
		{
			name: "tcp-healthy", script: httpServer, want: ci.ContainerHealthCheckStatusHealthy,
			check: ci.CreateContainerTcpHealthCheckDetails{
				Port: common.Int(8080), IntervalInSeconds: common.Int(1), TimeoutInSeconds: common.Int(2),
			},
		},
		{
			name: "http-unhealthy", script: "sleep 600", want: ci.ContainerHealthCheckStatusUnhealthy,
			check: ci.CreateContainerHttpHealthCheckDetails{
				Port: common.Int(8080), Path: common.String("/"), IntervalInSeconds: common.Int(1), TimeoutInSeconds: common.Int(1),
				FailureThreshold: common.Int(1), FailureAction: ci.ContainerHealthCheckFailureActionNone,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newRealEnv(t)
			resp := e.mustCreate(t, createOpts{mutate: func(d *ci.CreateContainerInstanceDetails) {
				shell(tc.script)(d)
				d.Containers[0].HealthChecks = []ci.CreateContainerHealthCheckDetails{tc.check}
			}})
			e.awaitState(t, resp.Id, ci.ContainerInstanceLifecycleStateActive)

			require.Eventually(t, func() bool {
				hc := e.firstContainer(t, resp.Id).HealthChecks

				return len(hc) == 1 && hc[0].GetStatus() == tc.want
			}, dockerWait, 200*time.Millisecond)
		})
	}
}

func TestDockerUnhealthyContainerIsKilledAndRestarted(t *testing.T) {
	t.Parallel()

	e := newRealEnv(t)
	resp := e.mustCreate(t, createOpts{mutate: func(d *ci.CreateContainerInstanceDetails) {
		shell("sleep 600")(d)
		d.ContainerRestartPolicy = ci.ContainerInstanceContainerRestartPolicyAlways
		d.Containers[0].HealthChecks = []ci.CreateContainerHealthCheckDetails{
			ci.CreateContainerTcpHealthCheckDetails{
				Port: common.Int(9999), IntervalInSeconds: common.Int(1), TimeoutInSeconds: common.Int(1),
				FailureThreshold: common.Int(1),
			},
		}
	}})

	require.Eventually(t, func() bool {
		n := e.firstContainer(t, resp.Id).ContainerRestartAttemptCount

		return n != nil && *n >= 1
	}, dockerWait, 200*time.Millisecond)
}

// TestDockerPullsFromEmulatorRegistry pushes an image to the emulator's OCI registry and has a container instance
// pull it with a BASIC image pull secret; a wrong token must fail the instance.
func TestDockerPullsFromEmulatorRegistry(t *testing.T) {
	t.Parallel()

	e := newRealEnv(t)

	art := ociartifacts.NewInMemoryBackend()
	art.SetNamespace(registryNS)

	ah := ociartifacts.NewHandler(art)
	ah.Port = freePort(t)

	require.NoError(t, ah.StartWorker(t.Context()))
	t.Cleanup(func() { ah.Shutdown(context.Background()) })

	endpoint := fmt.Sprintf("localhost:%d", ah.Port)
	ref := fmt.Sprintf("%s/%s/ci-pull:1", endpoint, registryNS)
	user := registryNS + "/tester@example.com"
	cfg := t.TempDir()

	docker := func(stdin string, args ...string) {
		t.Helper()

		cmd := exec.CommandContext(t.Context(), "docker", append([]string{"--config", cfg}, args...)...)
		cmd.Stdin = strings.NewReader(stdin)

		out, err := cmd.CombinedOutput()
		require.NoError(t, err, string(out))
	}

	docker(registryTok, "login", endpoint, "-u", user, "--password-stdin")
	docker("", "tag", testImage, ref)
	docker("", "push", ref)
	t.Cleanup(func() { _ = exec.CommandContext(context.Background(), "docker", "rmi", ref).Run() })
	docker("", "rmi", ref)

	pullWith := func(login string) ci.CreateContainerInstanceResponse {
		return e.mustCreate(t, createOpts{mutate: func(d *ci.CreateContainerInstanceDetails) {
			d.Containers[0].ImageUrl = common.String(ref)
			d.Containers[0].Command = []string{"sh", "-c", "echo pulled-from-registry"}
			d.ContainerRestartPolicy = ci.ContainerInstanceContainerRestartPolicyNever
			d.ImagePullSecrets = []ci.CreateImagePullSecretDetails{ci.CreateBasicImagePullSecretDetails{
				RegistryEndpoint: common.String(endpoint), Username: common.String(login), Password: common.String(registryTok),
			}}
		}})
	}

	good := pullWith(user)
	e.awaitState(t, good.Id, ci.ContainerInstanceLifecycleStateInactive)
	assert.Contains(t, e.logsOf(t, e.firstContainer(t, good.Id).Id), "pulled-from-registry")

	bad := pullWith("other-tenancy/mallory")
	failed := e.awaitState(t, bad.Id, ci.ContainerInstanceLifecycleStateFailed)
	assert.Contains(t, *failed.LifecycleDetails, "pull image")
	assert.NotContains(t, *failed.LifecycleDetails, registryTok)
	assert.Equal(t, ci.OperationStatusFailed, e.wr(t, bad.OpcWorkRequestId).Status)
}
