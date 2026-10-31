package ocicontainerinstances_test

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	ci "github.com/oracle/oci-go-sdk/v65/containerinstances"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	svc "github.com/blackbirdworks/gopherstack/services/ocicontainerinstances"
)

const waitFor = 10 * time.Second

type pullCall struct {
	ref  string
	cred *svc.Credential
}

// fakeRuntime is a scripted container runtime: containers run until the test (or Kill) ends them.
type fakeRuntime struct {
	pullErr error
	exits   map[string]chan int
	logs    map[string]string
	mu      sync.Mutex
	pulls   []pullCall
	specs   []container.Spec
	removed []string
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{exits: map[string]chan int{}, logs: map[string]string{}}
}

func (f *fakeRuntime) Pull(_ context.Context, ref string, cred *svc.Credential) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.pulls = append(f.pulls, pullCall{ref, cred})

	return f.pullErr
}

func (f *fakeRuntime) Start(_ context.Context, spec container.Spec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	id := "c" + strconv.Itoa(len(f.specs)+1)
	f.specs = append(f.specs, spec)
	f.exits[id] = make(chan int, 1)
	f.logs[id] = "output of " + id

	return id, nil
}

func (f *fakeRuntime) Wait(ctx context.Context, id string) (int, error) {
	f.mu.Lock()
	ch := f.exits[id]
	f.mu.Unlock()

	select {
	case code := <-ch:
		return code, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (f *fakeRuntime) Logs(_ context.Context, id string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.logs[id], nil
}

func (f *fakeRuntime) Kill(_ context.Context, id string) error {
	f.exit(id, 137)

	return nil
}

func (f *fakeRuntime) Remove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.removed = append(f.removed, id)

	return nil
}

func (f *fakeRuntime) exit(id string, code int) {
	f.mu.Lock()
	ch := f.exits[id]
	f.mu.Unlock()

	select {
	case ch <- code:
	default:
	}
}

func (f *fakeRuntime) started(t *testing.T, n int) []container.Spec {
	t.Helper()

	require.Eventually(t, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()

		return len(f.specs) >= n
	}, waitFor, time.Millisecond)

	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]container.Spec(nil), f.specs...)
}

func (f *fakeRuntime) startCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.specs)
}

type dockerEnv struct {
	*env
	rt     *fakeRuntime
	engine *svc.Engine
	probes chan error
}

// instantTimer fires short timers at once and never fires the long startup bound.
func instantTimer(d time.Duration) (<-chan time.Time, func()) {
	ch := make(chan time.Time, 1)
	if d < time.Minute {
		ch <- time.Time{}
	}

	return ch, func() {}
}

func newDockerEnv(t *testing.T, configure func(*svc.InMemoryBackend)) *dockerEnv {
	t.Helper()

	rt := newFakeRuntime()
	probes := make(chan error)
	eng := svc.NewEngine(func() (svc.Runtime, error) { return rt, nil })
	eng.SetTiming(instantTimer, func(int) time.Duration { return 0 },
		func(ctx context.Context, _ svc.HealthCheck, _ int) error {
			select {
			case err := <-probes:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		})

	e := newEnv(t, envOpts{configure: func(b *svc.InMemoryBackend) {
		b.SetEngine(eng)

		if configure != nil {
			configure(b)
		}
	}})
	t.Cleanup(e.backend.Close)

	return &dockerEnv{env: e, rt: rt, engine: eng, probes: probes}
}

func (d *dockerEnv) await(t *testing.T, id *string, state ci.ContainerInstanceLifecycleStateEnum) ci.ContainerInstance {
	t.Helper()

	var got ci.ContainerInstance

	require.Eventually(t, func() bool {
		got = d.get(t, id)

		return got.LifecycleState == state
	}, waitFor, time.Millisecond, "want %s, last %s", state, got.LifecycleState)

	return got
}

func (d *dockerEnv) container(t *testing.T, instID *string) ci.Container {
	t.Helper()

	list, err := d.client.ListContainers(t.Context(), ci.ListContainersRequest{
		CompartmentId: common.String(testCompartment), ContainerInstanceId: instID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, list.Items)

	got, err := d.client.GetContainer(t.Context(), ci.GetContainerRequest{ContainerId: list.Items[0].Id})
	require.NoError(t, err)

	return got.Container
}

func TestEngineEngineStartsContainers(t *testing.T) {
	t.Parallel()

	d := newDockerEnv(t, nil)
	resp := d.mustCreate(t, createOpts{mutate: func(c *ci.CreateContainerInstanceDetails) {
		c.Containers[0].ImageUrl = common.String("localhost:8166/ns/app:1")
		c.Containers[0].Command = []string{"/bin/app"}
		c.Containers[0].Arguments = []string{"--flag"}
		c.Containers[0].EnvironmentVariables = map[string]string{"GREETING": "hi"}
		c.Containers[0].WorkingDirectory = common.String("/srv")
		c.Containers[0].ResourceConfig = &ci.CreateContainerResourceConfigDetails{VcpusLimit: f32(1.5), MemoryLimitInGBs: f32(2)}
		c.Containers[0].VolumeMounts = []ci.CreateVolumeMountDetails{
			{MountPath: common.String("/etc/app"), VolumeName: common.String("cfg"), IsReadOnly: common.Bool(true)},
			{MountPath: common.String("/scratch"), VolumeName: common.String("tmp")},
		}
		c.Volumes = []ci.CreateContainerVolumeDetails{
			ci.CreateContainerConfigFileVolumeDetails{Name: common.String("cfg"), Configs: []ci.ContainerConfigFile{
				{FileName: common.String("app.conf"), Data: []byte("mode=test")},
				{FileName: common.String("nested.conf"), Path: common.String("sub/dir"), Data: []byte("deep")},
			}},
			ci.CreateContainerEmptyDirVolumeDetails{Name: common.String("tmp")},
		}
		c.ImagePullSecrets = []ci.CreateImagePullSecretDetails{ci.CreateBasicImagePullSecretDetails{
			RegistryEndpoint: common.String("localhost:8166"), Username: common.String("ns/user"), Password: common.String("tok"),
		}}
		c.DnsConfig = &ci.CreateContainerDnsConfigDetails{Nameservers: []string{"10.0.0.2"}, Searches: []string{"corp.example"}}
	}})

	got := d.await(t, resp.Id, ci.ContainerInstanceLifecycleStateActive)
	assert.Equal(t, ci.OperationStatusSucceeded, d.wr(t, resp.OpcWorkRequestId).Status)
	assert.Empty(t, got.LifecycleDetails)

	specs := d.rt.started(t, 1)
	sp := specs[0]
	assert.Equal(t, "localhost:8166/ns/app:1", sp.Image)
	assert.Equal(t, []string{"/bin/app"}, sp.Entrypoint)
	assert.Equal(t, []string{"--flag"}, sp.Cmd)
	assert.Equal(t, []string{"GREETING=hi"}, sp.Env)
	assert.Equal(t, "/srv", sp.WorkingDir)
	assert.Equal(t, int64(1_500_000_000), sp.NanoCPUs)
	assert.Equal(t, int64(2<<30), sp.MemoryBytes)

	d.rt.mu.Lock()
	pull := d.rt.pulls[0]
	d.rt.mu.Unlock()

	require.NotNil(t, pull.cred)
	assert.Equal(t, "ns/user", pull.cred.Username)
	assert.Equal(t, "tok", pull.cred.Password)

	var cfgDir, resolv string

	for _, m := range sp.Mounts {
		switch {
		case strings.HasSuffix(m, ":/etc/app:ro"):
			cfgDir = strings.TrimSuffix(m, ":/etc/app:ro")
		case strings.HasSuffix(m, ":/etc/resolv.conf:ro"):
			resolv = strings.TrimSuffix(m, ":/etc/resolv.conf:ro")
		}
	}

	require.NotEmpty(t, cfgDir)

	conf, err := os.ReadFile(cfgDir + "/app.conf")
	require.NoError(t, err)
	assert.Equal(t, "mode=test", string(conf))

	deep, err := os.ReadFile(cfgDir + "/sub/dir/nested.conf")
	require.NoError(t, err)
	assert.Equal(t, "deep", string(deep))

	dns, err := os.ReadFile(resolv)
	require.NoError(t, err)
	assert.Equal(t, "nameserver 10.0.0.2\nsearch corp.example\n", string(dns))

	assert.Len(t, sp.Mounts, 3)

	ctr := d.container(t, resp.Id)
	assert.Equal(t, ci.ContainerLifecycleStateActive, ctr.LifecycleState)

	logs, err := d.client.RetrieveLogs(t.Context(), ci.RetrieveLogsRequest{ContainerId: ctr.Id})
	require.NoError(t, err)

	body := make([]byte, 64)
	n, _ := logs.Content.Read(body)
	assert.Equal(t, "output of c1", string(body[:n]))

	_, err = d.client.RetrieveLogs(t.Context(), ci.RetrieveLogsRequest{ContainerId: ctr.Id, IsPrevious: common.Bool(true)})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = d.client.DeleteContainerInstance(t.Context(), ci.DeleteContainerInstanceRequest{ContainerInstanceId: resp.Id})
	require.NoError(t, err)
	d.await(t, resp.Id, ci.ContainerInstanceLifecycleStateDeleted)

	d.rt.mu.Lock()
	assert.Equal(t, []string{"c1"}, d.rt.removed)
	d.rt.mu.Unlock()

	_, statErr := os.Stat(cfgDir)
	assert.True(t, os.IsNotExist(statErr), "volume directories are removed with the instance")
}

func TestEngineRestartPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		policy       ci.ContainerInstanceContainerRestartPolicyEnum
		exit         int
		wantRestart  bool
		wantInstance ci.ContainerInstanceLifecycleStateEnum
	}{
		{"never-success", ci.ContainerInstanceContainerRestartPolicyNever, 0, false, ci.ContainerInstanceLifecycleStateInactive},
		{"never-failure", ci.ContainerInstanceContainerRestartPolicyNever, 3, false, ci.ContainerInstanceLifecycleStateInactive},
		{"on-failure-success", ci.ContainerInstanceContainerRestartPolicyOnFailure, 0, false, ci.ContainerInstanceLifecycleStateInactive},
		{"on-failure-failure", ci.ContainerInstanceContainerRestartPolicyOnFailure, 3, true, ci.ContainerInstanceLifecycleStateActive},
		{"always-success", ci.ContainerInstanceContainerRestartPolicyAlways, 0, true, ci.ContainerInstanceLifecycleStateActive},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d := newDockerEnv(t, nil)
			resp := d.mustCreate(t, createOpts{mutate: func(c *ci.CreateContainerInstanceDetails) {
				c.ContainerRestartPolicy = tc.policy
			}})
			d.await(t, resp.Id, ci.ContainerInstanceLifecycleStateActive)
			d.rt.started(t, 1)
			d.rt.exit("c1", tc.exit)

			if tc.wantRestart {
				d.rt.started(t, 2)

				require.Eventually(t, func() bool {
					return d.container(t, resp.Id).ContainerRestartAttemptCount != nil &&
						*d.container(t, resp.Id).ContainerRestartAttemptCount == 1
				}, waitFor, time.Millisecond)

				assert.Equal(t, tc.wantInstance, d.get(t, resp.Id).LifecycleState)

				prev, err := d.client.RetrieveLogs(t.Context(), ci.RetrieveLogsRequest{
					ContainerId: d.container(t, resp.Id).Id, IsPrevious: common.Bool(true),
				})
				require.NoError(t, err)

				b := make([]byte, 64)
				n, _ := prev.Content.Read(b)
				assert.Equal(t, "output of c1", string(b[:n]))

				return
			}

			d.await(t, resp.Id, tc.wantInstance)

			ctr := d.container(t, resp.Id)
			assert.Equal(t, ci.ContainerLifecycleStateInactive, ctr.LifecycleState)
			require.NotNil(t, ctr.ExitCode)
			assert.Equal(t, tc.exit, *ctr.ExitCode)
			assert.NotNil(t, ctr.TimeTerminated)
			assert.Equal(t, 1, d.rt.startCount())

			logs, err := d.client.RetrieveLogs(t.Context(), ci.RetrieveLogsRequest{ContainerId: ctr.Id})
			require.NoError(t, err, "logs of an exited container stay readable")

			b := make([]byte, 64)
			n, _ := logs.Content.Read(b)
			assert.Equal(t, "output of c1", string(b[:n]))
		})
	}
}

func TestEngineStartFailure(t *testing.T) {
	t.Parallel()

	d := newDockerEnv(t, nil)
	d.rt.pullErr = errors.New("manifest unknown")

	resp := d.mustCreate(t, createOpts{})
	got := d.await(t, resp.Id, ci.ContainerInstanceLifecycleStateFailed)
	assert.Contains(t, *got.LifecycleDetails, "manifest unknown")

	wr := d.wr(t, resp.OpcWorkRequestId)
	assert.Equal(t, ci.OperationStatusFailed, wr.Status)

	errs, err := d.client.ListWorkRequestErrors(t.Context(), ci.ListWorkRequestErrorsRequest{WorkRequestId: resp.OpcWorkRequestId})
	require.NoError(t, err)
	require.Len(t, errs.Items, 1)
	assert.Contains(t, *errs.Items[0].Message, "manifest unknown")

	assert.Equal(t, ci.ContainerLifecycleStateFailed, d.container(t, resp.Id).LifecycleState)

	_, err = d.client.DeleteContainerInstance(t.Context(), ci.DeleteContainerInstanceRequest{ContainerInstanceId: resp.Id})
	require.NoError(t, err)
	d.await(t, resp.Id, ci.ContainerInstanceLifecycleStateDeleted)
}

func TestEngineStopStartRestart(t *testing.T) {
	t.Parallel()

	d := newDockerEnv(t, nil)
	resp := d.mustCreate(t, createOpts{})
	d.await(t, resp.Id, ci.ContainerInstanceLifecycleStateActive)
	d.rt.started(t, 1)

	_, err := d.client.StopContainerInstance(t.Context(), ci.StopContainerInstanceRequest{ContainerInstanceId: resp.Id})
	require.NoError(t, err)
	d.await(t, resp.Id, ci.ContainerInstanceLifecycleStateInactive)
	assert.Equal(t, ci.ContainerLifecycleStateInactive, d.container(t, resp.Id).LifecycleState)

	logs, err := d.client.RetrieveLogs(t.Context(), ci.RetrieveLogsRequest{ContainerId: d.container(t, resp.Id).Id})
	require.NoError(t, err, "last logs stay readable after a stop")
	_ = logs.Content.Close()

	_, err = d.client.StartContainerInstance(t.Context(), ci.StartContainerInstanceRequest{ContainerInstanceId: resp.Id})
	require.NoError(t, err)
	d.await(t, resp.Id, ci.ContainerInstanceLifecycleStateActive)
	d.rt.started(t, 2)

	_, err = d.client.RestartContainerInstance(t.Context(), ci.RestartContainerInstanceRequest{ContainerInstanceId: resp.Id})
	require.NoError(t, err)
	d.rt.started(t, 3)
	d.await(t, resp.Id, ci.ContainerInstanceLifecycleStateActive)

	d.rt.mu.Lock()
	assert.Equal(t, []string{"c1", "c2"}, d.rt.removed)
	d.rt.mu.Unlock()
}

func TestEngineHealthChecks(t *testing.T) {
	t.Parallel()

	d := newDockerEnv(t, nil)
	resp := d.mustCreate(t, createOpts{mutate: func(c *ci.CreateContainerInstanceDetails) {
		c.Containers[0].HealthChecks = []ci.CreateContainerHealthCheckDetails{
			ci.CreateContainerHttpHealthCheckDetails{
				Port: common.Int(8080), Path: common.String("/healthz"), FailureThreshold: common.Int(2), SuccessThreshold: common.Int(1),
				Name: common.String("web"),
			},
		}
	}})
	d.await(t, resp.Id, ci.ContainerInstanceLifecycleStateActive)

	specs := d.rt.started(t, 1)
	require.Len(t, specs[0].Ports, 1)
	assert.True(t, strings.HasPrefix(specs[0].Ports[0], "127.0.0.1:"), "health check ports publish on loopback only")
	assert.True(t, strings.HasSuffix(specs[0].Ports[0], ":8080"))

	status := func() ci.ContainerHealthCheckStatusEnum {
		hc := d.container(t, resp.Id).HealthChecks
		require.Len(t, hc, 1)

		return hc[0].GetStatus()
	}

	assert.Equal(t, ci.ContainerHealthCheckStatusUnknown, status())

	d.probes <- nil

	require.Eventually(t, func() bool { return status() == ci.ContainerHealthCheckStatusHealthy }, waitFor, time.Millisecond)

	d.probes <- errors.New("boom")

	d.probes <- errors.New("boom")

	d.rt.started(t, 2)

	require.Eventually(t, func() bool {
		c := d.container(t, resp.Id)

		return c.ContainerRestartAttemptCount != nil && *c.ContainerRestartAttemptCount == 1
	}, waitFor, time.Millisecond, "an unhealthy KILL check kills the container and the restart policy restarts it")

	d.rt.mu.Lock()
	assert.Contains(t, d.rt.removed, "c1")
	d.rt.mu.Unlock()
}

func TestEngineHealthCheckActionNone(t *testing.T) {
	t.Parallel()

	d := newDockerEnv(t, nil)
	resp := d.mustCreate(t, createOpts{mutate: func(c *ci.CreateContainerInstanceDetails) {
		c.Containers[0].HealthChecks = []ci.CreateContainerHealthCheckDetails{
			ci.CreateContainerTcpHealthCheckDetails{
				Port: common.Int(9000), FailureThreshold: common.Int(1), FailureAction: ci.ContainerHealthCheckFailureActionNone,
			},
		}
	}})
	d.await(t, resp.Id, ci.ContainerInstanceLifecycleStateActive)
	d.rt.started(t, 1)

	d.probes <- errors.New("refused")

	require.Eventually(t, func() bool {
		hc := d.container(t, resp.Id).HealthChecks

		return len(hc) == 1 && hc[0].GetStatus() == ci.ContainerHealthCheckStatusUnhealthy
	}, waitFor, time.Millisecond)

	d.probes <- nil

	require.Eventually(t, func() bool {
		return d.container(t, resp.Id).HealthChecks[0].GetStatus() == ci.ContainerHealthCheckStatusHealthy
	}, waitFor, time.Millisecond)

	assert.Equal(t, 1, d.rt.startCount(), "failureAction NONE never restarts the container")
}

func TestEngineVaultPullSecret(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		read     svc.SecretReader
		wantHTTP int
	}{
		{"valid", func(string) (string, error) { return `{"username":"u","password":"p"}`, nil }, 0},
		{"unreadable-secret", func(string) (string, error) { return "", errors.New("gone") }, http.StatusNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d := newDockerEnv(t, func(b *svc.InMemoryBackend) { b.SetSecretReader(tc.read) })

			resp, err := d.create(t, createOpts{mutate: func(c *ci.CreateContainerInstanceDetails) {
				c.ImagePullSecrets = []ci.CreateImagePullSecretDetails{ci.CreateVaultImagePullSecretDetails{
					RegistryEndpoint: common.String("docker.io"), SecretId: common.String("ocid1.vaultsecret.oc1..s"),
				}}
			}})
			if tc.wantHTTP != 0 {
				assert.Equal(t, tc.wantHTTP, svcErr(t, err).GetHTTPStatusCode())

				return
			}

			require.NoError(t, err)
			d.await(t, resp.Id, ci.ContainerInstanceLifecycleStateActive)

			d.rt.mu.Lock()
			defer d.rt.mu.Unlock()

			require.NotNil(t, d.rt.pulls[0].cred)
			assert.Equal(t, "u", d.rt.pulls[0].cred.Username)
			assert.Equal(t, "p", d.rt.pulls[0].cred.Password)
		})
	}
}

func TestEngineRestoreLeavesInstancesInactive(t *testing.T) {
	t.Parallel()

	src := newDockerEnv(t, nil)
	resp := src.mustCreate(t, createOpts{})
	src.await(t, resp.Id, ci.ContainerInstanceLifecycleStateActive)

	snap := src.backend.Snapshot(t.Context())

	dst := newDockerEnv(t, nil)
	require.NoError(t, dst.backend.Restore(t.Context(), snap))

	got := dst.get(t, resp.Id)
	assert.Equal(t, ci.ContainerInstanceLifecycleStateInactive, got.LifecycleState)
	assert.Contains(t, *got.LifecycleDetails, "Restored from a snapshot")
	assert.Equal(t, ci.ContainerLifecycleStateInactive, dst.container(t, resp.Id).LifecycleState)

	_, err := dst.client.StartContainerInstance(t.Context(), ci.StartContainerInstanceRequest{ContainerInstanceId: resp.Id})
	require.NoError(t, err)
	dst.await(t, resp.Id, ci.ContainerInstanceLifecycleStateActive)
	dst.rt.started(t, 1)
}
