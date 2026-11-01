package azurearm_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerservice/armcontainerservice/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const (
	aksTestToken = "unit-test-aks-token-0123456"
	aksTestCA    = "-----BEGIN CERTIFICATE-----\nZmFrZQ==\n-----END CERTIFICATE-----\n"
)

var errAKSRuntimeDown = errors.New("runtime down")

type aksExecCall struct {
	id  string
	cmd []string
	env []string
}

type aksFakeRuntime struct {
	failErr error
	// execFn answers non-CA reads; nil answers exit 0 with no output.
	execFn  func(ctx context.Context, call aksExecCall) (container.ExecResult, error)
	gate    chan struct{}
	entered chan struct{}
	specs   []container.Spec
	removed []string
	stopped []string
	started []string
	execs   []aksExecCall
	volumes []string
	volGone []string
	mu      sync.Mutex
}

func (f *aksFakeRuntime) CreateVolume(_ context.Context, name string, _ map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.volumes = append(f.volumes, name)

	return nil
}

func (f *aksFakeRuntime) RemoveVolume(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.volGone = append(f.volGone, name)

	return nil
}

func (f *aksFakeRuntime) ListUnusedVolumes(context.Context, string, time.Duration) ([]string, error) {
	return nil, nil
}

func (f *aksFakeRuntime) Exec(ctx context.Context, id string, cmd []string) (container.ExecResult, error) {
	return f.ExecEnv(ctx, id, cmd, nil)
}

func (f *aksFakeRuntime) ExecEnv(ctx context.Context, id string, cmd, env []string) (container.ExecResult, error) {
	if len(cmd) > 0 && cmd[0] == "cat" {
		return container.ExecResult{Stdout: aksTestCA}, nil
	}

	call := aksExecCall{id: id, cmd: cmd, env: env}

	f.mu.Lock()
	f.execs = append(f.execs, call)
	fn := f.execFn
	f.mu.Unlock()

	if fn == nil {
		return container.ExecResult{}, nil
	}

	return fn(ctx, call)
}

func (f *aksFakeRuntime) execCalls() []aksExecCall {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]aksExecCall(nil), f.execs...)
}

func (f *aksFakeRuntime) volumeNames() ([]string, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.volumes...), append([]string(nil), f.volGone...)
}

// blockStart makes CreateAndStart wait until its context is cancelled; entered is closed once it is waiting.
func (f *aksFakeRuntime) blockStart() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.entered = make(chan struct{})
	f.gate = make(chan struct{})

	return f.entered
}

func (f *aksFakeRuntime) StopContainer(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.stopped = append(f.stopped, id)

	return nil
}

func (f *aksFakeRuntime) StartContainer(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.started = append(f.started, id)

	return nil
}

func (f *aksFakeRuntime) stoppedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.stopped...)
}

func (f *aksFakeRuntime) startedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.started...)
}

func (f *aksFakeRuntime) CreateAndStart(ctx context.Context, spec container.Spec) (string, error) {
	f.mu.Lock()

	if gate := f.gate; gate != nil {
		entered := f.entered
		f.gate, f.entered = nil, nil
		f.mu.Unlock()
		close(entered)

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-gate:
		}

		f.mu.Lock()
	}

	defer f.mu.Unlock()

	if f.failErr != nil {
		return "", f.failErr
	}

	f.specs = append(f.specs, spec)

	return "ctr-" + spec.Name, nil
}

func (f *aksFakeRuntime) StopAndRemove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.removed = append(f.removed, id)

	return nil
}

func (f *aksFakeRuntime) created() []container.Spec {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]container.Spec(nil), f.specs...)
}

func (f *aksFakeRuntime) removedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.removed...)
}

func (f *aksFakeRuntime) setFail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.failErr = err
}

func newAKSEngineStack(t *testing.T) (*aksStack, *aksFakeRuntime) {
	t.Helper()

	s := newAKSStack(t)

	return s, enableAKSEngine(t, s)
}

func enableAKSEngine(t *testing.T, s *aksStack) *aksFakeRuntime {
	t.Helper()

	rt := &aksFakeRuntime{}
	require.NoError(t, s.provider.EnableEngine(azurearm.AKSEngineConfig{
		Runtime: rt, Token: aksTestToken, StartTimeout: 10 * time.Second,
		Probe: func(context.Context, string, string) ([]byte, error) { return []byte(aksTestCA), nil },
	}))

	return rt
}

func (s *aksStack) userKubeconfig(t *testing.T, name string) (kubeconfigDoc, []byte, error) {
	t.Helper()

	res, err := s.factory.NewManagedClustersClient().ListClusterUserCredentials(t.Context(), aksRG, name, nil)
	if err != nil {
		return kubeconfigDoc{}, nil, err
	}

	doc, ca := parseKubeconfig(t, res.CredentialResults)

	return doc, ca, nil
}

func TestAKSEngineLifecycle(t *testing.T) {
	t.Parallel()

	s, rt := newAKSEngineStack(t)
	mc := s.mustPut(t, "eng", nil)

	specs := rt.created()
	require.Len(t, specs, 1)

	wantImage, _ := k3sengine.Image(*mc.Properties.CurrentKubernetesVersion, "1.36")
	assert.Equal(t, wantImage, specs[0].Image)
	assert.True(t, specs[0].Privileged)
	assert.Contains(t, specs[0].Name, "gopherstack-aks-eng-")

	doc, ca, err := s.userKubeconfig(t, "eng")
	require.NoError(t, err)
	assert.True(
		t,
		strings.HasPrefix(doc.Clusters[0].Cluster.Server, "https://127.0.0.1:"),
		doc.Clusters[0].Cluster.Server,
	)
	assert.Equal(t, aksTestCA, string(ca))
	assert.Equal(t, aksTestToken, doc.Users[0].User.Token)
	assert.Regexp(t, `^eng-[0-9a-f]{8}\.hcp\.`, *mc.Properties.Fqdn)

	c := s.factory.NewManagedClustersClient()

	stop, err := c.BeginStop(t.Context(), aksRG, "eng", nil)
	require.NoError(t, err)

	_, err = stop.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.Equal(t, []string{"ctr-" + specs[0].Name}, rt.stoppedIDs())
	assert.Empty(t, rt.removedIDs(), "stop must keep the container")

	stopped, _, err := s.userKubeconfig(t, "eng")
	require.NoError(t, err)
	assert.NotEqual(t, doc.Clusters[0].Cluster.Server, stopped.Clusters[0].Cluster.Server)

	start, err := c.BeginStart(t.Context(), aksRG, "eng", nil)
	require.NoError(t, err)

	_, err = start.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.Len(t, rt.created(), 1, "start must reuse the stopped container")
	assert.Equal(t, []string{"ctr-" + specs[0].Name}, rt.startedIDs())

	again, _, err := s.userKubeconfig(t, "eng")
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(again.Clusters[0].Cluster.Server, "https://127.0.0.1:"))

	_, err = s.put(t, "eng", func(m *armcontainerservice.ManagedCluster) { m.Tags = map[string]*string{"k": new("v")} })
	require.NoError(t, err)
	assert.Len(t, rt.created(), 1, "an update must not start another node")

	del, err := c.BeginDelete(t.Context(), aksRG, "eng", nil)
	require.NoError(t, err)

	_, err = del.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.Equal(t, []string{"ctr-" + specs[0].Name}, rt.removedIDs())
}

func TestAKSEngineStartFailure(t *testing.T) {
	t.Parallel()

	s, rt := newAKSEngineStack(t)
	rt.setFail(errAKSRuntimeDown)

	_, err := s.put(t, "bad", nil)
	assert.Equal(t, "ClusterStartFailed", sdkErrCode(err))

	_, err = s.factory.NewManagedClustersClient().Get(t.Context(), aksRG, "bad", nil)
	assert.Equal(t, "ResourceNotFound", sdkErrCode(err))

	nodeRG := s.raw(
		t,
		http.MethodGet,
		"/subscriptions/"+aciSub+"/resourcegroups/MC_rg-aks_bad_westus2",
		"",
		"2021-04-01",
	)
	assert.Equal(t, http.StatusNotFound, nodeRG.StatusCode)
}

func TestAKSEngineResourceGroupCascade(t *testing.T) {
	t.Parallel()

	s, rt := newAKSEngineStack(t)
	s.mustPut(t, "one", nil)
	s.mustPut(t, "two", nil)
	require.Len(t, rt.created(), 2)

	del := s.raw(t, http.MethodDelete, "/subscriptions/"+aciSub+"/resourcegroups/"+aksRG, "", "2021-04-01")
	require.Equal(t, http.StatusOK, del.StatusCode)
	assert.Len(t, rt.removedIDs(), 2)
}

func TestAKSEngineRestoreRelaunches(t *testing.T) {
	t.Parallel()

	s, rt := newAKSEngineStack(t)
	s.mustPut(t, "keep", nil)
	s.mustPut(t, "idle", nil)

	stop, err := s.factory.NewManagedClustersClient().BeginStop(t.Context(), aksRG, "idle", nil)
	require.NoError(t, err)

	_, err = stop.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	data := s.handler.Snapshot(t.Context())
	require.Len(t, rt.created(), 2)

	s2 := newAKSStack(t)
	rt2 := enableAKSEngine(t, s2)
	require.NoError(t, s2.handler.Restore(t.Context(), data))

	require.Eventually(t, func() bool {
		_, _, kerr := s2.userKubeconfig(t, "keep")

		return kerr == nil
	}, 10*time.Second, time.Millisecond)

	assert.Len(t, rt2.created(), 1, "only the running cluster is relaunched")

	got, err := s2.factory.NewManagedClustersClient().Get(t.Context(), aksRG, "idle", nil)
	require.NoError(t, err)
	assert.Equal(t, armcontainerservice.CodeStopped, *got.Properties.PowerState.Code)
}

type aksNoKeepRuntime struct{ f *aksFakeRuntime }

func (r aksNoKeepRuntime) CreateAndStart(ctx context.Context, spec container.Spec) (string, error) {
	return r.f.CreateAndStart(ctx, spec)
}

func (r aksNoKeepRuntime) StopAndRemove(ctx context.Context, id string) error {
	return r.f.StopAndRemove(ctx, id)
}

func TestAKSEngineStopFallsBackWithoutKeepSupport(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)
	rt := &aksFakeRuntime{}
	require.NoError(t, s.provider.EnableEngine(azurearm.AKSEngineConfig{
		Runtime: aksNoKeepRuntime{f: rt}, Token: aksTestToken, StartTimeout: 10 * time.Second,
		Probe: func(context.Context, string, string) ([]byte, error) { return []byte(aksTestCA), nil },
	}))
	s.mustPut(t, "plain", nil)

	c := s.factory.NewManagedClustersClient()

	stop, err := c.BeginStop(t.Context(), aksRG, "plain", nil)
	require.NoError(t, err)

	_, err = stop.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.Len(t, rt.removedIDs(), 1)

	start, err := c.BeginStart(t.Context(), aksRG, "plain", nil)
	require.NoError(t, err)

	_, err = start.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.Len(t, rt.created(), 2)
}
