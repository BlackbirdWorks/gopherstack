package azurearm_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appcontainers/armappcontainers/v4"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

type manualTimer struct {
	c  chan time.Time
	fn func()
}

func (m *manualTimer) C() <-chan time.Time { return m.c }

func (m *manualTimer) Stop() bool {
	m.fn = nil

	return true
}

// manualClock only fires its timers when the test says so.
type manualClock struct {
	timers []*manualTimer
	mu     sync.Mutex
}

func (c *manualClock) Now() time.Time { return time.Unix(1_700_000_000, 0) }

func (c *manualClock) NewTimer(time.Duration) gcpscale.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()

	t := &manualTimer{c: make(chan time.Time, 1)}
	c.timers = append(c.timers, t)

	return t
}

func (c *manualClock) AfterFunc(_ time.Duration, f func()) gcpscale.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()

	t := &manualTimer{c: make(chan time.Time), fn: f}
	c.timers = append(c.timers, t)

	return t
}

func (c *manualClock) fireIdle() {
	c.mu.Lock()
	timers := append([]*manualTimer(nil), c.timers...)
	c.timers = nil
	c.mu.Unlock()

	for _, t := range timers {
		if t.fn != nil {
			t.fn()
		}
	}
}

// replicaBackends answers HTTP on the host port each fake replica was given, naming the image and environment.
type replicaBackends struct {
	labels  map[int]string
	servers []*http.Server
	mu      sync.Mutex
}

func newReplicaBackends(t *testing.T) *replicaBackends {
	t.Helper()

	b := &replicaBackends{labels: map[int]string{}}

	t.Cleanup(func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		for _, s := range b.servers {
			_ = s.Close()
		}
	})

	return b
}

func (b *replicaBackends) hook(spec container.Spec) {
	if len(spec.Ports) == 0 {
		return
	}

	parts := strings.Split(spec.Ports[0], ":")

	var port int

	_, _ = fmt.Sscanf(parts[1], "%d", &port)

	b.mu.Lock()
	defer b.mu.Unlock()

	_, bound := b.labels[port]
	b.labels[port] = spec.Image + "|" + strings.Join(spec.Env, ",")

	if bound {
		return
	}

	var lc net.ListenConfig

	l, err := lc.Listen(context.Background(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return
	}

	srv := &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b.mu.Lock()
			label := b.labels[port]
			b.mu.Unlock()

			_, _ = fmt.Fprintf(w, "%s|host=%s|path=%s", label, r.Host, r.URL.Path)
		}),
	}

	b.servers = append(b.servers, srv)

	go func() { _ = srv.Serve(l) }()
}

type picker struct{ n atomic.Int64 }

func (p *picker) set(n int) { p.n.Store(int64(n)) }

func (p *picker) pick(int) int { return int(p.n.Load()) }

func TestACA_TrafficSplitRoutesByWeightAndLabel(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	backends := newReplicaBackends(t)
	s.rt.onStart = backends.hook

	pick := &picker{}
	s.apps.SetTrafficPicker(pick.pick)

	env := s.mustEnv(t, "rg", "env1")
	envID, domain := *env.ID, *env.Properties.DefaultDomain
	multi := armappcontainers.ActiveRevisionsModeMultiple

	s.mustApp(t, "rg", "split", envID, acaApp{mode: multi, suffix: "one", image: "img-one", extIng: true})
	s.mustApp(t, "rg", "split", envID, acaApp{
		mode: multi, suffix: "two", image: "img-two", extIng: true,
		traffic: []*armappcontainers.TrafficWeight{
			{RevisionName: new("split--one"), Weight: new(int32(80))},
			{RevisionName: new("split--two"), Weight: new(int32(20)), Label: new("canary")},
		},
	})

	tests := []struct {
		name string
		host string
		want string
		pick int
	}{
		{name: "low-draw-goes-to-first", host: "split." + domain, pick: 10, want: "img-one"},
		{name: "boundary-79-first", host: "split." + domain, pick: 79, want: "img-one"},
		{name: "boundary-80-second", host: "split." + domain, pick: 80, want: "img-two"},
		{name: "high-draw-goes-to-second", host: "split." + domain, pick: 95, want: "img-two"},
		{name: "revision-host-ignores-weights", host: "split--one." + domain, pick: 95, want: "img-one"},
		{name: "label-host", host: "split---canary." + domain, pick: 0, want: "img-two"},
		{name: "port-in-host-header", host: "split." + domain + ":443", pick: 0, want: "img-one"},
	}

	for _, tc := range tests {
		pick.set(tc.pick)

		status, body := s.raw(t, http.MethodGet, tc.host, "/hello")
		assert.Equal(t, http.StatusOK, status, tc.name)
		assert.Contains(t, body, tc.want+"|", tc.name)
		assert.Contains(t, body, "|path=/hello", tc.name)
		assert.Contains(t, body, "host="+strings.Split(tc.host, ":")[0], "the inbound Host reaches the app: "+tc.name)
	}

	status, _ := s.raw(t, http.MethodGet, "ghost.example.test", "/subscriptions?api-version=2020-01-01")
	assert.Equal(t, http.StatusOK, status, "unrelated hosts still reach ARM")

	got, err := s.factory.NewContainerAppsRevisionsClient().GetRevision(t.Context(), "rg", "split", "split--two", nil)
	require.NoError(t, err)
	assert.Equal(t, int32(20), *got.Properties.TrafficWeight)
}

func TestACA_SingleModeRoutesToLatestAndFailedRevisionKeepsOld(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	backends := newReplicaBackends(t)
	s.rt.onStart = backends.hook

	env := s.mustEnv(t, "rg", "env1")
	envID, domain := *env.ID, *env.Properties.DefaultDomain

	s.mustApp(t, "rg", "single", envID, acaApp{suffix: "good", image: "img-good", extIng: true})

	_, body := s.raw(t, http.MethodGet, "single."+domain, "/")
	assert.Contains(t, body, "img-good|")

	bad := s.mustApp(t, "rg", "single", envID, acaApp{suffix: "bad", image: "missing-image", extIng: true})
	assert.Equal(t, "single--bad", *bad.Properties.LatestRevisionName)
	assert.Equal(t, "single--good", *bad.Properties.LatestReadyRevisionName, "the failed revision is not ready")

	revs := map[string]*armappcontainers.Revision{}
	for _, r := range s.revisions(t, "single") {
		revs[*r.Name] = r
	}

	assert.Equal(t, armappcontainers.RevisionRunningStateFailed, *revs["single--bad"].Properties.RunningState)
	assert.Contains(t, *revs["single--bad"].Properties.ProvisioningError, "missing-image")
	assert.True(t, *revs["single--good"].Properties.Active, "the old revision keeps serving")
	assert.Equal(t, int32(100), *revs["single--good"].Properties.TrafficWeight)

	_, body = s.raw(t, http.MethodGet, "single."+domain, "/")
	assert.Contains(t, body, "img-good|")

	s.mustApp(t, "rg", "single", envID, acaApp{suffix: "fixed", image: "img-fixed", extIng: true})

	_, body = s.raw(t, http.MethodGet, "single."+domain, "/")
	assert.Contains(t, body, "img-fixed|")

	revs = map[string]*armappcontainers.Revision{}
	for _, r := range s.revisions(t, "single") {
		revs[*r.Name] = r
	}

	assert.False(t, *revs["single--good"].Properties.Active, "the healthy new revision retires the old one")
}

func TestACA_ScaleToZeroAndColdStart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		minReplicas  int32
		wantAfterIdl int
	}{
		{name: "min-zero-scales-to-zero", minReplicas: 0, wantAfterIdl: 0},
		{name: "min-one-keeps-a-replica", minReplicas: 1, wantAfterIdl: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACAStack(t)
			backends := newReplicaBackends(t)
			s.rt.onStart = backends.hook

			clock := &manualClock{}
			s.apps.SetClock(clock)

			env := s.mustEnv(t, "rg", "env1")
			domain := *env.Properties.DefaultDomain

			app := s.mustApp(t, "rg", "idle", *env.ID, acaApp{
				image: "img-idle", extIng: true,
				scale: &armappcontainers.Scale{MinReplicas: &tc.minReplicas, MaxReplicas: new(int32(3))},
			})

			rev := *app.Properties.LatestRevisionName
			replicas := func() int {
				l, err := s.factory.NewContainerAppsRevisionReplicasClient().
					ListReplicas(t.Context(), "rg", "idle", rev, nil)
				require.NoError(t, err)

				return len(l.Value)
			}

			assert.Equal(t, 1, replicas(), "a new revision starts one replica")
			assert.Equal(t, 1, s.rt.live())

			clock.fireIdle()

			assert.Equal(t, tc.wantAfterIdl, replicas())
			assert.Equal(t, tc.wantAfterIdl, s.rt.live())

			got, err := s.factory.NewContainerAppsRevisionsClient().GetRevision(t.Context(), "rg", "idle", rev, nil)
			require.NoError(t, err)
			assert.Equal(t, int32(tc.wantAfterIdl), *got.Properties.Replicas)

			status, body := s.raw(t, http.MethodGet, "idle."+domain, "/wake")
			assert.Equal(t, http.StatusOK, status)
			assert.Contains(t, body, "img-idle|")
			assert.Equal(t, max(tc.wantAfterIdl, 1), replicas(), "a request starts a replica when none runs")
		})
	}
}

func TestACA_StoppedAndInactiveRoutes(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	backends := newReplicaBackends(t)
	s.rt.onStart = backends.hook

	env := s.mustEnv(t, "rg", "env1")
	domain := *env.Properties.DefaultDomain
	multi := armappcontainers.ActiveRevisionsModeMultiple

	s.mustApp(t, "rg", "gate", *env.ID, acaApp{mode: multi, suffix: "one", image: "img-one", extIng: true})

	status, _ := s.raw(t, http.MethodGet, "gate."+domain, "/")
	assert.Equal(t, http.StatusOK, status)

	_, err := s.factory.NewContainerAppsRevisionsClient().
		DeactivateRevision(t.Context(), "rg", "gate", "gate--one", nil)
	require.NoError(t, err)

	status, _ = s.raw(t, http.MethodGet, "gate."+domain, "/")
	assert.Equal(t, http.StatusServiceUnavailable, status, "no active revision receives traffic")

	status, _ = s.raw(t, http.MethodGet, "gate--one."+domain, "/")
	assert.Equal(t, http.StatusNotFound, status, "an inactive revision's host is not an ingress host, so ARM answers")

	_, err = s.factory.NewContainerAppsRevisionsClient().ActivateRevision(t.Context(), "rg", "gate", "gate--one", nil)
	require.NoError(t, err)

	stop, err := s.factory.NewContainerAppsClient().BeginStop(t.Context(), "rg", "gate", nil)
	require.NoError(t, err)

	_, err = stop.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	status, _ = s.raw(t, http.MethodGet, "gate."+domain, "/")
	assert.Equal(t, http.StatusNotFound, status, "a stopped app serves nothing")
}

func TestACA_AppWithoutIngressHasNoHost(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	env := s.mustEnv(t, "rg", "env1")
	domain := *env.Properties.DefaultDomain

	app := s.mustApp(t, "rg", "private", *env.ID, acaApp{noIng: true})
	assert.Nil(t, app.Properties.LatestRevisionFqdn)
	assert.Nil(t, app.Properties.Configuration.Ingress)

	status, _ := s.raw(t, http.MethodGet, "private."+domain, "/")
	assert.NotEqual(t, http.StatusOK, status)
}

func TestACA_InternalIngressUsesInternalHost(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	backends := newReplicaBackends(t)
	s.rt.onStart = backends.hook

	env := s.mustEnv(t, "rg", "env1")
	domain := *env.Properties.DefaultDomain

	app := s.mustApp(t, "rg", "inner", *env.ID, acaApp{image: "img-inner"})
	assert.Equal(t, "inner.internal."+domain, *app.Properties.Configuration.Ingress.Fqdn)

	status, body := s.raw(t, http.MethodGet, "inner.internal."+domain, "/")
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, "img-inner|")
}

type disabledEngineConfig struct{}

func (disabledEngineConfig) GetAzureARMSettings() azurearm.Settings {
	st := azurearm.DefaultSettings()
	st.ACIEngine = azurearm.ACIEngineNone

	return st
}

func TestACA_EngineDisabledFailsRevisions(t *testing.T) {
	t.Parallel()

	armSvc, err := (&azurearm.Provider{}).Init(&service.AppContext{Config: disabledEngineConfig{}})
	require.NoError(t, err)

	h, ok := armSvc.(*azurearm.Handler)
	require.True(t, ok)

	t.Cleanup(func() { h.Shutdown(context.Background()) })

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewTLSServer(e)
	t.Cleanup(srv.Close)

	f, err := armappcontainers.NewClientFactory(aciSub, aciCred{}, &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: srv.URL, Audience: "https://management.core.windows.net/"},
		}},
		Transport: srv.Client(),
		Retry:     policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(t, err)

	s := &acaStack{factory: f}
	env := s.mustEnv(t, "rg", "env1")

	app := s.mustApp(t, "rg", "norun", *env.ID, acaApp{extIng: true})
	assert.Equal(t, armappcontainers.ContainerAppProvisioningStateSucceeded, *app.Properties.ProvisioningState,
		"ARM state is kept even when nothing can run")

	revs := s.revisions(t, "norun")
	require.Len(t, revs, 1)
	assert.Equal(t, armappcontainers.RevisionRunningStateFailed, *revs[0].Properties.RunningState)
	assert.Contains(t, *revs[0].Properties.ProvisioningError, "disabled")
}
