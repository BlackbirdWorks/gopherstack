package azurefunctions_test

import (
	"bytes"
	"context"
	"maps"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

type testClock struct{ ns atomic.Int64 }

func newTestClock() *testClock {
	c := &testClock{}
	c.ns.Store(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).UnixNano())

	return c
}

func (c *testClock) now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *testClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

func timerZip(t *testing.T) []byte {
	t.Helper()

	return zipOf(t, map[string]string{
		"host.json": `{"version":"2.0"}`,
		"tick/function.json": `{"bindings":[{"type":"timerTrigger","direction":"in","name":"t",` +
			`"schedule":"0 */5 * * * *"}]}`,
		"hello/function.json": `{"bindings":[{"authLevel":"anonymous","type":"httpTrigger","direction":"in",` +
			`"name":"req","route":"greet"}]}`,
	})
}

func idleEnv(t *testing.T, cfg azurefunctions.AppConfig, zip []byte) (*env, *testClock) {
	t.Helper()

	e := newEnv(t, true)
	clock := newTestClock()
	e.backend.SetClock(clock.now)

	cfg.Kind, cfg.LinuxFx = "functionapp,linux", "NODE|20"
	e.backend.PutApp("idle", cfg)

	_, err := e.backend.Deploy("idle", zip, "test", "m")
	require.NoError(t, err)

	return e, clock
}

func (e *env) hit(t *testing.T) {
	t.Helper()

	resp, _ := e.do(t, http.MethodGet, "idle."+testDomain, "/api/greet", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestReapIdleHosts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		cfg   azurefunctions.AppConfig
		timer bool
		idle  time.Duration
		reap  int
	}{
		{
			"consumption just under",
			azurefunctions.AppConfig{Hosting: azurefunctions.HostingConsumption},
			false,
			5*time.Minute - time.Second,
			0,
		},
		{
			"consumption at limit",
			azurefunctions.AppConfig{Hosting: azurefunctions.HostingConsumption},
			false,
			5 * time.Minute,
			1,
		},
		{
			"flex idle",
			azurefunctions.AppConfig{Hosting: azurefunctions.HostingFlex, Flex: true},
			false,
			5 * time.Minute,
			1,
		},
		{
			"flex always ready",
			azurefunctions.AppConfig{Hosting: azurefunctions.HostingFlex, Flex: true, AlwaysReady: true},
			false,
			48 * time.Hour,
			0,
		},
		{"premium never", azurefunctions.AppConfig{Hosting: azurefunctions.HostingPremium}, false, 48 * time.Hour, 0},
		{
			"dedicated just under",
			azurefunctions.AppConfig{Hosting: azurefunctions.HostingDedicated},
			false,
			20*time.Minute - time.Second,
			0,
		},
		{
			"dedicated at limit",
			azurefunctions.AppConfig{Hosting: azurefunctions.HostingDedicated},
			false,
			20 * time.Minute,
			1,
		},
		{
			"dedicated always on",
			azurefunctions.AppConfig{Hosting: azurefunctions.HostingDedicated, AlwaysOn: true},
			false,
			48 * time.Hour,
			0,
		},
		{"no hosting policy", azurefunctions.AppConfig{}, false, 48 * time.Hour, 0},
		{
			"consumption with timer trigger",
			azurefunctions.AppConfig{Hosting: azurefunctions.HostingConsumption},
			true,
			48 * time.Hour,
			0,
		},
		{
			"dedicated with timer trigger",
			azurefunctions.AppConfig{Hosting: azurefunctions.HostingDedicated},
			true,
			48 * time.Hour,
			0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			zip := helloZip(t)
			if tt.timer {
				zip = timerZip(t)
			}

			e, clock := idleEnv(t, tt.cfg, zip)
			e.hit(t)
			require.Equal(t, 1, e.rt.running())

			clock.advance(tt.idle)
			assert.Equal(t, tt.reap, e.backend.ReapIdle())
			assert.Equal(t, 1-tt.reap, e.rt.running())

			e.hit(t)
			assert.Equal(t, 1, e.rt.running(), "the next request starts the host again")
		})
	}
}

func TestRequestsKeepAHostAlive(t *testing.T) {
	t.Parallel()

	e, clock := idleEnv(t, azurefunctions.AppConfig{Hosting: azurefunctions.HostingConsumption}, helloZip(t))
	e.hit(t)

	clock.advance(4 * time.Minute)
	e.hit(t)
	clock.advance(4 * time.Minute)
	assert.Zero(t, e.backend.ReapIdle(), "8 minutes after start but 4 after the last request")

	clock.advance(time.Minute)
	assert.Equal(t, 1, e.backend.ReapIdle())
}

func TestInFlightRequestBlocksReaping(t *testing.T) {
	t.Parallel()

	e, clock := idleEnv(t, azurefunctions.AppConfig{Hosting: azurefunctions.HostingConsumption}, helloZip(t))
	e.hit(t)

	end := e.backend.BeginRequest("idle")

	clock.advance(time.Hour)
	assert.Zero(t, e.backend.ReapIdle(), "a request in flight keeps the host")

	end()
	assert.Zero(t, e.backend.ReapIdle(), "finishing a request counts as activity")

	clock.advance(5 * time.Minute)
	assert.Equal(t, 1, e.backend.ReapIdle())
}

func TestStoppedAndStartingAppsAreNotReaped(t *testing.T) {
	t.Parallel()

	e, clock := idleEnv(t, azurefunctions.AppConfig{Hosting: azurefunctions.HostingConsumption}, helloZip(t))
	clock.advance(time.Hour)
	assert.Zero(t, e.backend.ReapIdle(), "an app with no host has nothing to reap")

	require.NoError(t, e.backend.SetStopped("idle", true))
	assert.Zero(t, e.backend.ReapIdle())
}

func TestIdlePolicyPersists(t *testing.T) {
	t.Parallel()

	src := newEnv(t, false)
	src.backend.PutApp("idle", azurefunctions.AppConfig{
		Kind: "functionapp,linux", LinuxFx: "NODE|20", Hosting: azurefunctions.HostingDedicated, AlwaysOn: true,
	})
	src.backend.PutApp("cold", azurefunctions.AppConfig{
		Kind: "functionapp,linux", LinuxFx: "NODE|20", Hosting: azurefunctions.HostingDedicated,
	})
	_, err := src.backend.Deploy("idle", helloZip(t), "t", "m")
	require.NoError(t, err)
	_, err = src.backend.Deploy("cold", helloZip(t), "t", "m")
	require.NoError(t, err)

	dst := newEnv(t, true)
	clock := newTestClock()
	dst.backend.SetClock(clock.now)
	require.NoError(t, dst.handler.Restore(t.Context(), src.handler.Snapshot(t.Context())))

	for _, host := range []string{"idle", "cold"} {
		resp, _ := dst.do(t, http.MethodGet, host+"."+testDomain, "/api/greet", nil, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	require.Equal(t, 2, dst.rt.running())

	clock.advance(21 * time.Minute)
	assert.Equal(t, 1, dst.backend.ReapIdle(), "only the app without Always On is unloaded after a restore")
}

func TestReapingUnderConcurrentRequests(t *testing.T) {
	t.Parallel()

	e, clock := idleEnv(t, azurefunctions.AppConfig{Hosting: azurefunctions.HostingConsumption}, helloZip(t))

	var wg sync.WaitGroup

	var bad atomic.Int64

	for range 8 {
		wg.Go(func() {
			for range 10 {
				resp, _ := e.do(t, http.MethodGet, "idle."+testDomain, "/api/greet", nil, nil)
				if resp.StatusCode != http.StatusOK {
					bad.Add(1)
				}
			}
		})
	}

	wg.Go(func() {
		for range 40 {
			clock.advance(10 * time.Minute)
			e.backend.ReapIdle()
		}
	})

	wg.Wait()
	assert.Zero(t, bad.Load(), "no request is cut off by a reap")
}

func TestReaperGoroutineStopsOnShutdown(t *testing.T) {
	t.Parallel()

	b := azurefunctions.NewInMemoryBackend()
	h := azurefunctions.NewHandler(b, azurefunctions.DefaultSettings())
	h.Port = 0

	require.NoError(t, h.StartWorker(t.Context()))

	h.Shutdown(t.Context())
}

func TestAdminListHoldsTheHostAgainstReaping(t *testing.T) {
	t.Parallel()

	e, clock := idleEnv(t, azurefunctions.AppConfig{Hosting: azurefunctions.HostingConsumption}, helloZip(t))
	entered, release := make(chan struct{}), make(chan struct{})
	e.rt.onAdmin = func() {
		close(entered)
		<-release
	}

	e.hit(t)
	clock.advance(time.Hour)

	done := make(chan error, 1)

	go func() {
		_, err := e.backend.Functions(t.Context(), "idle")
		done <- err
	}()

	<-entered
	assert.Zero(t, e.backend.ReapIdle(), "the host answering an admin call is not unloaded")
	close(release)
	require.NoError(t, <-done)
}

func TestUnknownTriggersKeepTheHost(t *testing.T) {
	t.Parallel()

	httpFn := `{"bindings":[{"authLevel":"anonymous","type":"httpTrigger","direction":"in","name":"req","route":"greet"}]}`

	tests := []struct {
		files map[string]string
		name  string
		reap  int
	}{
		{map[string]string{"hello/function.json": httpFn}, "http only", 1},
		{
			map[string]string{"hello/function.json": httpFn, "broken/function.json": `{"bindings":[`},
			"unparsable function json", 0,
		},
		{map[string]string{"hello/function.json": httpFn, "big.js": strings.Repeat("a", 9<<20)}, "oversized script", 0},
		{
			map[string]string{"hello/function.json": httpFn, "app.js": `df.app.orchestration('o', function*(){})`},
			"durable orchestration", 0,
		},
		{map[string]string{"hello/function.json": httpFn, "f.py": "@app.kafka_trigger(topic='t')"}, "python kafka", 0},
		{
			map[string]string{"hello/function.json": httpFn, "a.dll": "xxRabbitMQTriggerAttributexx"},
			"dotnet rabbitmq", 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			files := map[string]string{"host.json": `{"version":"2.0"}`}
			maps.Copy(files, tt.files)

			cfg := azurefunctions.AppConfig{Hosting: azurefunctions.HostingConsumption}
			e, clock := idleEnv(t, cfg, zipOf(t, files))
			e.hit(t)

			clock.advance(time.Hour)
			assert.Equal(t, tt.reap, e.backend.ReapIdle())
		})
	}
}

func TestServedRequestHoldsTheHostWhileItRuns(t *testing.T) {
	t.Parallel()

	e, clock := idleEnv(t, azurefunctions.AppConfig{Hosting: azurefunctions.HostingConsumption}, helloZip(t))
	entered, release := make(chan struct{}), make(chan struct{})

	var once sync.Once

	e.rt.onEcho = func() {
		once.Do(func() { close(entered) })
		<-release
	}

	status := make(chan int, 1)

	go func() {
		status <- e.statusOf(http.MethodGet, "idle."+testDomain, "/api/greet", nil, "")
	}()

	<-entered
	clock.advance(time.Hour)
	assert.Zero(t, e.backend.ReapIdle(), "a request running on the host keeps it")
	close(release)
	assert.Equal(t, http.StatusOK, <-status, "the request is not cut off")

	assert.Zero(t, e.backend.ReapIdle(), "finishing counts as activity")
	clock.advance(5 * time.Minute)
	assert.Equal(t, 1, e.backend.ReapIdle())
}

func TestKuduRequestHoldsTheHostButNotItsClock(t *testing.T) {
	t.Parallel()

	e, clock := idleEnv(t, azurefunctions.AppConfig{Hosting: azurefunctions.HostingConsumption}, helloZip(t))
	entered, release := make(chan struct{}), make(chan struct{})
	e.rt.onExec = func(container.Spec, []string) container.ExecResult {
		close(entered)
		<-release

		return container.ExecResult{}
	}

	status := make(chan int, 1)
	auth := e.kuduAuth(t, "idle")["basic"]

	go func() {
		status <- e.statusOf(http.MethodPost, "idle.scm."+testDomain, "/api/command", []byte(`{"command":"ls"}`), auth)
	}()

	<-entered
	clock.advance(time.Hour)
	assert.Zero(t, e.backend.ReapIdle(), "a Kudu call in flight keeps the host")
	close(release)
	assert.Equal(t, http.StatusOK, <-status)

	assert.Equal(t, 1, e.backend.ReapIdle(), "Kudu traffic does not restart the idle clock")
}

// statusOf sends a request without asserting, so it can run off the test goroutine; -1 means it failed.
func (e *env) statusOf(method, host, path string, body []byte, basic string) int {
	req, err := http.NewRequestWithContext(context.Background(), method, e.srv.URL+path, bytes.NewReader(body))
	if err != nil {
		return -1
	}

	req.Host = host

	if u, p, ok := strings.Cut(basic, ":"); ok {
		req.SetBasicAuth(u, p)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return -1
	}

	_ = resp.Body.Close()

	return resp.StatusCode
}
