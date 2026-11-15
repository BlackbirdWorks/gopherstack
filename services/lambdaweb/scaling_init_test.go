package lambdaweb_test

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

// gatedLauncher blocks every launch until gate is closed.
type gatedLauncher struct {
	inner   *fakeLauncher
	gate    chan struct{}
	once    sync.Once
	entered atomic.Int32
}

func (g *gatedLauncher) open() { g.once.Do(func() { close(g.gate) }) }

func (g *gatedLauncher) Launch(ctx context.Context, spec lambdaweb.LaunchSpec) (lambdaweb.Environment, error) {
	g.entered.Add(1)
	<-g.gate

	return g.inner.Launch(ctx, spec)
}

func newGatedEnv(t *testing.T, o scaleOpts) (*dataEnv, *fakeLauncher, *gatedLauncher) {
	t.Helper()

	d, l := newScaledEnv(t, o)
	g := &gatedLauncher{inner: l, gate: make(chan struct{})}
	d.backend.SetLauncher(g)
	t.Cleanup(g.open)

	return d, l, g
}

type result struct {
	resp *http.Response
	done chan struct{}
}

func (d *dataEnv) async(t *testing.T) *result {
	t.Helper()

	r := &result{done: make(chan struct{})}

	go func() {
		defer close(r.done)

		r.resp = d.doRaw(t, http.MethodGet, d.domain, "/")
	}()

	return r
}

func TestScaling_QueuesBehindInitializingEnvironment(t *testing.T) {
	t.Parallel()

	d, l, g := newGatedEnv(t, scaleOpts{perEnv: 1, maxEnv: aws.Int32(2)})
	d.backend.SetMaxConcurrentInits(10)

	first := d.async(t)
	require.Eventually(t, func() bool { return g.entered.Load() == 1 }, 5*time.Second, time.Millisecond)

	second := d.async(t)
	require.Eventually(t, func() bool { return g.entered.Load() == 2 }, 5*time.Second, time.Millisecond)

	third := d.async(t)

	require.Never(t, func() bool {
		select {
		case <-third.done:
			return true
		default:
			return false
		}
	}, 200*time.Millisecond, 10*time.Millisecond, "a request waits for an initializing environment")

	g.open()
	<-third.done
	assert.Equal(t, http.StatusTooManyRequests, third.resp.StatusCode)
	assert.Equal(t, "Platform.EndpointMaxEnvThrottle", third.resp.Header.Get("Lambda-Web-Error-Type"))

	l.release()
	<-first.done
	<-second.done
}

func TestScaling_ConcurrentInitThrottle(t *testing.T) {
	t.Parallel()

	d, l, g := newGatedEnv(t, scaleOpts{perEnv: 1})
	d.backend.SetMaxConcurrentInits(1)

	m := &fakeMetrics{}
	d.backend.SetMetricEmitter(m)

	first := d.async(t)
	require.Eventually(t, func() bool { return g.entered.Load() == 1 }, 5*time.Second, time.Millisecond)

	resp := d.get(t)
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	assert.Equal(t, "Platform.EndpointConcurrentInitThrottle", resp.Header.Get("Lambda-Web-Error-Type"))

	for _, name := range []string{
		"EndpointRequestThrottles", "AccountRequestThrottles", "EndpointActiveEnvironmentThrottles",
		"AccountActiveVCPUArm64Throttles",
	} {
		assert.Nil(t, m.find(nsUsage, name), name)
	}

	g.open()
	l.release()
	<-first.done
}

func TestDataPlane_RequestRateQuotas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantType string
		wantStat string
		endpoint int32
		account  int32
	}{
		{name: "endpoint quota", endpoint: 2, account: 100, wantType: "Platform.EndpointRequestThrottle",
			wantStat: "EndpointRequestThrottles"},
		{name: "account quota", endpoint: 100, account: 2, wantType: "Platform.AccountRequestThrottle",
			wantStat: "AccountRequestThrottles"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d, m := newMetricsEnv(t)
			d.backend.SetRateQuotas(tc.endpoint, tc.account)

			require.Equal(t, http.StatusOK, d.get(t).StatusCode)
			require.Equal(t, http.StatusOK, d.get(t).StatusCode)

			resp := d.get(t)
			assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
			assert.Equal(t, tc.wantType, resp.Header.Get("Lambda-Web-Error-Type"))
			assert.InDelta(t, 1, m.sum(nsUsage, tc.wantStat), 0)

			d.clock.Advance(time.Second)
			assert.Equal(t, http.StatusOK, d.get(t).StatusCode, "a new second opens a new window")
		})
	}
}

// doRaw is do without testify assertions, for use outside the test goroutine.
func (d *dataEnv) doRaw(t *testing.T, method, host, path string) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, d.srvURL+path, nil)
	if err != nil {
		return nil
	}

	req.Host = host

	resp, err := d.http.Do(req)
	if err != nil {
		return nil
	}

	t.Cleanup(func() { _ = resp.Body.Close() })

	return resp
}
