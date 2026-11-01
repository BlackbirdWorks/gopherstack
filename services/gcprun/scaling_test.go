package gcprun_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const idleTimeout = 15 * time.Minute

type scaleOpts struct {
	concurrency int32
	minInst     int32
	maxInst     int32
}

// scaledEnv deploys "web" with the given scaling on an env whose clock the test drives.
func scaledEnv(t *testing.T, o scaleOpts) (*env, *fakeClock, string) {
	t.Helper()

	e := newEnv(t, true)
	clock := newFakeClock()
	e.handler.Backend.SetClock(clock)

	spec := svcSpec("example/web:1")
	spec.Template.MaxInstanceRequestConcurrency = o.concurrency
	spec.Template.Scaling = &runpb.RevisionScaling{MinInstanceCount: o.minInst, MaxInstanceCount: o.maxInst}

	op, err := e.services.CreateService(t.Context(), &runpb.CreateServiceRequest{
		Parent: parent, ServiceId: "web", Service: spec,
	})
	require.NoError(t, err)

	svc, err := op.Wait(t.Context())
	require.NoError(t, err)

	return e, clock, strings.TrimPrefix(svc.GetUri(), "http://")
}

type result struct {
	err  error
	body string
	code int
}

// fetch issues a request in the background and delivers its outcome on the returned channel.
func (e *env) fetch(t *testing.T, host, path string) <-chan result {
	t.Helper()

	out := make(chan result, 1)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+e.handler.IngressAddr()+path, nil)
	require.NoError(t, err)

	req.Host = host

	go func() {
		resp, derr := e.http.Do(req)
		if derr != nil {
			out <- result{err: derr}

			return
		}

		defer resp.Body.Close()

		var sb strings.Builder

		buf := make([]byte, 512)

		for {
			n, rerr := resp.Body.Read(buf)
			sb.Write(buf[:n])

			if rerr != nil {
				break
			}
		}

		out <- result{code: resp.StatusCode, body: sb.String()}
	}()

	return out
}

func TestScaling_ConcurrencyScalesOut(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		o        scaleOpts
		requests int
		wantRun  int
	}{
		{"one slot per instance", scaleOpts{concurrency: 1, maxInst: 5}, 3, 3},
		{"two slots per instance", scaleOpts{concurrency: 2, maxInst: 5}, 3, 2},
		{"capped by max instances", scaleOpts{concurrency: 1, maxInst: 2}, 2, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, _, host := scaledEnv(t, tt.o)
			results := make([]<-chan result, 0, tt.requests)

			for range tt.requests {
				results = append(results, e.fetch(t, host, "/slow"))
			}

			require.Eventually(t, func() bool { return e.rt.slow.Load() == int64(tt.requests) },
				10*time.Second, time.Millisecond)
			assert.Equal(t, tt.wantRun, e.rt.running())

			close(e.rt.release)

			for _, ch := range results {
				r := <-ch
				require.NoError(t, r.err)
				assert.Equal(t, 200, r.code)
			}
		})
	}
}

func TestScaling_MaxInstancesQueuesRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		freeSlot bool
		wantCode int
	}{
		{"queue timeout returns 429", false, http.StatusTooManyRequests},
		{"freed slot serves the queued request", true, http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, clock, host := scaledEnv(t, scaleOpts{concurrency: 1, maxInst: 1})

			first := e.fetch(t, host, "/slow")
			require.Eventually(t, func() bool { return e.rt.slow.Load() == 1 }, 10*time.Second, time.Millisecond)

			second := e.fetch(t, host, "/queued")
			clock.waitTimers(t, 1)

			if tt.freeSlot {
				close(e.rt.release)
				assert.Equal(t, http.StatusOK, (<-first).code)
			} else {
				clock.advance(10 * time.Second)
			}

			r := <-second
			require.NoError(t, r.err)
			assert.Equal(t, tt.wantCode, r.code)
			assert.Equal(t, 1, e.rt.running(), "never above max instances")

			if !tt.freeSlot {
				close(e.rt.release)
				assert.Equal(t, http.StatusOK, (<-first).code)
			}
		})
	}
}

func TestScaling_MinInstancesStayWarm(t *testing.T) {
	t.Parallel()

	e, clock, _ := scaledEnv(t, scaleOpts{minInst: 2, maxInst: 5})
	assert.Equal(t, 2, e.rt.running(), "deploy brings the minimum instances up")

	clock.advance(4 * idleTimeout)
	assert.Equal(t, 2, e.rt.running(), "idle instances at the minimum are kept")
}

func TestScaling_ScaleToZeroAfterIdle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		advance time.Duration
		running int
	}{
		{"before the idle timeout", idleTimeout - time.Second, 1},
		{"at the idle timeout", idleTimeout, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, clock, host := scaledEnv(t, scaleOpts{})
			require.Equal(t, 1, e.rt.running())

			clock.advance(tt.advance)
			assert.Equal(t, tt.running, e.rt.running())

			code, _ := e.get(t, host, "/")
			assert.Equal(t, http.StatusOK, code, "a request starts an instance again")
			assert.Equal(t, 1, e.rt.running())
		})
	}
}

func TestScaling_ActivityDefersScaleDown(t *testing.T) {
	t.Parallel()

	e, clock, host := scaledEnv(t, scaleOpts{})

	clock.advance(idleTimeout - time.Minute)

	code, _ := e.get(t, host, "/")
	require.Equal(t, http.StatusOK, code)

	clock.advance(idleTimeout - time.Minute)
	assert.Equal(t, 1, e.rt.running(), "the idle timer restarted after the request")

	clock.advance(time.Minute)
	assert.Zero(t, e.rt.running())
}

func TestScaling_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		o    *runpb.RevisionScaling
		name string
	}{
		{name: "min above max", o: &runpb.RevisionScaling{MinInstanceCount: 3, MaxInstanceCount: 2}},
		{name: "negative min", o: &runpb.RevisionScaling{MinInstanceCount: -1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			spec := svcSpec("i")
			spec.Template.Scaling = tt.o

			_, err := e.services.CreateService(t.Context(), &runpb.CreateServiceRequest{
				Parent: parent, ServiceId: "web", Service: spec,
			})
			require.Error(t, err)
		})
	}
}

func TestScaling_SidecarInstanceStopsTogetherAndColdStartsAgain(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	clock := newFakeClock()
	e.handler.Backend.SetClock(clock)

	app, sidecar := appAndSidecar()

	svc, err := e.deploy(t, "web", multiService(app, sidecar))
	require.NoError(t, err)
	require.Equal(t, 2, e.rt.running(), "ingress container and sidecar start together")

	clock.advance(idleTimeout)
	assert.Zero(t, e.rt.running(), "idle scale-down stops every container of the instance")
	assert.Equal(t, []string{"fake-2", "fake-1"}, e.rt.removedIDs(), "dependents stop before the network owner")

	code, _ := e.get(t, strings.TrimPrefix(svc.GetUri(), "http://"), "/")
	assert.Equal(t, http.StatusOK, code, "a request cold-starts a fresh instance")
	assert.Equal(t, 2, e.rt.running())
	assert.Len(t, e.rt.specs(), 4)
}
