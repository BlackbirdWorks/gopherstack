package lambdaweb_test

import (
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	lwsdk "github.com/aws/aws-sdk-go-v2/service/lambdaweb"
	"github.com/aws/aws-sdk-go-v2/service/lambdaweb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type scaleOpts struct {
	maxEnv *int32
	perEnv int32
}

// newScaledEnv creates function "app" whose requests block in the app until l.hold is closed.
func newScaledEnv(t *testing.T, o scaleOpts) (*dataEnv, *fakeLauncher) {
	t.Helper()

	env := newTestEnv(t)
	l := &fakeLauncher{hold: make(chan struct{})}
	env.backend.SetLauncher(l)

	t.Cleanup(l.release)

	rc := revisionConfig()
	rc.ServiceConfig.MaxConcurrencyPerEnvironment = aws.Int32(o.perEnv)
	ec := endpointConfig()

	if o.maxEnv != nil {
		ec.ScalingConfig = &types.ScalingConfig{MaxEnvironments: o.maxEnv}
	}

	created, err := env.client.CreateWebFunction(t.Context(), &lwsdk.CreateWebFunctionInput{
		FunctionName: aws.String("app"), RevisionConfig: rc, EndpointConfig: ec,
	})
	require.NoError(t, err)
	env.settle()

	d := &dataEnv{
		testEnv: env, launcher: l, domain: aws.ToString(created.Endpoint.DomainName),
		rev1: aws.ToString(created.Revision.RevisionId), http: &http.Client{Timeout: 30 * time.Second},
	}

	return d, l
}

// fire starts n held requests and waits until the apps have seen all of them.
func fire(t *testing.T, d *dataEnv, l *fakeLauncher, n int) *sync.WaitGroup {
	t.Helper()

	var wg sync.WaitGroup

	for range n {
		wg.Go(func() {
			resp := d.do(t, http.MethodGet, d.domain, "/", nil)
			assert.Equal(t, http.StatusOK, resp.StatusCode)
		})
	}

	require.Eventually(t, func() bool { return l.seenTotal() == n }, 5*time.Second, 5*time.Millisecond)

	return &wg
}

func TestScaling_ScalesOutPerEnvironmentConcurrency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxEnv   *int32
		name     string
		requests int
		perEnv   int32
		wantEnvs int32
	}{
		{name: "one fits", perEnv: 4, requests: 4, wantEnvs: 1},
		{name: "overflow adds one", perEnv: 4, requests: 5, wantEnvs: 2},
		{name: "serial environments", perEnv: 1, requests: 3, wantEnvs: 3},
		{name: "capped by max", perEnv: 1, requests: 2, maxEnv: aws.Int32(2), wantEnvs: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d, l := newScaledEnv(t, scaleOpts{perEnv: tc.perEnv, maxEnv: tc.maxEnv})
			wg := fire(t, d, l, tc.requests)

			assert.Equal(t, tc.wantEnvs, l.launches.Load())

			l.release()
			wg.Wait()
		})
	}
}

func TestScaling_ThrottlesAtMaxEnvironments(t *testing.T) {
	t.Parallel()

	d, l := newScaledEnv(t, scaleOpts{perEnv: 2, maxEnv: aws.Int32(2)})
	wg := fire(t, d, l, 4)

	resp := d.get(t)
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	assert.Equal(t, "Platform.EndpointMaxEnvThrottle", resp.Header.Get("Lambda-Web-Error-Type"))
	assert.Equal(t, int32(2), l.launches.Load())

	l.release()
	wg.Wait()
}

func TestScaling_ScaleToZeroAfterIdle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		advance    time.Duration
		held       bool
		wantClosed int32
	}{
		{name: "before the idle timeout", advance: time.Minute, wantClosed: 0},
		{name: "after the idle timeout", advance: 6 * time.Minute, wantClosed: 1},
		{name: "busy environment stays", advance: 6 * time.Minute, held: true, wantClosed: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d := newDataEnv(t)

			var wg *sync.WaitGroup

			if tc.held {
				l := &fakeLauncher{hold: make(chan struct{})}
				d.backend.SetLauncher(l)
				d.launcher = l
				wg = fire(t, d, l, 1)

				defer func() {
					l.release()
					wg.Wait()
				}()
			} else {
				require.Equal(t, http.StatusOK, d.get(t).StatusCode)
			}

			d.clock.Advance(tc.advance)
			d.backend.SweepEnvironments()

			assert.Eventually(t, func() bool { return d.launcher.closed.Load() == tc.wantClosed },
				5*time.Second, 5*time.Millisecond)
			assert.Equal(t, tc.wantClosed, d.launcher.closed.Load())
		})
	}
}

func TestScaling_ColdStartAfterScaleToZero(t *testing.T) {
	t.Parallel()

	d := newDataEnv(t)

	require.Equal(t, http.StatusOK, d.get(t).StatusCode)
	d.clock.Advance(6 * time.Minute)
	d.backend.SweepEnvironments()

	require.Equal(t, http.StatusOK, d.get(t).StatusCode)
	assert.Equal(t, int32(2), d.launcher.launches.Load(), "the next request cold-starts a new environment")
}

func TestScaling_IdleTimeoutIsConfigurable(t *testing.T) {
	t.Parallel()

	d := newDataEnv(t)
	d.backend.SetIdleTimeout(30 * time.Second)

	require.Equal(t, http.StatusOK, d.get(t).StatusCode)
	d.clock.Advance(31 * time.Second)
	d.backend.SweepEnvironments()

	assert.Eventually(t, func() bool { return d.launcher.closed.Load() == 1 }, 5*time.Second, 5*time.Millisecond)
}

func TestScaling_DrainsOnRevisionChange(t *testing.T) {
	t.Parallel()

	d, l := newScaledEnv(t, scaleOpts{perEnv: 8})
	wg := fire(t, d, l, 1)

	rev2 := routeToNewRevision(t, d)
	l.setHoldRev(d.rev1)

	d.backend.SweepEnvironments()
	assert.Equal(t, int32(0), l.closed.Load(), "the in-flight request keeps its environment alive")

	resp := d.get(t)
	assert.Equal(t, rev2, resp.Header.Get("X-App-Revision"), "new requests go to the new revision")
	assert.Equal(t, int32(2), l.launches.Load())

	l.release()
	wg.Wait()

	assert.Eventually(t, func() bool { return l.closed.Load() == 1 }, 5*time.Second, 5*time.Millisecond)
}

func TestScaling_DrainsUnroutedRevisionWithoutTraffic(t *testing.T) {
	t.Parallel()

	d := newDataEnv(t)
	require.Equal(t, http.StatusOK, d.get(t).StatusCode)

	routeToNewRevision(t, d)
	d.backend.SweepEnvironments()

	assert.Eventually(t, func() bool { return d.launcher.closed.Load() == 1 }, 5*time.Second, 5*time.Millisecond)
}

func routeToNewRevision(t *testing.T, d *dataEnv) string {
	t.Helper()

	rev2 := aws.ToString(d.createRevision(t).RevisionId)
	d.settle()

	_, err := d.client.UpdateWebFunctionEndpoint(t.Context(), &lwsdk.UpdateWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("prod"), RevisionWeights: weights(rev2, 100),
	})
	require.NoError(t, err)
	d.settle()

	return rev2
}
