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

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

type fakeMetrics struct {
	points []cwmetric.Point
	mu     sync.Mutex
}

func (m *fakeMetrics) EmitMetric(p cwmetric.Point) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.points = append(m.points, p)

	return nil
}

// sum totals the points named name in namespace ns.
func (m *fakeMetrics) sum(ns, name string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	var total float64

	for _, p := range m.points {
		if p.Namespace == ns && p.Name == name {
			total += p.Value
		}
	}

	return total
}

func (m *fakeMetrics) find(ns, name string) *cwmetric.Point {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range m.points {
		if m.points[i].Namespace == ns && m.points[i].Name == name {
			return &m.points[i]
		}
	}

	return nil
}

func dimMap(p *cwmetric.Point) map[string]string {
	out := map[string]string{}
	for _, d := range p.Dimensions {
		out[d.Name] = d.Value
	}

	return out
}

const (
	nsWeb   = "AWS/Lambda/Web"
	nsUsage = "AWS/Lambda/Web/Usage"
)

func newMetricsEnv(t *testing.T) (*dataEnv, *fakeMetrics) {
	t.Helper()

	d := newDataEnv(t)
	m := &fakeMetrics{}
	d.backend.SetMetricEmitter(m)

	return d, m
}

func TestMetrics_RequestMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		path         string
		wantExecuted float64
		want4xx      float64
		want5xx      float64
	}{
		{name: "ok", path: "/", wantExecuted: 1},
		{name: "app 404", path: "/missing", wantExecuted: 1, want4xx: 1},
		{name: "app 500", path: "/boom", wantExecuted: 1, want5xx: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d, m := newMetricsEnv(t)
			d.do(t, http.MethodGet, d.domain, tc.path, nil)

			assert.InDelta(t, tc.wantExecuted, m.sum(nsWeb, "ExecutedRequests"), 0)
			assert.InDelta(t, tc.wantExecuted, m.sum(nsUsage, "ExecutedRequests"), 0)
			assert.InDelta(t, tc.want4xx, m.sum(nsWeb, "HTTPResponseStatusClass4XX"), 0)
			assert.InDelta(t, tc.want5xx, m.sum(nsWeb, "HTTPResponseStatusClass5XX"), 0)
		})
	}
}

func TestMetrics_Dimensions(t *testing.T) {
	t.Parallel()

	d, m := newMetricsEnv(t)
	d.get(t)

	req := m.find(nsWeb, "ExecutedRequests")
	require.NotNil(t, req)
	assert.Equal(t, map[string]string{"FunctionName": "app", "EndpointName": "prod"}, dimMap(req))
	assert.Equal(t, "Count", req.Unit)
	assert.Equal(t, "us-east-1", req.Region)

	usage := m.find(nsUsage, "ExecutedRequests")
	require.NotNil(t, usage)
	assert.Equal(t, map[string]string{
		"FunctionName": "app", "EndpointName": "prod", "ExecutionRegion": "us-east-1",
	}, dimMap(usage))

	active := m.find(nsUsage, "EndpointActiveEnvironments")
	require.NotNil(t, active)
	assert.InDelta(t, 1, active.Value, 0)
}

func TestMetrics_SparseWhenNothingHappened(t *testing.T) {
	t.Parallel()

	d, m := newMetricsEnv(t)
	d.get(t)

	assert.Nil(t, m.find(nsWeb, "HTTPResponseStatusClass4XX"), "status-class metrics are sparse")
	assert.Nil(t, m.find(nsWeb, "HTTPResponseStatusClass5XX"))
	assert.Nil(t, m.find(nsUsage, "EndpointRequestThrottles"))
}

func TestMetrics_RateThrottle(t *testing.T) {
	t.Parallel()

	d, m := newMetricsEnv(t)

	_, err := d.client.UpdateWebFunctionEndpoint(t.Context(), &lwsdk.UpdateWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("prod"),
		ThrottleConfig: &types.ThrottleConfig{RateLimit: aws.Int32(100)},
	})
	require.NoError(t, err)
	d.settle()

	for range 101 {
		d.get(t)
	}

	assert.InDelta(t, 1, m.sum(nsUsage, "EndpointRequestThrottles"), 0)
	assert.InDelta(t, 1, m.sum(nsWeb, "HTTPResponseStatusClass4XX"), 0, "a throttle is a 4xx response")
	assert.InDelta(t, 100, m.sum(nsWeb, "ExecutedRequests"), 0, "throttled requests never reach an environment")
}

func TestMetrics_MaxEnvironmentsThrottle(t *testing.T) {
	t.Parallel()

	d, l := newScaledEnv(t, scaleOpts{perEnv: 1, maxEnv: aws.Int32(2)})
	m := &fakeMetrics{}
	d.backend.SetMetricEmitter(m)

	wg := fire(t, d, l, 2)

	require.Equal(t, http.StatusTooManyRequests, d.get(t).StatusCode)
	assert.InDelta(t, 1, m.sum(nsUsage, "EndpointActiveEnvironmentThrottles"), 0)
	assert.InDelta(t, 0, m.sum(nsUsage, "EndpointRequestThrottles"), 0, "a throttle counts against one limit only")

	l.release()
	wg.Wait()
}

func TestMetrics_DisabledEndpointIsA5xx(t *testing.T) {
	t.Parallel()

	d, m := newMetricsEnv(t)

	_, err := d.client.UpdateWebFunctionEndpoint(t.Context(), &lwsdk.UpdateWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("prod"),
		ThrottleConfig: &types.ThrottleConfig{RateLimit: aws.Int32(0)},
	})
	require.NoError(t, err)
	d.settle()

	d.get(t)
	assert.InDelta(t, 1, m.sum(nsWeb, "HTTPResponseStatusClass5XX"), 0)
	assert.InDelta(t, 0, m.sum(nsWeb, "ExecutedRequests"), 0)
}

func TestMetrics_EnvironmentMetrics(t *testing.T) {
	t.Parallel()

	d, m := newMetricsEnv(t)
	d.get(t)

	d.clock.Advance(61 * time.Second)
	d.backend.SweepEnvironments()

	active := m.find(nsWeb, "EnvironmentActiveTime")
	require.NotNil(t, active)
	assert.Equal(t, "Milliseconds", active.Unit)
	assert.GreaterOrEqual(t, active.Value, float64(61_000))
	assert.Equal(
		t,
		map[string]string{"FunctionName": "app"},
		dimMap(active),
		"environment metrics have no EndpointName",
	)

	conc := m.find(nsWeb, "EnvironmentConcurrentRequests")
	require.NotNil(t, conc)
	assert.InDelta(t, 1, conc.Value, 0)

	bytes := m.find(nsWeb, "ResponseBytes")
	require.NotNil(t, bytes)
	assert.Equal(t, "Bytes", bytes.Unit)
	assert.Positive(t, bytes.Value)
}
