package ociapigateway_test

import (
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type logRecord struct {
	data     map[string]any
	category string
	depID    string
}

type logCollector struct {
	records []logRecord
	mu      sync.Mutex
}

func (c *logCollector) sink(depID, category, _ string, _ time.Time, data map[string]any) {
	c.mu.Lock()
	c.records = append(c.records, logRecord{depID: depID, category: category, data: data})
	c.mu.Unlock()
}

func (c *logCollector) codes() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	var out []string

	for _, r := range c.records {
		if r.category == "execution" {
			out = append(out, r.data["code"].(string))
		}
	}

	return out
}

func (c *logCollector) access() []logRecord {
	c.mu.Lock()
	defer c.mu.Unlock()

	var out []logRecord

	for _, r := range c.records {
		if r.category == "access" {
			out = append(out, r)
		}
	}

	return out
}

func rateSpec(logging, routeLogging string) string {
	return `{` + logging + `"requestPolicies":{"rateLimiting":{"rateKey":"TOTAL","rateInRequestsPerSecond":1}},` +
		`"routes":[{"path":"/hi","methods":["GET"],` + routeLogging +
		`"backend":{"type":"STOCK_RESPONSE_BACKEND","status":200,"body":"ok"}}]}`
}

func TestDeploymentAccessLog(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	col := &logCollector{}
	e.backend.SetLogSink(col.sink)

	gw := e.mkGateway(t, "access-log")
	dep := e.mkDeployment(t, *gw.Id, "/p", stockSpec(200, "hello"))

	res := e.call(
		t,
		*gw.Id,
		http.MethodGet,
		"/p/hi",
		map[string]string{"Opc-Request-Id": "req-1", "User-Agent": "ua/1"},
		"",
	)
	require.Equal(t, http.StatusOK, res.status)

	got := col.access()
	require.Len(t, got, 1)
	assert.Equal(t, dep, got[0].depID)
	assert.Equal(t, "GET", got[0].data["httpMethod"])
	assert.Equal(t, 200, got[0].data["status"])
	assert.Equal(t, 5, got[0].data["bodyBytesSent"])
	assert.Equal(t, "req-1", got[0].data["opcRequestId"])
	assert.Equal(t, "ua/1", got[0].data["httpUserAgent"])
	assert.Equal(t, *gw.Id, got[0].data["gatewayId"])
	assert.Equal(t, dep, got[0].data["deploymentId"])
	assert.Equal(t, *gw.DisplayName, got[0].data["gatewayDisplayName"])
	assert.Equal(t, "/_apigw/"+*gw.Id+"/p/hi", got[0].data["requestUri"])
}

func TestDeploymentExecutionLogLevels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		logging string
		route   string
		want    []string
	}{
		{
			name: "default info", want: []string{"rateLimiting.requestPermitted", "rateLimiting.requestDenied"},
		},
		{
			name:    "deployment warn",
			logging: `"loggingPolicies":{"executionLog":{"isEnabled":true,"logLevel":"WARN"}},`,
			want:    []string{"rateLimiting.requestDenied"},
		},
		{
			name:    "deployment error",
			logging: `"loggingPolicies":{"executionLog":{"logLevel":"ERROR"}},`,
			want:    nil,
		},
		{
			name:    "route overrides deployment",
			logging: `"loggingPolicies":{"executionLog":{"logLevel":"ERROR"}},`,
			route:   `"loggingPolicies":{"executionLog":{"logLevel":"INFO"}},`,
			want:    []string{"rateLimiting.requestPermitted", "rateLimiting.requestDenied"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			col := &logCollector{}
			e.backend.SetLogSink(col.sink)

			gw := e.mkGateway(t, "exec-log")
			e.mkDeployment(t, *gw.Id, "/p", rateSpec(tt.logging, tt.route))

			assert.Equal(t, http.StatusOK, e.call(t, *gw.Id, http.MethodGet, "/p/hi", nil, "").status)
			assert.Equal(t, http.StatusTooManyRequests, e.call(t, *gw.Id, http.MethodGet, "/p/hi", nil, "").status)
			assert.Equal(t, tt.want, col.codes())
		})
	}
}

func TestExecutionLogLevelValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "log-validate")

	_, err := e.createDeployment(t, *gw.Id, "/p", specFromJSON(t,
		rateSpec(`"loggingPolicies":{"executionLog":{"logLevel":"DEBUG"}},`, "")))
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
}

func TestBackendExecutionLog(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	col := &logCollector{}
	e.backend.SetLogSink(col.sink)

	gw := e.mkGateway(t, "backend-log")
	e.mkDeployment(
		t,
		*gw.Id,
		"/p",
		`{"routes":[{"path":"/x","methods":["GET"],"backend":{"type":"HTTP_BACKEND","url":"http://127.0.0.1:1/"}}]}`,
	)

	assert.Equal(t, http.StatusBadGateway, e.call(t, *gw.Id, http.MethodGet, "/p/x", nil, "").status)
	assert.Equal(t, []string{
		"httpBackend.requestSent", "httpBackend.requestError",
	}, col.codes())
}
