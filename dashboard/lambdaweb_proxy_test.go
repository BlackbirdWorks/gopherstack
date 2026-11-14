package dashboard_test

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

const (
	lwRoot  = "/dashboard/api/v1/lambdaweb"
	lwRole  = "arn:aws:iam::123456789012:role/web-role"
	lwReg   = "?region=us-east-1"
	lwCtl   = "/2025-03-07/web-functions"
	lwEnvKV = "DB_PASSWORD"
)

type lwCodes struct{}

func (lwCodes) ResolveObject(_ context.Context, bucket, _, _ string) (string, error) {
	if bucket != "code" {
		return "", lambdaweb.ErrBucketNotFound
	}

	return "v1", nil
}

func (lwCodes) GetObject(context.Context, string, string, string) ([]byte, error) {
	return []byte("zip"), nil
}

type lwEnv struct{ srv *httptest.Server }

func (e lwEnv) Addr() (string, int) {
	host, port, _ := net.SplitHostPort(e.srv.Listener.Addr().String())
	n, _ := strconv.Atoi(port)

	return host, n
}

func (e lwEnv) Close(context.Context) { e.srv.Close() }

// lwLauncher serves an in-process app in place of a container.
type lwLauncher struct{}

func (lwLauncher) Launch(context.Context, lambdaweb.LaunchSpec) (lambdaweb.Environment, error) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("X-App", "yes")

		if r.URL.Path == "/fail" {
			http.Error(w, "boom", http.StatusInternalServerError)

			return
		}

		_ = json.MarshalEncode(jsontext.NewEncoder(w, jsonx.V1()), map[string]string{
			"method": r.Method, "path": r.URL.RequestURI(), "body": string(body), "accept": r.Header.Get("Accept"),
		})
	}))

	return lwEnv{srv: srv}, nil
}

type lwFixture struct {
	h       *dashboard.DashboardHandler
	backend *lambdaweb.InMemoryBackend
}

func newLambdaWebFixture(t *testing.T, withApp bool) lwFixture {
	t.Helper()

	var now atomic.Int64

	now.Store(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC).UnixNano())

	backend := lambdaweb.NewInMemoryBackend("123456789012")
	backend.SetClock(func() time.Time { return time.Unix(0, now.Add(int64(time.Second))).UTC() })
	backend.SetCodeStore(lwCodes{})
	t.Cleanup(backend.Close)

	if withApp {
		backend.SetLauncher(lwLauncher{})
	}

	lh := lambdaweb.NewHandler(backend)
	lh.AccountID, lh.DefaultRegion = "123456789012", "us-east-1"

	ctl := echo.New()
	ctl.Any("/*", lh.Handler())

	revision := `{"buildConfig":{"codeConfig":{"s3Object":{"bucket":"code","key":"app.zip"}},` +
		`"runtimeConfig":{"runtime":"nodejs24.x"}},"serviceConfig":{"executionRoleArn":"` + lwRole + `",` +
		`"environmentVariables":{"` + lwEnvKV + `":"hunter2"}}}`

	create := `{"functionName":"app","revisionConfig":` + revision + `,"endpointConfig":{"endpointName":"prod",` +
		`"endpointType":"HomeRegion","authType":"ApplicationManaged"}}`

	req := httptest.NewRequest(http.MethodPut, lwCtl, strings.NewReader(create))
	req.Header.Set("X-Amz-Region", "us-east-1")

	rec := httptest.NewRecorder()
	ctl.ServeHTTP(rec, req)
	require.Less(t, rec.Code, http.StatusBadRequest, rec.Body.String())

	return lwFixture{h: dashboard.NewHandler(dashboard.Config{LambdaWebOps: lh}), backend: backend}
}

func lwGet(t *testing.T, f lwFixture, path string) map[string]any {
	t.Helper()

	var out map[string]any

	require.Eventually(t, func() bool {
		rec := lbDo(t, f.h, http.MethodGet, lwRoot+path, "")
		if rec.Code != http.StatusOK {
			return false
		}

		out = map[string]any{}

		return json.Unmarshal(rec.Body.Bytes(), &out, jsonx.V1()) == nil
	}, 5*time.Second, 5*time.Millisecond, path)

	return out
}

func lwActive(t *testing.T, f lwFixture) {
	t.Helper()

	require.Eventually(t, func() bool {
		ep := lwGet(t, f, "/functions/app/endpoints/prod"+lwReg)

		return ep["state"] == "Active"
	}, 5*time.Second, 5*time.Millisecond)
}

func TestLambdaWebProxyReads(t *testing.T) {
	t.Parallel()

	f := newLambdaWebFixture(t, false)

	fns := lwGet(t, f, "/functions"+lwReg)
	list, _ := fns["functions"].([]any)
	require.Len(t, list, 1)

	one := lwGet(t, f, "/functions/app"+lwReg)
	assert.Equal(t, "app", one["functionName"])

	revs := lwGet(t, f, "/functions/app/revisions"+lwReg)
	items, _ := revs["revisions"].([]any)
	require.Len(t, items, 1)

	rev, _ := items[0].(map[string]any)
	id, _ := rev["revisionId"].(string)
	require.NotEmpty(t, id)

	full := lwGet(t, f, "/functions/app/revisions/"+id+lwReg)
	assert.Contains(t, jsonText(t, full), "app.zip")

	eps := lwGet(t, f, "/functions/app/endpoints"+lwReg)
	list, _ = eps["endpoints"].([]any)
	require.Len(t, list, 1)
	assert.Contains(t, jsonText(t, list[0]), "domainName")
}

func jsonText(t *testing.T, v any) string {
	t.Helper()

	raw, err := json.Marshal(v, jsonx.V1())
	require.NoError(t, err)

	return string(raw)
}

func TestLambdaWebProxyHidesEnvironmentValues(t *testing.T) {
	t.Parallel()

	f := newLambdaWebFixture(t, false)

	revs := lwGet(t, f, "/functions/app/revisions"+lwReg)
	items, _ := revs["revisions"].([]any)
	rev, _ := items[0].(map[string]any)

	rec := lbDo(t, f.h, http.MethodGet, lwRoot+"/functions/app/revisions/"+rev["revisionId"].(string)+lwReg, "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), lwEnvKV, "names stay visible")
	assert.NotContains(t, rec.Body.String(), "hunter2", "values never reach the browser")
}

func TestLambdaWebProxyRejectsBadRequests(t *testing.T) {
	t.Parallel()

	f := newLambdaWebFixture(t, false)

	tests := []struct {
		name   string
		method string
		path   string
		want   int
	}{
		{"bad region", http.MethodGet, "/functions?region=US%20EAST", http.StatusBadRequest},
		{"bad token", http.MethodGet, "/functions?nextToken=%3Cscript%3E", http.StatusBadRequest},
		{"bad function name", http.MethodGet, "/functions/a%20b", http.StatusBadRequest},
		{"bad revision name", http.MethodGet, "/functions/app/revisions/a%2Fb", http.StatusBadRequest},
		{"delete function", http.MethodDelete, "/functions/app", http.StatusNotFound},
		{"put function", http.MethodPut, "/functions", http.StatusNotFound},
		{"post revisions", http.MethodPost, "/functions/app/revisions", http.StatusNotFound},
		{"delete endpoint", http.MethodDelete, "/functions/app/endpoints/prod", http.StatusNotFound},
		{"patch endpoint", http.MethodPatch, "/functions/app/endpoints/prod", http.StatusNotFound},
		{"resource policy", http.MethodGet, "/resource-policy/x", http.StatusNotFound},
		{"unknown function", http.MethodGet, "/functions/ghost" + lwReg, http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := lbDo(t, f.h, tt.method, lwRoot+tt.path, `{}`)
			assert.Contains(t, []int{tt.want, http.StatusMethodNotAllowed}, rec.Code, rec.Body.String())
		})
	}

	still := lwGet(t, f, "/functions"+lwReg)
	list, _ := still["functions"].([]any)
	assert.Len(t, list, 1, "nothing was removed")
}

func lwInvoke(t *testing.T, f lwFixture, body string) *httptest.ResponseRecorder {
	t.Helper()

	return lbDo(t, f.h, http.MethodPost, lwRoot+"/invoke", body)
}

func TestLambdaWebInvokeReachesTheApp(t *testing.T) {
	t.Parallel()

	f := newLambdaWebFixture(t, true)
	lwActive(t, f)

	rec := lwInvoke(
		t,
		f,
		`{"region":"us-east-1","function":"app","endpoint":"prod","method":"POST","path":"/hello?x=1",`+
			`"headers":[{"name":"Accept","value":"application/json"}],"body":"ping"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out struct {
		Headers  map[string][]string `json:"headers"`
		Body     string              `json:"body"`
		Encoding string              `json:"encoding"`
		Domain   string              `json:"domain"`
		Status   int                 `json:"status"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out, jsonx.V1()))
	assert.Equal(t, http.StatusOK, out.Status)
	assert.Equal(t, "text", out.Encoding)
	assert.Contains(t, out.Domain, ".lambda-web.")
	assert.Contains(t, out.Body, `"path":"/hello?x=1"`)
	assert.Contains(t, out.Body, `"body":"ping"`)
	assert.Contains(t, out.Body, `"accept":"application/json"`)
	assert.Equal(t, []string{"yes"}, out.Headers["X-App"])

	failing := lwInvoke(t, f, `{"function":"app","endpoint":"prod","method":"GET","path":"/fail"}`)
	require.Equal(t, http.StatusOK, failing.Code)
	assert.Contains(t, failing.Body.String(), `"status":500`)
}

func TestLambdaWebInvokeWithoutAnEngineShowsThePlatformError(t *testing.T) {
	t.Parallel()

	f := newLambdaWebFixture(t, false)
	lwActive(t, f)

	rec := lwInvoke(t, f, `{"function":"app","endpoint":"prod","method":"GET","path":"/"}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"status":503`)
}

func TestLambdaWebInvokeValidation(t *testing.T) {
	t.Parallel()

	f := newLambdaWebFixture(t, true)
	lwActive(t, f)

	long := strings.Repeat("a", 2100)

	tests := []struct {
		name string
		body string
		want int
	}{
		{"unknown field", `{"function":"app","endpoint":"prod","method":"GET","path":"/","host":"evil"}`, 400},
		{"bad method", `{"function":"app","endpoint":"prod","method":"TRACE","path":"/"}`, 400},
		{"relative path", `{"function":"app","endpoint":"prod","method":"GET","path":"hello"}`, 400},
		{"scheme relative path", `{"function":"app","endpoint":"prod","method":"GET","path":"//evil.example/x"}`, 400},
		{"space in path", `{"function":"app","endpoint":"prod","method":"GET","path":"/a b"}`, 400},
		{"long path", `{"function":"app","endpoint":"prod","method":"GET","path":"/` + long + `"}`, 400},
		{"host header", `{"function":"app","endpoint":"prod","method":"GET","path":"/",` +
			`"headers":[{"name":"Host","value":"x"}]}`, 400},
		{"content length header", `{"function":"app","endpoint":"prod","method":"GET","path":"/",` +
			`"headers":[{"name":"content-length","value":"1"}]}`, 400},
		{"header injection", `{"function":"app","endpoint":"prod","method":"GET","path":"/",` +
			`"headers":[{"name":"X-A","value":"a\r\nX-B: b"}]}`, 400},
		{"bad header name", `{"function":"app","endpoint":"prod","method":"GET","path":"/",` +
			`"headers":[{"name":"X A","value":"a"}]}`, 400},
		{"bad function", `{"function":"a b","endpoint":"prod","method":"GET","path":"/"}`, 400},
		{"unknown endpoint", `{"function":"app","endpoint":"ghost","method":"GET","path":"/"}`, 404},
		{"unknown function", `{"function":"ghost","endpoint":"prod","method":"GET","path":"/"}`, 404},
		{"not json", `method=GET`, 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := lwInvoke(t, f, tt.body)
			assert.Equal(t, tt.want, rec.Code, rec.Body.String())
		})
	}
}

func TestLambdaWebAbsentBackendAnswers503(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})

	for _, tt := range []struct{ name, method, path string }{
		{"list", http.MethodGet, "/functions"},
		{"get", http.MethodGet, "/functions/app"},
		{"revisions", http.MethodGet, "/functions/app/revisions"},
		{"endpoints", http.MethodGet, "/functions/app/endpoints"},
		{"invoke", http.MethodPost, "/invoke"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := lbDo(t, h, tt.method, lwRoot+tt.path, `{}`)
			assert.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
		})
	}
}
