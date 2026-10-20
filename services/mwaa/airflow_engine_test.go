package mwaa_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

const healthyBody = `{"metadatabase":{"status":"healthy"},"scheduler":{"status":"healthy"}}`

type fakeRuntime struct {
	execs   map[string]container.ExecResult
	started []container.Spec
	removed []string
	cmds    [][]string
	mu      sync.Mutex
}

func (r *fakeRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.started = append(r.started, spec)

	return "ctr-" + string(rune('a'+len(r.started)-1)), nil
}

func (r *fakeRuntime) StopAndRemove(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.removed = append(r.removed, id)

	return nil
}

func (r *fakeRuntime) Exec(_ context.Context, _ string, cmd []string) (container.ExecResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.cmds = append(r.cmds, cmd)

	return r.execs[strings.Join(cmd, " ")], nil
}

func (r *fakeRuntime) startedSpecs() []container.Spec {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]container.Spec(nil), r.started...)
}

func (r *fakeRuntime) removedIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.removed...)
}

type fakeAirflowHTTP struct {
	lastURL string
	healthy atomic.Bool
	mu      sync.Mutex
}

func (f *fakeAirflowHTTP) Do(req *http.Request) (*http.Response, error) {
	f.mu.Lock()
	f.lastURL = req.Method + " " + req.URL.Path + "?" + req.URL.RawQuery
	f.mu.Unlock()

	switch {
	case req.URL.Path == "/health" && !f.healthy.Load():
		return fakeResponse(http.StatusServiceUnavailable, ""), nil
	case req.URL.Path == "/health":
		return fakeResponse(http.StatusOK, healthyBody), nil
	case req.URL.Path == "/api/v1/dags":
		return fakeResponse(http.StatusOK, `{"dags":[{"dag_id":"hello"}],"total_entries":1}`), nil
	}

	return fakeResponse(http.StatusNotFound, ""), nil
}

func (f *fakeAirflowHTTP) last() string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.lastURL
}

func fakeResponse(code int, body string) *http.Response {
	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}
}

type fakeObjects struct {
	files map[string]string
	mu    sync.Mutex
}

func (o *fakeObjects) set(key, val string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.files[key] = val
}

func (o *fakeObjects) List(_ context.Context, _, prefix string) ([]string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	var keys []string

	for k := range o.files {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}

	return keys, nil
}

func (o *fakeObjects) Get(_ context.Context, _, key, _ string) ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	return []byte(o.files[key]), nil
}

type airflowRig struct {
	b  *mwaa.InMemoryBackend
	h  *mwaa.Handler
	rt *fakeRuntime
	ht *fakeAirflowHTTP
	ob *fakeObjects
}

func newAirflowRig(t *testing.T) *airflowRig {
	t.Helper()

	rig := &airflowRig{
		rt: &fakeRuntime{execs: map[string]container.ExecResult{
			"airflow dags list": {Stdout: "dag_id | hello\n", Stderr: "warn\n"},
		}},
		ht: &fakeAirflowHTTP{},
		ob: &fakeObjects{files: map[string]string{
			"dags/hello.py":       "# dag hello",
			"dags/sub/other.py":   "# other",
			"requirements/req.tx": "apache-airflow-providers-amazon",
		}},
	}
	rig.b = mwaa.NewInMemoryBackend(testRegion, testAccountID)
	rig.b.EnableAirflow(mwaa.AirflowConfig{
		Runtime:      rig.rt,
		HTTP:         rig.ht,
		Objects:      rig.ob,
		EndpointURL:  "http://host.docker.internal:4566",
		PollInterval: 5 * time.Millisecond,
		SyncInterval: 10 * time.Millisecond,
		StartTimeout: 10 * time.Second,
	})
	t.Cleanup(rig.b.Close)

	rig.h = mwaa.NewHandler(rig.b)
	rig.h.AccountID = testAccountID
	rig.h.DefaultRegion = testRegion

	return rig
}

func (r *airflowRig) create(t *testing.T, name string, extra map[string]any) {
	t.Helper()

	body := map[string]any{
		"DagS3Path":            "dags",
		"ExecutionRoleArn":     "arn:aws:iam::123456789012:role/r",
		"SourceBucketArn":      "arn:aws:s3:::dag-bucket",
		"AirflowVersion":       "2.10.3",
		"NetworkConfiguration": networkConfigBody(),
	}
	maps.Copy(body, extra)

	rec := doMWAARequest(t, r.h, http.MethodPut, "/environments/"+name, body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func (r *airflowRig) status(t *testing.T, name string) string {
	t.Helper()

	env, err := r.b.GetEnvironment(t.Context(), name)
	require.NoError(t, err)

	return env.Status
}

func (r *airflowRig) waitStatus(t *testing.T, name, want string) {
	t.Helper()

	require.Eventually(t, func() bool { return r.status(t, name) == want }, 10*time.Second, 5*time.Millisecond)
}

func (r *airflowRig) createAvailable(t *testing.T, name string) {
	t.Helper()

	r.create(t, name, nil)
	assert.Equal(t, "CREATING", r.status(t, name))
	r.ht.healthy.Store(true)
	r.waitStatus(t, name, "AVAILABLE")
}

func rawRequest(
	t *testing.T, h *mwaa.Handler, method, path, body string, hdr map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, bytes.NewReader([]byte(body)))
	req.Host = "gopherstack.test:4566"

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

	return rec
}

func TestAirflowEngine_LifecycleAndSync(t *testing.T) {
	t.Parallel()

	rig := newAirflowRig(t)
	rig.create(t, "life", map[string]any{
		"AirflowConfigurationOptions": map[string]string{"core.dag_concurrency": "7", "bogus": "x"},
		"RequirementsS3Path":          "requirements/req.tx",
		"RequirementsS3ObjectVersion": "v1",
	})

	assert.Equal(t, "CREATING", rig.status(t, "life"))
	require.Eventually(t, func() bool { return len(rig.rt.startedSpecs()) == 1 }, 10*time.Second, 5*time.Millisecond)
	assert.Equal(t, "CREATING", rig.status(t, "life"), "unhealthy webserver must hold CREATING")

	rig.ht.healthy.Store(true)
	rig.waitStatus(t, "life", "AVAILABLE")

	spec := rig.rt.startedSpecs()[0]
	assert.Equal(t, "apache/airflow:2.10.3", spec.Image)
	assert.Contains(t, spec.Env, "AIRFLOW__CORE__DAG_CONCURRENCY=7")
	assert.Contains(t, spec.Env, "AWS_ENDPOINT_URL=http://host.docker.internal:4566")
	assert.Contains(t, spec.Env, "AWS_ACCESS_KEY_ID=test")
	assert.Contains(t, spec.Env, "AIRFLOW__CORE__LOAD_EXAMPLES=False")
	assert.NotContains(t, strings.Join(spec.Env, ","), "BOGUS")

	hostDags := strings.TrimSuffix(strings.SplitN(spec.Mounts[0], ":", 2)[0], "")
	assert.Equal(t, "/opt/airflow/dags", strings.SplitN(spec.Mounts[0], ":", 2)[1])
	got, err := os.ReadFile(filepath.Join(hostDags, "hello.py"))
	require.NoError(t, err)
	assert.Equal(t, "# dag hello", string(got))

	reqs, err := os.ReadFile(filepath.Join(filepath.Dir(hostDags), "requirements.txt"))
	require.NoError(t, err)
	assert.Equal(t, "apache-airflow-providers-amazon", string(reqs))

	env, err := rig.b.GetEnvironment(t.Context(), "life")
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(env.WebserverURL, "http://127.0.0.1:"), env.WebserverURL)

	rig.ob.set("dags/new.py", "# new dag")
	require.Eventually(t, func() bool {
		_, rerr := os.Stat(filepath.Join(hostDags, "new.py"))

		return rerr == nil
	}, 10*time.Second, 5*time.Millisecond)

	_, err = rig.b.DeleteEnvironment(t.Context(), "life")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return mwaa.EnvironmentCount(rig.b) == 0 }, 10*time.Second, 5*time.Millisecond)
	assert.Equal(t, []string{"ctr-a"}, rig.rt.removedIDs())
	_, statErr := os.Stat(hostDags)
	assert.True(t, os.IsNotExist(statErr))
}

func TestAirflowEngine_Update(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		wantStarts  int
		wantRemoved int
	}{
		{name: "resync_only", body: map[string]any{"MaxWorkers": 5}, wantStarts: 1},
		{
			name:        "config_restart",
			body:        map[string]any{"AirflowConfigurationOptions": map[string]string{"core.parallelism": "3"}},
			wantStarts:  2,
			wantRemoved: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rig := newAirflowRig(t)
			rig.createAvailable(t, "upd")
			rig.ob.set("dags/added.py", "# added")

			rec := doMWAARequest(t, rig.h, http.MethodPatch, "/environments/upd", tt.body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Equal(t, "UPDATING", rig.status(t, "upd"))
			rig.waitStatus(t, "upd", "AVAILABLE")

			specs := rig.rt.startedSpecs()
			assert.Len(t, specs, tt.wantStarts)
			assert.Len(t, rig.rt.removedIDs(), tt.wantRemoved)

			dags, _, _ := strings.Cut(specs[len(specs)-1].Mounts[0], ":")
			_, err := os.Stat(filepath.Join(dags, "added.py"))
			require.NoError(t, err)
		})
	}
}

func TestAirflowEngine_CLIAndTokens(t *testing.T) {
	t.Parallel()

	rig := newAirflowRig(t)
	rig.createAvailable(t, "cli")

	rec := rawRequest(t, rig.h, http.MethodPost, "/clitoken/cli", "", map[string]string{
		"Authorization": "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/airflow/aws4_request",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var tok map[string]string

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tok))
	assert.NotEmpty(t, tok["CliToken"])

	tests := []struct {
		name       string
		token      string
		body       string
		wantStatus int
	}{
		{name: "ok", token: tok["CliToken"], body: "dags list", wantStatus: http.StatusOK},
		{name: "bad_token", token: "nope", body: "dags list", wantStatus: http.StatusForbidden},
		{name: "empty_command", token: tok["CliToken"], body: "  ", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := rawRequest(t, rig.h, http.MethodPost, "/aws_mwaa/cli", tt.body,
				map[string]string{"Authorization": "Bearer " + tt.token})
			require.Equal(t, tt.wantStatus, out.Code, out.Body.String())

			if tt.wantStatus != http.StatusOK {
				return
			}

			var resp map[string]string

			require.NoError(t, json.Unmarshal(out.Body.Bytes(), &resp))
			stdout, err := base64.StdEncoding.DecodeString(resp["stdout"])
			require.NoError(t, err)
			stderr, err := base64.StdEncoding.DecodeString(resp["stderr"])
			require.NoError(t, err)
			assert.Equal(t, "dag_id | hello\n", string(stdout))
			assert.Equal(t, "warn\n", string(stderr))
		})
	}

	assert.Equal(t, "gopherstack.test:4566", tok["WebServerHostname"])
}

func TestAirflowEngine_WebLoginAndRest(t *testing.T) {
	t.Parallel()

	rig := newAirflowRig(t)
	rig.createAvailable(t, "web")

	rec := doMWAARequest(t, rig.h, http.MethodPost, "/webtoken/web", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var tok map[string]string

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tok))

	env, err := rig.b.GetEnvironment(t.Context(), "web")
	require.NoError(t, err)

	ssoPath := "/aws_mwaa/aws-console-sso?login=true&token=" + tok["WebToken"]
	redirect := rawRequest(t, rig.h, http.MethodGet, ssoPath, "", nil)
	assert.Equal(t, http.StatusFound, redirect.Code)
	assert.Equal(t, env.WebserverURL, redirect.Header().Get("Location"))

	denied := rawRequest(t, rig.h, http.MethodGet, "/aws_mwaa/aws-console-sso?login=true&token=bad", "", nil)
	assert.Equal(t, http.StatusForbidden, denied.Code)

	page := rawRequest(t, rig.h, http.MethodGet, "/aws_mwaa/aws-console-sso?login=true", "", nil)
	assert.Equal(t, http.StatusOK, page.Code)

	rest := doMWAARequest(t, rig.h, http.MethodPost, "/restapi/web", map[string]any{
		"Method": "GET", "Path": "/dags", "QueryParameters": map[string]any{"limit": 5},
	})
	require.Equal(t, http.StatusOK, rest.Code, rest.Body.String())
	assert.JSONEq(t, `{"RestApiStatusCode":200,"RestApiResponse":{"dags":[{"dag_id":"hello"}],"total_entries":1}}`,
		rest.Body.String())
	assert.Equal(t, "GET /api/v1/dags?limit=5", rig.ht.last())
}

func TestAirflowEngine_Disabled(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	hdr := map[string]string{"Authorization": "Bearer x"}
	out := rawRequest(t, h, http.MethodPost, "/aws_mwaa/cli", "dags list", hdr)
	assert.Equal(t, http.StatusNotFound, out.Code)
}
