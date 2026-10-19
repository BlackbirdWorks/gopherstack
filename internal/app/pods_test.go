package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type podFakeState struct {
	state string
	mu    sync.Mutex
}

func (f *podFakeState) Snapshot(context.Context) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.state == "" {
		return nil
	}

	return []byte(`"` + f.state + `"`)
}

func (f *podFakeState) Restore(_ context.Context, d []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state = strings.Trim(string(d), `"`)

	return nil
}

func (f *podFakeState) get() string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.state
}

type podFakeService struct {
	service.Registerable
	st *podFakeState
}

func (p podFakeService) Reset() {
	p.st.mu.Lock()
	defer p.st.mu.Unlock()
	p.st.state = ""
}

func podDo(e *echo.Echo, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	return rec
}

func TestPodEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy string
		// wantA/wantB are the states of services a and b after loading v1 (a=one,b=two)
		// into an emulator currently holding a=changed, b=extra, c=keep.
		wantA, wantB, wantC string
	}{
		{name: "overwrite", strategy: "overwrite", wantA: "one", wantB: "two", wantC: ""},
		{name: "service-merge", strategy: "service-merge", wantA: "one", wantB: "two", wantC: "keep"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := logger.Save(context.Background(), logger.NewTestLogger())
			a, b, c := &podFakeState{}, &podFakeState{}, &podFakeState{}
			mgr := persistence.NewManager(ctx, persistence.NullStore{})
			mgr.Register("a", a)
			mgr.Register("b", b)
			mgr.Register("c", c)

			e := echo.New()
			registerPodEndpoints(ctx, logger.NewTestLogger(), e, PodsConfig{Dir: t.TempDir()}, "",
				mgr, []service.Registerable{podFakeService{st: a}, podFakeService{st: b}, podFakeService{st: c}})

			a.state, b.state = "one", "two"
			rec := podDo(e, http.MethodPost, "/_gopherstack/pods/demo", `{"message":"m","services":["a","b"]}`)
			require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), `"version":1`)

			a.state, b.state, c.state = "changed", "extra", "keep"

			rec = podDo(e, http.MethodPost, "/_gopherstack/pods/demo/load?strategy="+tt.strategy, "")
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Equal(t, tt.wantA, a.get())
			assert.Equal(t, tt.wantB, b.get())
			assert.Equal(t, tt.wantC, c.get())

			rec = podDo(e, http.MethodGet, "/_gopherstack/pods", "")
			assert.Contains(t, rec.Body.String(), `"name":"demo"`)

			rec = podDo(e, http.MethodGet, "/_gopherstack/pods/demo/versions/latest", "")
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), `"message":"m"`)

			rec = podDo(e, http.MethodDelete, "/_gopherstack/pods/demo", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			rec = podDo(e, http.MethodGet, "/_gopherstack/pods/demo", "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestPodEndpointErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, method, path, body string
		want                     int
	}{
		{
			"bad strategy",
			http.MethodPost,
			"/_gopherstack/pods/x/load?strategy=account-region-merge",
			"",
			http.StatusBadRequest,
		},
		{"bad version", http.MethodPost, "/_gopherstack/pods/x/load?version=abc", "", http.StatusBadRequest},
		{"unknown pod", http.MethodPost, "/_gopherstack/pods/x/load", "", http.StatusNotFound},
		{"bad name", http.MethodPost, "/_gopherstack/pods/.bad", "", http.StatusBadRequest},
		{"unknown service", http.MethodPost, "/_gopherstack/pods/x", `{"services":["zzz"]}`, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := logger.Save(context.Background(), logger.NewTestLogger())
			e := echo.New()
			registerPodEndpoints(ctx, logger.NewTestLogger(), e, PodsConfig{Dir: t.TempDir()}, "",
				persistence.NewManager(ctx, persistence.NullStore{}), nil)

			assert.Equal(t, tt.want, podDo(e, tt.method, tt.path, tt.body).Code)
		})
	}
}

func TestPodAutoload(t *testing.T) {
	t.Parallel()

	ctx := logger.Save(context.Background(), logger.NewTestLogger())
	dir := t.TempDir()
	a := &podFakeState{state: "saved"}
	mgr := persistence.NewManager(ctx, persistence.NullStore{})
	mgr.Register("a", a)

	e := echo.New()
	registerPodEndpoints(ctx, logger.NewTestLogger(), e, PodsConfig{Dir: dir}, "", mgr, nil)
	require.Equal(t, http.StatusCreated, podDo(e, http.MethodPost, "/_gopherstack/pods/boot", "").Code)

	a.state = "dirty"

	registerPodEndpoints(
		ctx,
		logger.NewTestLogger(),
		echo.New(),
		PodsConfig{Dir: dir, Autoload: "boot:1"},
		"",
		mgr,
		nil,
	)
	assert.Equal(t, "saved", a.get())
}

func TestPodServicesCaseInsensitive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		saveServices string
		loadServices string
	}{
		{name: "lower-case save and load", saveServices: `["dynamodb"]`, loadServices: "dynamodb"},
		{name: "exact case", saveServices: `["DynamoDB"]`, loadServices: "DynamoDB"},
		{name: "upper-case", saveServices: `["S3"]`, loadServices: "s3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := logger.Save(context.Background(), logger.NewTestLogger())
			ddb, s3 := &podFakeState{}, &podFakeState{}
			mgr := persistence.NewManager(ctx, persistence.NullStore{})
			mgr.Register("DynamoDB", ddb)
			mgr.Register("S3", s3)

			e := echo.New()
			registerPodEndpoints(ctx, logger.NewTestLogger(), e, PodsConfig{Dir: t.TempDir()}, "",
				mgr, []service.Registerable{podFakeService{st: ddb}, podFakeService{st: s3}})

			ddb.state, s3.state = "d", "s"
			rec := podDo(e, http.MethodPost, "/_gopherstack/pods/demo", `{"services":`+tt.saveServices+`}`)
			require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

			ddb.state, s3.state = "x", "y"
			rec = podDo(e, http.MethodPost,
				"/_gopherstack/pods/demo/load?strategy=service-merge&services="+tt.loadServices, "")
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		})
	}
}
