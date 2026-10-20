package app

import (
	"context"
	"errors"
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

var errPodFakeRestore = errors.New("boom")

type podFailState struct{ podFakeState }

func (f *podFailState) Restore(ctx context.Context, d []byte) error {
	if strings.Contains(string(d), "fail") {
		return errPodFakeRestore
	}

	return f.podFakeState.Restore(ctx, d)
}

type podFailService struct {
	service.Registerable
	st *podFailState
}

func (p podFailService) Reset() {
	p.st.podFakeState.mu.Lock()
	p.st.state = ""
	p.st.podFakeState.mu.Unlock()
}

type podRawState struct {
	raw string
	mu  sync.Mutex
}

func (f *podRawState) Snapshot(context.Context) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()

	return []byte(f.raw)
}

func (f *podRawState) Restore(_ context.Context, d []byte) error {
	f.set(string(d))

	return nil
}

func (f *podRawState) set(v string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.raw = v
}

func (f *podRawState) get() string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.raw
}

type podRawService struct {
	service.Registerable
	st *podRawState
}

func (p podRawService) Reset() { p.st.set("") }

type podHarness struct {
	e      *echo.Echo
	a, b   *podFakeState
	r      *podRawState
	failer *podFailState
}

func newPodHarness(t *testing.T) *podHarness {
	t.Helper()

	ctx := logger.Save(context.Background(), logger.NewTestLogger())
	h := &podHarness{a: &podFakeState{}, b: &podFakeState{}, r: &podRawState{}, failer: &podFailState{}}
	mgr := persistence.NewManager(ctx, persistence.NullStore{})
	mgr.Register("a", h.a)
	mgr.Register("b", h.b)
	mgr.Register("f", h.failer)
	mgr.Register("r", h.r)

	h.e = echo.New()
	svcs := []service.Registerable{
		podFakeService{st: h.a}, podFakeService{st: h.b},
		podFailService{st: h.failer}, podRawService{st: h.r},
	}
	registerPodEndpoints(ctx, logger.NewTestLogger(), h.e, PodsConfig{Dir: t.TempDir()}, "", mgr, svcs)

	return h
}

func TestPodLoadAtomic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		saved    string // service f state when saving
		wantCode int
	}{
		{name: "restore error rolls back", saved: "fail", wantCode: http.StatusInternalServerError},
		{name: "ok", saved: "fine", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newPodHarness(t)
			h.a.state, h.failer.state = "saved", tt.saved
			require.Equal(t, http.StatusCreated, podDo(h.e, http.MethodPost, "/_gopherstack/pods/p", "").Code)

			h.a.state, h.b.state, h.failer.state = "current", "other", "orig"

			rec := podDo(h.e, http.MethodPost, "/_gopherstack/pods/p/load", "")
			require.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			if tt.wantCode != http.StatusOK {
				assert.Equal(t, "current", h.a.get())
				assert.Equal(t, "other", h.b.get())
				assert.Equal(t, "orig", h.failer.get())
			}
		})
	}
}

func TestPodLoadCorruptSection(t *testing.T) {
	t.Parallel()

	h := newPodHarness(t)
	h.a.state, h.r.raw = "saved", `{corrupt`
	require.Equal(t, http.StatusCreated, podDo(h.e, http.MethodPost, "/_gopherstack/pods/p", "").Code)

	h.a.state, h.b.state, h.r.raw = "current", "other", `{"live":true}`

	rec := podDo(h.e, http.MethodPost, "/_gopherstack/pods/p/load", "")
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
	assert.Equal(t, "current", h.a.get())
	assert.Equal(t, "other", h.b.get())
	assert.JSONEq(t, `{"live":true}`, h.r.get())
}

func TestPodLoadMerge(t *testing.T) {
	t.Parallel()

	h := newPodHarness(t)
	scoped := func(r string) string { return `{"items":[{"accountID":"1","region":"` + r + `"}]}` }

	h.r.set(scoped("east"))
	h.b.state = "bval"
	require.Equal(t, http.StatusCreated, podDo(h.e, http.MethodPost, "/_gopherstack/pods/p", "").Code)

	h.r.set(scoped("west"))
	h.b.state = "changed"

	rec := podDo(h.e, http.MethodPost, "/_gopherstack/pods/p/load?merge=true", "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"merge":true`)
	assert.Contains(t, h.r.get(), `"east"`)
	assert.Contains(t, h.r.get(), `"west"`)
	assert.Equal(t, "bval", h.b.get())
}

func TestPodLocalstackAliases(t *testing.T) {
	t.Parallel()

	h := newPodHarness(t)
	h.a.state = "one"

	rec := podDo(h.e, http.MethodPost, "/_localstack/pods", `{"pod_name":"ls","services":["a"]}`)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"pod_name":"ls"`)

	rec = podDo(h.e, http.MethodGet, "/_localstack/pods", "")
	assert.Contains(t, rec.Body.String(), `"cloudpods"`)
	assert.Contains(t, rec.Body.String(), `"max_version":1`)

	h.a.state = "two"
	rec = podDo(h.e, http.MethodPatch, "/_localstack/pods", `{"pod_name":"ls"}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, "one", h.a.get())

	rec = podDo(h.e, http.MethodGet, "/_localstack/state/export", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"services"`)

	assert.Equal(t, http.StatusNoContent, podDo(h.e, http.MethodDelete, "/_localstack/pods/ls", "").Code)
	assert.Equal(t, http.StatusNotFound, podDo(h.e, http.MethodDelete, "/_localstack/pods/ls", "").Code)
	assert.Equal(t, http.StatusBadRequest, podDo(h.e, http.MethodPost, "/_localstack/pods", `{}`).Code)
}

func TestPodCLI(t *testing.T) {
	t.Parallel()

	h := newPodHarness(t)
	srv := httptest.NewServer(h.e)
	t.Cleanup(srv.Close)

	tgt := PodTarget{Endpoint: srv.URL}
	h.a.state = "one"

	require.NoError(t, (&PodSaveCmd{PodTarget: tgt, Name: "cli", Message: "m", Services: []string{"a"}}).Run())
	require.NoError(t, (&PodListCmd{PodTarget: tgt}).Run())
	require.NoError(t, (&PodVersionsCmd{PodTarget: tgt, Name: "cli"}).Run())
	require.NoError(t, (&PodInspectCmd{PodTarget: tgt, Name: "cli", Version: "1"}).Run())

	h.a.state = "two"

	require.NoError(t, (&PodLoadCmd{PodTarget: tgt, Name: "cli", Version: "latest", Strategy: "overwrite"}).Run())
	assert.Equal(t, "one", h.a.get())

	h.a.state = "two"

	require.NoError(
		t,
		(&PodLoadCmd{PodTarget: tgt, Name: "cli", Version: "1", Strategy: "service-merge", Merge: true}).Run(),
	)
	assert.Equal(t, "one", h.a.get())

	require.NoError(t, (&PodDeleteCmd{PodTarget: tgt, Name: "cli"}).Run())
	require.ErrorIs(t, (&PodInspectCmd{PodTarget: tgt, Name: "cli", Version: "1"}).Run(), errPodServer)
	require.ErrorIs(
		t,
		(&PodLoadCmd{PodTarget: tgt, Name: "cli", Version: "latest", Strategy: "overwrite"}).Run(),
		errPodServer,
	)
}
