package extensions

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type fakeRuntime struct {
	container.Runtime

	specs   chan container.Spec
	stopped chan string
	pulled  bool
	mu      sync.Mutex
}

func (f *fakeRuntime) HasImage(context.Context, string) (bool, error) { return false, nil }

func (f *fakeRuntime) PullImage(context.Context, string) error {
	f.mu.Lock()
	f.pulled = true
	f.mu.Unlock()

	return nil
}

func (f *fakeRuntime) CreateAndStart(_ context.Context, s container.Spec) (string, error) {
	f.specs <- s

	return "cid-1", nil
}

func (f *fakeRuntime) StopAndRemove(_ context.Context, id string) error {
	f.stopped <- id

	return nil
}

func TestContainerExtension(t *testing.T) {
	t.Parallel()

	rt := &fakeRuntime{specs: make(chan container.Spec, 1), stopped: make(chan string, 1)}
	cfg, err := Config{
		Extensions: []Spec{{Name: "box", Image: "img:1", Port: 7777, Args: []string{"serve"}}},
	}.normalize()
	require.NoError(t, err)

	m := New(cfg, Options{
		Logger:     logger.NewTestLogger(),
		ControlURL: "http://host.docker.internal:8000",
		NewRuntime: func() (container.Runtime, error) { return rt, nil },
	})

	m.Start(t.Context())

	var spec container.Spec

	select {
	case spec = <-rt.specs:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "container not started")
	}

	assert.Equal(t, "img:1", spec.Image)
	assert.Equal(t, []string{"serve"}, spec.Cmd)
	assert.Equal(t, []string{"127.0.0.1:7777:7777"}, spec.Ports)
	assert.True(t, slices.Contains(spec.Env, "GOPHERSTACK_CONTROL_URL=http://host.docker.internal:8000"))
	assert.True(t, slices.Contains(spec.Env, "GOPHERSTACK_EXTENSION_PORT=7777"))

	i := slices.IndexFunc(spec.Env, func(e string) bool { return strings.HasPrefix(e, "GOPHERSTACK_EXTENSION_TOKEN=") })
	require.GreaterOrEqual(t, i, 0)

	req := httptest.NewRequest(
		http.MethodPost,
		"/_gopherstack/extensions/register",
		strings.NewReader(`{"routes":{"path_prefixes":["/box"]}}`),
	)
	req.RemoteAddr = "172.17.0.3:4444"
	req.Header.Set(tokenHeader, strings.TrimPrefix(spec.Env[i], "GOPHERSTACK_EXTENSION_TOKEN="))

	rec := httptest.NewRecorder()
	m.ServeRegister(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Eventually(t, func() bool { return statusOf(m) == StatusReady }, 5*time.Second, 10*time.Millisecond)

	m.Stop(context.WithoutCancel(t.Context()))

	assert.Equal(t, "cid-1", <-rt.stopped)
	assert.Equal(t, StatusStopped, statusOf(m))

	rt.mu.Lock()
	defer rt.mu.Unlock()
	assert.True(t, rt.pulled)
}
