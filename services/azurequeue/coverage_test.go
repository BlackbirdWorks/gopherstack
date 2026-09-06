package azurequeue_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
	"github.com/blackbirdworks/gopherstack/services/azurequeue"
)

// freeEphemeralPort returns a port that was free at the moment of the call
// (bound briefly via net.Listen("tcp", ":0") then released immediately).
// Mirrors services/azureblob's identical helper.
func freeEphemeralPort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	addr, ok := l.Addr().(*net.TCPAddr)
	require.True(t, ok)
	require.NoError(t, l.Close())

	return addr.Port
}

// reserveEphemeralPort binds and holds a real TCP port until the test ends,
// for tests that need a guaranteed-busy port to exercise a bind-failure path.
func reserveEphemeralPort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	addr, ok := l.Addr().(*net.TCPAddr)
	require.True(t, ok)

	return addr.Port
}

// TestRouteMatcher_AlwaysFalse pins RouteMatcher's documented contract:
// AzureQueue never participates in the shared AWS single-port Router.
func TestRouteMatcher_AlwaysFalse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/"+testAccount+"/q", http.NoBody)
	c := e.NewContext(req, httptest.NewRecorder())

	assert.False(t, matcher(c))
}

func TestMatchPriority_Lowest(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, 0, h.MatchPriority())
}

func TestExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{name: "list_queues", method: http.MethodGet, path: "/" + testAccount + "?comp=list", want: "ListQueues"},
		{name: "create_queue", method: http.MethodPut, path: "/" + testAccount + "/q", want: "CreateQueue"},
		{name: "delete_queue", method: http.MethodDelete, path: "/" + testAccount + "/q", want: "DeleteQueue"},
		{name: "put_message", method: http.MethodPost, path: "/" + testAccount + "/q/messages", want: "PutMessage"},
		{name: "get_messages", method: http.MethodGet, path: "/" + testAccount + "/q/messages", want: "GetMessages"},
		{
			name: "peek_messages", method: http.MethodGet,
			path: "/" + testAccount + "/q/messages?peekonly=true", want: "PeekMessages",
		},
		{
			name: "clear_messages", method: http.MethodDelete,
			path: "/" + testAccount + "/q/messages", want: "ClearMessages",
		},
		{
			name: "delete_message", method: http.MethodDelete,
			path: "/" + testAccount + "/q/messages/abc", want: "DeleteMessage",
		},
		{
			name: "update_message", method: http.MethodPut,
			path: "/" + testAccount + "/q/messages/abc", want: "UpdateMessage",
		},
		{name: "unknown", method: http.MethodOptions, path: "/" + testAccount + "?comp=list", want: "Unknown"},
	}

	h := newTestHandler(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, http.NoBody)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, h.ExtractOperation(c), tt.name)
		})
	}
}

func TestExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "queue_only", path: "/" + testAccount + "/myqueue", want: "myqueue"},
		{name: "queue_and_messages", path: "/" + testAccount + "/myqueue/messages", want: "myqueue/messages"},
		{
			name: "queue_and_message_id", path: "/" + testAccount + "/myqueue/messages/abc",
			want: "myqueue/messages/abc",
		},
	}

	h := newTestHandler(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, http.NoBody)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, h.ExtractResource(c), tt.name)
		})
	}
}

// TestCheckAuth_StructurallyValidHeaderAccepted covers checkAuth's third
// branch (a well-formed "SharedKey account:sig" header): parsing succeeds,
// and -- matching this milestone's permissive-by-default auth stance -- the
// request still proceeds normally rather than being rejected.
func TestCheckAuth_StructurallyValidHeaderAccepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/"+testAccount+"/myqueue", http.NoBody)
	req.Header.Set("Authorization", "SharedKey "+azureauth.DefaultAccountName+":c2lnbmF0dXJl")

	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	require.Equal(t, http.StatusCreated, rec.Code)
}

// TestCheckAuth_MalformedHeaderStillAccepted covers checkAuth's second
// branch: a present but structurally malformed Authorization header is
// logged, not rejected.
func TestCheckAuth_MalformedHeaderStillAccepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/"+testAccount+"/myqueue", http.NoBody)
	req.Header.Set("Authorization", "not-a-real-header")

	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	require.Equal(t, http.StatusCreated, rec.Code)
}

// TestStartWorker_BindsAndServes exercises the real synchronous bind: it
// starts the dedicated listener on a concrete port, makes a real HTTP
// request against it to prove the telemetry-wrapped handler is actually
// reachable, then shuts it down.
func TestStartWorker_BindsAndServes(t *testing.T) {
	t.Parallel()

	port := freeEphemeralPort(t)

	backend := azurequeue.NewInMemoryBackend()
	h := azurequeue.NewHandler(backend)
	h.Port = port

	ctx := t.Context()
	require.NoError(t, h.StartWorker(ctx))

	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		h.Shutdown(shutdownCtx)
	})

	url := fmt.Sprintf("http://127.0.0.1:%d/%s?comp=list", port, testAccount)

	require.Eventually(t, func() bool {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
		if reqErr != nil {
			return false
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		defer resp.Body.Close()

		return resp.StatusCode == http.StatusOK
	}, 2*time.Second, 10*time.Millisecond, "dedicated listener should become reachable")
}

// TestStartWorker_WithJanitorRuns exercises WithJanitor's non-no-op branch:
// StartWorker must launch the janitor goroutine without error or panic.
func TestStartWorker_WithJanitorRuns(t *testing.T) {
	t.Parallel()

	port := freeEphemeralPort(t)

	backend := azurequeue.NewInMemoryBackend()
	h := azurequeue.NewHandler(backend).WithJanitor(10 * time.Millisecond)
	h.Port = port

	ctx := t.Context()
	require.NoError(t, h.StartWorker(ctx))

	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		h.Shutdown(shutdownCtx)
	})
}

// TestWithJanitor_NonInMemoryBackendIsNoop covers WithJanitor's early-return
// branch when Backend isn't a concrete *InMemoryBackend.
func TestWithJanitor_NonInMemoryBackendIsNoop(t *testing.T) {
	t.Parallel()

	h := azurequeue.NewHandler(fakeBackend{})

	assert.NotPanics(t, func() {
		h.WithJanitor(time.Second)
	})
}

// TestStartWorker_BindFailureIsSynchronous is a regression test for the
// port-check race: binding an already-listening port must fail
// synchronously from StartWorker itself.
func TestStartWorker_BindFailureIsSynchronous(t *testing.T) {
	t.Parallel()

	port := reserveEphemeralPort(t)

	h := azurequeue.NewHandler(azurequeue.NewInMemoryBackend())
	h.Port = port

	err := h.StartWorker(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bind port")
}

// TestShutdown_NilServerIsNoop covers Shutdown's early return when
// StartWorker was never called (h.srv is nil).
func TestShutdown_NilServerIsNoop(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.NotPanics(t, func() {
		h.Shutdown(t.Context())
	})
}

// TestShutdown_ForcesCloseOnGracefulTimeout covers Shutdown's fallback path:
// an already-expired context makes srv.Shutdown return immediately with an
// error, forcing the srv.Close() fallback.
func TestShutdown_ForcesCloseOnGracefulTimeout(t *testing.T) {
	t.Parallel()

	h := azurequeue.NewHandler(azurequeue.NewInMemoryBackend())
	h.Port = 0

	require.NoError(t, h.StartWorker(t.Context()))

	expiredCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	assert.NotPanics(t, func() {
		h.Shutdown(expiredCtx)
	})

	// A second Shutdown call must also be a safe no-op (h.srv was cleared).
	assert.NotPanics(t, func() {
		h.Shutdown(t.Context())
	})
}

// fakeBackend is a minimal StorageBackend stub used only to exercise
// WithJanitor's type-assertion no-op branch; none of its methods are
// expected to be called.
type fakeBackend struct{}

func (fakeBackend) CreateQueue(string) (bool, error)   { return false, nil }
func (fakeBackend) DeleteQueue(string) error           { return nil }
func (fakeBackend) ListQueues() []azurequeue.QueueInfo { return nil }
func (fakeBackend) PutMessage(string, string, time.Duration, time.Duration) (azurequeue.MessageInfo, error) {
	return azurequeue.MessageInfo{}, nil
}
func (fakeBackend) GetMessages(string, int, time.Duration) ([]azurequeue.MessageInfo, error) {
	return nil, nil
}
func (fakeBackend) PeekMessages(string, int) ([]azurequeue.MessageInfo, error) { return nil, nil }
func (fakeBackend) DeleteMessage(string, string, string) error                 { return nil }
func (fakeBackend) UpdateMessage(
	string, string, string, time.Duration, *string,
) (azurequeue.MessageInfo, error) {
	return azurequeue.MessageInfo{}, nil
}
func (fakeBackend) ClearMessages(string) error { return nil }
func (fakeBackend) Reset()                     {}
