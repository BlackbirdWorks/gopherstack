package azureblob_test

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
	"github.com/blackbirdworks/gopherstack/services/azureblob"
)

// freeEphemeralPort returns a port that was free at the moment of the call
// (bound briefly via net.Listen("tcp", ":0") then released immediately).
// Used only to obtain a concrete port number for StartWorker to bind in
// tests; the resulting single-process TOCTOU race is an accepted tradeoff
// for test setup, not something StartWorker itself does (see its doc
// comment -- StartWorker's own net.Listen is the real, synchronous bind).
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
// AzureBlob never participates in the shared AWS single-port Router (see
// provider.go's Provider doc comment) -- it exists only so *Handler
// satisfies service.Registerable.
func TestRouteMatcher_AlwaysFalse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/"+testAccount+"/c", http.NoBody)
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
		{name: "list_containers", method: http.MethodGet, path: "/" + testAccount + "?comp=list", want: "ListContainers"},
		{
			name: "create_container", method: http.MethodPut,
			path: "/" + testAccount + "/c?restype=container", want: "CreateContainer",
		},
		{
			name: "delete_container", method: http.MethodDelete,
			path: "/" + testAccount + "/c?restype=container", want: "DeleteContainer",
		},
		{
			name: "list_blobs", method: http.MethodGet,
			path: "/" + testAccount + "/c?restype=container&comp=list", want: "ListBlobs",
		},
		{name: "put_blob", method: http.MethodPut, path: "/" + testAccount + "/c/b", want: "PutBlob"},
		{name: "get_blob", method: http.MethodGet, path: "/" + testAccount + "/c/b", want: "GetBlob"},
		{name: "get_blob_properties", method: http.MethodHead, path: "/" + testAccount + "/c/b", want: "GetBlobProperties"},
		{name: "delete_blob", method: http.MethodDelete, path: "/" + testAccount + "/c/b", want: "DeleteBlob"},
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
		{name: "container_only", path: "/" + testAccount + "/mycontainer", want: "mycontainer"},
		{name: "container_and_blob", path: "/" + testAccount + "/mycontainer/myblob", want: "mycontainer/myblob"},
		{
			name: "blob_name_with_slashes", path: "/" + testAccount + "/mycontainer/logs/2026/09.txt",
			want: "mycontainer/logs/2026/09.txt",
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

	rec := doRequest(t, h, http.MethodPut, "/"+testAccount+"/mycontainer?restype=container", nil, map[string]string{
		"Authorization": "SharedKey " + azureauth.DefaultAccountName + ":c2lnbmF0dXJl",
	})

	require.Equal(t, http.StatusCreated, rec.Code)
}

// TestCreateContainer_AlreadyExists covers createContainer's
// ErrContainerAlreadyExists branch (409 Conflict).
func TestCreateContainer_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createContainer(t, h, "dupe")

	rec := doRequest(t, h, http.MethodPut, "/"+testAccount+"/dupe?restype=container", nil, nil)

	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "ContainerAlreadyExists")
}

// TestStartWorker_BindsAndServes exercises the real synchronous bind added
// to fix the port-check race (net.Listen happens in StartWorker itself, not
// a separate probe-then-close step): it starts the dedicated listener on a
// concrete port, makes a real HTTP request against it to prove the
// telemetry-wrapped handler (logger.EchoMiddleware + telemetry.WrapEchoHandler,
// see StartWorker) is actually reachable, then shuts it down.
func TestStartWorker_BindsAndServes(t *testing.T) {
	t.Parallel()

	port := freeEphemeralPort(t)

	backend := azureblob.NewInMemoryBackend()
	h := azureblob.NewHandler(backend)
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

// TestStartWorker_BindFailureIsSynchronous is a regression test for the
// port-check race: binding an already-listening port must fail
// synchronously from StartWorker itself, not silently report success and
// only log the failure later from the background goroutine.
func TestStartWorker_BindFailureIsSynchronous(t *testing.T) {
	t.Parallel()

	// reserveEphemeralPort holds the port open for the rest of the test, so
	// h.StartWorker below is guaranteed to find it busy.
	port := reserveEphemeralPort(t)

	h := azureblob.NewHandler(azureblob.NewInMemoryBackend())
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
// error, forcing the srv.Close() fallback (both are logged, neither panics
// nor leaves the listener open).
func TestShutdown_ForcesCloseOnGracefulTimeout(t *testing.T) {
	t.Parallel()

	h := azureblob.NewHandler(azureblob.NewInMemoryBackend())
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
