package azuretable_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuretable"
)

// freeEphemeralPort returns a port that was free at the moment of the call
// (bound briefly via net.Listen("tcp", ":0") then released immediately).
// Mirrors services/azurequeue's identical helper.
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

// TestStartWorker_BindsAndServes exercises the real synchronous bind: it
// starts the dedicated listener on a concrete port, makes a real HTTP
// request against it to prove the telemetry-wrapped handler is actually
// reachable, then shuts it down.
func TestStartWorker_BindsAndServes(t *testing.T) {
	t.Parallel()

	port := freeEphemeralPort(t)

	backend := azuretable.NewInMemoryBackend()
	h := azuretable.NewHandler(backend)
	h.Port = port

	ctx := t.Context()
	require.NoError(t, h.StartWorker(ctx))

	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		h.Shutdown(shutdownCtx)
	})

	url := fmt.Sprintf("http://127.0.0.1:%d/%s/Tables", port, testAccount)

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
// synchronously from StartWorker itself.
func TestStartWorker_BindFailureIsSynchronous(t *testing.T) {
	t.Parallel()

	port := reserveEphemeralPort(t)

	h := azuretable.NewHandler(azuretable.NewInMemoryBackend())
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

	h := azuretable.NewHandler(azuretable.NewInMemoryBackend())
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
