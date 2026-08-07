package sagemakerruntime

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func doWhiteboxRequest(
	t *testing.T, h *Handler, method, path string, headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("Content-Type", "application/json")

	for name, value := range headers {
		req.Header.Set(name, value)
	}

	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

// expireSessionForTest forces the named session's ExpiresAt into the past,
// for deterministically testing TouchSession's expiry-driven closure without
// waiting out the real sessionDuration. AWS provides no API to force a
// stateful session's expiry directly -- it is entirely model/container-driven
// -- so this is a test-only backdoor, not a simulated SDK operation.
func expireSessionForTest(b *InMemoryBackend, sessionID string) {
	b.mu.Lock("test.expireSession")
	defer b.mu.Unlock()

	if session, ok := b.sessions.Get(sessionID); ok {
		session.ExpiresAt = time.Now().Add(-time.Minute)
	}
}

// TestBackend_TouchSession_ExpiryClosesSession verifies that TouchSession
// evicts a session past its ExpiresAt and reports the closure via
// SessionTouchOutcome.ClosedSessionID, matching real AWS's ClosedSessionId
// (InvokeEndpointOutput) semantics for a session the model container has
// torn down; a live session, by contrast, is touched without being closed.
func TestBackend_TouchSession_ExpiryClosesSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		expire        bool
		wantClosed    bool
		wantRemaining int
	}{
		{name: "live_session_is_touched_not_closed", expire: false, wantClosed: false, wantRemaining: 1},
		{name: "expired_session_is_evicted_and_closed", expire: true, wantClosed: true, wantRemaining: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1")
			session := b.StartSession("ep")

			if tt.expire {
				expireSessionForTest(b, session.ID)
			}

			outcome := b.TouchSession(session.ID)

			if tt.wantClosed {
				assert.Equal(t, session.ID, outcome.ClosedSessionID)
			} else {
				assert.Empty(t, outcome.ClosedSessionID)
			}

			assert.Len(t, b.ListSessions(), tt.wantRemaining)
		})
	}
}

// TestHandler_ClosedSessionIdHeader verifies that InvokeEndpoint reports
// X-Amzn-Sagemaker-Closed-Session-Id (and evicts the session) when the
// caller's SessionId has passed its ExpiresAt, and does not set the header
// for a live session.
func TestHandler_ClosedSessionIdHeader(t *testing.T) {
	t.Parallel()

	t.Run("expired_session_is_closed", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend("000000000000", "us-east-1")
		h := NewHandler(b)

		session := b.StartSession("ep")
		expireSessionForTest(b, session.ID)

		rec := doWhiteboxRequest(
			t, h, http.MethodPost, "/endpoints/ep/invocations",
			map[string]string{"X-Amzn-Sagemaker-Session-Id": session.ID},
		)

		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, session.ID, rec.Header().Get("X-Amzn-Sagemaker-Closed-Session-Id"))
		assert.Empty(t, rec.Header().Get("X-Amzn-Sagemaker-New-Session-Id"))
		assert.Empty(t, b.ListSessions(), "expired session must be evicted")
	})

	t.Run("live_session_is_not_closed", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend("000000000000", "us-east-1")
		h := NewHandler(b)

		session := b.StartSession("ep")

		rec := doWhiteboxRequest(
			t, h, http.MethodPost, "/endpoints/ep/invocations",
			map[string]string{"X-Amzn-Sagemaker-Session-Id": session.ID},
		)

		require.Equal(t, http.StatusOK, rec.Code)
		assert.Empty(t, rec.Header().Get("X-Amzn-Sagemaker-Closed-Session-Id"))
		assert.Len(t, b.ListSessions(), 1)
	})
}
