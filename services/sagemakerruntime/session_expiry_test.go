package sagemakerruntime_test

import (
	"net/http"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemakerruntime"
)

// newSessionHeaderPattern mirrors the real AWS SDK model's
// NewSessionResponseHeader regex from botocore's sagemaker-runtime
// service-2.json:
//
//	^[a-zA-Z0-9](-*[a-zA-Z0-9])*;\sExpires=[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$
//
// i.e. an RFC 3339 timestamp with no fractional seconds -- NOT an RFC 1123
// HTTP-date, which is what an earlier version of this handler emitted.
var newSessionHeaderPattern = regexp.MustCompile(
	`^[a-zA-Z0-9](-*[a-zA-Z0-9])*; Expires=\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$`,
)

// TestNewSessionHeaderFormat_MatchesSDKPattern verifies that the
// X-Amzn-SageMaker-New-Session-Id response header emitted for a NEW_SESSION
// request matches the exact wire format the real SDK model declares, byte
// for byte -- not just "contains Expires=".
func TestNewSessionHeaderFormat_MatchesSDKPattern(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequestWithHeaders(
		t, h, http.MethodPost,
		"/endpoints/ep/invocations",
		nil,
		map[string]string{"X-Amzn-Sagemaker-Session-Id": "NEW_SESSION"},
	)

	require.Equal(t, http.StatusOK, rec.Code)

	hdr := rec.Header().Get("X-Amzn-Sagemaker-New-Session-Id")
	require.NotEmpty(t, hdr)
	assert.Regexp(t, newSessionHeaderPattern, hdr,
		"new-session header must match the SDK's NewSessionResponseHeader pattern exactly")
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

			b := sagemakerruntime.NewInMemoryBackend("000000000000", "us-east-1")
			session := b.StartSession("ep")

			if tt.expire {
				sagemakerruntime.ExpireSessionForTest(b, session.ID)
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

// TestBackend_TouchSession_UnknownSessionIsNoop verifies that touching a
// session ID with no matching record is a silent no-op (no closure
// reported), matching this backend's pre-existing behaviour for
// unrecognised session IDs.
func TestBackend_TouchSession_UnknownSessionIsNoop(t *testing.T) {
	t.Parallel()

	b := sagemakerruntime.NewInMemoryBackend("000000000000", "us-east-1")
	outcome := b.TouchSession("never-created")
	assert.Empty(t, outcome.ClosedSessionID)
	assert.Empty(t, b.ListSessions())
}

// TestHandler_ClosedSessionIdHeader verifies that InvokeEndpoint reports
// X-Amzn-Sagemaker-Closed-Session-Id (and evicts the session) when the
// caller's SessionId has passed its ExpiresAt, and does not set the header
// for a live session.
func TestHandler_ClosedSessionIdHeader(t *testing.T) {
	t.Parallel()

	t.Run("expired_session_is_closed", func(t *testing.T) {
		t.Parallel()

		b := sagemakerruntime.NewInMemoryBackend("000000000000", "us-east-1")
		h := sagemakerruntime.NewHandler(b)

		session := b.StartSession("ep")
		sagemakerruntime.ExpireSessionForTest(b, session.ID)

		rec := doRequestWithHeaders(
			t, h, http.MethodPost, "/endpoints/ep/invocations", nil,
			map[string]string{"X-Amzn-Sagemaker-Session-Id": session.ID},
		)

		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, session.ID, rec.Header().Get("X-Amzn-Sagemaker-Closed-Session-Id"))
		assert.Empty(t, rec.Header().Get("X-Amzn-Sagemaker-New-Session-Id"))
		assert.Empty(t, b.ListSessions(), "expired session must be evicted")
	})

	t.Run("live_session_is_not_closed", func(t *testing.T) {
		t.Parallel()

		b := sagemakerruntime.NewInMemoryBackend("000000000000", "us-east-1")
		h := sagemakerruntime.NewHandler(b)

		session := b.StartSession("ep")

		rec := doRequestWithHeaders(
			t, h, http.MethodPost, "/endpoints/ep/invocations", nil,
			map[string]string{"X-Amzn-Sagemaker-Session-Id": session.ID},
		)

		require.Equal(t, http.StatusOK, rec.Code)
		assert.Empty(t, rec.Header().Get("X-Amzn-Sagemaker-Closed-Session-Id"))
		assert.Len(t, b.ListSessions(), 1)
	})
}
