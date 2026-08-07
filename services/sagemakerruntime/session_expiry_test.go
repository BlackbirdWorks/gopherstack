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

// TestBackend_TouchSession_ExpiryClosesSession and
// TestHandler_ClosedSessionIdHeader live in whitebox_test.go: they need
// direct access to the unexported session ExpiresAt field.

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
