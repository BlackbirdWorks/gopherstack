package appstream_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

// TestAppStream_Sessions covers session lifecycle and streaming URLs.
func TestAppStream_Sessions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateStreamingURL creates session and returns URL and Expires",
			action: "CreateStreamingURL",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "sess-stk")
				createFleet(t, h, "sess-fleet")
			},
			body: map[string]any{
				"StackName": "sess-stk",
				"FleetName": "sess-fleet",
				"UserId":    "user-1",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				assert.NotEmpty(t, resp["StreamingURL"])
				assert.NotEmpty(t, resp["Expires"])
			},
		},
		{
			name:   "DescribeSessions returns sessions",
			action: "DescribeSessions",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "desc-sess-stk")
				createFleet(t, h, "desc-sess-fleet")
				rec := doRequest(t, h, "CreateStreamingURL", map[string]any{
					"StackName": "desc-sess-stk",
					"FleetName": "desc-sess-fleet",
					"UserId":    "user-2",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body: map[string]any{
				"StackName": "desc-sess-stk",
				"FleetName": "desc-sess-fleet",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				sessions := resp["Sessions"].([]any)
				assert.Len(t, sessions, 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAppStream_SessionLifecycle covers ExpireSession with a real session ID.
func TestAppStream_SessionLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createStack(t, h, "lc-stk")
	createFleet(t, h, "lc-fleet")

	rec := doRequest(t, h, "CreateStreamingURL", map[string]any{
		"StackName": "lc-stk",
		"FleetName": "lc-fleet",
		"UserId":    "user-lc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe sessions to get the session ID.
	rec2 := doRequest(t, h, "DescribeSessions", map[string]any{
		"StackName": "lc-stk",
		"FleetName": "lc-fleet",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &descResp))
	sessions := descResp["Sessions"].([]any)
	require.Len(t, sessions, 1)
	sessionID := sessions[0].(map[string]any)["Id"].(string)

	// Expire the session.
	rec3 := doRequest(t, h, "ExpireSession", map[string]any{"SessionId": sessionID})
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Session gone — ExpireSession again should fail.
	rec4 := doRequest(t, h, "ExpireSession", map[string]any{"SessionId": sessionID})
	assert.Equal(t, http.StatusBadRequest, rec4.Code)
}

// TestAppStream_DrainSessionInstance covers DrainSessionInstance with a real session ID.
func TestAppStream_DrainSessionInstance(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createStack(t, h, "drain-stk")
	createFleet(t, h, "drain-fleet")

	doRequest(t, h, "CreateStreamingURL", map[string]any{
		"StackName": "drain-stk", "FleetName": "drain-fleet", "UserId": "user-drain",
	})

	rec := doRequest(t, h, "DescribeSessions", map[string]any{
		"StackName": "drain-stk", "FleetName": "drain-fleet",
	})
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	sessions := descResp["Sessions"].([]any)
	require.Len(t, sessions, 1)
	sessionID := sessions[0].(map[string]any)["Id"].(string)

	rec2 := doRequest(t, h, "DrainSessionInstance", map[string]any{"SessionId": sessionID})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// TestAppStream_SessionStreamingURL verifies that CreateStreamingURL returns a
// URL and an Expires timestamp honoring a caller-supplied Validity.
func TestAppStream_SessionStreamingURL(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStack", map[string]any{"Name": "url-stack"})
	doRequest(t, h, "CreateFleet", map[string]any{
		"Name":         "url-fleet",
		"InstanceType": "stream.standard.medium",
	})
	doRequest(t, h, "AssociateFleet", map[string]any{
		"FleetName": "url-fleet",
		"StackName": "url-stack",
	})

	beforeCall := float64(time.Now().UTC().Unix())

	rec := doRequest(t, h, "CreateStreamingURL", map[string]any{
		"StackName": "url-stack",
		"FleetName": "url-fleet",
		"UserId":    "user1",
		"Validity":  120,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["StreamingURL"])

	expires, ok := resp["Expires"].(float64)
	require.True(t, ok, "Expires must be a JSON number (epoch seconds)")
	// Expires should be ~120s after the call, well short of the 60s default.
	assert.Greater(t, expires, beforeCall+90)
	assert.Less(t, expires, beforeCall+150)
}

// TestAppStream_DescribeSessionsFilterByStack verifies DescribeSessions filters work.
func TestAppStream_DescribeSessionsFilterByStack(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStack", map[string]any{"Name": "sess-stack"})
	doRequest(t, h, "CreateFleet", map[string]any{
		"Name":         "sess-fleet",
		"InstanceType": "stream.standard.medium",
	})
	doRequest(t, h, "AssociateFleet", map[string]any{
		"FleetName": "sess-fleet",
		"StackName": "sess-stack",
	})
	doRequest(t, h, "CreateStreamingURL", map[string]any{
		"StackName": "sess-stack",
		"FleetName": "sess-fleet",
		"UserId":    "user-a",
	})
	doRequest(t, h, "CreateStreamingURL", map[string]any{
		"StackName": "sess-stack",
		"FleetName": "sess-fleet",
		"UserId":    "user-b",
	})

	rec := doRequest(t, h, "DescribeSessions", map[string]any{
		"StackName": "sess-stack",
		"FleetName": "sess-fleet",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	sessions, ok := resp["Sessions"].([]any)
	require.True(t, ok)
	assert.Len(t, sessions, 2)
}
