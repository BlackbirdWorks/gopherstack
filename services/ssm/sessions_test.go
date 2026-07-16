package ssm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestSessions(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Start a session
	sess, err := b.StartSession(context.TODO(), &ssm.StartSessionInput{Target: "i-001"})
	require.NoError(t, err)

	// DescribeSessions
	rec := doRequest(t, h, "DescribeSessions", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, sess.SessionID)

	// GetConnectionStatus
	rec = doRequest(t, h, "GetConnectionStatus", `{"Target":"i-001"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// ResumeSession
	rec = doRequest(t, h, "ResumeSession", `{"SessionId":"`+sess.SessionID+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assertBodyContains(t, rec, "SessionId")
}
func TestSessions_TerminatedEvictionBounded(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.TODO()

	// Create and terminate more sessions than the retained-history cap (200).
	const total = 260
	for range total {
		out, err := b.StartSession(ctx, &ssm.StartSessionInput{Target: "i-1"})
		require.NoError(t, err)

		_, err = b.TerminateSession(ctx, &ssm.TerminateSessionInput{SessionID: out.SessionID})
		require.NoError(t, err)
	}

	assert.LessOrEqual(t, b.SessionCount(), 200,
		"terminated sessions must be bounded to the retained-history cap")
}
func TestJanitor_SweepTerminatedSessions(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()

	nowUnix := float64(time.Now().Unix())
	oldEnd := nowUnix - (48 * 60 * 60) // 48h ago, past 24h retention
	recentEnd := nowUnix - 60          // 1 min ago, within retention

	b.AddTerminatedSessionInternal("session-old", oldEnd)
	b.AddTerminatedSessionInternal("session-recent", recentEnd)
	require.Equal(t, 2, b.SessionCount())

	j := ssm.NewJanitor(b, ssm.DefaultJanitorInterval)
	j.SweepOnce(context.TODO())

	assert.Equal(t, 1, b.SessionCount(),
		"the aged terminated session must be evicted, the recent one retained")

	sessions, err := b.DescribeSessions(context.TODO(), &ssm.DescribeSessionsInput{})
	require.NoError(t, err)
	require.Len(t, sessions.Sessions, 1)
	assert.Equal(t, "session-recent", sessions.Sessions[0].SessionID)
}
func TestStartSession_LoggingFields(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, out := postAudit1(t, h, "StartSession", map[string]any{
		"Target":                  "i-123",
		"DocumentName":            "AWS-StartSSHSession",
		"Reason":                  "debugging",
		"OutputS3BucketName":      "ssm-logs",
		"OutputS3KeyPrefix":       "sessions/",
		"CloudWatchOutputEnabled": true,
		"CloudWatchLogGroupName":  "/aws/ssm/sessions",
	})

	assert.Equal(t, http.StatusOK, code)
	sid := out["SessionId"].(string)
	assert.NotEmpty(t, sid)
}
func TestTerminateSession_SetsEndDate(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	_, startOut := postAudit1(t, h, "StartSession", map[string]any{"Target": "i-term"})
	sid := startOut["SessionId"].(string)

	code, out := postAudit1(t, h, "TerminateSession", map[string]any{"SessionId": sid})

	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, sid, out["SessionId"])

	// Verify EndDate set (filter by empty state = all sessions).
	_, dsOut := postAudit1(t, h, "DescribeSessions", map[string]any{"State": ""})
	sessions, ok := dsOut["Sessions"].([]any)
	require.True(t, ok)
	require.Len(t, sessions, 1)
	sess := sessions[0].(map[string]any)
	assert.Greater(t, sess["EndDate"].(float64), float64(0))
}
func TestFull_Session_StartTerminateDescribe(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	// Start
	code, out := mustPost(t, h, "StartSession", map[string]any{
		"Target":       "i-session-target",
		"DocumentName": "AWS-StartSSHSession",
		"Reason":       "deploy fix",
	})
	assert.Equal(t, http.StatusOK, code)
	sid := out["SessionId"].(string)
	assert.NotEmpty(t, sid)
	assert.NotEmpty(t, out["StreamUrl"])
	assert.NotEmpty(t, out["TokenValue"])

	// DescribeSessions - all
	code, out = mustPost(t, h, "DescribeSessions", map[string]any{"State": ""})
	assert.Equal(t, http.StatusOK, code)
	sessions := out["Sessions"].([]any)
	assert.Len(t, sessions, 1)
	sess := sessions[0].(map[string]any)
	assert.Equal(t, "AWS-StartSSHSession", sess["DocumentName"])
	assert.Equal(t, "deploy fix", sess["Reason"])

	// ResumeSession
	code, out = mustPost(t, h, "ResumeSession", map[string]any{"SessionId": sid})
	assert.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, out["SessionId"])

	// Terminate
	code, out = mustPost(t, h, "TerminateSession", map[string]any{"SessionId": sid})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, sid, out["SessionId"])

	// DescribeSessions after terminate — session should be Terminated with EndDate
	code, out = mustPost(t, h, "DescribeSessions", map[string]any{"State": ""})
	assert.Equal(t, http.StatusOK, code)
	allSessions := out["Sessions"].([]any)
	require.Len(t, allSessions, 1)
	terminated := allSessions[0].(map[string]any)
	assert.Equal(t, "Terminated", terminated["Status"])
	assert.Greater(t, terminated["EndDate"].(float64), float64(0))
}
func TestSession_Parameters_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		parameters map[string][]string
		wantKey    string
	}{
		{
			name:       "with_parameters",
			parameters: map[string][]string{"command": {"ls -la"}, "runAsEnabled": {"true"}},
			wantKey:    "command",
		},
		{
			name:       "without_parameters",
			parameters: nil,
			wantKey:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			body, _ := json.Marshal(map[string]any{
				"Target":     "i-1234567890abcdef0",
				"Parameters": tt.parameters,
			})
			rec := doRequest(t, h, "StartSession", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			var startResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
			assert.NotEmpty(t, startResp["SessionId"])

			// DescribeSessions should show the session.
			rec = doRequest(t, h, "DescribeSessions", `{"State":"Connected"}`)
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.wantKey != "" {
				assert.Contains(t, rec.Body.String(), tt.wantKey)
			}
		})
	}
}
func TestSession_StateFilter(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	sess, err := b.StartSession(context.TODO(), &ssm.StartSessionInput{Target: "i-filter-test"})
	require.NoError(t, err)

	// Filter by Connected — should find the session.
	rec := doRequest(t, h, "DescribeSessions", `{"State":"Connected"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), sess.SessionID)

	// Terminate session.
	body, _ := json.Marshal(map[string]any{"SessionId": sess.SessionID})
	doRequest(t, h, "TerminateSession", string(body))

	// Filter by Connected — should NOT find the terminated session.
	rec = doRequest(t, h, "DescribeSessions", `{"State":"Connected"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), sess.SessionID)
}
