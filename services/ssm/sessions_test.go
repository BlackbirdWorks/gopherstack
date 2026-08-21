package ssm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
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
	h := newHandler()

	code, out := postJSON(t, h, "StartSession", map[string]any{
		"Target":       "i-123",
		"DocumentName": "AWS-StartSSHSession",
		"Reason":       "debugging",
	})

	assert.Equal(t, http.StatusOK, code)
	sid := out["SessionId"].(string)
	assert.NotEmpty(t, sid)
}
func TestStartSession_DefaultsToStandardAccessType(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	sess, err := b.StartSession(context.TODO(), &ssm.StartSessionInput{Target: "i-access-type"})
	require.NoError(t, err)

	rec := doRequest(t, h, "DescribeSessions", `{"Filters":[{"key":"SessionId","value":"`+sess.SessionID+`"}]}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	sessions := out["Sessions"].([]any)
	require.Len(t, sessions, 1)
	assert.Equal(t, "Standard", sessions[0].(map[string]any)["AccessType"])
}
func TestTerminateSession_SetsEndDate(t *testing.T) {
	t.Parallel()
	h := newHandler()

	_, startOut := postJSON(t, h, "StartSession", map[string]any{"Target": "i-term"})
	sid := startOut["SessionId"].(string)

	code, out := postJSON(t, h, "TerminateSession", map[string]any{"SessionId": sid})

	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, sid, out["SessionId"])

	// Verify EndDate set (filter by empty state = all sessions).
	_, dsOut := postJSON(t, h, "DescribeSessions", map[string]any{"State": ""})
	sessions, ok := dsOut["Sessions"].([]any)
	require.True(t, ok)
	require.Len(t, sessions, 1)
	sess := sessions[0].(map[string]any)
	assert.Greater(t, sess["EndDate"].(float64), float64(0))
}
func TestFull_Session_StartTerminateDescribe(t *testing.T) {
	t.Parallel()
	h := newHandler()

	// Start
	code, out := postJSON(t, h, "StartSession", map[string]any{
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
	code, out = postJSON(t, h, "DescribeSessions", map[string]any{"State": ""})
	assert.Equal(t, http.StatusOK, code)
	sessions := out["Sessions"].([]any)
	assert.Len(t, sessions, 1)
	sess := sessions[0].(map[string]any)
	assert.Equal(t, "AWS-StartSSHSession", sess["DocumentName"])
	assert.Equal(t, "deploy fix", sess["Reason"])

	// ResumeSession
	code, out = postJSON(t, h, "ResumeSession", map[string]any{"SessionId": sid})
	assert.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, out["SessionId"])

	// Terminate
	code, out = postJSON(t, h, "TerminateSession", map[string]any{"SessionId": sid})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, sid, out["SessionId"])

	// DescribeSessions after terminate — session should be Terminated with EndDate
	code, out = postJSON(t, h, "DescribeSessions", map[string]any{"State": ""})
	assert.Equal(t, http.StatusOK, code)
	allSessions := out["Sessions"].([]any)
	require.Len(t, allSessions, 1)
	terminated := allSessions[0].(map[string]any)
	assert.Equal(t, "Terminated", terminated["Status"])
	assert.Greater(t, terminated["EndDate"].(float64), float64(0))
}

// TestSession_Parameters_RoundTrip locks in that StartSession accepts
// Parameters (used only to drive the session, e.g. a run-as document) but
// DescribeSessions never echoes them back -- types.Session
// (aws-sdk-go-v2/service/ssm@v1.73.4) has no Parameters member at all.
func TestSession_Parameters_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		parameters map[string][]string
		name       string
	}{
		{
			name:       "with_parameters",
			parameters: map[string][]string{"command": {"ls -la"}, "runAsEnabled": {"true"}},
		},
		{
			name:       "without_parameters",
			parameters: nil,
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

			// DescribeSessions should show the session, but never a
			// "Parameters" key -- real DescribeSessions has no such member.
			rec = doRequest(t, h, "DescribeSessions", `{"State":"Active"}`)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.NotContains(t, rec.Body.String(), "Parameters")
		})
	}
}

// TestSession_StateFilter locks in the real AWS DescribeSessions.State
// semantics: State is the coarse "Active"/"History" bucket (types.SessionState),
// NOT the same enum as a session's own Status (types.SessionStatus,
// "Connected"/"Terminated"/etc) — a request for State:"Active" must match a
// Connected session, and State:"History" must match a Terminated one, never
// the raw Status string itself.
func TestSession_StateFilter(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	sess, err := b.StartSession(context.TODO(), &ssm.StartSessionInput{Target: "i-filter-test"})
	require.NoError(t, err)

	// Filter by Active — should find the newly-connected session.
	rec := doRequest(t, h, "DescribeSessions", `{"State":"Active"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), sess.SessionID)

	// Filter by History — should NOT find a still-connected session.
	rec = doRequest(t, h, "DescribeSessions", `{"State":"History"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), sess.SessionID)

	// Terminate session.
	body, _ := json.Marshal(map[string]any{"SessionId": sess.SessionID})
	doRequest(t, h, "TerminateSession", string(body))

	// Filter by Active — should NOT find the terminated session.
	rec = doRequest(t, h, "DescribeSessions", `{"State":"Active"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), sess.SessionID)

	// Filter by History — should now find it.
	rec = doRequest(t, h, "DescribeSessions", `{"State":"History"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), sess.SessionID)
}

func TestDescribeSessions_Filters(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	s1, err := b.StartSession(context.TODO(), &ssm.StartSessionInput{Target: "i-aaa"})
	require.NoError(t, err)
	s2, err := b.StartSession(context.TODO(), &ssm.StartSessionInput{Target: "i-bbb"})
	require.NoError(t, err)

	rec := doRequest(t, h, "DescribeSessions", `{"Filters":[{"key":"Target","value":"i-aaa"}]}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), s1.SessionID)
	assert.NotContains(t, rec.Body.String(), s2.SessionID)

	rec = doRequest(t, h, "DescribeSessions", `{"Filters":[{"key":"SessionId","value":"`+s2.SessionID+`"}]}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), s2.SessionID)
	assert.NotContains(t, rec.Body.String(), s1.SessionID)
}

func TestDescribeSessions_Pagination(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	const total = 5
	for i := range total {
		_, err := b.StartSession(context.TODO(), &ssm.StartSessionInput{Target: "i-page"})
		require.NoError(t, err)
		_ = i
	}

	code, out := postJSON(t, h, "DescribeSessions", map[string]any{"MaxResults": 2})
	assert.Equal(t, http.StatusOK, code)
	sessions := out["Sessions"].([]any)
	require.Len(t, sessions, 2)
	nextToken, ok := out["NextToken"].(string)
	require.True(t, ok, "NextToken must be present when more results remain")
	require.NotEmpty(t, nextToken)

	seen := len(sessions)
	for nextToken != "" {
		code, out = postJSON(t, h, "DescribeSessions", map[string]any{
			"MaxResults": 2,
			"NextToken":  nextToken,
		})
		assert.Equal(t, http.StatusOK, code)
		page := out["Sessions"].([]any)
		seen += len(page)
		nextToken, _ = out["NextToken"].(string)
	}

	assert.Equal(t, total, seen, "pagination must eventually return every session exactly once")
}

func TestGetConnectionStatus_NotConnectedLowercase(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Real AWS's ConnectionStatus enum is lowercase ("connected"/"notconnected").
	code, out := postJSON(t, h, "GetConnectionStatus", map[string]any{"Target": "i-never-connected"})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "notconnected", out["Status"])
}

func TestAccessRequest_StartThenGetAccessToken(t *testing.T) {
	t.Parallel()

	h := newHandler()

	code, out := postJSON(t, h, "StartAccessRequest", map[string]any{
		"Reason": "on-call incident",
		"Targets": []map[string]any{
			{"Key": "InstanceIds", "Values": []string{"i-jit-001"}},
		},
	})
	require.Equal(t, http.StatusOK, code)
	arID, ok := out["AccessRequestId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, arID)

	code, out = postJSON(t, h, "GetAccessToken", map[string]any{"AccessRequestId": arID})
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "Approved", out["AccessRequestStatus"])

	creds, ok := out["Credentials"].(map[string]any)
	require.True(t, ok, "an approved access request must return Credentials")
	assert.NotEmpty(t, creds["AccessKeyId"])
	assert.NotEmpty(t, creds["SecretAccessKey"])
	assert.NotEmpty(t, creds["SessionToken"])
	assert.Greater(t, creds["ExpirationTime"].(float64), float64(0))
}

func TestAccessRequest_ValidationRequiresReasonAndTargets(t *testing.T) {
	t.Parallel()

	h := newHandler()

	code, _ := postJSON(t, h, "StartAccessRequest", map[string]any{"Reason": "no targets"})
	assert.Equal(t, http.StatusBadRequest, code)

	code, _ = postJSON(t, h, "StartAccessRequest", map[string]any{
		"Targets": []map[string]any{{"Key": "InstanceIds", "Values": []string{"i-1"}}},
	})
	assert.Equal(t, http.StatusBadRequest, code)
}

func TestGetAccessToken_UnknownAccessRequestIsNotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()

	code, out := postJSON(t, h, "GetAccessToken", map[string]any{"AccessRequestId": "ar-does-not-exist"})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "ResourceNotFoundException", out["__type"])
}

func TestGetAccessToken_RequiresAccessRequestID(t *testing.T) {
	t.Parallel()

	h := newHandler()

	code, _ := postJSON(t, h, "GetAccessToken", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, code)
}

// TestDescribeSessions_RealClient_NoFabricatedFields drives StartSession and
// DescribeSessions through the real aws-sdk-go-v2 client. types.Session has
// no Parameters/StreamUrl/TokenValue members, so gopherstack must not emit
// them on the wire even though the internal Session record carries all three
// (they used to leak straight through when DescribeSessionsOutputFull
// marshalled the internal Session struct directly).
func TestDescribeSessions_RealClient_NoFabricatedFields(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))

	started, err := client.StartSession(t.Context(), &ssmsdk.StartSessionInput{
		Target:     aws.String("i-real-client"),
		Parameters: map[string][]string{"command": {"ls -la"}},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(started.StreamUrl))
	require.NotEmpty(t, aws.ToString(started.TokenValue))

	out, err := client.DescribeSessions(t.Context(), &ssmsdk.DescribeSessionsInput{
		State: ssmtypes.SessionStateActive,
	})
	require.NoError(t, err)
	require.Len(t, out.Sessions, 1)

	sess := out.Sessions[0]
	assert.Equal(t, aws.ToString(started.SessionId), aws.ToString(sess.SessionId))
	assert.Equal(t, "i-real-client", aws.ToString(sess.Target))

	rec := doRequest(t, ssm.NewHandler(backend), "DescribeSessions", `{"State":"Active"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "Parameters")
	assert.NotContains(t, rec.Body.String(), "StreamUrl")
	assert.NotContains(t, rec.Body.String(), "TokenValue")
}
