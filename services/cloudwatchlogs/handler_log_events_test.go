package cloudwatchlogs_test

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_PutLogEvents_MismatchedSequenceToken_StillAccepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		sequenceToken string
		setupEvents   int
	}{
		{
			name:          "wrong_token_on_non_empty_stream",
			setupEvents:   2,
			sequenceToken: "99",
		},
		{
			name:          "wrong_token_on_empty_stream",
			setupEvents:   0,
			sequenceToken: "5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := cloudwatchlogs.NewInMemoryBackend()
			h := cloudwatchlogs.NewHandler(backend)

			doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
			doLogsRequest(t, h, e, "CreateLogStream", `{"logGroupName":"grp","logStreamName":"s"}`)

			// Use the real wall clock, not a hardcoded epoch: a fixed past timestamp
			// eventually falls outside PutLogEvents' rolling 14-day acceptance window
			// as time passes, which would make events be rejected as "too old" and
			// break this test's non-error-path assertions below.
			now := time.Now().UnixMilli()
			for i := range tt.setupEvents {
				body, _ := json.Marshal(map[string]any{
					"logGroupName":  "grp",
					"logStreamName": "s",
					"logEvents": []any{
						map[string]any{"message": "setup", "timestamp": now + int64(i)},
					},
				})
				doLogsRequest(t, h, e, "PutLogEvents", string(body))
			}

			reqBody, _ := json.Marshal(map[string]any{
				"logGroupName":  "grp",
				"logStreamName": "s",
				"sequenceToken": tt.sequenceToken,
				"logEvents":     []any{map[string]any{"message": "new", "timestamp": now}},
			})
			rec := doLogsRequest(t, h, e, "PutLogEvents", string(reqBody))

			assert.Equal(t, http.StatusOK, rec.Code,
				"a mismatched sequenceToken must not be rejected: AWS ignores it entirely")

			var okResp struct {
				NextSequenceToken string `json:"nextSequenceToken"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &okResp))
			assert.Equal(t, strconv.Itoa(tt.setupEvents+1), okResp.NextSequenceToken)
		})
	}
}

// ── Batch-2 Issue #2: SetRetentionPolicy bypasses validation for retentionInDays=0 ─────────────────────
//
// SetRetentionPolicy's guard was `if days != nil && *days != 0`, so passing &0 skipped the
// validRetentionDays() check and stored an invalid retention value. AWS PutRetentionPolicy
// rejects retentionInDays=0 with InvalidParameterException.
//
// Fix: remove the `&& *days != 0` clause so that 0 is tested against validRetentionDays()
// and rejected like any other out-of-range value.

func TestHandler_LogEventsOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup             func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body              map[string]any
		name              string
		action            string
		wantListField     string
		wantNotEmptyField string
		wantCode          int
		wantListLen       int
	}{
		{
			name: "PutLogEvents",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
				doLogsRequest(
					t,
					h,
					e,
					"CreateLogStream",
					`{"logGroupName":"grp","logStreamName":"s"}`,
				)
			},
			action: "PutLogEvents",
			body: map[string]any{
				"logGroupName":  "grp",
				"logStreamName": "s",
				"logEvents":     []any{map[string]any{"message": "hello", "timestamp": 1000}},
			},
			wantCode:          http.StatusOK,
			wantNotEmptyField: "nextSequenceToken",
		},
		{
			name:   "PutLogEvents/GroupNotFound",
			action: "PutLogEvents",
			body: map[string]any{
				"logGroupName":  "nonexistent",
				"logStreamName": "s",
				"logEvents":     []any{},
			},
			wantCode: http.StatusNotFound,
		},
		{
			name: "GetLogEvents",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
				doLogsRequest(
					t,
					h,
					e,
					"CreateLogStream",
					`{"logGroupName":"grp","logStreamName":"s"}`,
				)
				doLogsRequest(
					t,
					h,
					e,
					"PutLogEvents",
					`{"logGroupName":"grp","logStreamName":"s","logEvents":[{"message":"m1","timestamp":1000}]}`,
				)
			},
			action:        "GetLogEvents",
			body:          map[string]any{"logGroupName": "grp", "logStreamName": "s"},
			wantCode:      http.StatusOK,
			wantListField: "events",
			wantListLen:   1,
		},
		{
			name:     "GetLogEvents/NotFound",
			action:   "GetLogEvents",
			body:     map[string]any{"logGroupName": "nonexistent", "logStreamName": "s"},
			wantCode: http.StatusNotFound,
		},
		{
			name: "FilterLogEvents",
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"grp"}`)
				doLogsRequest(
					t,
					h,
					e,
					"CreateLogStream",
					`{"logGroupName":"grp","logStreamName":"s"}`,
				)
				doLogsRequest(
					t,
					h,
					e,
					"PutLogEvents",
					`{"logGroupName":"grp","logStreamName":"s","logEvents":[{"message":"ERROR: bad","timestamp":1000}]}`,
				)
			},
			action:        "FilterLogEvents",
			body:          map[string]any{"logGroupName": "grp", "filterPattern": "ERROR"},
			wantCode:      http.StatusOK,
			wantListField: "events",
			wantListLen:   1,
		},
		{
			name:     "FilterLogEvents/GroupNotFound",
			action:   "FilterLogEvents",
			body:     map[string]any{"logGroupName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantListField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Len(t, resp[tt.wantListField].([]any), tt.wantListLen)
			}

			if tt.wantNotEmptyField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp[tt.wantNotEmptyField])
			}
		})
	}
}

// TestHandler_PutBearerTokenAuthentication_RoundTrip proves gopherstack-4ggy's
// fix: the pre-fix handler was a total stub (body param `_ []byte`, always
// returned success with no backend effect). This drives a real log group
// through Put and confirms DescribeLogGroups echoes the enabled state back
// (types.LogGroup.BearerTokenAuthenticationEnabled, types.go:1366), and that
// an unknown log group returns ResourceNotFoundException rather than a
// silent success.
func TestHandler_PutBearerTokenAuthentication_RoundTrip(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)

	rec := doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"bearer-grp"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doLogsRequest(t, h, e, "PutBearerTokenAuthentication",
		`{"logGroupIdentifier":"bearer-grp","bearerTokenAuthenticationEnabled":true}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doLogsRequest(t, h, e, "DescribeLogGroups", `{"logGroupNamePrefix":"bearer-grp"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var desc struct {
		LogGroups []map[string]any `json:"logGroups"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc))
	require.Len(t, desc.LogGroups, 1)
	assert.Equal(t, true, desc.LogGroups[0]["bearerTokenAuthenticationEnabled"])

	rec = doLogsRequest(t, h, e, "PutBearerTokenAuthentication",
		`{"logGroupIdentifier":"does-not-exist","bearerTokenAuthenticationEnabled":true}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_LiveTailAndLogFieldOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		action        string
		wantListField string
		wantCode      int
	}{
		{
			// logGroupIdentifier and bearerTokenAuthenticationEnabled are
			// both required (validateOpPutBearerTokenAuthenticationInput,
			// validators.go) -- the pre-fix stub accepted an empty body
			// unconditionally.
			name:     "PutBearerTokenAuthentication/RequiresFields",
			action:   "PutBearerTokenAuthentication",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			// StartLiveTail is validation-only: logGroupIdentifiers is required.
			name:     "StartLiveTail/RequiresLogGroups",
			action:   "StartLiveTail",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			// GetLogFields requires a data source / log group identifier.
			name:     "GetLogFields/RequiresDataSource",
			action:   "GetLogFields",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			// GetLogObject requires a log object pointer.
			name:     "GetLogObject/RequiresPointer",
			action:   "GetLogObject",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := makeLogsRequest(t, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantListField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				list, ok := resp[tt.wantListField].([]any)
				require.True(t, ok, "expected list field %q in response", tt.wantListField)
				assert.Empty(t, list)
			}
		})
	}
}
