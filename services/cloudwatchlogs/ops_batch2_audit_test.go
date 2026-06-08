package cloudwatchlogs_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// ── Batch-2 Issue #1: ErrInvalidSequenceToken not in handler error switch ──────────────────────────────
//
// PutLogEvents with an incorrect sequenceToken calls backend.PutLogEvents which returns
// ErrInvalidSequenceToken. The handler's handleError switch had no case for this error, so
// it fell through to the default branch and returned 500 InternalServerError with
// __type="InternalServerError".
//
// AWS returns HTTP 400 with __type="InvalidSequenceTokenException".
// Fix: add ErrInvalidSequenceToken case to handleError mapping.

func TestBatch2_PutLogEvents_WrongSequenceToken_Returns400(t *testing.T) {
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

			now := int64(1_700_000_000_000)
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

			assert.Equal(t, http.StatusBadRequest, rec.Code,
				"wrong sequenceToken must return 400, not 500 InternalServerError")

			var errResp struct {
				Type string `json:"__type"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "InvalidSequenceTokenException", errResp.Type,
				"wrong sequenceToken must map to InvalidSequenceTokenException, not InternalServerError")
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

func TestBatch2_PutRetentionPolicy_ZeroDays_ReturnsInvalidParameter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		retentionInDays int
	}{
		{name: "zero_days_rejected", retentionInDays: 0},
		{name: "negative_days_rejected", retentionInDays: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h := cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend())

			doLogsRequest(t, h, e, "CreateLogGroup", `{"logGroupName":"ret-grp"}`)

			body, _ := json.Marshal(map[string]any{
				"logGroupName":    "ret-grp",
				"retentionInDays": tt.retentionInDays,
			})
			rec := doLogsRequest(t, h, e, "PutRetentionPolicy", string(body))

			assert.Equal(t, http.StatusBadRequest, rec.Code,
				"retentionInDays=%d must return 400 InvalidParameterException", tt.retentionInDays)

			var errResp struct {
				Type string `json:"__type"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "InvalidParameterException", errResp.Type,
				"retentionInDays=%d must return InvalidParameterException", tt.retentionInDays)
		})
	}
}

func TestBatch2_SetRetentionPolicy_ZeroDays_BackendReturnsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		days int32
	}{
		{name: "zero_is_invalid", days: 0},
		{name: "negative_is_invalid", days: -5},
		{name: "arbitrary_non_allowed_value", days: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatchlogs.NewInMemoryBackend()
			_, err := b.CreateLogGroup(context.Background(), "grp", "", "")
			require.NoError(t, err)

			days := tt.days
			err = b.SetRetentionPolicy(context.Background(), "grp", &days)
			require.ErrorIs(t, err, cloudwatchlogs.ErrValidation,
				"days=%d must be rejected with ErrValidation", tt.days)
		})
	}
}
