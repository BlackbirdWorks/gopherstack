package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func doWhiteboxRequest(t *testing.T, h *Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSScheduler."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

func idempotencyCacheLen(h *Handler) int {
	return h.idempotency.Len()
}

func locCacheLen(r *Runner) int {
	r.cacheMu.RLock()
	defer r.cacheMu.RUnlock()

	return len(r.locCache)
}

// TestScheduler_ParseAtExpression whitebox-tests the unexported
// parseAtExpression, which parses at() one-time expressions.
func TestScheduler_ParseAtExpression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want    time.Time
		name    string
		expr    string
		wantErr bool
	}{
		{
			name: "valid",
			expr: "at(2024-06-01T09:30:00)",
			want: time.Date(2024, 6, 1, 9, 30, 0, 0, time.UTC),
		},
		{
			name:    "missing time component",
			expr:    "at(2024-06-01)",
			wantErr: true,
		},
		{
			name:    "not a datetime at all",
			expr:    "at(not-a-date)",
			wantErr: true,
		},
		{
			name:    "empty",
			expr:    "at()",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseAtExpression(tt.expr, time.UTC)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.True(t, tt.want.Equal(got))
		})
	}
}

// TestSchedulerHandler_Reset_ClearsIdempotencyCache verifies Reset wipes cached
// ClientToken results along with backend state, matching its "wipe everything"
// semantics -- otherwise a stale cache entry could replay an ARN for a resource
// that Reset just deleted.
func TestSchedulerHandler_Reset_ClearsIdempotencyCache(t *testing.T) {
	t.Parallel()

	h := NewHandler(NewInMemoryBackend("000000000000", "us-east-1"))

	rec := doWhiteboxRequest(t, h, "CreateSchedule", map[string]any{
		"Name":               "reset-sched",
		"ScheduleExpression": "rate(1 hour)",
		"Target":             map[string]string{"Arn": "arn:aws:sqs:us-east-1:0:q", "RoleArn": "arn:aws:iam::0:role/r"},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
		"ClientToken":        "reset-token",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, 1, idempotencyCacheLen(h))

	h.Reset()

	assert.Equal(t, 0, idempotencyCacheLen(h))
}

// TestScheduler_Runner_LocCacheEviction verifies stale *time.Location cache entries
// are swept once no active schedule references their timezone anymore.
func TestScheduler_Runner_LocCacheEviction(t *testing.T) {
	t.Parallel()

	const lambdaARN = "arn:aws:lambda:us-east-1:000000000000:function:loc-evict-fn"
	const role = "arn:aws:iam::000000000000:role/r"

	backend := NewInMemoryBackend("000000000000", "us-east-1")
	_, err := backend.CreateSchedule(
		context.Background(),
		"loc-evict-sched", "", "cron(0 12 * * ? *)", "", "America/New_York",
		Target{ARN: lambdaARN, RoleARN: role},
		"ENABLED", FlexibleTimeWindow{Mode: "OFF"},
	)
	require.NoError(t, err)

	runner := NewRunner(backend)
	runner.SetLambdaInvoker(&whiteboxLambdaInvoker{})

	matchTime := time.Date(2024, 1, 15, 17, 0, 0, 0, time.UTC) // 12:00 EST
	runner.checkAndFireSchedules(t.Context(), matchTime)
	require.Equal(t, 1, locCacheLen(runner), "timezone should be cached after first poll")

	require.NoError(t, backend.DeleteSchedule(context.Background(), "loc-evict-sched", ""))

	runner.checkAndFireSchedules(t.Context(), matchTime.Add(time.Hour))
	assert.Equal(t, 0, locCacheLen(runner), "stale timezone cache entries should be evicted")
}

type whiteboxLambdaInvoker struct{}

func (*whiteboxLambdaInvoker) InvokeFunction(_ context.Context, _, _ string, _ []byte) ([]byte, int, error) {
	return nil, 200, nil
}
