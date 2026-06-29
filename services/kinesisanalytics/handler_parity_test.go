package kinesisanalytics_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalytics"
)

// TestParity_DeleteApplication_TimestampMismatch_ConcurrentModificationException verifies
// that supplying a wrong CreateTimestamp to DeleteApplication returns
// ConcurrentModificationException (not LimitExceededException). Real AWS Kinesis Analytics
// uses ConcurrentModificationException for this condition.
func TestParity_DeleteApplication_TimestampMismatch_ConcurrentModificationException(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	app, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "ts-mismatch-app", "", "", nil)
	require.NoError(t, err)

	// Use a timestamp that's one second off from the real value.
	wrongTimestamp := app.CreateTimestamp.Add(-time.Second)

	rec := doRequest(t, h, "DeleteApplication", map[string]any{
		"ApplicationName": "ts-mismatch-app",
		"CreateTimestamp": float64(wrongTimestamp.UnixNano()) / 1e9,
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ConcurrentModificationException", resp["__type"],
		"timestamp mismatch must return ConcurrentModificationException, not LimitExceededException")
}

// TestParity_DeleteApplication_CorrectTimestamp_Succeeds verifies that a matching
// CreateTimestamp allows deletion (regression guard for the timestamp check logic).
func TestParity_DeleteApplication_CorrectTimestamp_Succeeds(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	app, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "ts-correct-app", "", "", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, "DeleteApplication", map[string]any{
		"ApplicationName": "ts-correct-app",
		"CreateTimestamp": float64(app.CreateTimestamp.UnixNano()) / 1e9,
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestParity_UpdateApplication_ReturnsApplicationDetail verifies that UpdateApplication
// returns an ApplicationDetail in its response body. Real AWS Kinesis Analytics returns
// the full application detail after an update, not an empty object.
func TestParity_UpdateApplication_ReturnsApplicationDetail(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	_, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "update-resp-app", "", "old-code", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, "UpdateApplication", map[string]any{
		"ApplicationName":             "update-resp-app",
		"CurrentApplicationVersionID": 1,
		"ApplicationUpdate": map[string]any{
			"ApplicationCodeUpdate": "new-code",
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	detail, ok := resp["ApplicationDetail"].(map[string]any)
	require.True(t, ok, "UpdateApplication response must contain ApplicationDetail, got: %s", rec.Body.String())
	assert.Equal(t, "update-resp-app", detail["ApplicationName"],
		"ApplicationDetail must include ApplicationName")
	assert.InDelta(t, float64(2), detail["ApplicationVersionId"], 0,
		"ApplicationDetail must show incremented version after update")
}

// TestParity_UpdateApplication_CodeVisible verifies that the updated application code
// is visible in the ApplicationDetail returned by UpdateApplication.
func TestParity_UpdateApplication_CodeVisible(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	_, err := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "code-visible-app", "", "SELECT 1;", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, "UpdateApplication", map[string]any{
		"ApplicationName":             "code-visible-app",
		"CurrentApplicationVersionID": 1,
		"ApplicationUpdate": map[string]any{
			"ApplicationCodeUpdate": "SELECT 2;",
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	detail, ok := resp["ApplicationDetail"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "SELECT 2;", detail["ApplicationCode"],
		"updated code must be visible in UpdateApplication response")
}

// TestParity_ErrorTypes verifies that all mutating operations return the correct
// AWS Kinesis Analytics error type strings.
func TestParity_ErrorTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*kinesisanalytics.Handler, *kinesisanalytics.InMemoryBackend)
		body     map[string]any
		name     string
		action   string
		wantType string
		wantCode int
	}{
		{
			name:   "create duplicate app returns ResourceInUseException",
			action: "CreateApplication",
			body:   map[string]any{"ApplicationName": "dup-app"},
			setup: func(_ *kinesisanalytics.Handler, b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "dup-app", "", "", nil)
			},
			wantCode: http.StatusBadRequest,
			wantType: "ResourceInUseException",
		},
		{
			name:     "describe missing app returns ResourceNotFoundException",
			action:   "DescribeApplication",
			body:     map[string]any{"ApplicationName": "no-such-app"},
			wantCode: http.StatusNotFound,
			wantType: "ResourceNotFoundException",
		},
		{
			name:   "update with wrong version returns ConcurrentModificationException",
			action: "UpdateApplication",
			body: map[string]any{
				"ApplicationName":             "version-app",
				"CurrentApplicationVersionID": 999,
				"ApplicationUpdate":           map[string]any{"ApplicationCodeUpdate": "x"},
			},
			setup: func(_ *kinesisanalytics.Handler, b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "version-app", "", "", nil)
			},
			wantCode: http.StatusBadRequest,
			wantType: "ConcurrentModificationException",
		},
		{
			name:     "create missing name returns InvalidArgumentException",
			action:   "CreateApplication",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidArgumentException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			if tt.setup != nil {
				tt.setup(h, b)
			}

			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantType, resp["__type"])
		})
	}
}
