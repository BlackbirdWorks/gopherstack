package kinesisanalyticsv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertErrorType(t *testing.T, body []byte, wantType string) {
	t.Helper()

	var resp map[string]any
	require.NoError(t, json.Unmarshal(body, &resp))
	assert.Equal(t, wantType, resp["__type"])
}

func assertAppStatus(t *testing.T, body []byte, wantStatus string) {
	t.Helper()

	var resp map[string]any
	require.NoError(t, json.Unmarshal(body, &resp))

	detail, ok := resp["ApplicationDetail"].(map[string]any)
	require.True(t, ok, "response missing ApplicationDetail")
	assert.Equal(t, wantStatus, detail["ApplicationStatus"])
}

// TestParityLifecycle_StartApplication verifies StartApplication state transitions
// match real AWS Kinesis Analytics v2 behavior:
//   - READY → RUNNING succeeds (200)
//   - RUNNING → RUNNING fails with ResourceInUseException (409)
func TestParityLifecycle_StartApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantType   string
		wantStatus int
		preStart   bool
	}{
		{
			name:       "ready_to_running",
			preStart:   false,
			wantStatus: http.StatusOK,
		},
		{
			name:       "already_running",
			preStart:   true,
			wantStatus: http.StatusConflict,
			wantType:   "ResourceInUseException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
				"ApplicationName":    "lifecycle-app",
				"RuntimeEnvironment": "FLINK-1_18",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.preStart {
				pre := doKAV2Request(t, h, "StartApplication", map[string]any{
					"ApplicationName": "lifecycle-app",
				})
				require.Equal(t, http.StatusOK, pre.Code)
			}

			rec = doKAV2Request(t, h, "StartApplication", map[string]any{
				"ApplicationName": "lifecycle-app",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantType != "" {
				assertErrorType(t, rec.Body.Bytes(), tt.wantType)
			}
		})
	}
}

// TestParityLifecycle_StopApplication verifies StopApplication state transitions
// match real AWS Kinesis Analytics v2 behavior:
//   - RUNNING → READY succeeds (200)
//   - READY → READY fails with ResourceInUseException (409)
func TestParityLifecycle_StopApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantType   string
		wantStatus int
		preStart   bool
	}{
		{
			name:       "running_to_ready",
			preStart:   true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "already_ready",
			preStart:   false,
			wantStatus: http.StatusConflict,
			wantType:   "ResourceInUseException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
				"ApplicationName":    "lifecycle-stop-app",
				"RuntimeEnvironment": "FLINK-1_18",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.preStart {
				pre := doKAV2Request(t, h, "StartApplication", map[string]any{
					"ApplicationName": "lifecycle-stop-app",
				})
				require.Equal(t, http.StatusOK, pre.Code)
			}

			rec = doKAV2Request(t, h, "StopApplication", map[string]any{
				"ApplicationName": "lifecycle-stop-app",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantType != "" {
				assertErrorType(t, rec.Body.Bytes(), tt.wantType)
			}
		})
	}
}

// TestParityLifecycle_CreateApplicationSnapshot verifies snapshot creation requires
// the application to be in RUNNING state, matching real AWS behavior.
func TestParityLifecycle_CreateApplicationSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantType   string
		wantStatus int
		preStart   bool
	}{
		{
			name:       "running_succeeds",
			preStart:   true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "ready_fails",
			preStart:   false,
			wantStatus: http.StatusConflict,
			wantType:   "ResourceInUseException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
				"ApplicationName":    "snap-lifecycle-app",
				"RuntimeEnvironment": "FLINK-1_18",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.preStart {
				pre := doKAV2Request(t, h, "StartApplication", map[string]any{
					"ApplicationName": "snap-lifecycle-app",
				})
				require.Equal(t, http.StatusOK, pre.Code)
			}

			rec = doKAV2Request(t, h, "CreateApplicationSnapshot", map[string]any{
				"ApplicationName": "snap-lifecycle-app",
				"SnapshotName":    "snap-1",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantType != "" {
				assertErrorType(t, rec.Body.Bytes(), tt.wantType)
			}
		})
	}
}

// TestParityLifecycle_StartStop_RoundTrip verifies a full READY→RUNNING→READY cycle.
func TestParityLifecycle_StartStop_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "roundtrip-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify initial state is READY.
	descRec := doKAV2Request(t, h, "DescribeApplication", map[string]any{
		"ApplicationName": "roundtrip-app",
	})
	require.Equal(t, http.StatusOK, descRec.Code)
	assertAppStatus(t, descRec.Body.Bytes(), "READY")

	// Start → RUNNING.
	rec = doKAV2Request(t, h, "StartApplication", map[string]any{
		"ApplicationName": "roundtrip-app",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec = doKAV2Request(t, h, "DescribeApplication", map[string]any{
		"ApplicationName": "roundtrip-app",
	})
	require.Equal(t, http.StatusOK, descRec.Code)
	assertAppStatus(t, descRec.Body.Bytes(), "RUNNING")

	// Stop → READY.
	rec = doKAV2Request(t, h, "StopApplication", map[string]any{
		"ApplicationName": "roundtrip-app",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec = doKAV2Request(t, h, "DescribeApplication", map[string]any{
		"ApplicationName": "roundtrip-app",
	})
	require.Equal(t, http.StatusOK, descRec.Code)
	assertAppStatus(t, descRec.Body.Bytes(), "READY")
}

// TestParityLifecycle_SnapshotDuplicateName verifies duplicate snapshot name
// returns ResourceInUseException when app is RUNNING.
func TestParityLifecycle_SnapshotDuplicateName(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "dup-snap-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "StartApplication", map[string]any{
		"ApplicationName": "dup-snap-app",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "CreateApplicationSnapshot", map[string]any{
		"ApplicationName": "dup-snap-app",
		"SnapshotName":    "snap-dup",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "CreateApplicationSnapshot", map[string]any{
		"ApplicationName": "dup-snap-app",
		"SnapshotName":    "snap-dup",
	})
	assert.Equal(t, http.StatusConflict, rec.Code)
	assertErrorType(t, rec.Body.Bytes(), "ResourceInUseException")
}
