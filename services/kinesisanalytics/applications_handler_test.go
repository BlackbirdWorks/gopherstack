package kinesisanalytics_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalytics"
)

func TestHandler_CreateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		name       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "creates application",
			input:      map[string]any{"ApplicationName": "my-app"},
			wantStatus: http.StatusOK,
			wantKey:    "ApplicationSummary",
		},
		{
			name:       "missing application name",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateApplication", tt.input)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantKey != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, tt.wantKey)
			}
		})
	}
}

func TestHandler_DescribeApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend)
		input      map[string]any
		name       string
		appName    string
		wantStatus int
	}{
		{
			name:    "describes existing application",
			appName: "existing-app",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "existing-app", "", "", nil)
			},
			input:      map[string]any{"ApplicationName": "existing-app"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found for missing application",
			input:      map[string]any{"ApplicationName": "missing"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, "DescribeApplication", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "deletes existing application",
			setup: func(b *kinesisanalytics.InMemoryBackend) map[string]any {
				app, _ := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-app", "", "", nil)

				return map[string]any{
					"ApplicationName": "del-app",
					"CreateTimestamp": float64(app.CreateTimestamp.Unix()),
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found for missing application",
			setup: func(_ *kinesisanalytics.InMemoryBackend) map[string]any {
				return map[string]any{"ApplicationName": "ghost", "CreateTimestamp": float64(1234567890)}
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			input := tt.setup(b)

			rec := doRequest(t, h, "DeleteApplication", input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestDeleteApplication_TimestampMismatch_ConcurrentModificationException verifies
// that supplying a wrong CreateTimestamp to DeleteApplication returns
// ConcurrentModificationException (not LimitExceededException). Real AWS Kinesis Analytics
// uses ConcurrentModificationException for this condition.
func TestDeleteApplication_TimestampMismatch_ConcurrentModificationException(t *testing.T) {
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

// TestDeleteApplication_CorrectTimestamp_Succeeds verifies that a matching
// CreateTimestamp allows deletion (regression guard for the timestamp check logic).
func TestDeleteApplication_CorrectTimestamp_Succeeds(t *testing.T) {
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

func TestHandler_ListApplications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend)
		input      map[string]any
		name       string
		wantCount  int
		wantStatus int
	}{
		{
			name: "lists all applications",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "app-1", "", "", nil)
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "app-2", "", "", nil)
			},
			input:      map[string]any{},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "empty list",
			input:      map[string]any{},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, "ListApplications", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			summaries, _ := resp["ApplicationSummaries"].([]any)
			assert.Len(t, summaries, tt.wantCount)
		})
	}
}

func TestHandler_StartStopApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend)
		input      map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name: "starts application",
			op:   "StartApplication",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "start-app", "", "", nil)
			},
			input:      map[string]any{"ApplicationName": "start-app"},
			wantStatus: http.StatusOK,
		},
		{
			name: "stops application",
			op:   "StopApplication",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "stop-app", "", "", nil)
				_ = kinesisanalytics.StartAppNoConfig(b, "stop-app")
			},
			input:      map[string]any{"ApplicationName": "stop-app"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "start not found",
			op:         "StartApplication",
			input:      map[string]any{"ApplicationName": "ghost"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			// For stop operations, wait until app is RUNNING before proceeding
			if tt.op == "StopApplication" && tt.input["ApplicationName"] != nil {
				kinesisanalytics.WaitForStatus(t, b, tt.input["ApplicationName"].(string), "RUNNING")
			}

			rec := doRequest(t, h, tt.op, tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_StopApplication_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "StopApplication", map[string]any{"ApplicationName": "ghost"})

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestPersistenceSnapshotRestore verifies that Snapshot/Restore round-trips
// applications, and that the byARN index is rebuilt so tag operations keep working.
func TestPersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*kinesisanalytics.InMemoryBackend)
		name    string
		wantLen int
	}{
		{
			name:    "empty_backend",
			setup:   func(_ *kinesisanalytics.InMemoryBackend) {},
			wantLen: 0,
		},
		{
			name: "with_applications",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(
					b, testRegion, testAccountID,
					"persist-app-1", "desc", "SELECT 1",
					map[string]string{"env": "test"},
				)
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "persist-app-2", "", "", nil)
			},
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			tt.setup(b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := newBackend()
			require.NoError(t, b2.Restore(t.Context(), snap))

			apps, _, err := b2.ListApplications(context.Background(), "", 0)
			require.NoError(t, err)
			require.Len(t, apps, tt.wantLen)

			// Verify appsByARN index is rebuilt: tag operations should work via ARN.
			if tt.wantLen > 0 {
				descApp, descErr := b2.DescribeApplication(context.Background(), "persist-app-1")
				require.NoError(t, descErr)

				tagErr := b2.TagResource(context.Background(), descApp.ApplicationARN, map[string]string{"new": "tag"})
				require.NoError(t, tagErr)

				tags, listErr := b2.ListTagsForResource(context.Background(), descApp.ApplicationARN)
				require.NoError(t, listErr)
				assert.Equal(t, "tag", tags["new"])
				assert.Equal(t, "test", tags["env"])
			}

			// Snapshot isolation: mutating b2 should not affect original snapshot bytes.
			if tt.wantLen > 0 {
				_, _ = kinesisanalytics.CreateApp(b2, testRegion, testAccountID, "extra-app", "", "", nil)
				snap2 := b2.Snapshot(t.Context())
				assert.NotEqual(t, snap, snap2)
			}
		})
	}
}

// TestPersistenceRoundTrip verifies Snapshot/Restore preserves full state including nextID.
func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*kinesisanalytics.InMemoryBackend)
		name    string
		wantLen int
	}{
		{
			name:    "empty",
			setup:   func(_ *kinesisanalytics.InMemoryBackend) {},
			wantLen: 0,
		},
		{
			name: "with_applications",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				app, _ := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "persist-1", "desc", "SELECT 1",
					map[string]string{"env": "test"})
				// Add a sub-resource to exercise nextID persistence.
				_ = b.AddApplicationCloudWatchLoggingOption(context.Background(), "persist-1", app.ApplicationVersionID,
					kinesisanalytics.CloudWatchLoggingOptionDesc{
						LogStreamARN: "arn:aws:logs:us-east-1:000:log-group:x:log-stream:y",
						RoleARN:      "arn:aws:iam::000000000000:role/r",
					})
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "persist-2", "", "", nil)
			},
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			tt.setup(b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := newBackend()
			require.NoError(t, b2.Restore(t.Context(), snap))

			assert.Equal(t, tt.wantLen, kinesisanalytics.ApplicationCount(b2))

			if tt.wantLen > 0 {
				app, err := b2.DescribeApplication(context.Background(), "persist-1")
				require.NoError(t, err)
				assert.Equal(t, "test", app.Tags["env"])
				assert.Len(t, app.CloudWatchLoggingOptions, 1)

				// nextID is preserved: next allocation should not collide with existing IDs.
				_ = b2.AddApplicationCloudWatchLoggingOption(
					context.Background(),
					"persist-1",
					app.ApplicationVersionID,
					kinesisanalytics.CloudWatchLoggingOptionDesc{
						LogStreamARN: "arn:aws:logs:us-east-1:000000000000:log-group:g2:log-stream:s2",
						RoleARN:      "arn:aws:iam::000000000000:role/r",
					},
				)
				app2, _ := b2.DescribeApplication(context.Background(), "persist-1")
				// The new option must have a different ID from the restored one.
				assert.NotEqual(t, app.CloudWatchLoggingOptions[0].CloudWatchLoggingOptionID,
					app2.CloudWatchLoggingOptions[1].CloudWatchLoggingOptionID)
			}
		})
	}
}

// TestPersistenceEmpty verifies Snapshot on empty backend produces valid JSON.
func TestPersistenceEmpty(t *testing.T) {
	t.Parallel()

	b := newBackend()
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := newBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))
	assert.Zero(t, kinesisanalytics.ApplicationCount(b2))
}
