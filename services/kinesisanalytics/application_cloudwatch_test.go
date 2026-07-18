package kinesisanalytics_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalytics"
)

func TestHandler_AddApplicationCloudWatchLoggingOption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend)
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "adds logging option successfully",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "cwl-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "cwl-app",
				"CurrentApplicationVersionId": 1,
				"CloudWatchLoggingOption": map[string]any{
					"LogStreamARN": "arn:aws:logs:us-east-1:000000000000:log-group:test:log-stream:s1",
					"RoleARN":      "arn:aws:iam::000000000000:role/role",
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing application name",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "app not found",
			input: map[string]any{
				"ApplicationName":             "ghost",
				"CurrentApplicationVersionId": 1,
				"CloudWatchLoggingOption": map[string]any{
					"LogStreamARN": "arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s",
					"RoleARN":      "arn:aws:iam::000000000000:role/r",
				},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "version mismatch",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "cwl-ver-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "cwl-ver-app",
				"CurrentApplicationVersionId": 99,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, "AddApplicationCloudWatchLoggingOption", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				app, err := b.DescribeApplication(context.Background(), "cwl-app")
				require.NoError(t, err)
				assert.NotEmpty(t, app.CloudWatchLoggingOptions)
			}
		})
	}
}

func TestHandler_DeleteApplicationCloudWatchLoggingOption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend) string
		input      func(optID string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "deletes existing logging option",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-cwl-app", "", "", nil)
				_ = b.AddApplicationCloudWatchLoggingOption(
					context.Background(),
					"del-cwl-app",
					1,
					kinesisanalytics.CloudWatchLoggingOptionDesc{
						LogStreamARN: "arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s",
						RoleARN:      "arn:aws:iam::000000000000:role/r",
					},
				)
				app, _ := b.DescribeApplication(context.Background(), "del-cwl-app")

				return app.CloudWatchLoggingOptions[0].CloudWatchLoggingOptionID
			},
			input: func(optID string) map[string]any {
				return map[string]any{
					"ApplicationName":             "del-cwl-app",
					"CurrentApplicationVersionId": 2,
					"CloudWatchLoggingOptionId":   optID,
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:  "missing application name",
			setup: func(_ *kinesisanalytics.InMemoryBackend) string { return "" },
			input: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "logging option id not found",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-cwl-notfound", "", "", nil)

				return "nonexistent"
			},
			input: func(optID string) map[string]any {
				return map[string]any{
					"ApplicationName":             "del-cwl-notfound",
					"CurrentApplicationVersionId": 1,
					"CloudWatchLoggingOptionId":   optID,
				}
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			optID := tt.setup(b)
			rec := doRequest(t, h, "DeleteApplicationCloudWatchLoggingOption", tt.input(optID))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				app, err := b.DescribeApplication(context.Background(), "del-cwl-app")
				require.NoError(t, err)
				assert.Empty(t, app.CloudWatchLoggingOptions)
			}
		})
	}
}

// TestDeleteApplicationCloudWatchLoggingOption_Validation verifies empty ID/app name returns 400.
func TestDeleteApplicationCloudWatchLoggingOption_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "missing_app_name",
			action: "DeleteApplicationCloudWatchLoggingOption",
			body:   map[string]any{"CloudWatchLoggingOptionId": "cwl-1"},
		},
		{
			name:   "missing_option_id",
			action: "DeleteApplicationCloudWatchLoggingOption",
			body:   map[string]any{"ApplicationName": "my-app"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestVersionNotBumpedOnDeleteCWLNotFound verifies no phantom version bump when CWL ID not found.
func TestVersionNotBumpedOnDeleteCWLNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "bump-test", "", "", nil)

	// Try to delete a non-existent CWL option — version 1 passed.
	err := b.DeleteApplicationCloudWatchLoggingOption(context.Background(), "bump-test", 1, "nonexistent-cwl-id")
	require.ErrorIs(t, err, kinesisanalytics.ErrNotFound)

	// Version should still be 1 (not bumped) so the next version-1 call succeeds.
	app, err := b.DescribeApplication(context.Background(), "bump-test")
	require.NoError(t, err)
	assert.Equal(t, int64(1), app.ApplicationVersionID)
}

// TestAddCWLRoundTrip verifies CWL option appears in DescribeApplication.
func TestAddCWLRoundTrip(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	app, _ := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "cwl-rt-app", "", "", nil)

	rec := doRequest(t, h, "AddApplicationCloudWatchLoggingOption", map[string]any{
		"ApplicationName":             app.ApplicationName,
		"CurrentApplicationVersionId": app.ApplicationVersionID,
		"CloudWatchLoggingOption": map[string]any{
			"LogStreamARN": "arn:aws:logs:us-east-1:000:log-group:g:log-stream:s",
			"RoleARN":      "arn:aws:iam::000:role/r",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify it appears in describe.
	rec2 := doRequest(t, h, "DescribeApplication", map[string]any{"ApplicationName": app.ApplicationName})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	detail := resp["ApplicationDetail"].(map[string]any)
	cwlList := detail["CloudWatchLoggingOptionDescriptions"].([]any)
	assert.Len(t, cwlList, 1)
}

// TestDeleteCWLRoundTrip verifies CWL option is removed after delete.
func TestDeleteCWLRoundTrip(t *testing.T) {
	t.Parallel()

	b := newBackend()
	app, _ := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-cwl-app", "", "", nil)

	// Add a CWL option.
	_ = b.AddApplicationCloudWatchLoggingOption(context.Background(), app.ApplicationName, app.ApplicationVersionID,
		kinesisanalytics.CloudWatchLoggingOptionDesc{
			LogStreamARN: "arn:aws:logs:us-east-1:000:log-group:g:log-stream:s",
			RoleARN:      "arn:aws:iam::000000000000:role/r",
		})

	app2, _ := b.DescribeApplication(context.Background(), app.ApplicationName)
	require.Len(t, app2.CloudWatchLoggingOptions, 1)
	cwlID := app2.CloudWatchLoggingOptions[0].CloudWatchLoggingOptionID

	// Delete it.
	err := b.DeleteApplicationCloudWatchLoggingOption(
		context.Background(),
		app.ApplicationName,
		app2.ApplicationVersionID,
		cwlID,
	)
	require.NoError(t, err)

	app3, _ := b.DescribeApplication(context.Background(), app.ApplicationName)
	assert.Empty(t, app3.CloudWatchLoggingOptions)
}
