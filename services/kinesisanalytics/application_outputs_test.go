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

func TestHandler_AddApplicationOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend)
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "adds kinesis streams output",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "output-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "output-app",
				"CurrentApplicationVersionId": 1,
				"Output": map[string]any{
					"Name": "DESTINATION_SQL_STREAM",
					"KinesisStreamsOutput": map[string]any{
						"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/out",
						"RoleARN":     "arn:aws:iam::000000000000:role/role",
					},
					"DestinationSchema": map[string]any{"RecordFormatType": "JSON"},
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
				"Output": map[string]any{
					"Name": "OUT",
					"KinesisStreamsOutput": map[string]any{
						"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/out",
						"RoleARN":     "arn:aws:iam::000000000000:role/r",
					},
					"DestinationSchema": map[string]any{"RecordFormatType": "JSON"},
				},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "version mismatch",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "output-ver-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "output-ver-app",
				"CurrentApplicationVersionId": 99,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			// AWS models Output.Name as a required member; a request missing it must be
			// rejected rather than silently stored with an empty name.
			name: "missing Name is rejected",
			setup: func(b *kinesisanalytics.InMemoryBackend) {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "output-noname-app", "", "", nil)
			},
			input: map[string]any{
				"ApplicationName":             "output-noname-app",
				"CurrentApplicationVersionId": 1,
				"Output": map[string]any{
					"KinesisStreamsOutput": map[string]any{
						"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/out",
						"RoleARN":     "arn:aws:iam::000000000000:role/role",
					},
					"DestinationSchema": map[string]any{"RecordFormatType": "JSON"},
				},
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

			rec := doRequest(t, h, "AddApplicationOutput", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				app, err := b.DescribeApplication(context.Background(), "output-app")
				require.NoError(t, err)
				assert.NotEmpty(t, app.Outputs)
			}
		})
	}
}

func TestHandler_DeleteApplicationOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*kinesisanalytics.InMemoryBackend) string
		input      func(outputID string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "deletes existing output",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-out-app", "", "", nil)
				_ = b.AddApplicationOutput(
					context.Background(),
					"del-out-app",
					1,
					kinesisanalytics.OutputDescription{Name: "STREAM_OUT"},
				)
				app, _ := b.DescribeApplication(context.Background(), "del-out-app")

				return app.Outputs[0].OutputID
			},
			input: func(outputID string) map[string]any {
				return map[string]any{
					"ApplicationName":             "del-out-app",
					"CurrentApplicationVersionId": 2,
					"OutputId":                    outputID,
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
			name: "output id not found",
			setup: func(b *kinesisanalytics.InMemoryBackend) string {
				_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "del-out-notfound", "", "", nil)

				return "nonexistent"
			},
			input: func(outputID string) map[string]any {
				return map[string]any{
					"ApplicationName":             "del-out-notfound",
					"CurrentApplicationVersionId": 1,
					"OutputId":                    outputID,
				}
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			outputID := tt.setup(b)
			rec := doRequest(t, h, "DeleteApplicationOutput", tt.input(outputID))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				app, err := b.DescribeApplication(context.Background(), "del-out-app")
				require.NoError(t, err)
				assert.Empty(t, app.Outputs)
			}
		})
	}
}

// TestDeleteApplicationOutput_Validation verifies missing output ID returns 400.
func TestDeleteApplicationOutput_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DeleteApplicationOutput", map[string]any{"ApplicationName": "my-app"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestVersionNotBumpedOnDeleteOutputNotFound verifies no phantom bump for output.
func TestVersionNotBumpedOnDeleteOutputNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, _ = kinesisanalytics.CreateApp(b, testRegion, testAccountID, "out-bump", "", "", nil)

	err := b.DeleteApplicationOutput(context.Background(), "out-bump", 1, "nonexistent-output-id")
	require.ErrorIs(t, err, kinesisanalytics.ErrNotFound)

	app, _ := b.DescribeApplication(context.Background(), "out-bump")
	assert.Equal(t, int64(1), app.ApplicationVersionID)
}

// TestAddOutputRoundTrip verifies output appears in DescribeApplication.
func TestAddOutputRoundTrip(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	app, _ := kinesisanalytics.CreateApp(b, testRegion, testAccountID, "out-rt-app", "", "", nil)

	rec := doRequest(t, h, "AddApplicationOutput", map[string]any{
		"ApplicationName":             app.ApplicationName,
		"CurrentApplicationVersionId": app.ApplicationVersionID,
		"Output": map[string]any{
			"Name": "DESTINATION_SQL_STREAM",
			"KinesisStreamsOutput": map[string]any{
				"ResourceARN": "arn:aws:kinesis:us-east-1:000:stream/s",
				"RoleARN":     "arn:aws:iam::000:role/r",
			},
			"DestinationSchema": map[string]any{"RecordFormatType": "JSON"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, "DescribeApplication", map[string]any{"ApplicationName": app.ApplicationName})
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	detail := resp["ApplicationDetail"].(map[string]any)
	outList := detail["OutputDescriptions"].([]any)
	assert.Len(t, outList, 1)
}
