package kinesisanalyticsv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

func TestKAV2_DeleteApplicationCloudWatchLoggingOption(t *testing.T) {
	t.Parallel()

	// addCWLOption creates app and adds a CWL option, returning the option ID.
	addCWLOption := func(h *kinesisanalyticsv2.Handler, appName string) string {
		doKAV2Request(t, h, "CreateApplication", map[string]any{
			"ApplicationName":    appName,
			"RuntimeEnvironment": "SQL-1_0",
		})
		rec := doKAV2Request(t, h, "AddApplicationCloudWatchLoggingOption", map[string]any{
			"ApplicationName": appName,
			"CloudWatchLoggingOption": map[string]any{
				"LogStreamARN": "arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s",
			},
		})

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		descs := out["CloudWatchLoggingOptionDescriptions"].([]any)
		desc := descs[0].(map[string]any)

		return desc["CloudWatchLoggingOptionId"].(string)
	}

	tests := []struct {
		appName      string
		optionID     string
		name         string
		wantCWLCount int
		wantStatus   int
	}{
		{
			name:         "success",
			appName:      "del-cwl-app",
			wantStatus:   http.StatusOK,
			wantCWLCount: 0,
		},
		{
			name:       "app_not_found",
			appName:    "missing-app",
			optionID:   "cwl-1",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			optionID := tt.optionID

			if tt.name == "success" {
				optionID = addCWLOption(h, tt.appName)
			}

			rec := doKAV2Request(t, h, "DeleteApplicationCloudWatchLoggingOption", map[string]any{
				"ApplicationName":           tt.appName,
				"CloudWatchLoggingOptionId": optionID,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				descs, ok := out["CloudWatchLoggingOptionDescriptions"].([]any)
				require.True(t, ok)
				assert.Len(t, descs, tt.wantCWLCount)
			}
		})
	}
}

// TestKAV2_DeleteApplicationCWL_NotFound verifies deleting a nonexistent
// CloudWatch logging option returns 404, not a silent success.
func TestKAV2_DeleteApplicationCWL_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "del-cwl-app",
		"RuntimeEnvironment": "SQL-1_0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "DeleteApplicationCloudWatchLoggingOption", map[string]any{
		"ApplicationName":           "del-cwl-app",
		"CloudWatchLoggingOptionId": "cwl-nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestKAV2_DeleteApplicationInputProcessingConfiguration(t *testing.T) {
	t.Parallel()

	setupInputWithProc := func(h *kinesisanalyticsv2.Handler, appName string) string {
		doKAV2Request(t, h, "CreateApplication", map[string]any{
			"ApplicationName":    appName,
			"RuntimeEnvironment": "SQL-1_0",
		})
		rec := doKAV2Request(t, h, "AddApplicationInput", map[string]any{
			"ApplicationName": appName,
			"Input": map[string]any{
				"NamePrefix": "PREFIX",
				"KinesisStreamsInput": map[string]any{
					"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/s",
				},
			},
		})

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		inputs := out["InputDescriptions"].([]any)
		input := inputs[0].(map[string]any)
		inputID := input["InputId"].(string)

		doKAV2Request(t, h, "AddApplicationInputProcessingConfiguration", map[string]any{
			"ApplicationName": appName,
			"InputId":         inputID,
			"InputProcessingConfiguration": map[string]any{
				"InputLambdaProcessor": map[string]any{
					"ResourceARN": "arn:aws:lambda:us-east-1:000000000000:function:fn",
				},
			},
		})

		return inputID
	}

	tests := []struct {
		appName    string
		inputID    string
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			appName:    "del-proc-app",
			wantStatus: http.StatusOK,
		},
		{
			name:       "app_not_found",
			appName:    "missing-app",
			inputID:    "input-1",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			inputID := tt.inputID

			if tt.name == "success" {
				inputID = setupInputWithProc(h, tt.appName)
			}

			rec := doKAV2Request(t, h, "DeleteApplicationInputProcessingConfiguration", map[string]any{
				"ApplicationName": tt.appName,
				"InputId":         inputID,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestKAV2_DeleteApplicationInputProcessingConfig_NotFound verifies deleting
// a processing config for a nonexistent input returns 404.
func TestKAV2_DeleteApplicationInputProcessingConfig_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "del-ipc-app",
		"RuntimeEnvironment": "SQL-1_0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "DeleteApplicationInputProcessingConfiguration", map[string]any{
		"ApplicationName": "del-ipc-app",
		"InputId":         "input-nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestKAV2_DeleteApplicationOutput(t *testing.T) {
	t.Parallel()

	addOutput := func(h *kinesisanalyticsv2.Handler, appName string) string {
		doKAV2Request(t, h, "CreateApplication", map[string]any{
			"ApplicationName":    appName,
			"RuntimeEnvironment": "SQL-1_0",
		})
		rec := doKAV2Request(t, h, "AddApplicationOutput", map[string]any{
			"ApplicationName": appName,
			"Output": map[string]any{
				"Name": "OUTPUT_STREAM",
				"KinesisStreamsOutput": map[string]any{
					"ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/s",
				},
				"DestinationSchema": map[string]any{
					"RecordFormatType": "JSON",
				},
			},
		})

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		outputs := out["OutputDescriptions"].([]any)
		output := outputs[0].(map[string]any)

		return output["OutputId"].(string)
	}

	tests := []struct {
		appName      string
		outputID     string
		name         string
		wantOutCount int
		wantStatus   int
	}{
		{
			name:         "success",
			appName:      "del-out-app",
			wantStatus:   http.StatusOK,
			wantOutCount: 0,
		},
		{
			name:       "app_not_found",
			appName:    "missing-app",
			outputID:   "output-1",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "output_not_found",
			appName:    "del-out-notfound-app",
			outputID:   "nonexistent-output",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			outputID := tt.outputID

			switch tt.name {
			case "success":
				outputID = addOutput(h, tt.appName)
			case "output_not_found":
				doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    tt.appName,
					"RuntimeEnvironment": "SQL-1_0",
				})
			}

			rec := doKAV2Request(t, h, "DeleteApplicationOutput", map[string]any{
				"ApplicationName": tt.appName,
				"OutputId":        outputID,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out["ApplicationARN"])
			}
		})
	}
}

// TestKAV2_DeleteApplicationOutput_NotFound verifies deleting a nonexistent
// output returns 404.
func TestKAV2_DeleteApplicationOutput_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "del-out-app",
		"RuntimeEnvironment": "SQL-1_0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "DeleteApplicationOutput", map[string]any{
		"ApplicationName": "del-out-app",
		"OutputId":        "output-nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestKAV2_DeleteAppReferenceDataSource exercises
// DeleteApplicationReferenceDataSource's happy-dispatch code path.
func TestKAV2_DeleteAppReferenceDataSource(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)
	createKAV2App(t, h, "ref-app")

	rec := doKAV2Request(t, h, "DeleteApplicationReferenceDataSource", map[string]any{
		"ApplicationName":             "ref-app",
		"CurrentApplicationVersionId": 1,
		"ReferenceId":                 "ref-1",
	})
	assert.Positive(t, rec.Code)
}
