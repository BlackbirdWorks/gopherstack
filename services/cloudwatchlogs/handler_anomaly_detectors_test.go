package cloudwatchlogs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_LogAnomalyDetector_Create(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(backend)

	rec := doLogsRequest(
		t,
		h,
		e,
		"CreateLogAnomalyDetector",
		`{"logGroupArnList":["arn:aws:logs:us-east-1:123:log-group:/app"],`+
			`"detectorName":"my-detector","evaluationFrequency":"FIVE_MIN"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	arn, ok := out["anomalyDetectorArn"].(string)
	require.True(t, ok)
	assert.Contains(t, arn, "log-anomaly-detector")
}

func TestHandler_CreateLogAnomalyDetector_EvaluationFrequency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name: "valid_five_min",
			body: `{"logGroupArnList":["arn:aws:logs:us-east-1:123:log-group:/app"],` +
				`"evaluationFrequency":"FIVE_MIN"}`,
			wantCode: http.StatusOK,
		},
		{
			name: "valid_one_hour",
			body: `{"logGroupArnList":["arn:aws:logs:us-east-1:123:log-group:/app"],` +
				`"evaluationFrequency":"ONE_HOUR"}`,
			wantCode: http.StatusOK,
		},
		{
			name: "invalid_frequency",
			body: `{"logGroupArnList":["arn:aws:logs:us-east-1:123:log-group:/app"],` +
				`"evaluationFrequency":"EVERY_5_MINUTES"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "empty_frequency_ok",
			body: `{"logGroupArnList":["arn:aws:logs:us-east-1:123:log-group:/app"],` +
				`"evaluationFrequency":""}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "empty_log_group_list_fails",
			body:     `{"logGroupArnList":[]}`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := makeLogsRequest(t, "CreateLogAnomalyDetector", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_UpdateLogAnomalyDetector_EnabledPauseResume locks two things
// about the real UpdateLogAnomalyDetector contract (aws-sdk-go-v2
// UpdateLogAnomalyDetectorInput.Enabled, a required field): calling it with
// enabled=false must move the detector to PAUSED status, and enabled=true
// must resume a paused detector to ANALYZING. It also locks the wire key for
// status: anomalyDetectorStatus, not detectorStatus.
func TestHandler_UpdateLogAnomalyDetector_EnabledPauseResume(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(backend)

	createRec := doLogsRequest(t, h, e, "CreateLogAnomalyDetector",
		`{"logGroupArnList":["arn:aws:logs:us-east-1:123:log-group:/app"],"detectorName":"my-detector"}`)
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	detectorArn, ok := createOut["anomalyDetectorArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, detectorArn)

	getStatus := func(t *testing.T) string {
		t.Helper()

		rec := doLogsRequest(t, h, e, "GetLogAnomalyDetector",
			`{"anomalyDetectorArn":"`+detectorArn+`"}`)
		require.Equal(t, http.StatusOK, rec.Code)

		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		detector, detectorOK := out["anomalyDetector"].(map[string]any)
		require.True(t, detectorOK)
		_, hasOldKey := detector["detectorStatus"]
		assert.False(t, hasOldKey, "wire key must be anomalyDetectorStatus, not detectorStatus")
		status, statusOK := detector["anomalyDetectorStatus"].(string)
		require.True(t, statusOK)

		return status
	}

	assert.Equal(t, "INITIALIZING", getStatus(t))

	pauseRec := doLogsRequest(t, h, e, "UpdateLogAnomalyDetector",
		`{"anomalyDetectorArn":"`+detectorArn+`","enabled":false}`)
	require.Equal(t, http.StatusOK, pauseRec.Code)
	assert.Equal(t, "PAUSED", getStatus(t))

	resumeRec := doLogsRequest(t, h, e, "UpdateLogAnomalyDetector",
		`{"anomalyDetectorArn":"`+detectorArn+`","enabled":true}`)
	require.Equal(t, http.StatusOK, resumeRec.Code)
	assert.Equal(t, "ANALYZING", getStatus(t))
}

func TestHandler_CreateLogAnomalyDetectorOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantKey  string
		wantVal  string
		wantCode int
	}{
		// CreateLogAnomalyDetector
		{
			name:   "CreateLogAnomalyDetector/OK",
			action: "CreateLogAnomalyDetector",
			body: map[string]any{
				"logGroupArnList": []string{"arn:aws:logs:us-east-1:123:log-group:/my/group"},
			},
			wantCode: http.StatusOK,
			wantKey:  "anomalyDetectorArn",
		},
		{
			name:     "CreateLogAnomalyDetector/EmptyList",
			action:   "CreateLogAnomalyDetector",
			body:     map[string]any{"logGroupArnList": []string{}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "CreateLogAnomalyDetector/MissingList",
			action:   "CreateLogAnomalyDetector",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := cloudwatchlogs.NewInMemoryBackend()
			h := cloudwatchlogs.NewHandler(backend)

			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK && tt.wantKey != "" {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				if tt.wantVal != "" {
					assert.Equal(t, tt.wantVal, out[tt.wantKey])
				} else {
					assert.NotEmpty(t, out[tt.wantKey], "expected non-empty %s", tt.wantKey)
				}
			}
		})
	}
}
