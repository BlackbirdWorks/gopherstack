package cloudwatchlogs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
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

// TestHandler_UpdateLogAnomalyDetector_EnabledPauseResume locks three things
// about the real UpdateLogAnomalyDetector/GetLogAnomalyDetector contract:
// UpdateLogAnomalyDetectorInput.Enabled (a required field) with enabled=false
// must move the detector to PAUSED status, and enabled=true must resume a
// paused detector to ANALYZING; the wire key for status is
// anomalyDetectorStatus, not detectorStatus; and GetLogAnomalyDetectorOutput's
// members sit flat at the top level -- there is no "anomalyDetector" wrapper
// (deserializers.go's awsAwsjson11_deserializeOpDocumentGetLogAnomalyDetectorOutput
// switches directly on anomalyDetectorStatus/detectorName/etc.). Driven
// through the real aws-sdk-go-v2 client so a wrapped or mis-keyed response
// fails to compile-shape rather than merely failing a map-key assertion.
func TestHandler_UpdateLogAnomalyDetector_EnabledPauseResume(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(backend)
	client := newTestCloudWatchLogsClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateLogAnomalyDetector(ctx, &cwlsdk.CreateLogAnomalyDetectorInput{
		LogGroupArnList: []string{"arn:aws:logs:us-east-1:123:log-group:/app"},
		DetectorName:    aws.String("my-detector"),
	})
	require.NoError(t, err)
	detectorArn := aws.ToString(createOut.AnomalyDetectorArn)
	require.NotEmpty(t, detectorArn)

	getStatus := func(t *testing.T) cwltypes.AnomalyDetectorStatus {
		t.Helper()

		out, getErr := client.GetLogAnomalyDetector(ctx, &cwlsdk.GetLogAnomalyDetectorInput{
			AnomalyDetectorArn: aws.String(detectorArn),
		})
		require.NoError(t, getErr)
		assert.Equal(t, "my-detector", aws.ToString(out.DetectorName))

		return out.AnomalyDetectorStatus
	}

	assert.Equal(t, cwltypes.AnomalyDetectorStatusInitializing, getStatus(t))

	_, err = client.UpdateLogAnomalyDetector(ctx, &cwlsdk.UpdateLogAnomalyDetectorInput{
		AnomalyDetectorArn: aws.String(detectorArn),
		Enabled:            aws.Bool(false),
	})
	require.NoError(t, err)
	assert.Equal(t, cwltypes.AnomalyDetectorStatusPaused, getStatus(t))

	_, err = client.UpdateLogAnomalyDetector(ctx, &cwlsdk.UpdateLogAnomalyDetectorInput{
		AnomalyDetectorArn: aws.String(detectorArn),
		Enabled:            aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, cwltypes.AnomalyDetectorStatusAnalyzing, getStatus(t))
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

// TestListAnomalies_WireShape drives ListAnomalies through the real SDK
// client against an anomaly seeded (via the AddAnomalyInternal test seam --
// this backend has no pattern-detection engine, so anomalies are never
// generated from real log content) with every types.Anomaly member
// populated, and asserts each round-trips exactly. Field-diffed against
// aws-sdk-go-v2@v1.81.1 types.Anomaly: a previous revision had no Go struct
// field at all for Histogram/LogSamples/PatternId/PatternString/
// PatternTokens/Priority/PatternRegex/IsPatternLevelSuppression/Suppressed/
// SuppressedUntil (all real wire members), and used a made-up
// "suppressedState" wire key holding the raw suppressionType instead of the
// real "state" member.
func TestListAnomalies_WireShape(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))

	groupOut, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{
		LogGroupName: aws.String("anomaly-source-group"),
	})
	require.NoError(t, err)
	_ = groupOut

	groupARN := "arn:aws:logs:us-east-1:000000000000:log-group:anomaly-source-group"

	detOut, err := client.CreateLogAnomalyDetector(t.Context(), &cwlsdk.CreateLogAnomalyDetectorInput{
		LogGroupArnList: []string{groupARN},
	})
	require.NoError(t, err)

	suppressed := true
	patternLevel := false
	dynamic := true

	backend.AddAnomalyInternal(cloudwatchlogs.Anomaly{
		AnomalyDetectorArn: *detOut.AnomalyDetectorArn,
		AnomalyID:          "anomaly-1",
		Description:        "unusual rate of ERROR log events",
		State:              cloudwatchlogs.AnomalyStateSuppressed,
		PatternID:          "pattern-1",
		PatternString:      "<*> ERROR <*>",
		PatternRegex:       "^.*ERROR.*$",
		Priority:           "HIGH",
		Histogram:          map[string]int64{"1700000000": 3},
		LogSamples: []cloudwatchlogs.AnomalyLogSample{
			{Message: "2026-01-01 ERROR disk full", Timestamp: 1700000000000},
		},
		PatternTokens: []cloudwatchlogs.PatternToken{
			{
				TokenString:          "<*>",
				IsDynamic:            &dynamic,
				DynamicTokenPosition: 1,
				InferredTokenName:    "IPAddress-1",
				Enumerations:         map[string]int64{"10.0.0.1": 2},
			},
		},
		Suppressed:                &suppressed,
		IsPatternLevelSuppression: &patternLevel,
		FirstSeen:                 1700000000,
		LastSeen:                  1700000100,
		SuppressedDate:            1700000200,
		SuppressedUntil:           1700003800,
		Active:                    true,
	})

	out, err := client.ListAnomalies(t.Context(), &cwlsdk.ListAnomaliesInput{
		AnomalyDetectorArn: detOut.AnomalyDetectorArn,
	})
	require.NoError(t, err)
	require.Len(t, out.Anomalies, 1)

	got := out.Anomalies[0]
	assert.Equal(t, "anomaly-1", aws.ToString(got.AnomalyId))
	assert.Equal(t, "unusual rate of ERROR log events", aws.ToString(got.Description))
	assert.Equal(t, cwltypes.StateSuppressed, got.State)
	assert.Equal(t, "pattern-1", aws.ToString(got.PatternId))
	assert.Equal(t, "<*> ERROR <*>", aws.ToString(got.PatternString))
	assert.Equal(t, "^.*ERROR.*$", aws.ToString(got.PatternRegex))
	assert.Equal(t, "HIGH", aws.ToString(got.Priority))
	assert.Equal(t, map[string]int64{"1700000000": 3}, got.Histogram)
	require.Len(t, got.LogSamples, 1)
	assert.Equal(t, "2026-01-01 ERROR disk full", aws.ToString(got.LogSamples[0].Message))
	assert.Equal(t, int64(1700000000000), aws.ToInt64(got.LogSamples[0].Timestamp))
	require.Len(t, got.PatternTokens, 1)
	assert.Equal(t, "<*>", aws.ToString(got.PatternTokens[0].TokenString))
	assert.True(t, aws.ToBool(got.PatternTokens[0].IsDynamic))
	assert.Equal(t, int32(1), got.PatternTokens[0].DynamicTokenPosition)
	assert.Equal(t, "IPAddress-1", aws.ToString(got.PatternTokens[0].InferredTokenName))
	require.True(t, aws.ToBool(got.Suppressed))
	require.False(t, aws.ToBool(got.IsPatternLevelSuppression))
	assert.Equal(t, int64(1700000000), got.FirstSeen)
	assert.Equal(t, int64(1700000100), got.LastSeen)
	assert.Equal(t, int64(1700000200), got.SuppressedDate)
	assert.Equal(t, int64(1700003800), got.SuppressedUntil)
	assert.True(t, aws.ToBool(got.Active))
}

// TestUpdateAnomaly_SuppressionSemantics drives UpdateAnomaly through the
// real SDK client and locks the real-AWS suppression contract
// (api_op_UpdateAnomaly.go's own doc comment): a non-empty SuppressionType
// suppresses the anomaly, and calling the operation again while omitting
// SuppressionType ends the suppression. A previous revision inverted this --
// it treated the omitted (empty-string) case as "still suppressed, refresh
// SuppressedDate" -- so a real client ending a suppression was incorrectly
// left suppressed with a newly bumped SuppressedDate instead.
func TestUpdateAnomaly_SuppressionSemantics(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))

	_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{
		LogGroupName: aws.String("anomaly-source-group-2"),
	})
	require.NoError(t, err)

	groupARN := "arn:aws:logs:us-east-1:000000000000:log-group:anomaly-source-group-2"

	detOut, err := client.CreateLogAnomalyDetector(t.Context(), &cwlsdk.CreateLogAnomalyDetectorInput{
		LogGroupArnList: []string{groupARN},
	})
	require.NoError(t, err)

	backend.AddAnomalyInternal(cloudwatchlogs.Anomaly{
		AnomalyDetectorArn: *detOut.AnomalyDetectorArn,
		AnomalyID:          "anomaly-1",
		Active:             true,
	})

	getState := func(t *testing.T) (cwltypes.State, bool, int64) {
		t.Helper()

		out, listErr := client.ListAnomalies(t.Context(), &cwlsdk.ListAnomaliesInput{
			AnomalyDetectorArn: detOut.AnomalyDetectorArn,
		})
		require.NoError(t, listErr)
		require.Len(t, out.Anomalies, 1)

		return out.Anomalies[0].State, aws.ToBool(out.Anomalies[0].Suppressed), out.Anomalies[0].SuppressedDate
	}

	_, err = client.UpdateAnomaly(t.Context(), &cwlsdk.UpdateAnomalyInput{
		AnomalyDetectorArn: detOut.AnomalyDetectorArn,
		AnomalyId:          aws.String("anomaly-1"),
		SuppressionType:    cwltypes.SuppressionTypeLimited,
	})
	require.NoError(t, err)

	state, suppressed, suppressedDate := getState(t)
	assert.Equal(t, cwltypes.StateSuppressed, state)
	assert.True(t, suppressed)
	assert.NotZero(t, suppressedDate)

	_, err = client.UpdateAnomaly(t.Context(), &cwlsdk.UpdateAnomalyInput{
		AnomalyDetectorArn: detOut.AnomalyDetectorArn,
		AnomalyId:          aws.String("anomaly-1"),
	})
	require.NoError(t, err)

	state, suppressed, suppressedDate = getState(t)
	assert.Equal(t, cwltypes.StateActive, state)
	assert.False(t, suppressed)
	assert.Zero(t, suppressedDate)
}

// TestUpdateAnomaly_PatternID drives UpdateAnomaly with PatternId (instead of
// AnomalyId) through the real SDK client. UpdateAnomalyInput's own doc
// comment (api_op_UpdateAnomaly.go:12-19) says "Use this operation to
// suppress anomaly detection for a specified anomaly or pattern... If you
// suppress a pattern, CloudWatch Logs won't report any anomalies related to
// that pattern" -- gopherstack previously had no PatternId field at all, so
// a real caller following that pattern-suppression path always fell through
// to "anomalyId is required", even though AnomalyId is optional whenever
// PatternId is supplied instead. Suppressing a pattern must suppress every
// stored anomaly sharing that pattern, not just one.
func TestUpdateAnomaly_PatternID(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))

	_, err := client.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{
		LogGroupName: aws.String("pattern-source-group"),
	})
	require.NoError(t, err)

	groupARN := "arn:aws:logs:us-east-1:000000000000:log-group:pattern-source-group"

	detOut, err := client.CreateLogAnomalyDetector(t.Context(), &cwlsdk.CreateLogAnomalyDetectorInput{
		LogGroupArnList: []string{groupARN},
	})
	require.NoError(t, err)

	backend.AddAnomalyInternal(cloudwatchlogs.Anomaly{
		AnomalyDetectorArn: *detOut.AnomalyDetectorArn,
		AnomalyID:          "anomaly-a",
		PatternID:          "pattern-1",
		State:              cloudwatchlogs.AnomalyStateActive,
		Active:             true,
	})
	backend.AddAnomalyInternal(cloudwatchlogs.Anomaly{
		AnomalyDetectorArn: *detOut.AnomalyDetectorArn,
		AnomalyID:          "anomaly-b",
		PatternID:          "pattern-1",
		State:              cloudwatchlogs.AnomalyStateActive,
		Active:             true,
	})
	backend.AddAnomalyInternal(cloudwatchlogs.Anomaly{
		AnomalyDetectorArn: *detOut.AnomalyDetectorArn,
		AnomalyID:          "anomaly-other",
		PatternID:          "pattern-2",
		State:              cloudwatchlogs.AnomalyStateActive,
		Active:             true,
	})

	_, err = client.UpdateAnomaly(t.Context(), &cwlsdk.UpdateAnomalyInput{
		AnomalyDetectorArn: detOut.AnomalyDetectorArn,
		PatternId:          aws.String("pattern-1"),
		SuppressionType:    cwltypes.SuppressionTypeInfinite,
	})
	require.NoError(t, err)

	out, err := client.ListAnomalies(t.Context(), &cwlsdk.ListAnomaliesInput{
		AnomalyDetectorArn: detOut.AnomalyDetectorArn,
	})
	require.NoError(t, err)
	require.Len(t, out.Anomalies, 3)

	byID := make(map[string]cwltypes.Anomaly, len(out.Anomalies))
	for _, a := range out.Anomalies {
		byID[aws.ToString(a.AnomalyId)] = a
	}

	assert.Equal(t, cwltypes.StateSuppressed, byID["anomaly-a"].State)
	assert.Equal(t, cwltypes.StateSuppressed, byID["anomaly-b"].State)
	assert.Equal(t, cwltypes.StateActive, byID["anomaly-other"].State)
}

// TestUpdateAnomaly_AnomalyIDAndPatternIDMutualExclusion asserts
// UpdateAnomalyInput's "You must specify either anomalyId or patternId, but
// you can't specify both parameters in the same operation" --
// validateOpUpdateAnomalyInput only requires AnomalyDetectorArn, so both
// branches reach the wire unmodified via the real client.
func TestUpdateAnomaly_AnomalyIDAndPatternIDMutualExclusion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		anomalyID *string
		patternID *string
		name      string
	}{
		{name: "both set", anomalyID: aws.String("anomaly-1"), patternID: aws.String("pattern-1")},
		{name: "neither set"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := cloudwatchlogs.NewInMemoryBackend()
			client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))

			detOut, err := client.CreateLogAnomalyDetector(t.Context(), &cwlsdk.CreateLogAnomalyDetectorInput{
				LogGroupArnList: []string{
					"arn:aws:logs:us-east-1:000000000000:log-group:mutual-exclusion-group",
				},
			})
			require.NoError(t, err)

			_, err = client.UpdateAnomaly(t.Context(), &cwlsdk.UpdateAnomalyInput{
				AnomalyDetectorArn: detOut.AnomalyDetectorArn,
				AnomalyId:          tt.anomalyID,
				PatternId:          tt.patternID,
				SuppressionType:    cwltypes.SuppressionTypeInfinite,
			})
			require.Error(t, err)

			var ipe *cwltypes.InvalidParameterException
			require.ErrorAs(t, err, &ipe, "expected InvalidParameterException, got %v", err)
		})
	}
}
