package applicationautoscaling_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_GetPredictiveScalingForecast(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      map[string]any
		name      string
		wantCode  int
		preCreate bool
	}{
		{
			name:      "success",
			preCreate: true,
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "predictive-policy",
				"StartTime":         1704067200,
				"EndTime":           1704078000,
			},
			wantCode: http.StatusOK,
		},
		{
			name:      "policy_not_found",
			preCreate: false,
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "nonexistent-policy",
				"StartTime":         1704067200,
				"EndTime":           1704078000,
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:      "invalid_start_time",
			preCreate: false,
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "predictive-policy",
				"StartTime":         "not-a-time",
				"EndTime":           "2024-01-01T03:00:00Z",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:      "invalid_end_time",
			preCreate: false,
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "predictive-policy",
				"StartTime":         "2024-01-01T00:00:00Z",
				"EndTime":           "not-a-time",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.preCreate {
				seedTarget(t, h, "service/default/my-svc", 1, 10)
				doRequest(t, h, "PutScalingPolicy", map[string]any{
					"ServiceNamespace":  "ecs",
					"ResourceId":        "service/default/my-svc",
					"ScalableDimension": "ecs:service:DesiredCount",
					"PolicyName":        "predictive-policy",
					"PolicyType":        "PredictiveScaling",
				})
			}

			rec := doRequest(t, h, "GetPredictiveScalingForecast", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_GetPredictiveScalingForecast_DataPoints(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedTarget(t, h, "service/default/my-svc", 1, 10)
	doRequest(t, h, "PutScalingPolicy", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "predictive-policy",
		"PolicyType":        "PredictiveScaling",
	})

	rec := doRequest(t, h, "GetPredictiveScalingForecast", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "predictive-policy",
		"StartTime":         1704067200,
		"EndTime":           1704074400,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	// CapacityForecast should be present with timestamps and values
	cf, ok := resp["CapacityForecast"].(map[string]any)
	require.True(t, ok, "expected CapacityForecast in response")

	timestamps, ok := cf["Timestamps"].([]any)
	require.True(t, ok, "expected Timestamps in CapacityForecast")
	// 00:00→02:00 = exactly 2 hourly timestamps (00:00 and 01:00); EndTime is excluded.
	assert.Len(t, timestamps, 2, "expected exactly 2 hourly timestamps for 00:00→02:00 window")
	assert.InDelta(t, 1704067200, timestamps[0], 0.001, "first timestamp should be start of window")
	assert.InDelta(t, 1704070800, timestamps[1], 0.001, "second timestamp should be start+1h")

	values, ok := cf["Values"].([]any)
	require.True(t, ok, "expected Values in CapacityForecast")
	assert.Len(t, values, 2, "values count must match timestamps count")

	// LoadForecast should be a non-empty array
	lf, ok := resp["LoadForecast"].([]any)
	require.True(t, ok, "expected LoadForecast in response")
	assert.NotEmpty(t, lf)

	// UpdateTime is an AWS JSON-protocol epoch-seconds timestamp (a JSON number).
	updateTime, ok := resp["UpdateTime"].(float64)
	require.True(t, ok, "expected UpdateTime as epoch-seconds number in response")
	assert.NotZero(t, updateTime)
}

func TestHandler_GetPredictiveScalingForecast_TimeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "end_before_start",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "predictive-policy",
				"StartTime":         1704153600,
				"EndTime":           1704067200,
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "equal_start_end",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "predictive-policy",
				"StartTime":         1704067200,
				"EndTime":           1704067200,
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "window_exceeds_14_days",
			body: map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "predictive-policy",
				"StartTime":         1704067200,
				"EndTime":           1705363200,
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seedTarget(t, h, "service/default/my-svc", 1, 10)
			doRequest(t, h, "PutScalingPolicy", map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "predictive-policy",
				"PolicyType":        "PredictiveScaling",
			})

			rec := doRequest(t, h, "GetPredictiveScalingForecast", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_GetPredictiveScalingForecast_NonHourBoundaryStart(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedTarget(t, h, "service/default/my-svc", 1, 10)
	doRequest(t, h, "PutScalingPolicy", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "predictive-policy",
		"PolicyType":        "PredictiveScaling",
	})

	// StartTime is mid-hour (00:30). First complete hour boundary >= 00:30 is 01:00.
	// EndTime is 03:00. So expected timestamps: 01:00, 02:00 (2 points).
	rec := doRequest(t, h, "GetPredictiveScalingForecast", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "predictive-policy",
		"StartTime":         1704069000, // 2024-01-01T00:30:00Z
		"EndTime":           1704078000, // 2024-01-01T03:00:00Z
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	cf, ok := resp["CapacityForecast"].(map[string]any)
	require.True(t, ok)

	timestamps, ok := cf["Timestamps"].([]any)
	require.True(t, ok)

	// All timestamps must be >= StartTime (no timestamp before 00:30)
	for _, ts := range timestamps {
		tsFloat, isFloat := ts.(float64)
		require.True(t, isFloat)
		assert.GreaterOrEqual(t, tsFloat, float64(1704069000),
			"timestamp %v must not precede StartTime 00:30", tsFloat)
	}
	assert.Len(t, timestamps, 2, "expected 2 hourly points (01:00, 02:00) for 00:30→03:00 window")
	assert.InDelta(t, 1704070800, timestamps[0], 0.001) // 2024-01-01T01:00:00Z
	assert.InDelta(t, 1704074400, timestamps[1], 0.001) // 2024-01-01T02:00:00Z
}

func TestHandler_GetPredictiveScalingForecast_WrongPolicyType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a TargetTracking policy (not PredictiveScaling)
	seedTarget(t, h, "service/default/my-svc", 1, 10)
	doRequest(t, h, "PutScalingPolicy", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "tt-policy",
		"PolicyType":        "TargetTrackingScaling",
	})

	rec := doRequest(t, h, "GetPredictiveScalingForecast", map[string]any{
		"ServiceNamespace":  "ecs",
		"ResourceId":        "service/default/my-svc",
		"ScalableDimension": "ecs:service:DesiredCount",
		"PolicyName":        "tt-policy",
		"StartTime":         1704067200,
		"EndTime":           1704153600,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "expected 400 for non-PredictiveScaling policy")
}
