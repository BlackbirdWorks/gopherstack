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

// TestHandler_GetPredictiveScalingForecast_HonestlyEmpty verifies that
// CapacityForecast/LoadForecast never contain fabricated data points.
// gopherstack has no real metric history to build an ML forecast from
// (unlike real AWS's predictive scaling forecaster), so the forecast is
// always returned honestly empty regardless of the requested window -- this
// also matches real AWS's own behavior for a predictive scaling policy that
// has not yet accumulated enough history. See PARITY.md gaps.
func TestHandler_GetPredictiveScalingForecast_HonestlyEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		startTime int64
		endTime   int64
	}{
		{
			name:      "hour_aligned_window",
			startTime: 1704067200, // 2024-01-01T00:00:00Z
			endTime:   1704074400, // 2024-01-01T02:00:00Z
		},
		{
			name:      "non_hour_boundary_start",
			startTime: 1704069000, // 2024-01-01T00:30:00Z
			endTime:   1704078000, // 2024-01-01T03:00:00Z
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

			rec := doRequest(t, h, "GetPredictiveScalingForecast", map[string]any{
				"ServiceNamespace":  "ecs",
				"ResourceId":        "service/default/my-svc",
				"ScalableDimension": "ecs:service:DesiredCount",
				"PolicyName":        "predictive-policy",
				"StartTime":         tt.startTime,
				"EndTime":           tt.endTime,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			cf, ok := resp["CapacityForecast"].(map[string]any)
			require.True(t, ok, "expected CapacityForecast in response")
			assert.Empty(t, cf["Timestamps"], "CapacityForecast.Timestamps must not be fabricated")
			assert.Empty(t, cf["Values"], "CapacityForecast.Values must not be fabricated")

			lf, ok := resp["LoadForecast"].([]any)
			require.True(t, ok, "expected LoadForecast in response")
			require.Len(t, lf, 1, "one LoadForecastData entry per policy, with empty data points")

			lfEntry, ok := lf[0].(map[string]any)
			require.True(t, ok)
			assert.Empty(t, lfEntry["Timestamps"], "LoadForecast.Timestamps must not be fabricated")
			assert.Empty(t, lfEntry["Values"], "LoadForecast.Values must not be fabricated")
			assert.Equal(t, "ecs/service/default/my-svc/ecs:service:DesiredCount", lfEntry["MetricSpecification"])

			// UpdateTime is an AWS JSON-protocol epoch-seconds timestamp (a JSON number).
			updateTime, ok := resp["UpdateTime"].(float64)
			require.True(t, ok, "expected UpdateTime as epoch-seconds number in response")
			assert.NotZero(t, updateTime)
		})
	}
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
