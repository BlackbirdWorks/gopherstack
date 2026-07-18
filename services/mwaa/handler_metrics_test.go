package mwaa_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_PublishMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		envName    string
		seed       bool
		wantStatus int
	}{
		{
			name:    "publish_metrics",
			envName: "publish-metrics-env",
			seed:    true,
			body: map[string]any{
				"MetricData": []map[string]any{
					{"MetricName": "TaskInstance", "Value": 1.0, "Unit": "Count"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:    "publish_empty_metrics",
			envName: "publish-empty-metrics-env",
			seed:    true,
			body: map[string]any{
				"MetricData": []map[string]any{},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:    "env_not_found",
			envName: "nonexistent-metrics-env",
			seed:    false,
			body: map[string]any{
				"MetricData": []map[string]any{},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid_json",
			envName:    "metrics-invalid-json-env",
			seed:       true,
			body:       nil,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			if tt.seed {
				seedRec := doMWAARequest(t, h, http.MethodPut, "/environments/"+tt.envName, map[string]any{
					"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
				})
				require.Equal(t, http.StatusOK, seedRec.Code)
			}

			if tt.name == "invalid_json" {
				e := echo.New()
				req := httptest.NewRequest(
					http.MethodPost,
					"/metrics/environments/"+tt.envName,
					strings.NewReader("{invalid"),
				)
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)
				require.NoError(t, h.Handler()(c))
				assert.Equal(t, tt.wantStatus, rec.Code)

				return
			}

			rec := doMWAARequest(t, h, http.MethodPost, "/metrics/environments/"+tt.envName, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetMetrics(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	// Create environment.
	seedRec := doMWAARequest(t, h, http.MethodPut, "/environments/metrics-env", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})
	require.Equal(t, http.StatusOK, seedRec.Code)

	// Publish some metrics.
	pubRec := doMWAARequest(t, h, http.MethodPost, "/metrics/environments/metrics-env", map[string]any{
		"MetricData": []map[string]any{
			{"MetricName": "TaskInstance", "Value": 5.0, "Unit": "Count"},
		},
	})
	require.Equal(t, http.StatusOK, pubRec.Code)

	// Get metrics.
	getRec := doMWAARequest(t, h, http.MethodGet, "/metrics/environments/metrics-env", nil)
	assert.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	data, ok := resp["MetricData"].([]any)
	assert.True(t, ok)
	assert.Len(t, data, 1)

	// Not found.
	notFoundRec := doMWAARequest(t, h, http.MethodGet, "/metrics/environments/missing-env", nil)
	assert.Equal(t, http.StatusNotFound, notFoundRec.Code)
}

func TestGetMetrics_HTTP_ResponseShape(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodPut, "/environments/metrics-shape-env", map[string]any{
		"DagS3Path":        "dags/",
		"ExecutionRoleArn": "arn:aws:iam::123456789012:role/r",
		"SourceBucketArn":  "arn:aws:s3:::b",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	pubRec := doMWAARequest(
		t, h, http.MethodPost, "/metrics/environments/metrics-shape-env",
		map[string]any{
			"MetricData": []any{
				map[string]any{"MetricName": "TestMetric"},
			},
		},
	)
	require.Equal(t, http.StatusOK, pubRec.Code)

	getRec := doMWAARequest(t, h, http.MethodGet, "/metrics/environments/metrics-shape-env", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))

	_, ok := resp["MetricData"]
	assert.True(t, ok, "response must have MetricData key")

	metricData, ok := resp["MetricData"].([]any)
	require.True(t, ok)
	assert.Len(t, metricData, 1)
}

func TestGetMetrics_HTTP_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodGet, "/metrics/environments/does-not-exist", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ─────────────────────────────────────────────────────────────
// 8. ListTagsForResource HTTP scenarios
// ─────────────────────────────────────────────────────────────

func TestHTTP_MetricsPublishAndRetrieve(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	doMWAARequest(t, h, http.MethodPut, "/environments/http-metrics-full", map[string]any{
		"DagS3Path": "dags/", "ExecutionRoleArn": "arn:r", "SourceBucketArn": "arn:b",
	})

	pubRec := doMWAARequest(t, h, http.MethodPost, "/metrics/environments/http-metrics-full", map[string]any{
		"MetricData": []map[string]any{
			{"MetricName": "DagRunDuration", "Value": 123.4, "Unit": "Seconds"},
			{"MetricName": "ActiveDAGs", "Value": 5.0, "Unit": "Count"},
		},
	})
	require.Equal(t, http.StatusOK, pubRec.Code)

	getRec := doMWAARequest(t, h, http.MethodGet, "/metrics/environments/http-metrics-full", nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp struct {
		MetricData []map[string]any `json:"MetricData"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	assert.Len(t, resp.MetricData, 2)
}
