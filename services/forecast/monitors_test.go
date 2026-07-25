package forecast_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMonitors_EvaluationsAndErrors verifies ListMonitorEvaluations returns the
// synthetic evaluation created alongside the monitor, and that DescribeForecast
// on a missing ARN still returns ResourceNotFoundException.
func TestMonitors_EvaluationsAndErrors(t *testing.T) {
	t.Parallel()

	h := newHandler()
	predictorARN := createPredictor(t, h)
	_, created := request(t, h, "CreateMonitor", map[string]any{
		"MonitorName": "monitor-evaluations", "ResourceArn": predictorARN,
	})
	code, response := request(t, h, "ListMonitorEvaluations", map[string]any{"MonitorArn": created["MonitorArn"]})
	require.Equal(t, http.StatusOK, code)
	evaluations, ok := response["PredictorMonitorEvaluations"].([]any)
	require.True(t, ok)
	require.Len(t, evaluations, 1)
	assert.Equal(t, "SUCCESS", evaluations[0].(map[string]any)["EvaluationState"])

	code, response = request(t, h, "DescribeForecast", map[string]any{"ForecastArn": "missing"})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "ResourceNotFoundException", response["__type"])
}

// TestMonitors_Evaluations verifies ListMonitorEvaluations field shapes and
// the ResourceNotFoundException path for an unknown monitor ARN.
func TestMonitors_Evaluations(t *testing.T) {
	t.Parallel()

	h := newHandler()
	predictorARN := createPredictor(t, h)

	code, created := request(t, h, "CreateMonitor", map[string]any{
		"MonitorName": "eval-monitor",
		"ResourceArn": predictorARN,
	})
	require.Equal(t, http.StatusOK, code)
	monitorARN := created["MonitorArn"].(string)

	code, m := request(t, h, "ListMonitorEvaluations", map[string]any{"MonitorArn": monitorARN})
	require.Equal(t, http.StatusOK, code)
	evaluations, ok := m["PredictorMonitorEvaluations"].([]any)
	require.True(t, ok)
	require.Len(t, evaluations, 1)

	eval := evaluations[0].(map[string]any)
	assert.Equal(t, "SUCCESS", eval["EvaluationState"])
	assert.NotEmpty(t, eval["MonitorArn"])
	assert.NotEmpty(t, eval["MonitorName"])
	assert.NotNil(t, eval["MetricResults"])

	// Not found
	code, _ = request(t, h, "ListMonitorEvaluations", map[string]any{"MonitorArn": "missing"})
	assert.Equal(t, http.StatusBadRequest, code)
}
