package forecast_test

// Tests that GetAccuracyMetrics returns populated, deterministic backtest
// metrics (previously it always returned an empty PredictorEvaluationResults).

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/forecast"
)

func getMetrics(t *testing.T, h *forecast.Handler, predictorArn string) map[string]any {
	t.Helper()

	rec := a1ForecastDo(t, h, "GetAccuracyMetrics", map[string]any{"PredictorArn": predictorArn})
	require.Equal(t, http.StatusOK, rec.Code)

	return a1ForecastUnmarshal(t, rec)
}

func TestGetAccuracyMetrics_Populated(t *testing.T) {
	t.Parallel()

	h := a1ForecastHandler(t)

	rec := a1ForecastDo(t, h, "CreatePredictor", map[string]any{
		"PredictorName":   "acc-pred",
		"ForecastHorizon": 7,
		"ForecastTypes":   []any{"0.1", "0.5", "0.9"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	predictorArn, ok := a1ForecastUnmarshal(t, rec)["PredictorArn"].(string)
	require.True(t, ok)

	m := getMetrics(t, h, predictorArn)

	results, ok := m["PredictorEvaluationResults"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, results)

	first, ok := results[0].(map[string]any)
	require.True(t, ok)
	windows, ok := first["TestWindows"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, windows)

	win0, ok := windows[0].(map[string]any)
	require.True(t, ok)
	metrics, ok := win0["Metrics"].(map[string]any)
	require.True(t, ok)

	rmse, ok := metrics["RMSE"].(float64)
	require.True(t, ok)
	assert.Positive(t, rmse)

	losses, ok := metrics["WeightedQuantileLosses"].([]any)
	require.True(t, ok)
	assert.Len(t, losses, 3, "one loss entry per configured quantile")

	errMetrics, ok := metrics["ErrorMetrics"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, errMetrics)
	em0, ok := errMetrics[0].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, em0, "WAPE")
	assert.Contains(t, em0, "MAPE")
	assert.Contains(t, em0, "MASE")
}

func TestGetAccuracyMetrics_Deterministic(t *testing.T) {
	t.Parallel()

	h := a1ForecastHandler(t)

	rec := a1ForecastDo(t, h, "CreatePredictor", map[string]any{
		"PredictorName": "det-pred", "ForecastHorizon": 7,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	predictorArn, ok := a1ForecastUnmarshal(t, rec)["PredictorArn"].(string)
	require.True(t, ok)

	first := getMetrics(t, h, predictorArn)
	second := getMetrics(t, h, predictorArn)

	assert.Equal(t, first, second, "GetAccuracyMetrics must be deterministic for a given predictor")
}
