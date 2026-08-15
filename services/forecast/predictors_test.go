package forecast_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPredictors_FieldShapes verifies Create/Describe field shapes for
// Predictor, including AutoML/HPO flags.
func TestPredictors_FieldShapes(t *testing.T) {
	t.Parallel()

	h := newHandler()

	code, created := request(t, h, "CreatePredictor", map[string]any{
		"PredictorName":       "audit-predictor",
		"ForecastHorizon":     30,
		"PerformAutoML":       true,
		"PerformHPO":          true,
		"InputDataConfig":     map[string]any{"DatasetGroupArn": createDatasetGroup(t, h)},
		"FeaturizationConfig": map[string]any{},
	})
	require.Equal(t, http.StatusOK, code)
	arn := created["PredictorArn"].(string)
	assert.Contains(t, arn, "arn:aws:forecast:")

	// Advance to ACTIVE
	request(t, h, "DescribePredictor", map[string]any{"PredictorArn": arn})
	code, m := request(t, h, "DescribePredictor", map[string]any{"PredictorArn": arn})
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "audit-predictor", m["PredictorName"])
	assert.Equal(t, "ACTIVE", m["Status"])
	assert.Equal(t, true, m["PerformAutoML"])
}

// TestPredictors_AutoPredictor verifies CreateAutoPredictor/DescribeAutoPredictor,
// both of which alias the Predictor resource kind.
func TestPredictors_AutoPredictor(t *testing.T) {
	t.Parallel()

	h := newHandler()

	code, created := request(t, h, "CreateAutoPredictor", map[string]any{
		"PredictorName":   "auto-pred",
		"ForecastHorizon": 14,
	})
	require.Equal(t, http.StatusOK, code)
	arn, ok := created["PredictorArn"].(string)
	require.True(t, ok)
	assert.Contains(t, arn, "arn:aws:forecast:")

	code, m := request(t, h, "DescribeAutoPredictor", map[string]any{"PredictorArn": arn})
	require.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, m["PredictorArn"])
	assert.NotEmpty(t, m["Status"])
}

// TestPredictors_StopResume verifies that StopResource/ResumeResource
// transition a predictor between STOPPED and ACTIVE.
func TestPredictors_StopResume(t *testing.T) {
	t.Parallel()

	h := newHandler()

	code, created := request(t, h, "CreatePredictor", map[string]any{
		"PredictorName": "sr-predictor", "ForecastHorizon": 5,
		"InputDataConfig":     map[string]any{"DatasetGroupArn": createDatasetGroup(t, h)},
		"FeaturizationConfig": map[string]any{},
	})
	require.Equal(t, http.StatusOK, code)
	arn := created["PredictorArn"].(string)

	// Stop
	code, _ = request(t, h, "StopResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, code)

	// Advance and verify STOPPED
	request(t, h, "DescribePredictor", map[string]any{"PredictorArn": arn})
	_, m := request(t, h, "DescribePredictor", map[string]any{"PredictorArn": arn})
	assert.Equal(t, "STOPPED", m["Status"])

	// Resume
	code, _ = request(t, h, "ResumeResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, code)

	_, m = request(t, h, "DescribePredictor", map[string]any{"PredictorArn": arn})
	assert.Equal(t, "ACTIVE", m["Status"])
}
