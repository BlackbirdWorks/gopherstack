package personalize_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersonalize_MetricAttribution_CRUD(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	rec := personalizeDo(t, h, "CreateMetricAttribution", map[string]any{
		"name":            "my-ma",
		"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1",
		"metrics": []map[string]any{
			{"eventType": "click", "expression": "SUM(Items.PRICE)", "metricName": "click-sum"},
		},
		"metricsOutputConfig": map[string]any{
			"s3DataDestination": map[string]any{"path": "s3://my-bucket/metrics"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	maArn := personalizeUnmarshal(t, rec)["metricAttributionArn"].(string)
	assert.NotEmpty(t, maArn)

	rec = personalizeDo(t, h, "DescribeMetricAttribution", map[string]any{"metricAttributionArn": maArn})
	ma := personalizeUnmarshal(t, rec)["metricAttribution"].(map[string]any)
	assert.Equal(t, "my-ma", ma["name"])
	assert.Equal(t, "ACTIVE", ma["status"])

	// ListMetricAttributionMetrics returns the metrics configured at creation,
	// not a fabricated static list.
	rec = personalizeDo(t, h, "ListMetricAttributionMetrics", map[string]any{"metricAttributionArn": maArn})
	require.Equal(t, http.StatusOK, rec.Code)
	listed := personalizeUnmarshal(t, rec)
	metrics := listed["metrics"].([]any)
	require.Len(t, metrics, 1)
	first := metrics[0].(map[string]any)
	assert.Equal(t, "click-sum", first["metricName"])
	assert.Equal(t, "click", first["eventType"])
	assert.Equal(t, "SUM(Items.PRICE)", first["expression"])

	// UpdateMetricAttribution: add a metric and remove the original one.
	rec = personalizeDo(t, h, "UpdateMetricAttribution", map[string]any{
		"metricAttributionArn": maArn,
		"addMetrics": []map[string]any{
			{"eventType": "purchase", "expression": "SUM(Items.PRICE)", "metricName": "purchase-sum"},
		},
		"removeMetrics": []string{"click-sum"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = personalizeDo(t, h, "ListMetricAttributionMetrics", map[string]any{"metricAttributionArn": maArn})
	require.Equal(t, http.StatusOK, rec.Code)
	metrics = personalizeUnmarshal(t, rec)["metrics"].([]any)
	require.Len(t, metrics, 1)
	assert.Equal(t, "purchase-sum", metrics[0].(map[string]any)["metricName"])

	// Delete
	rec = personalizeDo(t, h, "DeleteMetricAttribution", map[string]any{"metricAttributionArn": maArn})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestPersonalize_MetricAttribution_Create_RequiresMetrics(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	rec := personalizeDo(t, h, "CreateMetricAttribution", map[string]any{
		"name":            "no-metrics-ma",
		"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1",
		"metricsOutputConfig": map[string]any{
			"s3DataDestination": map[string]any{"path": "s3://my-bucket/metrics"},
		},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	body := personalizeUnmarshal(t, rec)
	assert.Equal(t, "InvalidInputException", body["__type"])
}

// --- Async jobs ---
