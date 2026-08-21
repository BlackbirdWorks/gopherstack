package forecast_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	forecastsdk "github.com/aws/aws-sdk-go-v2/service/forecast"
	"github.com/aws/aws-sdk-go-v2/service/forecast/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetAccuracyMetrics_QuantileAndWindowsDecode drives GetAccuracyMetrics
// through the real Forecast client.
//
// WeightedQuantileLoss.Quantile deserializes from a json.Number, or one of
// the Smithy-special "NaN"/"Infinity"/"-Infinity" strings -- any other
// string fails with "unknown JSON number value" (deserializers.go,
// awsAwsjson11_deserializeDocumentWeightedQuantileLoss). gopherstack
// previously emitted the raw ForecastType label ("0.1") as that string,
// which is neither a number nor one of those three, so the real client
// could never decode this response at all.
//
// TestWindowSummary.TestWindowStart/TestWindowEnd deserialize the same way
// (case "TestWindowStart"/"TestWindowEnd") -- gopherstack previously emitted
// RFC3339 strings there.
func TestGetAccuracyMetrics_QuantileAndWindowsDecode(t *testing.T) {
	t.Parallel()

	h := newHandler()
	client := newTestForecastClient(t, h)

	dsGroupOut, err := client.CreateDatasetGroup(t.Context(), &forecastsdk.CreateDatasetGroupInput{
		DatasetGroupName: aws.String("acc-metrics-dsg"),
		Domain:           types.DomainCustom,
	})
	require.NoError(t, err)

	predOut, err := client.CreatePredictor(t.Context(), &forecastsdk.CreatePredictorInput{
		PredictorName:   aws.String("acc-metrics-pred"),
		ForecastHorizon: aws.Int32(7),
		ForecastTypes:   []string{"0.1", "0.5", "0.9", "mean"},
		InputDataConfig: &types.InputDataConfig{
			DatasetGroupArn: dsGroupOut.DatasetGroupArn,
		},
		FeaturizationConfig: &types.FeaturizationConfig{
			ForecastFrequency: aws.String("D"),
		},
	})
	require.NoError(t, err)

	out, err := client.GetAccuracyMetrics(t.Context(), &forecastsdk.GetAccuracyMetricsInput{
		PredictorArn: predOut.PredictorArn,
	})
	require.NoError(t, err, "real SDK client must decode GetAccuracyMetrics without error")
	require.NotEmpty(t, out.PredictorEvaluationResults)

	results := out.PredictorEvaluationResults[0]
	require.NotEmpty(t, results.TestWindows)
	win := results.TestWindows[0]
	assert.NotNil(t, win.TestWindowStart)
	assert.NotNil(t, win.TestWindowEnd)
	require.NotNil(t, win.Metrics)
	// "mean" is not a quantile: exactly the 3 numeric quantiles get a
	// WeightedQuantileLosses entry.
	assert.Len(t, win.Metrics.WeightedQuantileLosses, 3)

	for _, l := range win.Metrics.WeightedQuantileLosses {
		require.NotNil(t, l.Quantile)
	}
}
