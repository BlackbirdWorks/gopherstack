package forecast_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	forecastsdk "github.com/aws/aws-sdk-go-v2/service/forecast"
	"github.com/aws/aws-sdk-go-v2/service/forecast/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPredictorAndForecastLineage(t *testing.T) {
	t.Parallel()

	h := newHandler()
	client := newTestForecastClient(t, h)
	ctx := t.Context()

	dgA := sdkDatasetGroup(t, client, "lineage-dg-a")
	dgB := sdkDatasetGroup(t, client, "lineage-dg-b")
	classic := sdkPredictor(t, client, "lineage-classic", dgA)

	auto, err := client.CreateAutoPredictor(ctx, &forecastsdk.CreateAutoPredictorInput{
		PredictorName:         aws.String("lineage-auto"),
		DataConfig:            &types.DataConfig{DatasetGroupArn: aws.String(dgB)},
		ReferencePredictorArn: aws.String(classic),
	})
	require.NoError(t, err)

	fcClassic, err := client.CreateForecast(ctx, &forecastsdk.CreateForecastInput{
		ForecastName: aws.String("lineage-fc-classic"), PredictorArn: aws.String(classic),
	})
	require.NoError(t, err)

	fcAuto, err := client.CreateForecast(ctx, &forecastsdk.CreateForecastInput{
		ForecastName: aws.String("lineage-fc-auto"), PredictorArn: auto.PredictorArn,
	})
	require.NoError(t, err)

	preds, err := client.ListPredictors(ctx, &forecastsdk.ListPredictorsInput{})
	require.NoError(t, err)

	byName := map[string]types.PredictorSummary{}
	for _, p := range preds.Predictors {
		byName[aws.ToString(p.PredictorName)] = p
	}

	assert.Equal(t, dgA, aws.ToString(byName["lineage-classic"].DatasetGroupArn))
	assert.False(t, aws.ToBool(byName["lineage-classic"].IsAutoPredictor))
	assert.Nil(t, byName["lineage-classic"].ReferencePredictorSummary)
	assert.Equal(t, dgB, aws.ToString(byName["lineage-auto"].DatasetGroupArn))
	assert.True(t, aws.ToBool(byName["lineage-auto"].IsAutoPredictor))
	require.NotNil(t, byName["lineage-auto"].ReferencePredictorSummary)
	assert.Equal(t, classic, aws.ToString(byName["lineage-auto"].ReferencePredictorSummary.Arn))
	assert.Equal(t, types.StateActive, byName["lineage-auto"].ReferencePredictorSummary.State)

	filtered, err := client.ListPredictors(ctx, &forecastsdk.ListPredictorsInput{
		Filters: []types.Filter{
			{Condition: types.FilterConditionStringIs, Key: aws.String("DatasetGroupArn"), Value: aws.String(dgB)},
		},
	})
	require.NoError(t, err)
	require.Len(t, filtered.Predictors, 1)
	assert.Equal(t, "lineage-auto", aws.ToString(filtered.Predictors[0].PredictorName))

	fcs, err := client.ListForecasts(ctx, &forecastsdk.ListForecastsInput{
		Filters: []types.Filter{
			{Condition: types.FilterConditionStringIs, Key: aws.String("DatasetGroupArn"), Value: aws.String(dgA)},
		},
	})
	require.NoError(t, err)
	require.Len(t, fcs.Forecasts, 1)
	assert.Equal(t, aws.ToString(fcClassic.ForecastArn), aws.ToString(fcs.Forecasts[0].ForecastArn))
	assert.False(t, aws.ToBool(fcs.Forecasts[0].CreatedUsingAutoPredictor))

	allFcs, err := client.ListForecasts(ctx, &forecastsdk.ListForecastsInput{})
	require.NoError(t, err)

	for _, f := range allFcs.Forecasts {
		if aws.ToString(f.ForecastArn) == aws.ToString(fcAuto.ForecastArn) {
			assert.True(t, aws.ToBool(f.CreatedUsingAutoPredictor))
			assert.Equal(t, dgB, aws.ToString(f.DatasetGroupArn))
		}
	}

	descAuto, err := client.DescribeAutoPredictor(
		ctx,
		&forecastsdk.DescribeAutoPredictorInput{PredictorArn: auto.PredictorArn},
	)
	require.NoError(t, err)
	require.NotNil(t, descAuto.ReferencePredictorSummary)
	assert.Equal(t, types.StateActive, descAuto.ReferencePredictorSummary.State)

	_, err = client.DescribePredictor(ctx, &forecastsdk.DescribePredictorInput{PredictorArn: aws.String(classic)})
	require.NoError(t, err)

	_, err = client.DeletePredictor(ctx, &forecastsdk.DeletePredictorInput{PredictorArn: aws.String(classic)})
	require.NoError(t, err)

	descAuto, err = client.DescribeAutoPredictor(
		ctx,
		&forecastsdk.DescribeAutoPredictorInput{PredictorArn: auto.PredictorArn},
	)
	require.NoError(t, err)
	assert.Equal(t, types.StateDeleted, descAuto.ReferencePredictorSummary.State)
}
