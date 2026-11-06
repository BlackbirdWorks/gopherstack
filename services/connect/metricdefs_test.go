package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func calc(formula string, aliases ...string) *types.MetricCalculation {
	c := &types.MetricCalculation{Calculation: aws.String(formula)}
	for _, a := range aliases {
		c.CalculationComponents = append(c.CalculationComponents,
			types.CalculationComponent{Alias: aws.String(a), MetricName: aws.String("CONTACTS_HANDLED")})
	}

	return c
}

func TestCreateMetricValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mut      func(in *sdk.CreateMetricInput)
		name     string
		wantCode string
	}{
		{name: "ok", mut: func(*sdk.CreateMetricInput) {}},
		{
			name: "undeclared alias", wantCode: "InvalidParameterException",
			mut: func(in *sdk.CreateMetricInput) { in.MetricCalculation = calc("100 * SUM(M1) / SUM(M9)", "M1") },
		},
		{
			name: "unused alias", wantCode: "InvalidParameterException",
			mut: func(in *sdk.CreateMetricInput) { in.MetricCalculation = calc("SUM(M1)", "M1", "M2") },
		},
		{
			name: "unbalanced", wantCode: "InvalidParameterException",
			mut: func(in *sdk.CreateMetricInput) { in.MetricCalculation = calc("SUM(M1", "M1") },
		},
		{
			name: "duplicate alias", wantCode: "InvalidParameterException",
			mut: func(in *sdk.CreateMetricInput) { in.MetricCalculation = calc("SUM(M1)", "M1", "M1") },
		},
		{
			name: "too many components", wantCode: "InvalidParameterException",
			mut: func(in *sdk.CreateMetricInput) {
				in.MetricCalculation = calc("A+B+C+D+E+F", "A", "B", "C", "D", "E", "F")
			},
		},
		{
			name: "bad unit", wantCode: "InvalidParameterException",
			mut: func(in *sdk.CreateMetricInput) { in.Unit = "MINUTES" },
		},
		{
			name: "bad status", wantCode: "InvalidParameterException",
			mut: func(in *sdk.CreateMetricInput) { in.Status = "DRAFT" },
		},
		{
			name: "component needs one reference", wantCode: "InvalidParameterException",
			mut: func(in *sdk.CreateMetricInput) {
				in.MetricCalculation.CalculationComponents[0].MetricId = aws.String("x")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			inst := f.newInstance(t, "metric")
			in := &sdk.CreateMetricInput{
				InstanceId: aws.String(inst), Name: aws.String("handled"), Unit: types.MetricUnitPercent,
				MetricCalculation: calc("100 * SUM(M1) / SUM(M2)", "M1", "M2"),
			}
			tt.mut(in)

			_, err := f.client.CreateMetric(t.Context(), in)
			assert.Equal(t, tt.wantCode, apiCode(err))
		})
	}
}

func TestMetricDefinitionLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := f.newInstance(t, "metric")
	create := func(name string, token *string) (*sdk.CreateMetricOutput, error) {
		return f.client.CreateMetric(t.Context(), &sdk.CreateMetricInput{
			InstanceId: aws.String(inst), Name: aws.String(name), Unit: types.MetricUnitSeconds,
			MetricCalculation: calc("AVG(M1)", "M1"), Description: aws.String("d"), ClientToken: token,
			PositiveTrendIndicator: types.TrendIndicatorNegative, Tags: map[string]string{"team": "a"},
		})
	}

	first, err := create("Handle time", aws.String("tok"))
	require.NoError(t, err)

	replay, err := create("Handle time", aws.String("tok"))
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(first.MetricId), aws.ToString(replay.MetricId))

	_, err = create("handle TIME", nil)
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	second, err := create("Other", nil)
	require.NoError(t, err)

	got, err := f.client.DescribeMetric(t.Context(), &sdk.DescribeMetricInput{
		InstanceId: aws.String(inst), MetricId: first.MetricId,
	})
	require.NoError(t, err)

	m := got.Metric
	assert.Equal(t, types.MetricTypeCustomerManaged, m.Type)
	assert.Equal(t, types.MetricStatusSaved, m.Status)
	assert.Equal(t, types.MetricUnitSeconds, m.Unit)
	assert.Equal(t, aws.ToString(first.MetricArn), aws.ToString(m.Arn))
	assert.Equal(t, "AVG(M1)", aws.ToString(m.MetricCalculation.Calculation))
	assert.NotNil(t, m.CreatedTime)
	assert.Equal(t, map[string]string{"team": "a"}, m.Tags)

	list, err := f.client.ListMetrics(t.Context(), &sdk.ListMetricsInput{
		InstanceId: aws.String(inst), Type: types.MetricTypeCustomerManaged, MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, list.MetricSummaryList, 1)
	assert.Equal(t, "Handle time", aws.ToString(list.MetricSummaryList[0].Name))
	require.NotNil(t, list.NextToken)

	managed, err := f.client.ListMetrics(t.Context(), &sdk.ListMetricsInput{
		InstanceId: aws.String(inst), Type: types.MetricTypeAwsManaged,
	})
	require.NoError(t, err)
	assert.Empty(t, managed.MetricSummaryList)

	f.clock.advance(5_000_000_000)
	_, err = f.client.UpdateMetricContent(t.Context(), &sdk.UpdateMetricContentInput{
		InstanceId: aws.String(inst), MetricId: first.MetricId, Unit: types.MetricUnitDouble,
		MetricCalculation: calc("MAX(M1)", "M1"),
	})
	require.NoError(t, err)

	_, err = f.client.UpdateMetricMetadata(t.Context(), &sdk.UpdateMetricMetadataInput{
		InstanceId: aws.String(inst), MetricId: first.MetricId, Name: aws.String("OTHER"),
	})
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	_, err = f.client.UpdateMetricMetadata(t.Context(), &sdk.UpdateMetricMetadataInput{
		InstanceId: aws.String(inst), MetricId: first.MetricId, Name: aws.String("Renamed"),
		Description: aws.String("new"),
	})
	require.NoError(t, err)

	got, err = f.client.DescribeMetric(t.Context(), &sdk.DescribeMetricInput{
		InstanceId: aws.String(inst), MetricId: first.MetricId,
	})
	require.NoError(t, err)
	assert.Equal(t, "Renamed", aws.ToString(got.Metric.Name))
	assert.Equal(t, "new", aws.ToString(got.Metric.Description))
	assert.Equal(t, types.MetricUnitDouble, got.Metric.Unit)
	assert.Equal(t, "MAX(M1)", aws.ToString(got.Metric.MetricCalculation.Calculation))
	assert.True(t, got.Metric.LastModifiedTime.After(*got.Metric.CreatedTime))

	tags, err := f.client.ListTagsForResource(t.Context(), &sdk.ListTagsForResourceInput{ResourceArn: first.MetricArn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "a"}, tags.Tags)

	found, err := f.client.SearchMetrics(t.Context(), &sdk.SearchMetricsInput{
		InstanceId: aws.String(inst),
		SearchCriteria: &types.MetricSearchCriteria{StringCondition: &types.StringCondition{
			FieldName: aws.String(
				"name",
			),
			Value:          aws.String("rename"),
			ComparisonType: types.StringComparisonTypeContains,
		}},
	})
	require.NoError(t, err)
	require.Len(t, found.Metrics, 1)
	assert.Equal(t, aws.ToString(first.MetricId), aws.ToString(found.Metrics[0].Id))

	restored := f.restored(t)
	back, err := restored.client.DescribeMetric(t.Context(), &sdk.DescribeMetricInput{
		InstanceId: aws.String(inst), MetricId: first.MetricId,
	})
	require.NoError(t, err)
	assert.Equal(t, "Renamed", aws.ToString(back.Metric.Name))

	del := &sdk.DeleteMetricInput{InstanceId: aws.String(inst), MetricId: second.MetricId}
	_, err = f.client.DeleteMetric(t.Context(), del)
	require.NoError(t, err)

	_, err = f.client.DeleteMetric(t.Context(), del)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.DescribeMetric(
		t.Context(), &sdk.DescribeMetricInput{InstanceId: aws.String(inst), MetricId: second.MetricId})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}
