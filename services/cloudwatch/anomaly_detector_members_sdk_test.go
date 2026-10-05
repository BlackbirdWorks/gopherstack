package cloudwatch_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func putSingleDetector(t *testing.T, client *cwsdk.Client, name string, dims []types.Dimension) string {
	t.Helper()

	out, err := client.PutAnomalyDetector(t.Context(), &cwsdk.PutAnomalyDetectorInput{
		SingleMetricAnomalyDetector: &types.SingleMetricAnomalyDetector{
			Namespace:  aws.String("NS"),
			MetricName: aws.String(name),
			Stat:       aws.String("Average"),
			Dimensions: dims,
		},
	})
	require.NoError(t, err)

	return aws.ToString(out.AnomalyDetectorId)
}

func TestAnomalyDetector_ConfigurationAndCharacteristicsRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	start := time.Unix(1_700_000_000, 0).UTC()
	end := start.Add(2 * time.Hour)

	_, err := client.PutAnomalyDetector(t.Context(), &cwsdk.PutAnomalyDetectorInput{
		SingleMetricAnomalyDetector: &types.SingleMetricAnomalyDetector{
			Namespace: aws.String("NS"), MetricName: aws.String("M"), Stat: aws.String("Sum"),
		},
		Configuration: &types.AnomalyDetectorConfiguration{
			MetricTimezone:     aws.String("America/Chicago"),
			ExcludedTimeRanges: []types.Range{{StartTime: &start, EndTime: &end}},
		},
		MetricCharacteristics: &types.MetricCharacteristics{PeriodicSpikes: aws.Bool(true)},
	})
	require.NoError(t, err)

	out, err := client.DescribeAnomalyDetectors(t.Context(), &cwsdk.DescribeAnomalyDetectorsInput{
		Namespace: aws.String("NS"),
	})
	require.NoError(t, err)
	require.Len(t, out.AnomalyDetectors, 1)

	d := out.AnomalyDetectors[0]
	require.NotNil(t, d.Configuration)
	assert.Equal(t, "America/Chicago", aws.ToString(d.Configuration.MetricTimezone))
	require.Len(t, d.Configuration.ExcludedTimeRanges, 1)
	assert.True(t, start.Equal(aws.ToTime(d.Configuration.ExcludedTimeRanges[0].StartTime)))
	assert.True(t, end.Equal(aws.ToTime(d.Configuration.ExcludedTimeRanges[0].EndTime)))
	require.NotNil(t, d.MetricCharacteristics)
	assert.True(t, aws.ToBool(d.MetricCharacteristics.PeriodicSpikes))
}

func TestAnomalyDetector_DescribeFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input     cwsdk.DescribeAnomalyDetectorsInput
		name      string
		wantCode  string
		wantNames []string
	}{
		{name: "default is single metric", wantNames: []string{"a", "b"}},
		{
			name: "metric math type",
			input: cwsdk.DescribeAnomalyDetectorsInput{
				AnomalyDetectorTypes: []types.AnomalyDetectorType{types.AnomalyDetectorTypeMetricMath},
			},
			wantNames: []string{"math"},
		},
		{
			name: "both types",
			input: cwsdk.DescribeAnomalyDetectorsInput{AnomalyDetectorTypes: []types.AnomalyDetectorType{
				types.AnomalyDetectorTypeSingleMetric, types.AnomalyDetectorTypeMetricMath,
			}},
			wantNames: []string{"a", "b", "math"},
		},
		{
			name: "dimensions",
			input: cwsdk.DescribeAnomalyDetectorsInput{
				Dimensions: []types.Dimension{{Name: aws.String("Host"), Value: aws.String("h1")}},
			},
			wantNames: []string{"b"},
		},
		{
			name: "ids with filter",
			input: cwsdk.DescribeAnomalyDetectorsInput{
				AnomalyDetectorIds: []string{"x"},
				Namespace:          aws.String("NS"),
			},
			wantCode: "InvalidParameterCombinationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			putSingleDetector(t, client, "a", nil)
			putSingleDetector(t, client, "b", []types.Dimension{{Name: aws.String("Host"), Value: aws.String("h1")}})

			_, err := client.PutAnomalyDetector(t.Context(), &cwsdk.PutAnomalyDetectorInput{
				MetricMathAnomalyDetector: &types.MetricMathAnomalyDetector{
					MetricDataQueries: []types.MetricDataQuery{{
						Id: aws.String("m1"),
						MetricStat: &types.MetricStat{
							Metric: &types.Metric{Namespace: aws.String("NS"), MetricName: aws.String("math")},
							Period: aws.Int32(60),
							Stat:   aws.String("Average"),
						},
					}},
				},
			})
			require.NoError(t, err)

			out, err := client.DescribeAnomalyDetectors(t.Context(), &tt.input)

			if tt.wantCode != "" {
				var apiErr smithy.APIError

				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.wantCode, apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)

			got := make([]string, 0, len(out.AnomalyDetectors))

			for _, d := range out.AnomalyDetectors {
				switch {
				case d.SingleMetricAnomalyDetector != nil:
					got = append(got, aws.ToString(d.SingleMetricAnomalyDetector.MetricName))
				case d.MetricMathAnomalyDetector != nil:
					require.Len(t, d.MetricMathAnomalyDetector.MetricDataQueries, 1)
					got = append(
						got,
						aws.ToString(d.MetricMathAnomalyDetector.MetricDataQueries[0].MetricStat.Metric.MetricName),
					)
				}
			}

			assert.ElementsMatch(t, tt.wantNames, got)
		})
	}
}

func TestAnomalyDetector_DeleteByIDAndMath(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	id := putSingleDetector(t, client, "a", nil)

	mathDetector := &types.MetricMathAnomalyDetector{
		MetricDataQueries: []types.MetricDataQuery{{
			Id: aws.String("m1"),
			MetricStat: &types.MetricStat{
				Metric: &types.Metric{Namespace: aws.String("NS"), MetricName: aws.String("math")},
				Period: aws.Int32(60),
				Stat:   aws.String("Average"),
			},
		}},
	}

	_, err := client.PutAnomalyDetector(
		t.Context(),
		&cwsdk.PutAnomalyDetectorInput{MetricMathAnomalyDetector: mathDetector},
	)
	require.NoError(t, err)

	_, err = client.DeleteAnomalyDetector(
		t.Context(),
		&cwsdk.DeleteAnomalyDetectorInput{AnomalyDetectorId: aws.String(id)},
	)
	require.NoError(t, err)

	_, err = client.DeleteAnomalyDetector(
		t.Context(),
		&cwsdk.DeleteAnomalyDetectorInput{AnomalyDetectorId: aws.String(id)},
	)

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())

	_, err = client.DeleteAnomalyDetector(
		t.Context(),
		&cwsdk.DeleteAnomalyDetectorInput{MetricMathAnomalyDetector: mathDetector},
	)
	require.NoError(t, err)

	out, err := client.DescribeAnomalyDetectors(t.Context(), &cwsdk.DescribeAnomalyDetectorsInput{
		AnomalyDetectorTypes: []types.AnomalyDetectorType{
			types.AnomalyDetectorTypeSingleMetric,
			types.AnomalyDetectorTypeMetricMath,
		},
	})
	require.NoError(t, err)
	assert.Empty(t, out.AnomalyDetectors)
}
