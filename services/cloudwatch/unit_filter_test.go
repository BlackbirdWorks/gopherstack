package cloudwatch_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetMetricStatistics_UnitFilter_RealClient covers a dropped-field bug:
// GetMetricStatisticsInput.Unit (api_op_GetMetricStatistics.go: "If you
// specify a unit, the operation returns only data that was collected with
// that unit specified. If you specify a unit that does not match the data
// collected, the results of the operation are null.") was never read by
// either wire path (the legacy XML form handler or the real client's CBOR
// path), so a caller-supplied Unit had no effect at all.
func TestGetMetricStatistics_UnitFilter_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filterUnit cwtypes.StandardUnit
		wantCount  int
	}{
		{name: "matching_unit_included", filterUnit: cwtypes.StandardUnitSeconds, wantCount: 1},
		{name: "mismatched_unit_returns_no_datapoints", filterUnit: cwtypes.StandardUnitBytes, wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			now := time.Now().UTC().Truncate(time.Minute)
			_, err := client.PutMetricData(ctx, &cwsdk.PutMetricDataInput{
				Namespace: aws.String("unit-filter-ns"),
				MetricData: []cwtypes.MetricDatum{
					{
						MetricName: aws.String("Latency"),
						Value:      aws.Float64(10),
						Unit:       cwtypes.StandardUnitSeconds,
						Timestamp:  aws.Time(now),
					},
				},
			})
			require.NoError(t, err)

			out, err := client.GetMetricStatistics(ctx, &cwsdk.GetMetricStatisticsInput{
				Namespace:  aws.String("unit-filter-ns"),
				MetricName: aws.String("Latency"),
				StartTime:  aws.Time(now.Add(-time.Minute)),
				EndTime:    aws.Time(now.Add(time.Minute)),
				Period:     aws.Int32(60),
				Statistics: []cwtypes.Statistic{cwtypes.StatisticAverage},
				Unit:       tt.filterUnit,
			})
			require.NoError(t, err)
			assert.Len(t, out.Datapoints, tt.wantCount)
		})
	}
}
