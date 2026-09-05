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

// TestGetMetricStatistics_ExtendedStatistics_RealClient covers a
// missing-field bug: the backend genuinely computes each Datapoint's
// ExtendedStatistics (metrics.go, computeExtendedStats) and even emits it on
// the legacy XML GetMetricStatistics path (handler_metrics.go's extStatXML),
// but cborGetMetricStatistics never touched it -- a real
// aws-sdk-go-v2 client (which only speaks rpc-v2-cbor for this service, per
// cloudwatch@v1.66.3 api_client.go's rpcv2.NewCBOR) always saw a nil
// ExtendedStatistics map regardless of what ExtendedStatistics the caller
// requested.
func TestGetMetricStatistics_ExtendedStatistics_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	now := time.Now().UTC().Truncate(time.Minute)
	_, err := client.PutMetricData(ctx, &cwsdk.PutMetricDataInput{
		Namespace: aws.String("ext-stat-ns"),
		MetricData: []cwtypes.MetricDatum{
			{MetricName: aws.String("Latency"), Value: aws.Float64(10), Timestamp: aws.Time(now)},
			{MetricName: aws.String("Latency"), Value: aws.Float64(20), Timestamp: aws.Time(now)},
			{MetricName: aws.String("Latency"), Value: aws.Float64(30), Timestamp: aws.Time(now)},
			{MetricName: aws.String("Latency"), Value: aws.Float64(40), Timestamp: aws.Time(now)},
		},
	})
	require.NoError(t, err)

	out, err := client.GetMetricStatistics(ctx, &cwsdk.GetMetricStatisticsInput{
		Namespace:          aws.String("ext-stat-ns"),
		MetricName:         aws.String("Latency"),
		StartTime:          aws.Time(now.Add(-time.Minute)),
		EndTime:            aws.Time(now.Add(time.Minute)),
		Period:             aws.Int32(60),
		ExtendedStatistics: []string{"p90"},
	})
	require.NoError(t, err)
	require.Len(t, out.Datapoints, 1)
	require.NotEmpty(t, out.Datapoints[0].ExtendedStatistics,
		"ExtendedStatistics empty - GetMetricStatistics dropped it entirely on the CBOR wire")
	require.Contains(t, out.Datapoints[0].ExtendedStatistics, "p90")
	// Linear-interpolated 90th percentile of the sorted [10,20,30,40] sample
	// (metricmath.go's percentile: idx = 0.9*(4-1) = 2.7, so 30*0.3 + 40*0.7).
	assert.InDelta(t, 37.0, out.Datapoints[0].ExtendedStatistics["p90"], 1e-9)
}
