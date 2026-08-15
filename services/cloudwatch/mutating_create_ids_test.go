package cloudwatch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPutAnomalyDetector_ReturnsID covers gopherstack-7185: the real
// PutAnomalyDetectorOutput carries AnomalyDetectorId (aws-sdk-go-v2
// cloudwatch@v1.66.3 api_op_PutAnomalyDetector.go), but the handler returned
// an empty CBOR envelope. Verifies the returned id is non-empty and matches
// what a subsequent DescribeAnomalyDetectors reports, and that repeating the
// Put for the same detector preserves the same id rather than minting a new
// one each time.
func TestPutAnomalyDetector_ReturnsID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "new detector"},
		{name: "updated detector"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			in := &cwsdk.PutAnomalyDetectorInput{
				SingleMetricAnomalyDetector: &cwtypes.SingleMetricAnomalyDetector{
					Namespace:  aws.String("Custom/" + tc.name),
					MetricName: aws.String("Latency"),
					Stat:       aws.String("Average"),
				},
			}

			first, err := client.PutAnomalyDetector(ctx, in)
			require.NoError(t, err)
			require.NotEmpty(
				t, aws.ToString(first.AnomalyDetectorId),
				"PutAnomalyDetector must return the created detector's id",
			)

			out, err := client.DescribeAnomalyDetectors(ctx, &cwsdk.DescribeAnomalyDetectorsInput{
				Namespace:  aws.String("Custom/" + tc.name),
				MetricName: aws.String("Latency"),
			})
			require.NoError(t, err)
			require.Len(t, out.AnomalyDetectors, 1)
			assert.Equal(
				t, aws.ToString(first.AnomalyDetectorId), aws.ToString(out.AnomalyDetectors[0].AnomalyDetectorId),
				"Describe must report the same id Put returned",
			)

			second, err := client.PutAnomalyDetector(ctx, in)
			require.NoError(t, err)
			assert.Equal(
				t, aws.ToString(first.AnomalyDetectorId), aws.ToString(second.AnomalyDetectorId),
				"re-Put of the same detector must preserve its id",
			)
		})
	}
}

// TestPutMetricStream_ReturnsArn covers gopherstack-7185: the real
// PutMetricStreamOutput carries the stream's Arn (aws-sdk-go-v2
// cloudwatch@v1.66.3 api_op_PutMetricStream.go), but the handler returned an
// empty CBOR envelope. Verifies the returned Arn is non-empty and matches
// what a subsequent GetMetricStream reports, and is preserved across an
// update to the same stream name.
func TestPutMetricStream_ReturnsArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "new stream"},
		{name: "updated stream"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			streamName := "stream-" + tc.name
			in := &cwsdk.PutMetricStreamInput{
				Name:         aws.String(streamName),
				FirehoseArn:  aws.String("arn:aws:firehose:us-east-1:000000000000:deliverystream/test"),
				RoleArn:      aws.String("arn:aws:iam::000000000000:role/test"),
				OutputFormat: cwtypes.MetricStreamOutputFormatJson,
			}

			first, err := client.PutMetricStream(ctx, in)
			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(first.Arn), "PutMetricStream must return the stream's Arn")

			out, err := client.GetMetricStream(ctx, &cwsdk.GetMetricStreamInput{Name: aws.String(streamName)})
			require.NoError(t, err)
			assert.Equal(
				t, aws.ToString(first.Arn), aws.ToString(out.Arn),
				"GetMetricStream must report the same Arn Put returned",
			)

			second, err := client.PutMetricStream(ctx, in)
			require.NoError(t, err)
			assert.Equal(
				t, aws.ToString(first.Arn), aws.ToString(second.Arn),
				"re-Put of the same stream must preserve its Arn",
			)
		})
	}
}
