package cloudwatch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/require"
)

// TestSDK_StartStopMetricStreams drives StartMetricStreams/StopMetricStreams
// through a real aws-sdk-go-v2 cloudwatch@v1.66.3 client, which speaks
// smithy rpc-v2-cbor exclusively. gopherstack-jqh2 pass 4: these two ops
// were reachable and correctly listed in GetSupportedOperations and the
// query/form dispatch chain, but dispatchCBOR (rpcv2cbor.go) -- a separate
// op-name table the real client's protocol actually uses -- had no case for
// either, so every real client call fell through to the "unknown
// operation" InvalidAction error. Fixed by adding
// cborStartMetricStreams/cborStopMetricStreams and wiring them into
// dispatchAnomalyMetricStreamCBOR.
func TestSDK_StartStopMetricStreams(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.PutMetricStream(ctx, &cwsdk.PutMetricStreamInput{
		Name:         aws.String("jqh2-stream"),
		FirehoseArn:  aws.String("arn:aws:firehose:us-east-1:000000000000:deliverystream/jqh2"),
		RoleArn:      aws.String("arn:aws:iam::000000000000:role/jqh2"),
		OutputFormat: cwtypes.MetricStreamOutputFormatJson,
	})
	require.NoError(t, err)

	_, err = client.StartMetricStreams(ctx, &cwsdk.StartMetricStreamsInput{
		Names: []string{"jqh2-stream"},
	})
	require.NoError(t, err)

	got, err := client.GetMetricStream(ctx, &cwsdk.GetMetricStreamInput{Name: aws.String("jqh2-stream")})
	require.NoError(t, err)
	require.Equal(t, "RUNNING", *got.State)

	_, err = client.StopMetricStreams(ctx, &cwsdk.StopMetricStreamsInput{
		Names: []string{"jqh2-stream"},
	})
	require.NoError(t, err)

	got, err = client.GetMetricStream(ctx, &cwsdk.GetMetricStreamInput{Name: aws.String("jqh2-stream")})
	require.NoError(t, err)
	require.Equal(t, "STOPPED", *got.State)
}
