package kinesis_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// TestDeleteStream_EnforceConsumerDeletion drives DeleteStream through the
// real SDK client and locks the real-AWS contract from
// DeleteStreamInput.EnforceConsumerDeletion's own doc comment: "If this
// parameter is unset (null) or if you set it to false, and the stream has
// registered consumers, the call to DeleteStream fails with a
// ResourceInUseException." A previous revision deleted the stream
// unconditionally regardless of registered consumers, which is more
// permissive than real AWS.
func TestDeleteStream_EnforceConsumerDeletion(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	streamName := "consumer-guarded-stream"
	_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(t.Context(), &kinesissdk.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)

	streamARN := desc.StreamDescription.StreamARN

	_, err = client.RegisterStreamConsumer(t.Context(), &kinesissdk.RegisterStreamConsumerInput{
		StreamARN:    streamARN,
		ConsumerName: aws.String("watcher"),
	})
	require.NoError(t, err)

	_, err = client.DeleteStream(t.Context(), &kinesissdk.DeleteStreamInput{
		StreamName: aws.String(streamName),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceInUseException", apiErr.ErrorCode())

	// The stream must still exist -- the failed delete must not have
	// mutated any state.
	_, err = client.DescribeStream(t.Context(), &kinesissdk.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)

	_, err = client.DeleteStream(t.Context(), &kinesissdk.DeleteStreamInput{
		StreamName:              aws.String(streamName),
		EnforceConsumerDeletion: aws.Bool(true),
	})
	require.NoError(t, err)

	_, err = client.DescribeStream(t.Context(), &kinesissdk.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	require.Error(t, err)
}
