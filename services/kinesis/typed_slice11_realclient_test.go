package kinesis_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// TestTypedSlice11RealClient drives kinesis's last typed-coverage-blind op
// (gopherstack-n3zi slice 11) through the real aws-sdk-go-v2 client.
func TestTypedSlice11RealClient(t *testing.T) {
	t.Parallel()

	t.Run("stop stream encryption", func(t *testing.T) {
		t.Parallel()

		h := kinesis.NewHandler(kinesis.NewInMemoryBackend())
		client := newTestKinesisClient(t, h)

		streamName := "s11-encryption-stream"
		_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
			StreamName: aws.String(streamName),
			ShardCount: aws.Int32(1),
		})
		require.NoError(t, err)

		_, err = client.StartStreamEncryption(t.Context(), &kinesissdk.StartStreamEncryptionInput{
			StreamName:     aws.String(streamName),
			EncryptionType: types.EncryptionTypeKms,
			KeyId:          aws.String("alias/aws/kinesis"),
		})
		require.NoError(t, err)

		_, err = client.StopStreamEncryption(t.Context(), &kinesissdk.StopStreamEncryptionInput{
			StreamName:     aws.String(streamName),
			EncryptionType: types.EncryptionTypeKms,
			KeyId:          aws.String("alias/aws/kinesis"),
		})
		require.NoError(t, err)

		descOut, err := client.DescribeStreamSummary(t.Context(), &kinesissdk.DescribeStreamSummaryInput{
			StreamName: aws.String(streamName),
		})
		require.NoError(t, err)
		assert.Equal(t, types.EncryptionTypeNone, descOut.StreamDescriptionSummary.EncryptionType)
		assert.Nil(t, descOut.StreamDescriptionSummary.KeyId)
	})
}
