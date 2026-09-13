package firehose_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	firehosesdk "github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_TagsRecordsAndEncryption drives firehose's typed-coverage-blind ops
// (gopherstack-n3zi) through the real aws-sdk-go-v2 client.
func TestRealClient_TagsRecordsAndEncryption(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "tags", run: func(t *testing.T) {
			t.Helper()

			client := newTestClient(t)
			ctx := t.Context()

			_, err := client.CreateDeliveryStream(ctx, &firehosesdk.CreateDeliveryStreamInput{
				DeliveryStreamName: aws.String("s11-tag-stream"),
				Tags: []types.Tag{
					{Key: aws.String("env"), Value: aws.String("test")},
				},
			})
			require.NoError(t, err)

			_, err = client.TagDeliveryStream(ctx, &firehosesdk.TagDeliveryStreamInput{
				DeliveryStreamName: aws.String("s11-tag-stream"),
				Tags: []types.Tag{
					{Key: aws.String("team"), Value: aws.String("platform")},
				},
			})
			require.NoError(t, err)

			listOut, err := client.ListTagsForDeliveryStream(ctx, &firehosesdk.ListTagsForDeliveryStreamInput{
				DeliveryStreamName: aws.String("s11-tag-stream"),
			})
			require.NoError(t, err)

			tags := map[string]string{}
			for _, tg := range listOut.Tags {
				tags[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
			}
			assert.Equal(t, "test", tags["env"])
			assert.Equal(t, "platform", tags["team"])

			_, err = client.UntagDeliveryStream(ctx, &firehosesdk.UntagDeliveryStreamInput{
				DeliveryStreamName: aws.String("s11-tag-stream"),
				TagKeys:            []string{"team"},
			})
			require.NoError(t, err)

			afterOut, err := client.ListTagsForDeliveryStream(ctx, &firehosesdk.ListTagsForDeliveryStreamInput{
				DeliveryStreamName: aws.String("s11-tag-stream"),
			})
			require.NoError(t, err)

			afterTags := map[string]string{}
			for _, tg := range afterOut.Tags {
				afterTags[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
			}
			assert.Equal(t, "test", afterTags["env"])
			assert.NotContains(t, afterTags, "team")
		}},
		{name: "put record batch", run: func(t *testing.T) {
			t.Helper()

			client := newTestClient(t)
			ctx := t.Context()

			_, err := client.CreateDeliveryStream(ctx, &firehosesdk.CreateDeliveryStreamInput{
				DeliveryStreamName: aws.String("s11-batch-stream"),
			})
			require.NoError(t, err)

			out, err := client.PutRecordBatch(ctx, &firehosesdk.PutRecordBatchInput{
				DeliveryStreamName: aws.String("s11-batch-stream"),
				Records: []types.Record{
					{Data: []byte("record-one")},
					{Data: []byte("record-two")},
				},
			})
			require.NoError(t, err)
			assert.Equal(t, int32(0), aws.ToInt32(out.FailedPutCount))
			require.Len(t, out.RequestResponses, 2)
			for _, entry := range out.RequestResponses {
				assert.NotEmpty(t, aws.ToString(entry.RecordId))
				assert.Empty(t, aws.ToString(entry.ErrorCode))
			}
		}},
		{name: "stream encryption", run: func(t *testing.T) {
			t.Helper()

			client := newTestClient(t)
			ctx := t.Context()

			_, err := client.CreateDeliveryStream(ctx, &firehosesdk.CreateDeliveryStreamInput{
				DeliveryStreamName: aws.String("s11-encrypt-stream"),
			})
			require.NoError(t, err)

			_, err = client.StartDeliveryStreamEncryption(ctx, &firehosesdk.StartDeliveryStreamEncryptionInput{
				DeliveryStreamName: aws.String("s11-encrypt-stream"),
				DeliveryStreamEncryptionConfigurationInput: &types.DeliveryStreamEncryptionConfigurationInput{
					KeyType: types.KeyTypeAwsOwnedCmk,
				},
			})
			require.NoError(t, err)

			afterStart, err := client.DescribeDeliveryStream(ctx, &firehosesdk.DescribeDeliveryStreamInput{
				DeliveryStreamName: aws.String("s11-encrypt-stream"),
			})
			require.NoError(t, err)
			require.NotNil(t, afterStart.DeliveryStreamDescription.DeliveryStreamEncryptionConfiguration)
			assert.Equal(t, types.DeliveryStreamEncryptionStatusEnabled,
				afterStart.DeliveryStreamDescription.DeliveryStreamEncryptionConfiguration.Status)

			_, err = client.StopDeliveryStreamEncryption(ctx, &firehosesdk.StopDeliveryStreamEncryptionInput{
				DeliveryStreamName: aws.String("s11-encrypt-stream"),
			})
			require.NoError(t, err)

			afterStop, err := client.DescribeDeliveryStream(ctx, &firehosesdk.DescribeDeliveryStreamInput{
				DeliveryStreamName: aws.String("s11-encrypt-stream"),
			})
			require.NoError(t, err)
			require.NotNil(t, afterStop.DeliveryStreamDescription.DeliveryStreamEncryptionConfiguration)
			assert.Equal(t, types.DeliveryStreamEncryptionStatusDisabled,
				afterStop.DeliveryStreamDescription.DeliveryStreamEncryptionConfiguration.Status)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
