package firehose_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	firehosesdk "github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateDestination_MergesOmittedMembers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		update     *types.ExtendedS3DestinationUpdate
		name       string
		wantPrefix string
		wantComp   types.CompressionFormat
		wantSize   int32
	}{
		{
			name:       "prefix only keeps compression and buffering",
			update:     &types.ExtendedS3DestinationUpdate{Prefix: aws.String("q/")},
			wantPrefix: "q/", wantComp: types.CompressionFormatGzip, wantSize: 64,
		},
		{
			name: "buffering only keeps prefix and compression",
			update: &types.ExtendedS3DestinationUpdate{
				BufferingHints: &types.BufferingHints{SizeInMBs: aws.Int32(8), IntervalInSeconds: aws.Int32(90)},
			},
			wantPrefix: "p/", wantComp: types.CompressionFormatGzip, wantSize: 8,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestClient(t)
			_, err := c.CreateDeliveryStream(t.Context(), &firehosesdk.CreateDeliveryStreamInput{
				DeliveryStreamName: aws.String("s"),
				ExtendedS3DestinationConfiguration: &types.ExtendedS3DestinationConfiguration{
					BucketARN:         aws.String("arn:aws:s3:::b"),
					RoleARN:           aws.String("arn:aws:iam::123456789012:role/r"),
					Prefix:            aws.String("p/"),
					CompressionFormat: types.CompressionFormatGzip,
					BufferingHints: &types.BufferingHints{
						SizeInMBs:         aws.Int32(64),
						IntervalInSeconds: aws.Int32(60),
					},
				},
			})
			require.NoError(t, err)

			d, err := c.DescribeDeliveryStream(
				t.Context(),
				&firehosesdk.DescribeDeliveryStreamInput{DeliveryStreamName: aws.String("s")},
			)
			require.NoError(t, err)

			desc := d.DeliveryStreamDescription
			_, err = c.UpdateDestination(t.Context(), &firehosesdk.UpdateDestinationInput{
				DeliveryStreamName:             aws.String("s"),
				CurrentDeliveryStreamVersionId: desc.VersionId,
				DestinationId:                  desc.Destinations[0].DestinationId,
				ExtendedS3DestinationUpdate:    tc.update,
			})
			require.NoError(t, err)

			d, err = c.DescribeDeliveryStream(
				t.Context(),
				&firehosesdk.DescribeDeliveryStreamInput{DeliveryStreamName: aws.String("s")},
			)
			require.NoError(t, err)

			got := d.DeliveryStreamDescription.Destinations[0].ExtendedS3DestinationDescription
			assert.Equal(t, tc.wantPrefix, aws.ToString(got.Prefix))
			assert.Equal(t, tc.wantComp, got.CompressionFormat)
			assert.Equal(t, tc.wantSize, aws.ToInt32(got.BufferingHints.SizeInMBs))
			assert.Equal(t, "arn:aws:s3:::b", aws.ToString(got.BucketARN))
		})
	}
}
