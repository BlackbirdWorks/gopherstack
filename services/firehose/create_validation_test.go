package firehose_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	firehosesdk "github.com/aws/aws-sdk-go-v2/service/firehose"
	firehosetypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

func TestCreateDeliveryStream_SourceAndBucketValidation(t *testing.T) {
	t.Parallel()

	s3cfg := func(bucket string) *firehosetypes.ExtendedS3DestinationConfiguration {
		return &firehosetypes.ExtendedS3DestinationConfiguration{
			RoleARN: aws.String("arn:aws:iam::123456789012:role/r"), BucketARN: aws.String(bucket),
		}
	}

	tests := []struct {
		name    string
		in      firehosesdk.CreateDeliveryStreamInput
		wantErr bool
	}{
		{
			name: "direct put ok",
			in:   firehosesdk.CreateDeliveryStreamInput{ExtendedS3DestinationConfiguration: s3cfg("arn:aws:s3:::b")},
		},
		{
			name:    "malformed bucket arn",
			in:      firehosesdk.CreateDeliveryStreamInput{ExtendedS3DestinationConfiguration: s3cfg("not-an-arn")},
			wantErr: true,
		},
		{
			name: "non s3 arn",
			in: firehosesdk.CreateDeliveryStreamInput{
				ExtendedS3DestinationConfiguration: s3cfg("arn:aws:sqs:us-east-1:1:q"),
			},
			wantErr: true,
		},
		{
			name: "kinesis source without config",
			in: firehosesdk.CreateDeliveryStreamInput{
				DeliveryStreamType:                 firehosetypes.DeliveryStreamTypeKinesisStreamAsSource,
				ExtendedS3DestinationConfiguration: s3cfg("arn:aws:s3:::b"),
			},
			wantErr: true,
		},
		{
			name: "kinesis source with config",
			in: firehosesdk.CreateDeliveryStreamInput{
				DeliveryStreamType: firehosetypes.DeliveryStreamTypeKinesisStreamAsSource,
				KinesisStreamSourceConfiguration: &firehosetypes.KinesisStreamSourceConfiguration{
					KinesisStreamARN: aws.String("arn:aws:kinesis:us-east-1:123456789012:stream/s"),
					RoleARN:          aws.String("arn:aws:iam::123456789012:role/r"),
				},
				ExtendedS3DestinationConfiguration: s3cfg("arn:aws:s3:::b"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := firehose.NewInMemoryBackend("123456789012", "us-east-1")
			client := newTestFirehoseClient(t, firehose.NewHandler(backend))
			tt.in.DeliveryStreamName = aws.String("s")

			_, err := client.CreateDeliveryStream(t.Context(), &tt.in)
			if tt.wantErr {
				require.ErrorContains(t, err, "InvalidArgumentException")

				return
			}

			require.NoError(t, err)
		})
	}
}
