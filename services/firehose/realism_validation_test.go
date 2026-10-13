package firehose_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	firehosesdk "github.com/aws/aws-sdk-go-v2/service/firehose"
	firehosetypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSDKCreateDeliveryStreamNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		stream  string
		wantErr bool
	}{
		{name: "valid", stream: "my-stream_1.v2"},
		{name: "space", stream: "bad name", wantErr: true},
		{name: "slash", stream: "a/b", wantErr: true},
		{name: "too_long", stream: strings.Repeat("a", 65), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t)
			_, err := client.CreateDeliveryStream(t.Context(), &firehosesdk.CreateDeliveryStreamInput{
				DeliveryStreamName: aws.String(tt.stream),
			})

			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var invalid *firehosetypes.InvalidArgumentException
			require.ErrorAs(t, err, &invalid)
			assert.False(t, strings.HasPrefix(aws.ToString(invalid.Message), "InvalidArgumentException"))
		})
	}
}

func TestSDKUpdateDestinationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
		destID  string
		want    string
	}{
		{
			name:    "stale_version",
			version: "99",
			destID:  "destinationId-000000000001",
			want:    "ConcurrentModificationException",
		},
		{
			name:    "unknown_destination",
			version: "1",
			destID:  "destinationId-000000000009",
			want:    "InvalidArgumentException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t)
			_, err := client.CreateDeliveryStream(t.Context(), &firehosesdk.CreateDeliveryStreamInput{
				DeliveryStreamName: aws.String("s"),
				ExtendedS3DestinationConfiguration: &firehosetypes.ExtendedS3DestinationConfiguration{
					RoleARN:   aws.String("arn:aws:iam::000000000000:role/r"),
					BucketARN: aws.String("arn:aws:s3:::b"),
				},
			})
			require.NoError(t, err)

			_, err = client.UpdateDestination(t.Context(), &firehosesdk.UpdateDestinationInput{
				DeliveryStreamName:             aws.String("s"),
				CurrentDeliveryStreamVersionId: aws.String(tt.version),
				DestinationId:                  aws.String(tt.destID),
				ExtendedS3DestinationUpdate: &firehosetypes.ExtendedS3DestinationUpdate{
					RoleARN: aws.String("arn:aws:iam::000000000000:role/r2"),
				},
			})

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.want, apiErr.ErrorCode())
		})
	}
}
