package firehose_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	firehosesdk "github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDocumentIdOptions_OpenSearchRoundTrips covers the OpenSearch/Elasticsearch
// items_still_open entry: DocumentIdOptions (firehose@v1.46.4 types/types.go:1133,
// DefaultDocumentIdFormat FIREHOSE_DEFAULT|NO_DOCUMENT_ID) was accepted on the real
// AmazonopensearchserviceDestinationConfiguration/-Update and
// ElasticsearchDestinationConfiguration/-Update but had no field at all in this
// backend, so it was silently dropped and never echoed by DescribeDeliveryStream.
func TestDocumentIdOptions_OpenSearchRoundTrips(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)

	_, err := client.CreateDeliveryStream(t.Context(), &firehosesdk.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("aos-docid-stream"),
		AmazonopensearchserviceDestinationConfiguration: &types.AmazonopensearchserviceDestinationConfiguration{
			DomainARN: aws.String("arn:aws:es:us-east-1:000000000000:domain/my-domain"),
			IndexName: aws.String("access-logs"),
			RoleARN:   aws.String("arn:aws:iam::000000000000:role/firehose"),
			S3Configuration: &types.S3DestinationConfiguration{
				BucketARN: aws.String("arn:aws:s3:::aos-docid-bucket"),
				RoleARN:   aws.String("arn:aws:iam::000000000000:role/firehose"),
			},
			DocumentIdOptions: &types.DocumentIdOptions{
				DefaultDocumentIdFormat: types.DefaultDocumentIdFormatNoDocumentId,
			},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeDeliveryStream(t.Context(), &firehosesdk.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String("aos-docid-stream"),
	})
	require.NoError(t, err)
	require.Len(t, desc.DeliveryStreamDescription.Destinations, 1)
	dest := desc.DeliveryStreamDescription.Destinations[0].AmazonopensearchserviceDestinationDescription
	require.NotNil(t, dest)
	require.NotNil(t, dest.DocumentIdOptions)
	assert.Equal(t, types.DefaultDocumentIdFormatNoDocumentId, dest.DocumentIdOptions.DefaultDocumentIdFormat)

	_, err = client.UpdateDestination(t.Context(), &firehosesdk.UpdateDestinationInput{
		DeliveryStreamName:             aws.String("aos-docid-stream"),
		CurrentDeliveryStreamVersionId: aws.String("1"),
		DestinationId:                  desc.DeliveryStreamDescription.Destinations[0].DestinationId,
		AmazonopensearchserviceDestinationUpdate: &types.AmazonopensearchserviceDestinationUpdate{
			DocumentIdOptions: &types.DocumentIdOptions{
				DefaultDocumentIdFormat: types.DefaultDocumentIdFormatFirehoseDefault,
			},
		},
	})
	require.NoError(t, err)

	desc2, err := client.DescribeDeliveryStream(t.Context(), &firehosesdk.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String("aos-docid-stream"),
	})
	require.NoError(t, err)
	dest2 := desc2.DeliveryStreamDescription.Destinations[0].AmazonopensearchserviceDestinationDescription
	require.NotNil(t, dest2.DocumentIdOptions)
	assert.Equal(t, types.DefaultDocumentIdFormatFirehoseDefault, dest2.DocumentIdOptions.DefaultDocumentIdFormat)
}

// TestDocumentIdOptions_ElasticsearchRoundTrips is the legacy-Elasticsearch sibling of
// TestDocumentIdOptions_OpenSearchRoundTrips (same DocumentIdOptions field, wire-distinct
// destination type).
func TestDocumentIdOptions_ElasticsearchRoundTrips(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)

	_, err := client.CreateDeliveryStream(t.Context(), &firehosesdk.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("es-docid-stream"),
		ElasticsearchDestinationConfiguration: &types.ElasticsearchDestinationConfiguration{
			DomainARN: aws.String("arn:aws:es:us-east-1:000000000000:domain/legacy-domain"),
			IndexName: aws.String("access-logs"),
			RoleARN:   aws.String("arn:aws:iam::000000000000:role/firehose"),
			S3Configuration: &types.S3DestinationConfiguration{
				BucketARN: aws.String("arn:aws:s3:::es-docid-bucket"),
				RoleARN:   aws.String("arn:aws:iam::000000000000:role/firehose"),
			},
			DocumentIdOptions: &types.DocumentIdOptions{
				DefaultDocumentIdFormat: types.DefaultDocumentIdFormatNoDocumentId,
			},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeDeliveryStream(t.Context(), &firehosesdk.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String("es-docid-stream"),
	})
	require.NoError(t, err)
	dest := desc.DeliveryStreamDescription.Destinations[0].ElasticsearchDestinationDescription
	require.NotNil(t, dest)
	require.NotNil(t, dest.DocumentIdOptions)
	assert.Equal(t, types.DefaultDocumentIdFormatNoDocumentId, dest.DocumentIdOptions.DefaultDocumentIdFormat)
}

// TestDocumentIdOptions_InvalidFormatRejected asserts an unrecognized
// DefaultDocumentIdFormat is rejected rather than silently accepted -- the same
// unknown-enum-value InvalidArgumentException class as this service's other
// destination-field validation.
func TestDocumentIdOptions_InvalidFormatRejected(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)

	_, err := client.CreateDeliveryStream(t.Context(), &firehosesdk.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("aos-docid-bad-stream"),
		AmazonopensearchserviceDestinationConfiguration: &types.AmazonopensearchserviceDestinationConfiguration{
			DomainARN: aws.String("arn:aws:es:us-east-1:000000000000:domain/my-domain"),
			IndexName: aws.String("access-logs"),
			RoleARN:   aws.String("arn:aws:iam::000000000000:role/firehose"),
			S3Configuration: &types.S3DestinationConfiguration{
				BucketARN: aws.String("arn:aws:s3:::aos-docid-bad-bucket"),
				RoleARN:   aws.String("arn:aws:iam::000000000000:role/firehose"),
			},
			DocumentIdOptions: &types.DocumentIdOptions{
				DefaultDocumentIdFormat: "NOT_A_REAL_FORMAT",
			},
		},
	})
	require.Error(t, err)
}
