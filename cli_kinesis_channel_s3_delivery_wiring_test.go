package main

import (
	"compress/gzip"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

// TestInitializeServices_KinesisChannelS3DeliveryWiring drives the actual
// composition root (initializeServices, the function cli.go's Run() calls)
// rather than calling wireKinesisS3Delivery directly, so that deleting the
// wiring call from wireCrossServiceIntegrations -- not just breaking the
// helper function itself -- is what this test is sensitive to.
//
// Regression test for gopherstack-s781r: without InMemoryBackend.SetS3Writer
// wired, a Kinesis channel's buffered records have nowhere to go and are
// silently dropped on flush. This asserts the real production composition
// root binds the Kinesis backend's ChannelS3Writer seam to the real S3
// backend: a bucket created through the real S3 backend, a real
// CreateChannel + PutRecord + FlushChannel call sequence, and a real object
// appearing in the S3 backend's GetObject.
func TestInitializeServices_KinesisChannelS3DeliveryWiring(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1"}
	portAlloc, err := portalloc.New(19200, 19300)
	require.NoError(t, err)

	appCtx := &service.AppContext{
		Logger:     slog.Default(),
		Config:     cli,
		JanitorCtx: t.Context(),
		PortAlloc:  portAlloc,
	}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServices(appCtx)
	require.NoError(t, err)

	byName := serviceByName(services)

	kinesisH, ok := byName["Kinesis"].(*kinesisbackend.Handler)
	require.True(t, ok, "Kinesis handler must be registered")

	kinesisBk, ok := kinesisH.Backend.(*kinesisbackend.InMemoryBackend)
	require.True(t, ok, "Kinesis backend must be an InMemoryBackend")

	s3H, ok := byName["S3"].(*s3backend.S3Handler)
	require.True(t, ok, "S3 handler must be registered")

	s3Bk, ok := s3H.Backend.(*s3backend.InMemoryBackend)
	require.True(t, ok, "S3 backend must be an InMemoryBackend")

	ctx := t.Context()

	bucketName := "kinesis-channel-wiring-bucket"
	_, err = s3Bk.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	require.NoError(t, err)

	require.NoError(t, kinesisBk.CreateStream(ctx, &kinesisbackend.CreateStreamInput{
		StreamName: "kinesis-channel-wiring-stream",
		StreamMode: kinesisbackend.StreamModeOnDemand,
	}))

	created, err := kinesisBk.CreateChannel(ctx, &kinesisbackend.CreateChannelInput{
		ChannelName:             "kinesis-channel-wiring-channel",
		ServiceExecutionRoleARN: "arn:aws:iam::000000000000:role/kinesis-channel-wiring-role",
		StreamConfigurationList: []kinesisbackend.ChannelStreamConfig{
			{
				StreamARN: mustKinesisStreamARN(t, kinesisBk, "kinesis-channel-wiring-stream"),
				RecordConfiguration: kinesisbackend.ChannelRecordConfig{
					RecordFormatType: "STRING",
				},
			},
		},
		S3DestinationConfiguration: &kinesisbackend.ChannelS3Destination{
			StorageConfiguration: kinesisbackend.ChannelS3StorageConfig{
				BucketARN:           "arn:aws:s3:::" + bucketName,
				CompressionType:     "GZIP",
				ExpectedBucketOwner: "000000000000",
			},
		},
	})
	require.NoError(t, err)

	_, err = kinesisBk.PutRecord(ctx, &kinesisbackend.PutRecordInput{
		StreamName:   "kinesis-channel-wiring-stream",
		PartitionKey: "pk",
		Data:         []byte("wired-via-cli-composition-root"),
	})
	require.NoError(t, err)

	kinesisBk.FlushChannel(ctx, created.ChannelDescription.ChannelARN)

	var objectKey string
	require.Eventually(t, func() bool {
		out, listErr := s3Bk.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucketName)})
		if listErr != nil || len(out.Contents) == 0 {
			return false
		}
		objectKey = aws.ToString(out.Contents[0].Key)

		return true
	}, 5*time.Second, 20*time.Millisecond,
		"a record put through the real cli.go composition root's Kinesis-to-S3 channel wiring "+
			"(wireKinesisS3Delivery) must actually land as an object in the real S3 backend")

	obj, err := s3Bk.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucketName), Key: aws.String(objectKey)})
	require.NoError(t, err)

	r, err := gzip.NewReader(obj.Body)
	require.NoError(t, err)

	body, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "wired-via-cli-composition-root", string(body))
}

// mustKinesisStreamARN resolves streamName's ARN through the real backend,
// for building a CreateChannelInput.StreamConfigurationList entry.
func mustKinesisStreamARN(t *testing.T, bk *kinesisbackend.InMemoryBackend, streamName string) string {
	t.Helper()

	out, err := bk.DescribeStream(t.Context(), &kinesisbackend.DescribeStreamInput{StreamName: streamName})
	require.NoError(t, err)

	return out.StreamARN
}
