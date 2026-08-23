package firehose_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	firehosesdk "github.com/aws/aws-sdk-go-v2/service/firehose"
	firehosetypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

// TestCreateDeliveryStream_S3DestinationRequiredDefaultsRoundTrip drives
// CreateDeliveryStream/DescribeDeliveryStream through the real
// aws-sdk-go-v2/service/firehose client. Real S3DestinationDescription
// (types.go, aliased on the wire as ExtendedS3DestinationDescription) marks
// BufferingHints and EncryptionConfiguration required, but they are optional
// on the request side (validateExtendedS3DestinationConfiguration only
// null-checks RoleARN/BucketARN) -- AWS's own doc comments say it applies
// documented defaults ("The default value is 300"/"5", "the default is no
// encryption") when a client omits them. gopherstack's buildS3DestinationDescription
// previously passed the nil pointers straight through, and both wire fields were
// tagged omitempty, so a real client that simply never set these two optional
// fields (the common case) got a response missing both required members entirely.
func TestCreateDeliveryStream_S3DestinationRequiredDefaultsRoundTrip(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("123456789012", "us-east-1")
	h := firehose.NewHandler(b)
	client := newTestFirehoseClient(t, h)

	_, err := client.CreateDeliveryStream(t.Context(), &firehosesdk.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("s3-minimal-stream"),
		ExtendedS3DestinationConfiguration: &firehosetypes.ExtendedS3DestinationConfiguration{
			BucketARN: aws.String("arn:aws:s3:::my-bucket"),
			RoleARN:   aws.String("arn:aws:iam::123456789012:role/r"),
			// BufferingHints and EncryptionConfiguration deliberately omitted --
			// both are optional on the real request shape.
		},
	})
	require.NoError(t, err, "real SDK client's CreateDeliveryStream request must decode without error")

	out, err := client.DescribeDeliveryStream(t.Context(), &firehosesdk.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String("s3-minimal-stream"),
	})
	require.NoError(t, err, "real SDK client must decode DescribeDeliveryStream response without error")
	require.Len(t, out.DeliveryStreamDescription.Destinations, 1)

	dest := out.DeliveryStreamDescription.Destinations[0].ExtendedS3DestinationDescription
	require.NotNil(t, dest, "ExtendedS3DestinationDescription must be present")

	require.NotNil(t, dest.BufferingHints, "BufferingHints is required on the real response shape")
	require.NotNil(t, dest.BufferingHints.SizeInMBs)
	require.NotNil(t, dest.BufferingHints.IntervalInSeconds)
	require.EqualValues(t, 5, *dest.BufferingHints.SizeInMBs)
	require.EqualValues(t, 300, *dest.BufferingHints.IntervalInSeconds)

	require.NotNil(t, dest.EncryptionConfiguration, "EncryptionConfiguration is required on the real response shape")
	require.Equal(t, firehosetypes.NoEncryptionConfigNoEncryption, dest.EncryptionConfiguration.NoEncryptionConfig)
}

// TestCreateDeliveryStream_S3EmptyStringARNsRoundTrip covers the "client only
// null-checks the pointer, not its content" variant of this bug class
// (established by the cognitoidp batch of this campaign): the real client-side
// validator (validateExtendedS3DestinationConfiguration) rejects a nil
// BucketARN/RoleARN but not an explicit empty string, so a real client can
// legally send "" for either. gopherstack's S3DestinationDescription.BucketARN/
// RoleARN were plain (non-pointer) strings tagged omitempty, so an
// explicit-empty value vanished from the wire exactly like an omitted one --
// but the real SDK fields are *string, so a real client can tell the
// difference (omitted decodes nil, present-empty decodes to a non-nil pointer
// to "").
func TestCreateDeliveryStream_S3EmptyStringARNsRoundTrip(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("123456789012", "us-east-1")
	h := firehose.NewHandler(b)
	client := newTestFirehoseClient(t, h)

	_, err := client.CreateDeliveryStream(t.Context(), &firehosesdk.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("s3-empty-arn-stream"),
		ExtendedS3DestinationConfiguration: &firehosetypes.ExtendedS3DestinationConfiguration{
			BucketARN: aws.String(""),
			RoleARN:   aws.String(""),
		},
	})
	require.NoError(t, err, "real SDK client's CreateDeliveryStream request must decode without error")

	out, err := client.DescribeDeliveryStream(t.Context(), &firehosesdk.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String("s3-empty-arn-stream"),
	})
	require.NoError(t, err, "real SDK client must decode DescribeDeliveryStream response without error")
	require.Len(t, out.DeliveryStreamDescription.Destinations, 1)

	dest := out.DeliveryStreamDescription.Destinations[0].ExtendedS3DestinationDescription
	require.NotNil(t, dest)

	require.NotNil(t, dest.BucketARN, "BucketARN is required -- must be present, even when empty")
	require.Empty(t, *dest.BucketARN)
	require.NotNil(t, dest.RoleARN, "RoleARN is required -- must be present, even when empty")
	require.Empty(t, *dest.RoleARN)
}

// TestCreateDeliveryStream_S3BackupEncryptionConfigurationRoundTrip covers the
// "structurally absent" shape of this bug class. The real SDK's
// S3BackupConfiguration/S3BackupDescription fields are literally typed as
// S3DestinationConfiguration/S3DestinationDescription (types.go:1496,1575) --
// the exact same type used for a primary S3 destination -- so its required
// EncryptionConfiguration member applies to a backup destination too.
// gopherstack modeled the backup slot as its own narrower S3BackupDescription
// struct that never had an EncryptionConfiguration field at all, so any
// backup-enabled destination unconditionally dropped this required member on
// every call, not merely when a client happened to omit it.
func TestCreateDeliveryStream_S3BackupEncryptionConfigurationRoundTrip(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("123456789012", "us-east-1")
	h := firehose.NewHandler(b)
	client := newTestFirehoseClient(t, h)

	_, err := client.CreateDeliveryStream(t.Context(), &firehosesdk.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("s3-backup-stream"),
		ExtendedS3DestinationConfiguration: &firehosetypes.ExtendedS3DestinationConfiguration{
			BucketARN:    aws.String("arn:aws:s3:::my-bucket"),
			RoleARN:      aws.String("arn:aws:iam::123456789012:role/r"),
			S3BackupMode: firehosetypes.S3BackupModeEnabled,
			S3BackupConfiguration: &firehosetypes.S3DestinationConfiguration{
				BucketARN: aws.String("arn:aws:s3:::my-backup-bucket"),
				RoleARN:   aws.String("arn:aws:iam::123456789012:role/r"),
				// EncryptionConfiguration deliberately omitted -- optional on
				// input, required on output.
			},
		},
	})
	require.NoError(t, err, "real SDK client's CreateDeliveryStream request must decode without error")

	out, err := client.DescribeDeliveryStream(t.Context(), &firehosesdk.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String("s3-backup-stream"),
	})
	require.NoError(t, err, "real SDK client must decode DescribeDeliveryStream response without error")
	require.Len(t, out.DeliveryStreamDescription.Destinations, 1)

	dest := out.DeliveryStreamDescription.Destinations[0].ExtendedS3DestinationDescription
	require.NotNil(t, dest)
	require.NotNil(t, dest.S3BackupDescription, "S3BackupDescription must be present once backup mode is enabled")

	backup := dest.S3BackupDescription
	require.NotNil(
		t,
		backup.EncryptionConfiguration,
		"EncryptionConfiguration is required on S3DestinationDescription, the exact type "+
			"the real SDK reuses for S3BackupDescription",
	)
	require.Equal(t, firehosetypes.NoEncryptionConfigNoEncryption, backup.EncryptionConfiguration.NoEncryptionConfig)
}
