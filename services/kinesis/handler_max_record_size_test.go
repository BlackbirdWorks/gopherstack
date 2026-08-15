package kinesis_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// TestUpdateMaxRecordSize_RoundTrip drives UpdateMaxRecordSize through the
// real aws-sdk-go-v2 client. Its real Input field is MaxRecordSizeInKiB, not
// MaxRecordSizeBytes (kinesis@v1.46.4 api_op_UpdateMaxRecordSize.go:30-47),
// and the unit is KiB, not bytes -- decoding the fabricated key left the
// value at zero, which the backend's own bounds check then always rejected
// with InvalidArgumentException (gopherstack-nbg8: every real call 400'd).
// Proves the fix via an actual PutRecord round trip, not just a 2xx from
// UpdateMaxRecordSize itself: raises the limit to 2 MiB, then a 1.5 MiB
// record -- rejected under the untouched 1 MiB default -- succeeds.
func TestUpdateMaxRecordSize_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	streamName := "max-record-size-stream"

	_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(t.Context(), &kinesissdk.DescribeStreamInput{StreamName: aws.String(streamName)})
	require.NoError(t, err)

	const oneAndHalfMiB = 3 * 512 * 1024

	_, err = client.PutRecord(t.Context(), &kinesissdk.PutRecordInput{
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String("pk"),
		Data:         make([]byte, oneAndHalfMiB),
	})
	require.Error(t, err, "1.5 MiB record must be rejected under the untouched 1 MiB default limit")

	// UpdateMaxRecordSizeInput has no StreamName member on the real wire
	// shape -- only StreamARN (kinesis@v1.46.4 api_op_UpdateMaxRecordSize.go:30-47).
	_, err = client.UpdateMaxRecordSize(t.Context(), &kinesissdk.UpdateMaxRecordSizeInput{
		StreamARN:          desc.StreamDescription.StreamARN,
		MaxRecordSizeInKiB: aws.Int32(2 * 1024),
	})
	require.NoError(t, err)

	_, err = client.PutRecord(t.Context(), &kinesissdk.PutRecordInput{
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String("pk"),
		Data:         make([]byte, oneAndHalfMiB),
	})
	assert.NoError(t, err, "1.5 MiB record must now be accepted under the raised 2 MiB limit")
}

// TestUpdateMaxRecordSize_OutOfRangeRejected verifies the KiB bounds check
// (1024-10240 KiB, i.e. 1-10 MiB) rejects a value outside that range.
func TestUpdateMaxRecordSize_OutOfRangeRejected(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	streamName := "max-record-size-oor"

	_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(t.Context(), &kinesissdk.DescribeStreamInput{StreamName: aws.String(streamName)})
	require.NoError(t, err)

	_, err = client.UpdateMaxRecordSize(t.Context(), &kinesissdk.UpdateMaxRecordSizeInput{
		StreamARN:          desc.StreamDescription.StreamARN,
		MaxRecordSizeInKiB: aws.Int32(1),
	})
	require.Error(t, err)
}
