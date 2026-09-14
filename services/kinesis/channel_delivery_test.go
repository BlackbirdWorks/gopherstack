package kinesis_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesissdktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// fakeChannelS3Writer implements kinesis.ChannelS3Writer, capturing every
// PutObject call for assertion.
type fakeChannelS3Writer struct {
	calls []fakeChannelPutCall
	mu    sync.Mutex
}

type fakeChannelPutCall struct {
	bucket          string
	key             string
	contentEncoding string
	body            []byte
}

func (f *fakeChannelS3Writer) PutObject(
	_ context.Context, input *sdk_s3.PutObjectInput,
) (*sdk_s3.PutObjectOutput, error) {
	body, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakeChannelPutCall{
		bucket:          aws.ToString(input.Bucket),
		key:             aws.ToString(input.Key),
		body:            body,
		contentEncoding: aws.ToString(input.ContentEncoding),
	})

	return &sdk_s3.PutObjectOutput{}, nil
}

// allCalls returns a snapshot of every PutObject call captured so far. All
// scenarios in this file deliver to a single bucket (the channel's main
// destination and its default-prefix dead-letter queue are the same
// bucket), so callers assert on c.bucket directly rather than filtering here.
func (f *fakeChannelS3Writer) allCalls() []fakeChannelPutCall {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]fakeChannelPutCall, len(f.calls))
	copy(out, f.calls)

	return out
}

func mustGunzip(t *testing.T, data []byte) []byte {
	t.Helper()

	r, err := gzip.NewReader(bytes.NewReader(data))
	require.NoError(t, err)

	out, err := io.ReadAll(r)
	require.NoError(t, err)

	return out
}

// TestChannelDelivery_PutRecordToS3 drives CreateStream/CreateChannel/
// PutRecord(s) through the real SDK client, force-flushes via
// InMemoryBackend.FlushChannel (this backend has no injectable clock -- see
// PARITY.md -- so tests never sleep on the interval flusher), and asserts
// the delivered S3 object's bucket/key/body against the documented default
// key template and "no transformation applied" body format
// (docs.aws.amazon.com/streams/latest/dev/data-delivery-s3-key-template.html,
// data-delivery-s3-about.html).
func TestChannelDelivery_PutRecordToS3(t *testing.T) {
	t.Parallel()

	tests := []struct {
		putRecords    func(t *testing.T, client *kinesissdk.Client, streamName string)
		verify        func(t *testing.T, writer *fakeChannelS3Writer)
		name          string
		outputKeyTmpl string
	}{
		{
			name: "single_record_default_key_template_gzip",
			putRecords: func(t *testing.T, client *kinesissdk.Client, streamName string) {
				t.Helper()
				_, err := client.PutRecord(t.Context(), &kinesissdk.PutRecordInput{
					StreamName:   aws.String(streamName),
					PartitionKey: aws.String("pk"),
					Data:         []byte(`{"order":1}`),
				})
				require.NoError(t, err)
			},
			verify: func(t *testing.T, writer *fakeChannelS3Writer) {
				t.Helper()
				calls := writer.allCalls()
				require.Len(t, calls, 1)

				call := calls[0]
				assert.True(t, strings.HasPrefix(call.key, "kinesis-channel/chan-1/"), "key: %s", call.key)
				assert.True(t, strings.HasSuffix(call.key, ".gz"), "key: %s", call.key)
				assert.Equal(t, "gzip", call.contentEncoding)
				assert.Equal(t, `{"order":1}`, string(mustGunzip(t, call.body)))
			},
		},
		{
			name: "batch_records_concatenated_with_no_delimiter",
			putRecords: func(t *testing.T, client *kinesissdk.Client, streamName string) {
				t.Helper()
				_, err := client.PutRecords(t.Context(), &kinesissdk.PutRecordsInput{
					StreamName: aws.String(streamName),
					Records: []kinesissdktypes.PutRecordsRequestEntry{
						{PartitionKey: aws.String("pk1"), Data: []byte(`{"a":1}`)},
						{PartitionKey: aws.String("pk2"), Data: []byte(`{"b":2}`)},
					},
				})
				require.NoError(t, err)
			},
			verify: func(t *testing.T, writer *fakeChannelS3Writer) {
				t.Helper()
				calls := writer.allCalls()
				require.Len(t, calls, 1)
				assert.Equal(t, `{"a":1}{"b":2}`, string(mustGunzip(t, calls[0].body)))
			},
		},
		{
			name:          "custom_output_key_template",
			outputKeyTmpl: "raw/!{stream-name}/!{yyyy}/!{MM}/!{dd}!{extension}",
			putRecords: func(t *testing.T, client *kinesissdk.Client, streamName string) {
				t.Helper()
				_, err := client.PutRecord(t.Context(), &kinesissdk.PutRecordInput{
					StreamName:   aws.String(streamName),
					PartitionKey: aws.String("pk"),
					Data:         []byte(`{"order":2}`),
				})
				require.NoError(t, err)
			},
			verify: func(t *testing.T, writer *fakeChannelS3Writer) {
				t.Helper()
				calls := writer.allCalls()
				require.Len(t, calls, 1)
				assert.Contains(t, calls[0].key, "/2026/", "key: %s", calls[0].key)
				assert.True(t, strings.HasSuffix(calls[0].key, ".gz"), "key: %s", calls[0].key)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := kinesis.NewInMemoryBackend()
			writer := &fakeChannelS3Writer{}
			backend.SetS3Writer(writer)
			client := newTestKinesisClient(t, kinesis.NewHandler(backend))

			streamName := "delivery-stream-" + tt.name
			streamARN := createOnDemandStream(t, client, streamName)

			s3Dest := minimalS3DestinationConfig()
			s3Dest.StorageConfiguration.OutputKeyTemplate = aws.String(tt.outputKeyTmpl)
			if tt.outputKeyTmpl == "" {
				s3Dest.StorageConfiguration.OutputKeyTemplate = nil
			}

			created, err := client.CreateChannel(t.Context(), &kinesissdk.CreateChannelInput{
				ChannelName:                aws.String("chan-1"),
				ServiceExecutionRoleARN:    aws.String(channelTestServiceRoleARN),
				StreamConfigurationList:    minimalStreamConfigList(streamARN),
				S3DestinationConfiguration: s3Dest,
			})
			require.NoError(t, err)
			channelARN := aws.ToString(created.ChannelDescription.ChannelARN)

			tt.putRecords(t, client, streamName)

			backend.FlushChannel(t.Context(), channelARN)

			tt.verify(t, writer)
		})
	}
}

// TestChannelDelivery_InvalidRecordGoesToDeadLetterQueue exercises S3
// delivery's documented per-record validation: a RecordFormatType STRING
// record that fails UTF-8 validation is routed to the dead-letter queue
// instead of the main destination (data-delivery-s3-about.html's "Validate"
// step). No DeadLetterQueueS3Configuration is set, so it must default to
// the main bucket under the disclosed default error prefix.
func TestChannelDelivery_InvalidRecordGoesToDeadLetterQueue(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	writer := &fakeChannelS3Writer{}
	backend.SetS3Writer(writer)
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	streamARN := createOnDemandStream(t, client, "dlq-stream")

	s3Dest := minimalS3DestinationConfig()
	streamCfg := []kinesissdktypes.ChannelStreamConfiguration{
		{
			StreamARN: aws.String(streamARN),
			RecordConfiguration: &kinesissdktypes.RecordConfiguration{
				RecordFormatType: kinesissdktypes.RecordFormatTypeString,
			},
		},
	}

	created, err := client.CreateChannel(t.Context(), &kinesissdk.CreateChannelInput{
		ChannelName:                aws.String("dlq-chan"),
		ServiceExecutionRoleARN:    aws.String(channelTestServiceRoleARN),
		StreamConfigurationList:    streamCfg,
		S3DestinationConfiguration: s3Dest,
	})
	require.NoError(t, err)
	channelARN := aws.ToString(created.ChannelDescription.ChannelARN)

	_, err = client.PutRecord(t.Context(), &kinesissdk.PutRecordInput{
		StreamName:   aws.String("dlq-stream"),
		PartitionKey: aws.String("pk"),
		Data:         []byte{0xff, 0xfe, 0xfd},
	})
	require.NoError(t, err)

	backend.FlushChannel(t.Context(), channelARN)

	mainCalls := writer.allCalls()
	require.Len(t, mainCalls, 1, "invalid record must not land in the main destination object")
	assert.Contains(t, mainCalls[0].key, "kinesis-channel-errors/", "key: %s", mainCalls[0].key)
	assert.Contains(t, string(mainCalls[0].body), `"streamARN"`)
	assert.Contains(t, string(mainCalls[0].body), `"errorMessage"`)
}

// TestChannelDelivery_DeleteChannelFlushesBuffer regression-tests that
// buffered-but-not-yet-flushed records are delivered when the channel is
// deleted, rather than silently discarded (see channels.go's DeleteChannel).
func TestChannelDelivery_DeleteChannelFlushesBuffer(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	writer := &fakeChannelS3Writer{}
	backend.SetS3Writer(writer)
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	streamARN := createOnDemandStream(t, client, "delete-flush-stream")

	created, err := client.CreateChannel(t.Context(), &kinesissdk.CreateChannelInput{
		ChannelName:                aws.String("delete-flush-chan"),
		ServiceExecutionRoleARN:    aws.String(channelTestServiceRoleARN),
		StreamConfigurationList:    minimalStreamConfigList(streamARN),
		S3DestinationConfiguration: minimalS3DestinationConfig(),
	})
	require.NoError(t, err)
	channelARN := aws.ToString(created.ChannelDescription.ChannelARN)

	_, err = client.PutRecord(t.Context(), &kinesissdk.PutRecordInput{
		StreamName:   aws.String("delete-flush-stream"),
		PartitionKey: aws.String("pk"),
		Data:         []byte(`{"never":"flushed-manually"}`),
	})
	require.NoError(t, err)

	_, err = client.DeleteChannel(t.Context(), &kinesissdk.DeleteChannelInput{ChannelARN: aws.String(channelARN)})
	require.NoError(t, err)

	calls := writer.allCalls()
	require.Len(t, calls, 1, "DeleteChannel must flush buffered records before removing the channel")
	assert.JSONEq(t, `{"never":"flushed-manually"}`, string(mustGunzip(t, calls[0].body)))
}

// TestChannelDelivery_NoWriterWiredIsNoop covers the honest fallback when no
// S3Writer has been wired (production wires it via cli.go's
// wireKinesisS3Delivery; a backend constructed directly in a test, as most
// of this package's tests do, has none): PutRecord and FlushChannel must
// not panic or error, they simply do not deliver.
func TestChannelDelivery_NoWriterWiredIsNoop(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	streamARN := createOnDemandStream(t, client, "no-writer-stream")

	created, err := client.CreateChannel(t.Context(), &kinesissdk.CreateChannelInput{
		ChannelName:                aws.String("no-writer-chan"),
		ServiceExecutionRoleARN:    aws.String(channelTestServiceRoleARN),
		StreamConfigurationList:    minimalStreamConfigList(streamARN),
		S3DestinationConfiguration: minimalS3DestinationConfig(),
	})
	require.NoError(t, err)
	channelARN := aws.ToString(created.ChannelDescription.ChannelARN)

	_, err = client.PutRecord(t.Context(), &kinesissdk.PutRecordInput{
		StreamName:   aws.String("no-writer-stream"),
		PartitionKey: aws.String("pk"),
		Data:         []byte("hello"),
	})
	require.NoError(t, err)

	require.NotPanics(t, func() { backend.FlushChannel(t.Context(), channelARN) })
	require.NotPanics(t, func() { backend.FlushAllChannels(t.Context()) })
}
