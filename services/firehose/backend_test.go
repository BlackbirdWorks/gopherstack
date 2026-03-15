package firehose_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

var errLambdaUnavailable = errors.New("lambda unavailable")

func TestCreateDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		setup      func(b *firehose.InMemoryBackend)
		name       string
		streamName string
	}{
		{
			name:       "success",
			streamName: "my-stream",
		},
		{
			name:       "already_exists",
			streamName: "my-stream",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{Name: "my-stream"})
			},
			wantErr: firehose.ErrAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			s, err := b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{Name: tt.streamName})
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.streamName, s.Name)
			assert.Equal(t, "ACTIVE", s.Status)
			assert.Contains(t, s.ARN, "arn:aws:firehose:")
		})
	}
}

func TestDeleteDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		setup      func(b *firehose.InMemoryBackend)
		name       string
		streamName string
	}{
		{
			name:       "success",
			streamName: "my-stream",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{Name: "my-stream"})
			},
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			wantErr:    firehose.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			err := b.DeleteDeliveryStream(tt.streamName)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			names := b.ListDeliveryStreams()
			assert.Empty(t, names)
		})
	}
}

func TestDescribeDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		setup      func(b *firehose.InMemoryBackend)
		name       string
		streamName string
	}{
		{
			name:       "success",
			streamName: "my-stream",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{Name: "my-stream"})
			},
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			wantErr:    firehose.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			s, err := b.DescribeDeliveryStream(tt.streamName)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.streamName, s.Name)
		})
	}
}

func TestPutRecord(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{Name: "my-stream"})

	err := b.PutRecord("my-stream", []byte("hello world"))
	require.NoError(t, err)
}

func TestPutRecordBatch(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{Name: "my-stream"})

	failed, err := b.PutRecordBatch("my-stream", [][]byte{[]byte("a"), []byte("b")})
	require.NoError(t, err)
	assert.Equal(t, 0, failed)
}

func TestListDeliveryStreams(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{Name: "s1"})
	_, _ = b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{Name: "s2"})

	names := b.ListDeliveryStreams()
	assert.Len(t, names, 2)
}

func TestTagDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		setup      func(b *firehose.InMemoryBackend)
		tags       map[string]string
		wantTags   map[string]string
		name       string
		streamName string
	}{
		{
			name:       "success",
			streamName: "tagged-stream",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{Name: "tagged-stream"})
			},
			tags:     map[string]string{"env": "prod", "team": "platform"},
			wantTags: map[string]string{"env": "prod", "team": "platform"},
		},
		{
			name:       "overwrite",
			streamName: "overwrite-stream",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{Name: "overwrite-stream"})
				_ = b.TagDeliveryStream("overwrite-stream", map[string]string{"env": "dev"})
			},
			tags:     map[string]string{"env": "prod"},
			wantTags: map[string]string{"env": "prod"},
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			tags:       map[string]string{"k": "v"},
			wantErr:    firehose.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			err := b.TagDeliveryStream(tt.streamName, tt.tags)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			tags, err := b.ListTagsForDeliveryStream(tt.streamName)
			require.NoError(t, err)
			for k, v := range tt.wantTags {
				assert.Equal(t, v, tags[k])
			}
		})
	}
}

func TestUntagDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr        error
		setup          func(b *firehose.InMemoryBackend)
		wantTags       map[string]string
		name           string
		streamName     string
		keysToRemove   []string
		wantAbsentKeys []string
	}{
		{
			name:       "success",
			streamName: "untag-stream",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{Name: "untag-stream"})
				_ = b.TagDeliveryStream("untag-stream", map[string]string{"env": "prod", "team": "platform"})
			},
			keysToRemove:   []string{"env"},
			wantAbsentKeys: []string{"env"},
			wantTags:       map[string]string{"team": "platform"},
		},
		{
			name:         "not_found",
			streamName:   "nonexistent",
			keysToRemove: []string{"k"},
			wantErr:      firehose.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			err := b.UntagDeliveryStream(tt.streamName, tt.keysToRemove)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			tags, err := b.ListTagsForDeliveryStream(tt.streamName)
			require.NoError(t, err)
			for _, k := range tt.wantAbsentKeys {
				assert.NotContains(t, tags, k)
			}
			for k, v := range tt.wantTags {
				assert.Equal(t, v, tags[k])
			}
		})
	}
}

func TestListTagsForDeliveryStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		setup      func(b *firehose.InMemoryBackend)
		name       string
		streamName string
		wantEmpty  bool
	}{
		{
			name:       "not_found",
			streamName: "nonexistent",
			wantErr:    firehose.ErrNotFound,
		},
		{
			name:       "empty",
			streamName: "empty-tags",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{Name: "empty-tags"})
			},
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			tags, err := b.ListTagsForDeliveryStream(tt.streamName)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			if tt.wantEmpty {
				assert.Empty(t, tags)
			}
		})
	}
}

// mockS3Storer captures PutObject calls for assertions.
type mockS3Storer struct {
	calls []*mockS3PutCall
}

type mockS3PutCall struct {
	bucket string
	key    string
	body   []byte
}

func (m *mockS3Storer) PutObject(
	_ context.Context,
	input *s3sdk.PutObjectInput,
) (*s3sdk.PutObjectOutput, error) {
	body, _ := io.ReadAll(input.Body)
	m.calls = append(m.calls, &mockS3PutCall{
		bucket: aws.ToString(input.Bucket),
		key:    aws.ToString(input.Key),
		body:   body,
	})

	return &s3sdk.PutObjectOutput{}, nil
}

// mockLambdaInvoker simulates Lambda transformation responses.
type mockLambdaInvoker struct {
	err      error
	response []byte
}

func (m *mockLambdaInvoker) InvokeFunction(
	_ context.Context,
	_ string,
	_ string,
	_ []byte,
) ([]byte, int, error) {
	return m.response, 200, m.err
}

func TestS3Delivery_SizeBasedFlush(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}
	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetS3Backend(s3mock)

	_, err := b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{
		Name: "flush-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::my-bucket",
			BufferingHints: &firehose.BufferingHints{
				SizeInMBs:         1,
				IntervalInSeconds: 300,
			},
		},
	})
	require.NoError(t, err)

	// Write just under 1 MB — should not flush yet.
	underLimit := make([]byte, 900*1024)
	require.NoError(t, b.PutRecord("flush-stream", underLimit))
	assert.Empty(t, s3mock.calls, "should not flush before size limit")

	// Push over 1 MB — should trigger a flush.
	overLimit := make([]byte, 200*1024)
	require.NoError(t, b.PutRecord("flush-stream", overLimit))
	assert.Len(t, s3mock.calls, 1, "should have flushed to S3")
	assert.Equal(t, "my-bucket", s3mock.calls[0].bucket)
}

func TestS3Delivery_FlushAll(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}
	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetS3Backend(s3mock)

	_, err := b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{
		Name: "flush-all-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::flush-bucket",
		},
	})
	require.NoError(t, err)

	require.NoError(t, b.PutRecord("flush-all-stream", []byte("hello")))
	b.FlushAll(t.Context())

	require.Len(t, s3mock.calls, 1)
	assert.Equal(t, "flush-bucket", s3mock.calls[0].bucket)
	assert.Contains(t, string(s3mock.calls[0].body), "hello")
}

func TestS3Delivery_GzipCompression(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}
	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetS3Backend(s3mock)

	_, err := b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{
		Name: "gzip-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN:         "arn:aws:s3:::gzip-bucket",
			CompressionFormat: "GZIP",
		},
	})
	require.NoError(t, err)

	require.NoError(t, b.PutRecord("gzip-stream", []byte("compressed content")))
	b.FlushAll(t.Context())

	require.Len(t, s3mock.calls, 1)
	// GZIP magic bytes.
	assert.Equal(t, []byte{0x1f, 0x8b}, s3mock.calls[0].body[:2])
}

func TestS3Delivery_NoS3Backend(t *testing.T) {
	t.Parallel()

	// Without S3 backend, records are buffered but no delivery is attempted.
	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{
		Name: "no-s3-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::bucket",
			BufferingHints: &firehose.BufferingHints{
				SizeInMBs: 1,
			},
		},
	})
	require.NoError(t, err)

	overLimit := make([]byte, 2*1024*1024)
	require.NoError(t, b.PutRecord("no-s3-stream", overLimit))
}

func TestLambdaTransformation_OkRecordsDelivered(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}
	lambdaResponse := `{"records":[` +
		`{"recordId":"r1","result":"Ok","data":"aGVsbG8="},` +
		`{"recordId":"r2","result":"Dropped","data":""},` +
		`{"recordId":"r3","result":"ProcessingFailed","data":""}` +
		`]}`
	lambdaMock := &mockLambdaInvoker{response: []byte(lambdaResponse)}

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetS3Backend(s3mock)
	b.SetLambdaBackend(lambdaMock)

	_, err := b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{
		Name: "lambda-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::transform-bucket",
			ProcessingConfiguration: &firehose.ProcessingConfiguration{
				Enabled: true,
				Processors: []firehose.Processor{
					{
						Type: "Lambda",
						Parameters: []firehose.ProcessorParameter{
							{ParameterName: "LambdaArn", ParameterValue: "my-transform-fn"},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	require.NoError(t, b.PutRecord("lambda-stream", []byte("input")))
	b.FlushAll(t.Context())

	require.Len(t, s3mock.calls, 1)
	// Only "Ok" record data ("hello" from base64 "aGVsbG8=") should be delivered.
	assert.Contains(t, string(s3mock.calls[0].body), "hello")
}

func TestLambdaTransformation_AllDropped(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}
	lambdaResponse := `{"records":[{"recordId":"r1","result":"Dropped","data":""}]}`
	lambdaMock := &mockLambdaInvoker{response: []byte(lambdaResponse)}

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetS3Backend(s3mock)
	b.SetLambdaBackend(lambdaMock)

	_, err := b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{
		Name: "drop-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::drop-bucket",
			ProcessingConfiguration: &firehose.ProcessingConfiguration{
				Enabled: true,
				Processors: []firehose.Processor{
					{
						Type: "Lambda",
						Parameters: []firehose.ProcessorParameter{
							{ParameterName: "LambdaArn", ParameterValue: "drop-fn"},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	require.NoError(t, b.PutRecord("drop-stream", []byte("input")))
	b.FlushAll(t.Context())

	// All records dropped → no S3 delivery.
	assert.Empty(t, s3mock.calls)
}

// TestDeliverToS3_EmptyRecord verifies that empty records do not cause a panic
// and are silently skipped during S3 delivery (bug fix: rec[len(rec)-1] panic).
func TestDeliverToS3_EmptyRecord(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}
	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetS3Backend(s3mock)

	_, err := b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{
		Name: "empty-rec-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::empty-bucket",
		},
	})
	require.NoError(t, err)

	// Put an empty record followed by a non-empty one — must not panic.
	require.NoError(t, b.PutRecord("empty-rec-stream", []byte{}))
	require.NoError(t, b.PutRecord("empty-rec-stream", []byte("data")))
	b.FlushAll(t.Context())

	// The non-empty record is delivered; empty records are skipped.
	require.Len(t, s3mock.calls, 1)
	assert.Contains(t, string(s3mock.calls[0].body), "data")
}

// TestDeliverToS3_AllEmptyRecords verifies that if every record in a flush is empty,
// no S3 PutObject call is made (avoids writing empty objects).
func TestDeliverToS3_AllEmptyRecords(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}
	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetS3Backend(s3mock)

	_, err := b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{
		Name: "all-empty-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::empty-bucket",
		},
	})
	require.NoError(t, err)

	require.NoError(t, b.PutRecord("all-empty-stream", []byte{}))
	require.NoError(t, b.PutRecord("all-empty-stream", []byte{}))
	b.FlushAll(t.Context())

	// All records empty → no S3 delivery.
	assert.Empty(t, s3mock.calls)
}

// TestDeleteDeliveryStream_ClosesTags verifies that Tags resources are released when a
// stream is deleted, preventing Prometheus registry leaks.
func TestDeleteDeliveryStream_ClosesTags(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{
		Name: "tag-leak-stream",
	})
	require.NoError(t, err)

	// Tag the stream so the Tags collection is active.
	require.NoError(t, b.TagDeliveryStream("tag-leak-stream", map[string]string{"env": "test"}))

	// Delete must succeed without panicking; tags are closed internally.
	require.NoError(t, b.DeleteDeliveryStream("tag-leak-stream"))

	// Subsequent lookup must return not-found.
	_, descErr := b.DescribeDeliveryStream("tag-leak-stream")
	require.Error(t, descErr)
	assert.ErrorIs(t, descErr, firehose.ErrNotFound)
}

// TestLambdaTransformation_ErrorDropsRecords verifies that a Lambda invocation error
// causes the records to be dropped (not silently delivered as originals).
func TestLambdaTransformation_ErrorDropsRecords(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}
	lambdaMock := &mockLambdaInvoker{err: errLambdaUnavailable}

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetS3Backend(s3mock)
	b.SetLambdaBackend(lambdaMock)

	_, err := b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{
		Name: "err-lambda-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::err-bucket",
			ProcessingConfiguration: &firehose.ProcessingConfiguration{
				Enabled: true,
				Processors: []firehose.Processor{
					{
						Type: "Lambda",
						Parameters: []firehose.ProcessorParameter{
							{ParameterName: "LambdaArn", ParameterValue: "my-fn"},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	require.NoError(t, b.PutRecord("err-lambda-stream", []byte("input")))
	b.FlushAll(t.Context())

	// Lambda error → records must not be delivered to S3.
	assert.Empty(t, s3mock.calls)
}

// TestPutRecord_FlushSnapshotUnderLock verifies that after a size-based flush the
// buffer is reset atomically: a subsequent PutRecord starts with a zeroed counter and
// the old records are not double-delivered.
func TestPutRecord_FlushSnapshotUnderLock(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}
	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetS3Backend(s3mock)

	_, err := b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{
		Name: "atomic-flush-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::atomic-bucket",
			BufferingHints: &firehose.BufferingHints{
				SizeInMBs:         1,
				IntervalInSeconds: 300,
			},
		},
	})
	require.NoError(t, err)

	// Write exactly enough to trigger one flush.
	overLimit := make([]byte, 2*1024*1024)
	require.NoError(t, b.PutRecord("atomic-flush-stream", overLimit))

	// After the flush, the buffer is zeroed; a small subsequent record should not
	// trigger another flush automatically.
	require.NoError(t, b.PutRecord("atomic-flush-stream", []byte("small")))

	// Only one S3 delivery should have occurred (from the over-limit put).
	assert.Len(t, s3mock.calls, 1)
}

func TestUpdateDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		setup      func(b *firehose.InMemoryBackend)
		newDest    *firehose.S3DestinationDescription
		name       string
		streamName string
	}{
		{
			name:       "success",
			streamName: "update-stream",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{Name: "update-stream"})
			},
			newDest: &firehose.S3DestinationDescription{BucketARN: "arn:aws:s3:::new-bucket"},
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			wantErr:    firehose.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			err := b.UpdateDestination(tt.streamName, tt.newDest)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			s, descErr := b.DescribeDeliveryStream(tt.streamName)
			require.NoError(t, descErr)
			require.NotNil(t, s.S3Destination)
			assert.Equal(t, "arn:aws:s3:::new-bucket", s.S3Destination.BucketARN)
		})
	}
}
