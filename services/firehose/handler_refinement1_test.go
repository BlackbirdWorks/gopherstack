package firehose_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "s1"})
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "s2"})

	h.Reset()

	rec := doFirehoseRequest(t, h, "ListDeliveryStreams", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"DeliveryStreamNames":[]`)
}

func TestHandler_OpsLen(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	assert.Equal(t, 12, firehose.HandlerOpsLen(h))
}

func TestBackend_StreamCount(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, 0, firehose.StreamCount(b))

	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "s1"})
	require.NoError(t, err)
	assert.Equal(t, 1, firehose.StreamCount(b))

	b.Reset()
	assert.Equal(t, 0, firehose.StreamCount(b))
}

func TestBackend_AddStreamInternal(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	s := &firehose.DeliveryStream{
		Name:   "injected",
		ARN:    "arn:aws:firehose:us-east-1:000000000000:deliverystream/injected",
		Status: "ACTIVE",
	}
	b.AddStreamInternal(s)

	got, err := b.DescribeDeliveryStream(context.TODO(), "injected")
	require.NoError(t, err)
	assert.Equal(t, "injected", got.Name)
}

func TestCreateDeliveryStream_EmptyName(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "   "})
	require.Error(t, err)
	assert.ErrorIs(t, err, firehose.ErrValidation)
}

func TestCreateDeliveryStream_SetsVersionAndType(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "typed-stream"})
	require.NoError(t, err)

	s, err := b.DescribeDeliveryStream(context.TODO(), "typed-stream")
	require.NoError(t, err)
	assert.Equal(t, "DirectPut", s.DeliveryStreamType)
	assert.Equal(t, "1", s.VersionID)
}

func TestCreateDeliveryStream_SetsDestinationID(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
		Name: "dest-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::my-bucket",
		},
	})
	require.NoError(t, err)

	s, err := b.DescribeDeliveryStream(context.TODO(), "dest-stream")
	require.NoError(t, err)
	require.NotNil(t, s.S3Destination)
	assert.Equal(t, "destinationId-000000000001", s.S3Destination.DestinationID)
}

func TestUpdateDestination_VersionCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		currentVersionID string
		wantErr          bool
	}{
		{
			name:             "matching_version",
			currentVersionID: "1",
			wantErr:          false,
		},
		{
			name:             "empty_version_skips_check",
			currentVersionID: "",
			wantErr:          false,
		},
		{
			name:             "mismatched_version",
			currentVersionID: "99",
			wantErr:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "ver-stream"})
			require.NoError(t, err)

			err = b.UpdateDestination(
				context.TODO(),
				"ver-stream",
				tt.currentVersionID,
				firehose.UpdateDestinationInput{
					S3Destination: &firehose.S3DestinationDescription{
						BucketARN: "arn:aws:s3:::new-bucket",
					},
				},
			)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, firehose.ErrValidation)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestUpdateDestination_IncrementsVersionID(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "ver-inc"})
	require.NoError(t, err)

	err = b.UpdateDestination(context.TODO(), "ver-inc", "1", firehose.UpdateDestinationInput{
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::bucket",
		},
	})
	require.NoError(t, err)

	s, err := b.DescribeDeliveryStream(context.TODO(), "ver-inc")
	require.NoError(t, err)
	assert.Equal(t, "2", s.VersionID)
}

func TestDescribeDeliveryStream_ReturnsEncryption(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "enc-stream"})
	doFirehoseRequest(t, h, "StartDeliveryStreamEncryption", map[string]any{
		"DeliveryStreamName": "enc-stream",
	})

	rec := doFirehoseRequest(t, h, "DescribeDeliveryStream", map[string]any{
		"DeliveryStreamName": "enc-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"EncryptionConfiguration"`)
	assert.Contains(t, rec.Body.String(), `"ENABLED"`)
}

func TestDescribeDeliveryStream_ReturnsVersionAndType(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "vt-stream"})

	rec := doFirehoseRequest(t, h, "DescribeDeliveryStream", map[string]any{
		"DeliveryStreamName": "vt-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"DeliveryStreamType":"DirectPut"`)
	assert.Contains(t, rec.Body.String(), `"VersionId":"1"`)
}

func TestListTagsForDeliveryStream_Sorted(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "tagged-stream"})
	doFirehoseRequest(t, h, "TagDeliveryStream", map[string]any{
		"DeliveryStreamName": "tagged-stream",
		"Tags": []map[string]any{
			{"Key": "zebra", "Value": "z"},
			{"Key": "alpha", "Value": "a"},
			{"Key": "middle", "Value": "m"},
		},
	})

	rec := doFirehoseRequest(t, h, "ListTagsForDeliveryStream", map[string]any{
		"DeliveryStreamName": "tagged-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	alphaIdx := indexOf(body, "alpha")
	middleIdx := indexOf(body, "middle")
	zebraIdx := indexOf(body, "zebra")

	assert.Less(t, alphaIdx, middleIdx, "alpha should come before middle")
	assert.Less(t, middleIdx, zebraIdx, "middle should come before zebra")
}

func TestCreateDeliveryStream_WithTags(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "tagged-create",
		"Tags": []map[string]any{
			{"Key": "env", "Value": "test"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	tagRec := doFirehoseRequest(t, h, "ListTagsForDeliveryStream", map[string]any{
		"DeliveryStreamName": "tagged-create",
	})
	require.Equal(t, http.StatusOK, tagRec.Code)
	assert.Contains(t, tagRec.Body.String(), "env")
	assert.Contains(t, tagRec.Body.String(), "test")
}

func TestCreateDeliveryStream_EmptyName_Handler(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": ""})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestProvider_Init_NilCtx(t *testing.T) {
	t.Parallel()

	p := &firehose.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, firehose.ErrNilAppContext)
}

func TestPutRecord_ReturnsNonEmptyRecordID(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "rec-stream"})

	rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
		"DeliveryStreamName": "rec-stream",
		"Record":             map[string]any{"Data": "aGVsbG8="},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "RecordId")
	assert.NotContains(t, rec.Body.String(), "stub-record-id")
}

func TestHandlerUpdateDestination_VersionMismatch(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "ver-mismatch"})

	rec := doFirehoseRequest(t, h, "UpdateDestination", map[string]any{
		"DeliveryStreamName":             "ver-mismatch",
		"CurrentDeliveryStreamVersionId": "99",
		"DestinationId":                  "destinationId-000000000001",
		"S3DestinationUpdate": map[string]any{
			"BucketARN": "arn:aws:s3:::new-bucket",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// indexOf returns the byte offset of substr in s, or -1 if not found.
func indexOf(s, substr string) int {
	if len(substr) > len(s) {
		return -1
	}

	for i := range len(s) - len(substr) + 1 {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}

	return -1
}
