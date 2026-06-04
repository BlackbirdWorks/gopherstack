package firehose_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- DeleteDeliveryStream ---

func TestAccuracy_DeleteDeliveryStream_Success(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "del-stream")

	rec := doFirehoseRequest(t, h, "DeleteDeliveryStream",
		map[string]any{"DeliveryStreamName": "del-stream"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Stream no longer appears in list.
	list := doFirehoseRequest(t, h, "ListDeliveryStreams", map[string]any{})
	require.Equal(t, http.StatusOK, list.Code)
	assert.NotContains(t, list.Body.String(), "del-stream")
}

func TestAccuracy_DeleteDeliveryStream_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "DeleteDeliveryStream",
		map[string]any{"DeliveryStreamName": "no-such-stream"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- DescribeDeliveryStream not-found ---

func TestAccuracy_DescribeDeliveryStream_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "no-such-stream"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- PutRecord success ---

func TestAccuracy_PutRecord_Success_ReturnsRecordId(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "put-stream")

	rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
		"DeliveryStreamName": "put-stream",
		"Record":             map[string]any{"Data": "aGVsbG8="},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		RecordID string `json:"RecordId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.RecordID)
}

func TestAccuracy_PutRecord_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
		"DeliveryStreamName": "no-such-stream",
		"Record":             map[string]any{"Data": "aGVsbG8="},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- PutRecordBatch success ---

func TestAccuracy_PutRecordBatch_Success_ResponseShape(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "batch-stream")

	rec := doFirehoseRequest(t, h, "PutRecordBatch", map[string]any{
		"DeliveryStreamName": "batch-stream",
		"Records": []map[string]any{
			{"Data": "aGVsbG8="},
			{"Data": "d29ybGQ="},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		FailedPutCount   int           `json:"FailedPutCount"`
		RequestResponses []interface{} `json:"RequestResponses"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, 0, out.FailedPutCount)
	assert.NotNil(t, out.RequestResponses)
}

// --- UntagDeliveryStream ---

func TestAccuracy_UntagDeliveryStream_RemovesSpecificKeys(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "untag-stream")

	doFirehoseRequest(t, h, "TagDeliveryStream", map[string]any{
		"DeliveryStreamName": "untag-stream",
		"Tags": []map[string]string{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "platform"},
			{"Key": "version", "Value": "1"},
		},
	})

	rec := doFirehoseRequest(t, h, "UntagDeliveryStream", map[string]any{
		"DeliveryStreamName": "untag-stream",
		"TagKeys":            []string{"env", "version"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	list := doFirehoseRequest(t, h, "ListTagsForDeliveryStream",
		map[string]any{"DeliveryStreamName": "untag-stream"})
	require.Equal(t, http.StatusOK, list.Code)

	var out struct {
		Tags []map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(list.Body.Bytes(), &out))
	require.Len(t, out.Tags, 1)
	assert.Equal(t, "team", out.Tags[0]["Key"])
}

func TestAccuracy_UntagDeliveryStream_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "UntagDeliveryStream", map[string]any{
		"DeliveryStreamName": "no-such-stream",
		"TagKeys":            []string{"k"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- UpdateDestination ---

func TestAccuracy_UpdateDestination_Success_VersionIncrements(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "upd-stream", map[string]any{
		"S3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::original-bucket",
			"RoleARN":   "arn:aws:iam::000000000000:role/r",
		},
	})

	desc := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "upd-stream"})
	require.Equal(t, http.StatusOK, desc.Code)

	var descOut struct {
		DeliveryStreamDescription struct {
			VersionId               string `json:"VersionId"`
			S3DestinationDescriptions []struct {
				DestinationId string `json:"DestinationId"`
			} `json:"S3DestinationDescriptions"`
		} `json:"DeliveryStreamDescription"`
	}
	require.NoError(t, json.Unmarshal(desc.Body.Bytes(), &descOut))
	versionBefore := descOut.DeliveryStreamDescription.VersionId

	rec := doFirehoseRequest(t, h, "UpdateDestination", map[string]any{
		"DeliveryStreamName":             "upd-stream",
		"CurrentDeliveryStreamVersionId": versionBefore,
		"S3DestinationUpdate": map[string]any{
			"BucketARN": "arn:aws:s3:::updated-bucket",
			"RoleARN":   "arn:aws:iam::000000000000:role/r",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	desc2 := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "upd-stream"})
	require.Equal(t, http.StatusOK, desc2.Code)

	var descOut2 struct {
		DeliveryStreamDescription struct {
			VersionId                 string `json:"VersionId"`
			S3DestinationDescriptions []struct {
				BucketARN string `json:"BucketARN"`
			} `json:"S3DestinationDescriptions"`
		} `json:"DeliveryStreamDescription"`
	}
	require.NoError(t, json.Unmarshal(desc2.Body.Bytes(), &descOut2))
	assert.NotEqual(t, versionBefore, descOut2.DeliveryStreamDescription.VersionId)
	require.Len(t, descOut2.DeliveryStreamDescription.S3DestinationDescriptions, 1)
	assert.Equal(t, "arn:aws:s3:::updated-bucket",
		descOut2.DeliveryStreamDescription.S3DestinationDescriptions[0].BucketARN)
}

func TestAccuracy_UpdateDestination_VersionMismatch(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "ver-stream")

	rec := doFirehoseRequest(t, h, "UpdateDestination", map[string]any{
		"DeliveryStreamName":             "ver-stream",
		"CurrentDeliveryStreamVersionId": "999",
		"S3DestinationUpdate": map[string]any{
			"BucketARN": "arn:aws:s3:::new-bucket",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- StopDeliveryStreamEncryption ---

func TestAccuracy_StopDeliveryStreamEncryption_DisablesEncryption(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "enc-stop-stream")

	doFirehoseRequest(t, h, "StartDeliveryStreamEncryption",
		map[string]any{"DeliveryStreamName": "enc-stop-stream"})

	rec := doFirehoseRequest(t, h, "StopDeliveryStreamEncryption",
		map[string]any{"DeliveryStreamName": "enc-stop-stream"})
	assert.Equal(t, http.StatusOK, rec.Code)

	desc := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "enc-stop-stream"})
	require.Equal(t, http.StatusOK, desc.Code)
	assert.Contains(t, desc.Body.String(), "DISABLED")
}

func TestAccuracy_StopDeliveryStreamEncryption_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "StopDeliveryStreamEncryption",
		map[string]any{"DeliveryStreamName": "no-such-stream"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- StartDeliveryStreamEncryption: status in Describe ---

func TestAccuracy_StartEncryption_StatusAppearsInDescribe(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "enc-desc-stream")

	rec := doFirehoseRequest(t, h, "StartDeliveryStreamEncryption", map[string]any{
		"DeliveryStreamName": "enc-desc-stream",
		"DeliveryStreamEncryptionConfigurationInput": map[string]any{
			"KeyType": "AWS_OWNED_CMK",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	desc := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "enc-desc-stream"})
	require.Equal(t, http.StatusOK, desc.Code)

	body := desc.Body.String()
	assert.Contains(t, body, "EncryptionConfiguration")
	assert.Contains(t, body, "ENABLED")
	assert.Contains(t, body, "AWS_OWNED_CMK")
}

// --- ListDeliveryStreams: empty result ---

func TestAccuracy_ListDeliveryStreams_Empty(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "ListDeliveryStreams", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		DeliveryStreamNames    []string `json:"DeliveryStreamNames"`
		HasMoreDeliveryStreams bool     `json:"HasMoreDeliveryStreams"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out.DeliveryStreamNames)
	assert.False(t, out.HasMoreDeliveryStreams)
}

// --- MSKSourceDescription fields in Describe ---

func TestAccuracy_DescribeDeliveryStream_MSKSourceDescription_Fields(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "msk-desc-stream", map[string]any{
		"DeliveryStreamType": "MSKAsSource",
		"MSKSourceConfiguration": map[string]any{
			"MSKClusterARN": "arn:aws:kafka:us-east-1:000000000000:cluster/my-cluster/abc",
			"TopicName":     "events-topic",
			"AuthenticationConfiguration": map[string]any{
				"Connectivity": "PRIVATE",
				"RoleARN":      "arn:aws:iam::000000000000:role/msk-reader",
			},
		},
	})

	desc := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "msk-desc-stream"})
	require.Equal(t, http.StatusOK, desc.Code)

	body := desc.Body.String()
	assert.Contains(t, body, "MSKSourceDescription")
	assert.Contains(t, body, "events-topic")
	assert.Contains(t, body, "arn:aws:kafka:us-east-1:000000000000:cluster/my-cluster/abc")
	assert.Contains(t, body, "PRIVATE")
}

// --- S3DestinationDescriptions fields preserved ---

func TestAccuracy_DescribeDeliveryStream_S3Destination_FieldsPreserved(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "s3-fields-stream", map[string]any{
		"S3DestinationConfiguration": map[string]any{
			"BucketARN":         "arn:aws:s3:::my-test-bucket",
			"RoleARN":           "arn:aws:iam::000000000000:role/firehose",
			"Prefix":            "logs/",
			"CompressionFormat": "GZIP",
		},
	})

	desc := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "s3-fields-stream"})
	require.Equal(t, http.StatusOK, desc.Code)

	body := desc.Body.String()
	assert.Contains(t, body, "S3DestinationDescriptions")
	assert.Contains(t, body, "arn:aws:s3:::my-test-bucket")
	assert.Contains(t, body, "logs/")
	assert.Contains(t, body, "GZIP")
}

// --- UpdateDestination with ExtendedS3 ---

func TestAccuracy_UpdateDestination_ExtendedS3Update(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "ext-upd-stream", map[string]any{
		"ExtendedS3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::orig-bucket",
			"RoleARN":   "arn:aws:iam::000000000000:role/r",
		},
	})

	rec := doFirehoseRequest(t, h, "UpdateDestination", map[string]any{
		"DeliveryStreamName":             "ext-upd-stream",
		"CurrentDeliveryStreamVersionId": "1",
		"ExtendedS3DestinationUpdate": map[string]any{
			"BucketARN":         "arn:aws:s3:::new-ext-bucket",
			"RoleARN":           "arn:aws:iam::000000000000:role/r",
			"ErrorOutputPrefix": "errors/",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	desc := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "ext-upd-stream"})
	require.Equal(t, http.StatusOK, desc.Code)
	assert.Contains(t, desc.Body.String(), "new-ext-bucket")
	assert.Contains(t, desc.Body.String(), "errors/")
}
