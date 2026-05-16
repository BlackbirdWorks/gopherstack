package firehose_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

// createStream is a helper that creates a delivery stream via the API.
func createStream(t *testing.T, h *firehose.Handler, name string, extra ...map[string]any) {
	t.Helper()

	body := map[string]any{"DeliveryStreamName": name}
	if len(extra) > 0 {
		maps.Copy(body, extra[0])
	}

	rec := doFirehoseRequest(t, h, "CreateDeliveryStream", body)
	require.Equal(t, http.StatusOK, rec.Code, "create stream %q: %s", name, rec.Body.String())
}

// --- Issue #1, #2: DeliveryStreamType and KinesisStreamSourceConfiguration ---

func TestAccuracy_CreateDeliveryStream_DeliveryStreamType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantType string
		wantCode int
	}{
		{
			name:     "default_is_DirectPut",
			body:     map[string]any{"DeliveryStreamName": "s"},
			wantCode: http.StatusOK,
			wantType: "DirectPut",
		},
		{
			name: "explicit_DirectPut",
			body: map[string]any{
				"DeliveryStreamName": "s",
				"DeliveryStreamType": "DirectPut",
			},
			wantCode: http.StatusOK,
			wantType: "DirectPut",
		},
		{
			name: "KinesisStreamAsSource",
			body: map[string]any{
				"DeliveryStreamName": "s",
				"DeliveryStreamType": "KinesisStreamAsSource",
				"KinesisStreamSourceConfiguration": map[string]any{
					"KinesisStreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
					"RoleARN":          "arn:aws:iam::000000000000:role/firehose",
				},
			},
			wantCode: http.StatusOK,
			wantType: "KinesisStreamAsSource",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestFirehoseHandler(t)
			rec := doFirehoseRequest(t, h, "CreateDeliveryStream", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK && tt.wantType != "" {
				// Describe and verify type.
				desc := doFirehoseRequest(t, h, "DescribeDeliveryStream",
					map[string]any{"DeliveryStreamName": tt.body["DeliveryStreamName"]})
				require.Equal(t, http.StatusOK, desc.Code)
				assert.Contains(t, desc.Body.String(), tt.wantType)
			}
		})
	}
}

func TestAccuracy_DescribeDeliveryStream_KinesisSource(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "ks-stream", map[string]any{
		"DeliveryStreamType": "KinesisStreamAsSource",
		"KinesisStreamSourceConfiguration": map[string]any{
			"KinesisStreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/my-ks",
			"RoleARN":          "arn:aws:iam::000000000000:role/r",
		},
	})

	rec := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "ks-stream"})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "KinesisStreamSourceDescription")
	assert.Contains(t, body, "arn:aws:kinesis:us-east-1:000000000000:stream/my-ks")
}

// --- Issue #3: MSKSourceConfiguration ---

func TestAccuracy_CreateDeliveryStream_MSKSource(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "msk-stream",
		"DeliveryStreamType": "MSKAsSource",
		"MSKSourceConfiguration": map[string]any{
			"MSKClusterARN": "arn:aws:kafka:us-east-1:000000000000:cluster/my-cluster/abc",
			"TopicName":     "my-topic",
			"AuthenticationConfiguration": map[string]any{
				"Connectivity": "PUBLIC",
				"RoleARN":      "arn:aws:iam::000000000000:role/msk",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	desc := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "msk-stream"})
	require.Equal(t, http.StatusOK, desc.Code)

	body := desc.Body.String()
	assert.Contains(t, body, "MSKSourceDescription")
	assert.Contains(t, body, "my-topic")
}

// --- Issue #6: CUSTOMER_MANAGED_CMK requires KeyARN ---

func TestAccuracy_StartEncryption_CustomerManagedCMK_MissingKeyARN(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "enc-stream")

	rec := doFirehoseRequest(t, h, "StartDeliveryStreamEncryption", map[string]any{
		"DeliveryStreamName": "enc-stream",
		"DeliveryStreamEncryptionConfigurationInput": map[string]any{
			"KeyType": "CUSTOMER_MANAGED_CMK",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "KeyARN")
}

func TestAccuracy_StartEncryption_CustomerManagedCMK_WithKeyARN(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "enc-stream")

	rec := doFirehoseRequest(t, h, "StartDeliveryStreamEncryption", map[string]any{
		"DeliveryStreamName": "enc-stream",
		"DeliveryStreamEncryptionConfigurationInput": map[string]any{
			"KeyType": "CUSTOMER_MANAGED_CMK",
			"KeyARN":  "arn:aws:kms:us-east-1:000000000000:key/my-key",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- Issue #5: SSE rejected on KinesisStreamAsSource ---

func TestAccuracy_StartEncryption_Rejected_OnKinesisSource(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "ks-enc-stream", map[string]any{
		"DeliveryStreamType": "KinesisStreamAsSource",
		"KinesisStreamSourceConfiguration": map[string]any{
			"KinesisStreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/s",
			"RoleARN":          "arn:aws:iam::000000000000:role/r",
		},
	})

	rec := doFirehoseRequest(t, h, "StartDeliveryStreamEncryption", map[string]any{
		"DeliveryStreamName": "ks-enc-stream",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- Issue #7: ExtendedS3 additional fields preserved ---

func TestAccuracy_ExtendedS3_AdditionalFieldsPreserved(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "ext-s3",
		"ExtendedS3DestinationConfiguration": map[string]any{
			"BucketARN":         "arn:aws:s3:::my-bucket",
			"RoleARN":           "arn:aws:iam::000000000000:role/r",
			"ErrorOutputPrefix": "errors/",
			"FileExtension":     ".json.gz",
			"CustomTimeZone":    "America/New_York",
			"EncryptionConfiguration": map[string]any{
				"KMSEncryptionConfig": map[string]any{
					"AWSKMSKeyARN": "arn:aws:kms:us-east-1:000000000000:key/abc",
				},
			},
			"CloudWatchLoggingOptions": map[string]any{
				"Enabled":       true,
				"LogGroupName":  "/aws/firehose/my-stream",
				"LogStreamName": "errors",
			},
			"DynamicPartitioningConfiguration": map[string]any{
				"Enabled": true,
				"RetryOptions": map[string]any{
					"DurationInSeconds": 300,
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	desc := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "ext-s3"})
	require.Equal(t, http.StatusOK, desc.Code)

	body := desc.Body.String()
	assert.Contains(t, body, "errors/")
	assert.Contains(t, body, ".json.gz")
	assert.Contains(t, body, "America/New_York")
	assert.Contains(t, body, "AWSKMSKeyARN")
	assert.Contains(t, body, "CloudWatchLoggingOptions")
	assert.Contains(t, body, "DynamicPartitioningConfiguration")
}

// --- Issue #25: CreateTimestamp / LastUpdateTimestamp ---

func TestAccuracy_DescribeDeliveryStream_Timestamps(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "ts-stream")

	rec := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "ts-stream"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		DeliveryStreamDescription struct {
			CreateTimestamp     *int64 `json:"CreateTimestamp"`
			LastUpdateTimestamp *int64 `json:"LastUpdateTimestamp"`
		} `json:"DeliveryStreamDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotNil(t, out.DeliveryStreamDescription.CreateTimestamp)
	assert.NotNil(t, out.DeliveryStreamDescription.LastUpdateTimestamp)
	assert.Positive(t, *out.DeliveryStreamDescription.CreateTimestamp)
}

// --- Issue #26: ListDeliveryStreams pagination ---

func TestAccuracy_ListDeliveryStreams_Limit(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	for _, name := range []string{"a-stream", "b-stream", "c-stream"} {
		createStream(t, h, name)
	}

	rec := doFirehoseRequest(t, h, "ListDeliveryStreams", map[string]any{"Limit": 2})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		DeliveryStreamNames    []string `json:"DeliveryStreamNames"`
		HasMoreDeliveryStreams bool     `json:"HasMoreDeliveryStreams"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.DeliveryStreamNames, 2)
	assert.True(t, out.HasMoreDeliveryStreams)
}

func TestAccuracy_ListDeliveryStreams_ExclusiveStart(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	for _, name := range []string{"a-stream", "b-stream", "c-stream"} {
		createStream(t, h, name)
	}

	rec := doFirehoseRequest(t, h, "ListDeliveryStreams",
		map[string]any{"ExclusiveStartDeliveryStreamName": "a-stream"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		DeliveryStreamNames []string `json:"DeliveryStreamNames"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, []string{"b-stream", "c-stream"}, out.DeliveryStreamNames)
}

// --- Issue #27: ListTagsForDeliveryStream pagination ---

func TestAccuracy_ListTagsForDeliveryStream_Limit(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "tagged-stream")

	tags := make([]map[string]string, 10)
	for i := range tags {
		tags[i] = map[string]string{"Key": string(rune('a'+i)) + "-key", "Value": "v"}
	}

	doFirehoseRequest(t, h, "TagDeliveryStream", map[string]any{
		"DeliveryStreamName": "tagged-stream",
		"Tags":               tags,
	})

	rec := doFirehoseRequest(t, h, "ListTagsForDeliveryStream", map[string]any{
		"DeliveryStreamName": "tagged-stream",
		"Limit":              3,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Tags        []map[string]string `json:"Tags"`
		HasMoreTags bool                `json:"HasMoreTags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.Tags, 3)
	assert.True(t, out.HasMoreTags)
}

func TestAccuracy_ListTagsForDeliveryStream_ExclusiveStart(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "tagged-stream")

	doFirehoseRequest(t, h, "TagDeliveryStream", map[string]any{
		"DeliveryStreamName": "tagged-stream",
		"Tags": []map[string]string{
			{"Key": "a-key", "Value": "1"},
			{"Key": "b-key", "Value": "2"},
			{"Key": "c-key", "Value": "3"},
		},
	})

	rec := doFirehoseRequest(t, h, "ListTagsForDeliveryStream", map[string]any{
		"DeliveryStreamName":   "tagged-stream",
		"ExclusiveStartTagKey": "a-key",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Tags, 2)
	assert.Equal(t, "b-key", out.Tags[0].Key)
	assert.Equal(t, "c-key", out.Tags[1].Key)
}

// --- Issue #33: Tag limits ---

func TestAccuracy_TagValidation_TooManyTags(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	tags := make([]map[string]string, 51)
	for i := range tags {
		tags[i] = map[string]string{"Key": string(rune('A'+i%26)) + "-key", "Value": "v"}
	}

	rec := doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "tag-overflow",
		"Tags":               tags,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_TagValidation_KeyTooLong(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "tag-key-len")

	longKey := make([]byte, 129)
	for i := range longKey {
		longKey[i] = 'x'
	}

	rec := doFirehoseRequest(t, h, "TagDeliveryStream", map[string]any{
		"DeliveryStreamName": "tag-key-len",
		"Tags":               []map[string]string{{"Key": string(longKey), "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_TagValidation_ValueTooLong(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "tag-val-len")

	longVal := make([]byte, 257)
	for i := range longVal {
		longVal[i] = 'v'
	}

	rec := doFirehoseRequest(t, h, "TagDeliveryStream", map[string]any{
		"DeliveryStreamName": "tag-val-len",
		"Tags":               []map[string]string{{"Key": "k", "Value": string(longVal)}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- Issue #34: Empty record Data rejected ---

func TestAccuracy_PutRecord_EmptyData(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "empty-data-stream")

	rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
		"DeliveryStreamName": "empty-data-stream",
		"Record":             map[string]any{"Data": ""},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_PutRecordBatch_EmptyData(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "empty-batch-stream")

	rec := doFirehoseRequest(t, h, "PutRecordBatch", map[string]any{
		"DeliveryStreamName": "empty-batch-stream",
		"Records":            []map[string]any{{"Data": ""}, {"Data": "aGVsbG8="}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- Issue #28: PutRecord/PutRecordBatch rejected on non-DirectPut streams ---

func TestAccuracy_PutRecord_Rejected_On_KinesisSource(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "ks-put-stream", map[string]any{
		"DeliveryStreamType": "KinesisStreamAsSource",
		"KinesisStreamSourceConfiguration": map[string]any{
			"KinesisStreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/s",
			"RoleARN":          "arn:aws:iam::000000000000:role/r",
		},
	})

	rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
		"DeliveryStreamName": "ks-put-stream",
		"Record":             map[string]any{"Data": "aGVsbG8="},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_PutRecordBatch_Rejected_On_KinesisSource(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "ks-batch-stream", map[string]any{
		"DeliveryStreamType": "KinesisStreamAsSource",
		"KinesisStreamSourceConfiguration": map[string]any{
			"KinesisStreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/s",
			"RoleARN":          "arn:aws:iam::000000000000:role/r",
		},
	})

	rec := doFirehoseRequest(t, h, "PutRecordBatch", map[string]any{
		"DeliveryStreamName": "ks-batch-stream",
		"Records":            []map[string]any{{"Data": "aGVsbG8="}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- Issue #29: DescribeDeliveryStream HasMoreDestinations ---

func TestAccuracy_DescribeDeliveryStream_HasMoreDestinations(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "hmd-stream", map[string]any{
		"S3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::my-bucket",
		},
	})

	rec := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "hmd-stream"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "HasMoreDestinations")
}

// --- Issue #31: S3 object key includes UUID suffix ---

func TestAccuracy_S3Key_IncludesUUIDSuffix(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}
	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetS3Backend(s3mock)

	_, err := b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{
		Name: "key-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::my-bucket",
		},
	})
	require.NoError(t, err)

	err = b.PutRecord("key-stream", []byte("hello"))
	require.NoError(t, err)

	b.FlushAll(t.Context())

	require.Len(t, s3mock.calls, 1)
	// Key should have format: {ts}/key-stream-1-{ts}-{uuid}
	key := s3mock.calls[0].key
	assert.Contains(t, key, "key-stream-1-")
	// UUID is 36 chars; key ends with a UUID segment
	assert.Greater(t, len(key), 36, "key should contain UUID: %s", key)
}

// --- Issue #16: RedshiftDestinationConfiguration modeled ---

func TestAccuracy_CreateDeliveryStream_Redshift(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "rs-stream",
		"RedshiftDestinationConfiguration": map[string]any{
			"ClusterJDBCURL": "jdbc:redshift://cluster.us-east-1.redshift.amazonaws.com:5439/db",
			"RoleARN":        "arn:aws:iam::000000000000:role/rs",
			"S3BackupConfiguration": map[string]any{
				"BucketARN": "arn:aws:s3:::rs-bucket",
				"RoleARN":   "arn:aws:iam::000000000000:role/rs",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	desc := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "rs-stream"})
	require.Equal(t, http.StatusOK, desc.Code)
	assert.Contains(t, desc.Body.String(), "RedshiftDestinationDescriptions")
}

// --- Issue #17: HTTPEndpoint extended fields ---

func TestAccuracy_CreateDeliveryStream_HTTPEndpoint_Extended(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "http-ext",
		"HTTPEndpointDestinationConfiguration": map[string]any{
			"EndpointConfiguration": map[string]any{
				"Url":  "https://my-endpoint.example.com",
				"Name": "my-endpoint",
			},
			"BufferingHints": map[string]any{
				"SizeInMBs":         5,
				"IntervalInSeconds": 300,
			},
			"RetryOptions": map[string]any{
				"DurationInSeconds": 300,
			},
			"RequestConfiguration": map[string]any{
				"ContentEncoding": "GZIP",
				"CommonAttributes": []map[string]any{
					{"AttributeName": "x-custom", "AttributeValue": "val"},
				},
			},
			"CloudWatchLoggingOptions": map[string]any{
				"Enabled":      true,
				"LogGroupName": "/aws/firehose/http",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	desc := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "http-ext"})
	require.Equal(t, http.StatusOK, desc.Code)

	body := desc.Body.String()
	assert.Contains(t, body, "BufferingHints")
	assert.Contains(t, body, "RetryOptions")
	assert.Contains(t, body, "RequestConfiguration")
}

// --- Accuracy: DescribeDeliveryStream Source field ---

func TestAccuracy_DescribeDeliveryStream_SourceField(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "src-stream", map[string]any{
		"DeliveryStreamType": "KinesisStreamAsSource",
		"KinesisStreamSourceConfiguration": map[string]any{
			"KinesisStreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/my-ks",
			"RoleARN":          "arn:aws:iam::000000000000:role/r",
		},
	})

	rec := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "src-stream"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		DeliveryStreamDescription struct {
			Source *firehose.SourceDescription `json:"Source"`
		} `json:"DeliveryStreamDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.DeliveryStreamDescription.Source)
	require.NotNil(t, out.DeliveryStreamDescription.Source.KinesisStreamSourceDescription)
	assert.Equal(t, "arn:aws:kinesis:us-east-1:000000000000:stream/my-ks",
		out.DeliveryStreamDescription.Source.KinesisStreamSourceDescription.KinesisStreamARN)
}

// --- Accuracy: ErrValidation sentinel reachable ---

func TestAccuracy_ErrValidation_Exported(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{Name: "s"})
	require.NoError(t, err)

	err = b.PutRecord("s", []byte{})
	require.Error(t, err)
	assert.ErrorIs(t, err, firehose.ErrValidation)
}
