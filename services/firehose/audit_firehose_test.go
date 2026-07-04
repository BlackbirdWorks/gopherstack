package firehose_test

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

// auditHandler returns a handler with a mock S3 backend wired in.
func auditHandler(t *testing.T) (*firehose.Handler, *mockS3Storer) {
	t.Helper()

	h := newTestFirehoseHandler(t)
	s3mock := &mockS3Storer{}
	h.Backend.(*firehose.InMemoryBackend).SetS3Backend(s3mock)

	return h, s3mock
}

// auditCreateStream creates a delivery stream via the handler and returns the ARN.
func auditCreateStream(t *testing.T, h *firehose.Handler, name string, extra map[string]any) string {
	t.Helper()

	body := map[string]any{"DeliveryStreamName": name}
	maps.Copy(body, extra)

	rec := doFirehoseRequest(t, h, "CreateDeliveryStream", body)
	require.Equal(t, http.StatusOK, rec.Code, "createStream %q: %s", name, rec.Body.String())

	var out struct {
		DeliveryStreamARN string `json:"DeliveryStreamARN"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out.DeliveryStreamARN
}

// auditDescribe calls DescribeDeliveryStream and returns the raw parsed map.
func auditDescribe(t *testing.T, h *firehose.Handler, name string) map[string]any {
	t.Helper()

	rec := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": name})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out["DeliveryStreamDescription"].(map[string]any)
}

// auditPut puts a single base64-encoded record via the handler.
func auditPut(t *testing.T, h *firehose.Handler, name string, data string) {
	t.Helper()

	rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
		"DeliveryStreamName": name,
		"Record":             map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(data))},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// gunzip decompresses gzip bytes and returns the raw content.
func gunzip(t *testing.T, data []byte) []byte {
	t.Helper()

	r, err := gzip.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer r.Close()

	out, err := io.ReadAll(r)
	require.NoError(t, err)

	return out
}

// --- DescribeDeliveryStream field audit ---

// TestAuditFirehose_DescribeDeliveryStream_CoreFields verifies that Describe
// returns the expected fields: ARN, status, type, VersionId, CreateTimestamp.
func TestAuditFirehose_DescribeDeliveryStream_CoreFields(t *testing.T) {
	t.Parallel()

	h, _ := auditHandler(t)
	arn := auditCreateStream(t, h, "core-stream", nil)

	desc := auditDescribe(t, h, "core-stream")

	assert.Equal(t, "core-stream", desc["DeliveryStreamName"])
	assert.Equal(t, arn, desc["DeliveryStreamARN"])
	assert.True(t, strings.HasPrefix(arn, "arn:aws:firehose:"), "ARN must start with arn:aws:firehose:")
	assert.Equal(t, "ACTIVE", desc["DeliveryStreamStatus"])
	assert.Equal(t, "DirectPut", desc["DeliveryStreamType"])
	assert.Equal(t, "1", desc["VersionId"])
	assert.NotNil(t, desc["CreateTimestamp"], "CreateTimestamp must be non-nil")
}

// --- Destination round-trip audit ---

// TestAuditFirehose_DestinationDescribe_AllTypes verifies that each of the
// five destination types is persisted and returned in DescribeDeliveryStream
// in the correct destination-specific field.
func TestAuditFirehose_DestinationDescribe_AllTypes(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // field order is chosen for readability
		name          string
		createExtra   map[string]any
		checkDescribe func(t *testing.T, desc map[string]any)
	}{
		{
			name: "s3_destination",
			createExtra: map[string]any{
				"S3DestinationConfiguration": map[string]any{
					"BucketARN": "arn:aws:s3:::audit-bucket",
					"RoleARN":   "arn:aws:iam::000000000000:role/firehose",
					"Prefix":    "logs/",
				},
			},
			checkDescribe: func(t *testing.T, desc map[string]any) {
				t.Helper()
				dests, ok := desc["S3DestinationDescriptions"].([]any)
				require.True(t, ok, "S3DestinationDescriptions must be present")
				require.Len(t, dests, 1)
				d := dests[0].(map[string]any)
				assert.Equal(t, "arn:aws:s3:::audit-bucket", d["BucketARN"])
				assert.Equal(t, "logs/", d["Prefix"])
			},
		},
		{
			name: "extended_s3_destination",
			createExtra: map[string]any{
				"ExtendedS3DestinationConfiguration": map[string]any{
					"BucketARN":         "arn:aws:s3:::ext-bucket",
					"RoleARN":           "arn:aws:iam::000000000000:role/firehose",
					"CompressionFormat": "GZIP",
					"Prefix":            "ext/",
				},
			},
			checkDescribe: func(t *testing.T, desc map[string]any) {
				t.Helper()
				// ExtendedS3 maps to S3DestinationDescriptions.
				dests, ok := desc["S3DestinationDescriptions"].([]any)
				require.True(t, ok, "ExtendedS3 must appear in S3DestinationDescriptions")
				require.Len(t, dests, 1)
				d := dests[0].(map[string]any)
				assert.Equal(t, "arn:aws:s3:::ext-bucket", d["BucketARN"])
				assert.Equal(t, "GZIP", d["CompressionFormat"])
				assert.Equal(t, "ext/", d["Prefix"])
			},
		},
		{
			name: "http_endpoint_destination",
			createExtra: map[string]any{
				"HTTPEndpointDestinationConfiguration": map[string]any{
					"EndpointConfiguration": map[string]any{
						"Url":  "https://ingest.example.com/firehose",
						"Name": "audit-endpoint",
					},
					"BufferingHints": map[string]any{
						"SizeInMBs":         5,
						"IntervalInSeconds": 60,
					},
				},
			},
			checkDescribe: func(t *testing.T, desc map[string]any) {
				t.Helper()
				dests, ok := desc["HTTPEndpointDestinationDescriptions"].([]any)
				require.True(t, ok, "HTTPEndpointDestinationDescriptions must be present")
				require.Len(t, dests, 1)
				d := dests[0].(map[string]any)
				ep := d["EndpointConfiguration"].(map[string]any)
				assert.Equal(t, "https://ingest.example.com/firehose", ep["Url"])
				assert.Equal(t, "audit-endpoint", ep["Name"])
			},
		},
		{
			name: "redshift_destination",
			createExtra: map[string]any{
				"RedshiftDestinationConfiguration": map[string]any{
					"ClusterJDBCURL":   "jdbc:redshift://my-cluster.abc.us-east-1.redshift.amazonaws.com:5439/mydb",
					"RoleARN":          "arn:aws:iam::000000000000:role/firehose",
					"DataTableName":    "events",
					"DataTableColumns": "ts,payload",
					"Username":         "admin",
				},
			},
			checkDescribe: func(t *testing.T, desc map[string]any) {
				t.Helper()
				dests, ok := desc["RedshiftDestinationDescriptions"].([]any)
				require.True(t, ok, "RedshiftDestinationDescriptions must be present")
				require.Len(t, dests, 1)
				d := dests[0].(map[string]any)
				assert.Contains(t, d["ClusterJDBCURL"].(string), "redshift")
				assert.Equal(t, "events", d["DataTableName"])
				assert.Equal(t, "admin", d["Username"])
			},
		},
		{
			name: "opensearch_destination",
			createExtra: map[string]any{
				"AmazonOpenSearchServiceDestinationConfiguration": map[string]any{
					"ClusterEndpoint":     "https://search-my-domain.us-east-1.es.amazonaws.com",
					"IndexName":           "firehose-logs",
					"IndexRotationPeriod": "OneDay",
					"RoleARN":             "arn:aws:iam::000000000000:role/firehose",
				},
			},
			checkDescribe: func(t *testing.T, desc map[string]any) {
				t.Helper()
				dests, ok := desc["AmazonOpenSearchServiceDestinationDescriptions"].([]any)
				require.True(t, ok, "AmazonOpenSearchServiceDestinationDescriptions must be present")
				require.Len(t, dests, 1)
				d := dests[0].(map[string]any)
				assert.Equal(t, "https://search-my-domain.us-east-1.es.amazonaws.com", d["ClusterEndpoint"])
				assert.Equal(t, "firehose-logs", d["IndexName"])
				assert.Equal(t, "OneDay", d["IndexRotationPeriod"])
			},
		},
		{
			name: "splunk_destination",
			createExtra: map[string]any{
				"SplunkDestinationConfiguration": map[string]any{
					"HECEndpoint":     "https://splunk.example.com:8088",
					"HECEndpointType": "Raw",
					"HECToken":        "splunk-token-abc",
				},
			},
			checkDescribe: func(t *testing.T, desc map[string]any) {
				t.Helper()
				dests, ok := desc["SplunkDestinationDescriptions"].([]any)
				require.True(t, ok, "SplunkDestinationDescriptions must be present")
				require.Len(t, dests, 1)
				d := dests[0].(map[string]any)
				assert.Equal(t, "https://splunk.example.com:8088", d["HECEndpoint"])
				assert.Equal(t, "Raw", d["HECEndpointType"])
				assert.Equal(t, "splunk-token-abc", d["HECToken"])
			},
		},
		{
			name: "msk_source",
			createExtra: map[string]any{
				"MSKSourceConfiguration": map[string]any{
					"MSKClusterARN": "arn:aws:kafka:us-east-1:000000000000:cluster/my-msk/abc",
					"TopicName":     "events",
					"AuthenticationConfiguration": map[string]any{
						"Connectivity": "PUBLIC",
						"RoleARN":      "arn:aws:iam::000000000000:role/firehose",
					},
				},
			},
			checkDescribe: func(t *testing.T, desc map[string]any) {
				t.Helper()
				src, ok := desc["Source"].(map[string]any)
				require.True(t, ok, "Source must be present for MSK")
				msk, ok := src["MSKSourceDescription"].(map[string]any)
				require.True(t, ok, "MSKSourceDescription must be present")
				assert.Equal(t, "events", msk["TopicName"])
				assert.Contains(t, msk["MSKClusterARN"].(string), "kafka")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := auditHandler(t)
			streamName := "audit-dest-" + tc.name
			auditCreateStream(t, h, streamName, tc.createExtra)
			desc := auditDescribe(t, h, streamName)
			tc.checkDescribe(t, desc)
		})
	}
}

// --- S3 delivery audit ---

// TestAuditFirehose_S3Delivery_KeyFormat verifies the S3 key follows the
// AWS Firehose convention: {prefix}{yyyy/MM/dd/HH/}{stream}-1-{ts}-{uuid}.
func TestAuditFirehose_S3Delivery_KeyFormat(t *testing.T) {
	t.Parallel()

	h, s3mock := auditHandler(t)

	auditCreateStream(t, h, "key-stream", map[string]any{
		"S3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::key-bucket",
			"Prefix":    "my-prefix/",
		},
	})

	auditPut(t, h, "key-stream", "hello")
	h.Backend.(*firehose.InMemoryBackend).FlushAll(t.Context())

	require.Len(t, s3mock.calls, 1)
	key := s3mock.calls[0].key

	assert.Equal(t, "key-bucket", s3mock.calls[0].bucket)
	assert.True(t, strings.HasPrefix(key, "my-prefix/"), "key must start with prefix; got %q", key)
	assert.Contains(t, key, "key-stream", "key must contain stream name")
	// date segment: 4 digit year followed by /MM/dd/HH/
	assert.Regexp(t, `\d{4}/\d{2}/\d{2}/\d{2}/`, key, "key must contain date path")
}

// TestAuditFirehose_S3Delivery_GZIPCompression verifies that records delivered
// with CompressionFormat=GZIP are gzip-compressed and contain the original content.
func TestAuditFirehose_S3Delivery_GZIPCompression(t *testing.T) {
	t.Parallel()

	h, s3mock := auditHandler(t)

	auditCreateStream(t, h, "gzip-stream", map[string]any{
		"S3DestinationConfiguration": map[string]any{
			"BucketARN":         "arn:aws:s3:::gzip-bucket",
			"CompressionFormat": "GZIP",
		},
	})

	auditPut(t, h, "gzip-stream", "compressed-record")
	h.Backend.(*firehose.InMemoryBackend).FlushAll(t.Context())

	require.Len(t, s3mock.calls, 1)
	body := s3mock.calls[0].body

	decompressed := gunzip(t, body)
	assert.Contains(t, string(decompressed), "compressed-record")
}

// TestAuditFirehose_S3Delivery_MultiRecordNewlineSeparated verifies that multiple
// records delivered in one flush are joined with newlines.
func TestAuditFirehose_S3Delivery_MultiRecordNewlineSeparated(t *testing.T) {
	t.Parallel()

	h, s3mock := auditHandler(t)

	auditCreateStream(t, h, "multi-stream", map[string]any{
		"S3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::multi-bucket",
		},
	})

	rec := doFirehoseRequest(t, h, "PutRecordBatch", map[string]any{
		"DeliveryStreamName": "multi-stream",
		"Records": []map[string]any{
			{"Data": base64.StdEncoding.EncodeToString([]byte("rec-A"))},
			{"Data": base64.StdEncoding.EncodeToString([]byte("rec-B"))},
			{"Data": base64.StdEncoding.EncodeToString([]byte("rec-C"))},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	h.Backend.(*firehose.InMemoryBackend).FlushAll(t.Context())

	require.Len(t, s3mock.calls, 1)
	body := string(s3mock.calls[0].body)
	assert.Contains(t, body, "rec-A")
	assert.Contains(t, body, "rec-B")
	assert.Contains(t, body, "rec-C")
	assert.Equal(t, 3, strings.Count(body, "\n"), "each record gets a trailing newline; three records → three newlines")
}

// TestAuditFirehose_S3Delivery_BufferingHintsSizeOverride verifies that setting
// SizeInMBs=1 causes a size-based flush when records exceed 1 MB.
func TestAuditFirehose_S3Delivery_BufferingHintsSizeOverride(t *testing.T) {
	t.Parallel()

	h, s3mock := auditHandler(t)

	auditCreateStream(t, h, "size-hint-stream", map[string]any{
		"S3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::size-hint-bucket",
			"BufferingHints": map[string]any{
				"SizeInMBs":         1,
				"IntervalInSeconds": 300,
			},
		},
	})

	// Under 1 MB — no flush expected.
	smallRecord := make([]byte, 900*1024)
	rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
		"DeliveryStreamName": "size-hint-stream",
		"Record":             map[string]any{"Data": base64.StdEncoding.EncodeToString(smallRecord)},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, s3mock.calls, "no flush below size limit")

	// Over 1 MB — flush expected.
	overRecord := make([]byte, 200*1024)
	rec = doFirehoseRequest(t, h, "PutRecord", map[string]any{
		"DeliveryStreamName": "size-hint-stream",
		"Record":             map[string]any{"Data": base64.StdEncoding.EncodeToString(overRecord)},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Len(t, s3mock.calls, 1, "exceeding SizeInMBs must trigger flush")
}

// --- UpdateDestination audit ---

// TestAuditFirehose_UpdateDestination_ChangesS3Bucket verifies that after
// UpdateDestination with a new S3 bucket, subsequent flush delivers to the new bucket.
func TestAuditFirehose_UpdateDestination_ChangesS3Bucket(t *testing.T) {
	t.Parallel()

	h, s3mock := auditHandler(t)

	auditCreateStream(t, h, "upd-stream", map[string]any{
		"S3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::original-bucket",
		},
	})

	// Update to new bucket, version must match current ("1").
	upd := doFirehoseRequest(t, h, "UpdateDestination", map[string]any{
		"DeliveryStreamName":             "upd-stream",
		"CurrentDeliveryStreamVersionId": "1",
		"DestinationId":                  "destinationId-000000000001",
		"S3DestinationUpdate": map[string]any{
			"BucketARN": "arn:aws:s3:::updated-bucket",
		},
	})
	require.Equal(t, http.StatusOK, upd.Code)

	// Verify VersionId incremented.
	desc := auditDescribe(t, h, "upd-stream")
	assert.Equal(t, "2", desc["VersionId"])

	// Flush goes to new bucket.
	auditPut(t, h, "upd-stream", "payload")
	h.Backend.(*firehose.InMemoryBackend).FlushAll(t.Context())

	require.Len(t, s3mock.calls, 1)
	assert.Equal(t, "updated-bucket", s3mock.calls[0].bucket)
}

// TestAuditFirehose_UpdateDestination_VersionMismatchRejected verifies that a
// stale VersionId is rejected with 400 InvalidArgumentException.
func TestAuditFirehose_UpdateDestination_VersionMismatchRejected(t *testing.T) {
	t.Parallel()

	h, _ := auditHandler(t)

	auditCreateStream(t, h, "ver-stream", map[string]any{
		"S3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::ver-bucket",
		},
	})

	rec := doFirehoseRequest(t, h, "UpdateDestination", map[string]any{
		"DeliveryStreamName":             "ver-stream",
		"CurrentDeliveryStreamVersionId": "99",
		"DestinationId":                  "destinationId-000000000001",
		"S3DestinationUpdate": map[string]any{
			"BucketARN": "arn:aws:s3:::new-bucket",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "InvalidArgumentException", body["__type"])
}

// --- Tags audit ---

// TestAuditFirehose_Tags_Lifecycle verifies add-at-create, add, list (with
// ExclusiveStartTagKey pagination), and untag.
func TestAuditFirehose_Tags_Lifecycle(t *testing.T) {
	t.Parallel()

	h, _ := auditHandler(t)

	// Create with tags.
	auditCreateStream(t, h, "tag-stream", map[string]any{
		"Tags": []map[string]string{
			{"Key": "env", "Value": "prod"},
		},
	})

	// Add more tags.
	doFirehoseRequest(t, h, "TagDeliveryStream", map[string]any{
		"DeliveryStreamName": "tag-stream",
		"Tags": []map[string]string{
			{"Key": "region", "Value": "us-east-1"},
			{"Key": "team", "Value": "platform"},
			{"Key": "version", "Value": "2"},
		},
	})

	// List all tags (sorted alphabetically by key).
	listRec := doFirehoseRequest(t, h, "ListTagsForDeliveryStream",
		map[string]any{"DeliveryStreamName": "tag-stream"})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		Tags        []map[string]string `json:"Tags"`
		HasMoreTags bool                `json:"HasMoreTags"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.Len(t, listOut.Tags, 4)
	assert.False(t, listOut.HasMoreTags)
	// Keys must be sorted: env, region, team, version.
	assert.Equal(t, "env", listOut.Tags[0]["Key"])
	assert.Equal(t, "region", listOut.Tags[1]["Key"])

	// Paginate: list first 2 only.
	page1Rec := doFirehoseRequest(t, h, "ListTagsForDeliveryStream", map[string]any{
		"DeliveryStreamName": "tag-stream",
		"Limit":              2,
	})
	var page1 struct {
		Tags        []map[string]string `json:"Tags"`
		HasMoreTags bool                `json:"HasMoreTags"`
	}
	require.NoError(t, json.Unmarshal(page1Rec.Body.Bytes(), &page1))
	require.Len(t, page1.Tags, 2)
	assert.True(t, page1.HasMoreTags)
	assert.Equal(t, "env", page1.Tags[0]["Key"])
	lastKey := page1.Tags[1]["Key"] // "region"

	// Page 2 using ExclusiveStartTagKey cursor.
	page2Rec := doFirehoseRequest(t, h, "ListTagsForDeliveryStream", map[string]any{
		"DeliveryStreamName":   "tag-stream",
		"Limit":                2,
		"ExclusiveStartTagKey": lastKey,
	})
	var page2 struct {
		Tags        []map[string]string `json:"Tags"`
		HasMoreTags bool                `json:"HasMoreTags"`
	}
	require.NoError(t, json.Unmarshal(page2Rec.Body.Bytes(), &page2))
	require.Len(t, page2.Tags, 2)
	assert.False(t, page2.HasMoreTags)
	assert.Equal(t, "team", page2.Tags[0]["Key"])
	assert.Equal(t, "version", page2.Tags[1]["Key"])

	// Untag two keys.
	doFirehoseRequest(t, h, "UntagDeliveryStream", map[string]any{
		"DeliveryStreamName": "tag-stream",
		"TagKeys":            []string{"env", "team"},
	})

	// Verify only 2 remain.
	finalRec := doFirehoseRequest(t, h, "ListTagsForDeliveryStream",
		map[string]any{"DeliveryStreamName": "tag-stream"})
	var finalOut struct {
		Tags []map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(finalRec.Body.Bytes(), &finalOut))
	require.Len(t, finalOut.Tags, 2)

	keys := map[string]bool{}
	for _, tag := range finalOut.Tags {
		keys[tag["Key"]] = true
	}
	assert.True(t, keys["region"])
	assert.True(t, keys["version"])
	assert.False(t, keys["env"])
	assert.False(t, keys["team"])
}

// --- Encryption audit ---

// TestAuditFirehose_Encryption_StartStopLifecycle verifies that StartDeliveryStreamEncryption
// transitions the stream to ENABLED and StopDeliveryStreamEncryption to DISABLED.
func TestAuditFirehose_Encryption_StartStopLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		startBody   map[string]any
		wantKeyType string
	}{
		{
			name: "aws_owned_cmk",
			startBody: map[string]any{
				"DeliveryStreamName": "enc-stream-aws",
				"DeliveryStreamEncryptionConfigurationInput": map[string]any{
					"KeyType": "AWS_OWNED_CMK",
				},
			},
			wantKeyType: "AWS_OWNED_CMK",
		},
		{
			name: "customer_managed_cmk",
			startBody: map[string]any{
				"DeliveryStreamName": "enc-stream-cmk",
				"DeliveryStreamEncryptionConfigurationInput": map[string]any{
					"KeyType": "CUSTOMER_MANAGED_CMK",
					"KeyARN":  "arn:aws:kms:us-east-1:000000000000:key/audit-key",
				},
			},
			wantKeyType: "CUSTOMER_MANAGED_CMK",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := auditHandler(t)
			streamName := tc.startBody["DeliveryStreamName"].(string)
			auditCreateStream(t, h, streamName, nil)

			// Start encryption.
			startRec := doFirehoseRequest(t, h, "StartDeliveryStreamEncryption", tc.startBody)
			require.Equal(t, http.StatusOK, startRec.Code)

			// Describe: EncryptionConfiguration must show ENABLED.
			desc := auditDescribe(t, h, streamName)
			enc, ok := desc["EncryptionConfiguration"].(map[string]any)
			require.True(t, ok, "EncryptionConfiguration must be present")
			assert.Equal(t, "ENABLED", enc["Status"])
			assert.Equal(t, tc.wantKeyType, enc["KeyType"])

			// Stop encryption.
			stopRec := doFirehoseRequest(t, h, "StopDeliveryStreamEncryption",
				map[string]any{"DeliveryStreamName": streamName})
			require.Equal(t, http.StatusOK, stopRec.Code)

			// Describe: status must be DISABLED.
			desc2 := auditDescribe(t, h, streamName)
			enc2, ok := desc2["EncryptionConfiguration"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "DISABLED", enc2["Status"])
		})
	}
}

// TestAuditFirehose_Encryption_CustomerManagedCMK_RequiresKeyARN verifies that
// CUSTOMER_MANAGED_CMK without a KeyARN is rejected.
func TestAuditFirehose_Encryption_CustomerManagedCMK_RequiresKeyARN(t *testing.T) {
	t.Parallel()

	h, _ := auditHandler(t)
	auditCreateStream(t, h, "cmk-noarn", nil)

	rec := doFirehoseRequest(t, h, "StartDeliveryStreamEncryption", map[string]any{
		"DeliveryStreamName": "cmk-noarn",
		"DeliveryStreamEncryptionConfigurationInput": map[string]any{
			"KeyType": "CUSTOMER_MANAGED_CMK",
			// KeyARN intentionally omitted.
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- KinesisStreamAsSource audit ---

// TestAuditFirehose_KinesisStreamAsSource_PutRecordRejected verifies that
// PutRecord on a KinesisStreamAsSource stream returns InvalidArgumentException.
func TestAuditFirehose_KinesisStreamAsSource_PutRecordRejected(t *testing.T) {
	t.Parallel()

	h, _ := auditHandler(t)
	auditCreateStream(t, h, "kinesis-src-stream", map[string]any{
		"DeliveryStreamType": "KinesisStreamAsSource",
		"KinesisStreamSourceConfiguration": map[string]any{
			"KinesisStreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/my-src",
			"RoleARN":          "arn:aws:iam::000000000000:role/firehose",
		},
	})

	rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
		"DeliveryStreamName": "kinesis-src-stream",
		"Record":             map[string]any{"Data": "aGVsbG8="},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "InvalidArgumentException", body["__type"])
}

// TestAuditFirehose_KinesisStreamAsSource_DescribeShowsSource verifies that
// the Source field is populated in DescribeDeliveryStream.
func TestAuditFirehose_KinesisStreamAsSource_DescribeShowsSource(t *testing.T) {
	t.Parallel()

	h, _ := auditHandler(t)
	auditCreateStream(t, h, "ks-describe-stream", map[string]any{
		"DeliveryStreamType": "KinesisStreamAsSource",
		"KinesisStreamSourceConfiguration": map[string]any{
			"KinesisStreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/src-stream",
			"RoleARN":          "arn:aws:iam::000000000000:role/firehose",
		},
	})

	desc := auditDescribe(t, h, "ks-describe-stream")
	assert.Equal(t, "KinesisStreamAsSource", desc["DeliveryStreamType"])

	src, ok := desc["Source"].(map[string]any)
	require.True(t, ok, "Source must be present")
	ksSrc, ok := src["KinesisStreamSourceDescription"].(map[string]any)
	require.True(t, ok, "KinesisStreamSourceDescription must be present")
	assert.Equal(t,
		"arn:aws:kinesis:us-east-1:000000000000:stream/src-stream",
		ksSrc["KinesisStreamARN"],
	)
}

// --- ListDeliveryStreams audit ---

// TestAuditFirehose_ListDeliveryStreams_Pagination verifies ExclusiveStartDeliveryStreamName
// cursor and HasMoreDeliveryStreams flag.
func TestAuditFirehose_ListDeliveryStreams_Pagination(t *testing.T) {
	t.Parallel()

	h, _ := auditHandler(t)

	for i := range 5 {
		auditCreateStream(t, h, strings.ToLower(string(rune('a'+i)))+"-list-stream", nil)
	}

	// Page 1: limit 2.
	page1Rec := doFirehoseRequest(t, h, "ListDeliveryStreams",
		map[string]any{"Limit": 2})
	var page1 struct {
		DeliveryStreamNames    []string `json:"DeliveryStreamNames"`
		HasMoreDeliveryStreams bool     `json:"HasMoreDeliveryStreams"`
	}
	require.NoError(t, json.Unmarshal(page1Rec.Body.Bytes(), &page1))
	assert.Len(t, page1.DeliveryStreamNames, 2)
	assert.True(t, page1.HasMoreDeliveryStreams)

	// Page 2 using cursor.
	cursor := page1.DeliveryStreamNames[1]
	page2Rec := doFirehoseRequest(t, h, "ListDeliveryStreams", map[string]any{
		"Limit":                            2,
		"ExclusiveStartDeliveryStreamName": cursor,
	})
	var page2 struct {
		DeliveryStreamNames    []string `json:"DeliveryStreamNames"`
		HasMoreDeliveryStreams bool     `json:"HasMoreDeliveryStreams"`
	}
	require.NoError(t, json.Unmarshal(page2Rec.Body.Bytes(), &page2))
	assert.Len(t, page2.DeliveryStreamNames, 2)
	// No overlap between pages.
	for _, n := range page2.DeliveryStreamNames {
		assert.NotContains(t, page1.DeliveryStreamNames, n, "page 2 must not repeat page 1 names")
	}
}

// TestAuditFirehose_ListDeliveryStreams_TypeFilter verifies that the
// DeliveryStreamType filter is honored: only streams of the requested type are returned.
func TestAuditFirehose_ListDeliveryStreams_TypeFilter(t *testing.T) {
	t.Parallel()

	h, _ := auditHandler(t)

	auditCreateStream(t, h, "direct-put-stream", nil)
	auditCreateStream(t, h, "kinesis-src-stream-lf", map[string]any{
		"DeliveryStreamType": "KinesisStreamAsSource",
		"KinesisStreamSourceConfiguration": map[string]any{
			"KinesisStreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/my-src",
			"RoleARN":          "arn:aws:iam::000000000000:role/firehose",
		},
	})

	// Filter for DirectPut only — the Kinesis-source stream must be excluded.
	rec := doFirehoseRequest(t, h, "ListDeliveryStreams",
		map[string]any{"DeliveryStreamType": "DirectPut"})
	var out struct {
		DeliveryStreamNames []string `json:"DeliveryStreamNames"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Contains(t, out.DeliveryStreamNames, "direct-put-stream")
	assert.NotContains(t, out.DeliveryStreamNames, "kinesis-src-stream-lf")

	// Filter for KinesisStreamAsSource only — the DirectPut stream must be excluded.
	rec2 := doFirehoseRequest(t, h, "ListDeliveryStreams",
		map[string]any{"DeliveryStreamType": "KinesisStreamAsSource"})
	var out2 struct {
		DeliveryStreamNames []string `json:"DeliveryStreamNames"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	assert.Contains(t, out2.DeliveryStreamNames, "kinesis-src-stream-lf")
	assert.NotContains(t, out2.DeliveryStreamNames, "direct-put-stream")

	// An invalid filter value is rejected with InvalidArgumentException.
	recInvalid := doFirehoseRequest(t, h, "ListDeliveryStreams",
		map[string]any{"DeliveryStreamType": "Bogus"})
	assert.Equal(t, http.StatusBadRequest, recInvalid.Code)
}

// --- PutRecordBatch response audit ---

// TestAuditFirehose_PutRecordBatch_ResponseShape verifies that each record
// gets a RecordId and FailedPutCount is always 0.
func TestAuditFirehose_PutRecordBatch_ResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		recordData []string
	}{
		{
			name:       "single_record",
			recordData: []string{"aGVsbG8="},
		},
		{
			name: "five_records",
			recordData: []string{
				base64.StdEncoding.EncodeToString([]byte("rec-1")),
				base64.StdEncoding.EncodeToString([]byte("rec-2")),
				base64.StdEncoding.EncodeToString([]byte("rec-3")),
				base64.StdEncoding.EncodeToString([]byte("rec-4")),
				base64.StdEncoding.EncodeToString([]byte("rec-5")),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := auditHandler(t)
			auditCreateStream(t, h, "batch-resp-stream", nil)

			records := make([]map[string]any, len(tc.recordData))
			for i, d := range tc.recordData {
				records[i] = map[string]any{"Data": d}
			}

			rec := doFirehoseRequest(t, h, "PutRecordBatch", map[string]any{
				"DeliveryStreamName": "batch-resp-stream",
				"Records":            records,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				RequestResponses []struct {
					RecordID string `json:"RecordId"`
				} `json:"RequestResponses"`
				FailedPutCount int `json:"FailedPutCount"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			assert.Equal(t, 0, out.FailedPutCount, "FailedPutCount must always be 0")
			assert.Len(t, out.RequestResponses, len(tc.recordData), "one response per record")
			for i, resp := range out.RequestResponses {
				assert.NotEmpty(t, resp.RecordID, "RecordId must be non-empty for record %d", i)
			}
		})
	}
}

// --- S3BackupMode audit ---

// TestAuditFirehose_S3BackupMode_PersistedInDescribe verifies that S3BackupMode=Enabled
// on HTTP and ExtendedS3 destinations is persisted and returned in DescribeDeliveryStream.
func TestAuditFirehose_S3BackupMode_PersistedInDescribe(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // field order is chosen for readability
		name          string
		createExtra   map[string]any
		checkDescribe func(t *testing.T, desc map[string]any)
	}{
		{
			name: "extended_s3_backup_enabled",
			createExtra: map[string]any{
				"ExtendedS3DestinationConfiguration": map[string]any{
					"BucketARN":    "arn:aws:s3:::main-bucket",
					"RoleARN":      "arn:aws:iam::000000000000:role/firehose",
					"S3BackupMode": "Enabled",
					"S3BackupConfiguration": map[string]any{
						"BucketARN": "arn:aws:s3:::backup-bucket",
						"RoleARN":   "arn:aws:iam::000000000000:role/firehose",
					},
				},
			},
			checkDescribe: func(t *testing.T, desc map[string]any) {
				t.Helper()
				dests := desc["S3DestinationDescriptions"].([]any)
				require.Len(t, dests, 1)
				d := dests[0].(map[string]any)
				assert.Equal(t, "Enabled", d["S3BackupMode"])
				backup := d["S3BackupDescription"].(map[string]any)
				assert.Equal(t, "arn:aws:s3:::backup-bucket", backup["BucketARN"])
			},
		},
		{
			name: "http_endpoint_backup_enabled",
			createExtra: map[string]any{
				"HTTPEndpointDestinationConfiguration": map[string]any{
					"EndpointConfiguration": map[string]any{
						"Url": "https://endpoint.example.com",
					},
					"S3BackupMode": "Enabled",
					"S3BackupConfiguration": map[string]any{
						"BucketARN": "arn:aws:s3:::http-backup-bucket",
						"RoleARN":   "arn:aws:iam::000000000000:role/firehose",
					},
				},
			},
			checkDescribe: func(t *testing.T, desc map[string]any) {
				t.Helper()
				dests := desc["HTTPEndpointDestinationDescriptions"].([]any)
				require.Len(t, dests, 1)
				d := dests[0].(map[string]any)
				assert.Equal(t, "Enabled", d["S3BackupMode"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := auditHandler(t)
			streamName := "backup-" + strings.ReplaceAll(tc.name, "_", "-")
			auditCreateStream(t, h, streamName, tc.createExtra)
			desc := auditDescribe(t, h, streamName)
			tc.checkDescribe(t, desc)
		})
	}
}

// --- HTTP endpoint delivery audit ---

// TestAuditFirehose_HTTPEndpoint_DeliveryFormat verifies the AWS Firehose HTTP
// delivery payload format: requestId, timestamp, and base64-encoded records.
func TestAuditFirehose_HTTPEndpoint_DeliveryFormat(t *testing.T) {
	t.Parallel()

	srv := newCaptureServer(t, http.StatusOK)

	h := newTestFirehoseHandler(t)
	b := h.Backend.(*firehose.InMemoryBackend)

	auditCreateStream(t, h, "http-fmt-stream", map[string]any{
		"HTTPEndpointDestinationConfiguration": map[string]any{
			"EndpointConfiguration": map[string]any{
				"Url":       srv.srv.URL,
				"AccessKey": "test-access-key",
			},
			"BufferingHints": map[string]any{
				"SizeInMBs":         1,
				"IntervalInSeconds": 300,
			},
		},
	})

	auditPut(t, h, "http-fmt-stream", "http-payload-data")
	b.FlushAll(t.Context())

	requests := srv.captured()
	require.Len(t, requests, 1)

	var payload struct { //nolint:govet // field order is chosen for readability
		RequestID string `json:"requestId"`
		Timestamp int64  `json:"timestamp"`
		Records   []struct {
			Data string `json:"data"`
		} `json:"records"`
	}
	require.NoError(t, json.Unmarshal(requests[0].body, &payload))

	assert.NotEmpty(t, payload.RequestID)
	assert.Positive(t, payload.Timestamp)
	require.Len(t, payload.Records, 1)
	decoded, err := base64.StdEncoding.DecodeString(payload.Records[0].Data)
	require.NoError(t, err)
	assert.Equal(t, "http-payload-data", string(decoded))

	// Access key must be in the Authorization header.
	authHeader := requests[0].headers.Get("X-Amz-Firehose-Access-Key")
	assert.Equal(t, "test-access-key", authHeader)
}

// --- OpenSearch delivery audit ---

// TestAuditFirehose_OpenSearch_BulkNDJSON verifies that records are delivered
// to OpenSearch as NDJSON _bulk requests.
func TestAuditFirehose_OpenSearch_BulkNDJSON(t *testing.T) {
	t.Parallel()

	srv := newCaptureServer(t, http.StatusOK)

	h := newTestFirehoseHandler(t)
	b := h.Backend.(*firehose.InMemoryBackend)

	auditCreateStream(t, h, "os-audit-stream", map[string]any{
		"AmazonOpenSearchServiceDestinationConfiguration": map[string]any{
			"ClusterEndpoint": srv.srv.URL,
			"IndexName":       "audit-index",
			"RoleARN":         "arn:aws:iam::000000000000:role/firehose",
		},
	})

	auditPut(t, h, "os-audit-stream", `{"event":"test"}`)
	b.FlushAll(t.Context())

	requests := srv.captured()
	require.Len(t, requests, 1)

	// NDJSON: first line is action line, second is document.
	lines := strings.Split(strings.TrimRight(string(requests[0].body), "\n"), "\n")
	require.GreaterOrEqual(t, len(lines), 2, "NDJSON must have at least action+document lines")

	var action map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &action))
	_, hasIndex := action["index"]
	assert.True(t, hasIndex, "first NDJSON line must be an index action")
}

// --- Splunk delivery audit ---

// TestAuditFirehose_Splunk_Modes verifies both Raw and Event HEC delivery modes.
func TestAuditFirehose_Splunk_Modes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		endpointType   string
		wantBodyPrefix string
		contentType    string
	}{
		{
			name:           "raw_mode",
			endpointType:   "Raw",
			wantBodyPrefix: "splunk-raw-data",
			contentType:    "text/plain",
		},
		{
			name:         "event_mode",
			endpointType: "Event",
			contentType:  "application/json",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newCaptureServer(t, http.StatusOK)

			h := newTestFirehoseHandler(t)
			b := h.Backend.(*firehose.InMemoryBackend)
			streamName := "splunk-" + tc.endpointType

			auditCreateStream(t, h, streamName, map[string]any{
				"SplunkDestinationConfiguration": map[string]any{
					"HECEndpoint":     srv.srv.URL,
					"HECEndpointType": tc.endpointType,
					"HECToken":        "tok-" + tc.endpointType,
				},
			})

			auditPut(t, h, streamName, "splunk-raw-data")
			b.FlushAll(t.Context())

			requests := srv.captured()
			require.Len(t, requests, 1)

			auth := requests[0].headers.Get("Authorization")
			assert.Equal(t, "Splunk tok-"+tc.endpointType, auth)

			contentType := requests[0].headers.Get("Content-Type")
			assert.Contains(t, contentType, tc.contentType)

			if tc.endpointType == "Raw" {
				assert.Contains(t, string(requests[0].body), "splunk-raw-data")
			} else {
				var evtBody map[string]any
				require.NoError(t, json.Unmarshal(requests[0].body, &evtBody))
				_, hasEvent := evtBody["event"]
				assert.True(t, hasEvent, "Event mode must wrap data in {event:...}")
			}
		})
	}
}

// --- Delete + Describe audit ---

// TestAuditFirehose_DeleteThenDescribeReturns404 verifies that a deleted stream
// cannot be described (ResourceNotFoundException).
func TestAuditFirehose_DeleteThenDescribeReturns404(t *testing.T) {
	t.Parallel()

	h, _ := auditHandler(t)
	auditCreateStream(t, h, "del-desc-stream", nil)

	delRec := doFirehoseRequest(t, h, "DeleteDeliveryStream",
		map[string]any{"DeliveryStreamName": "del-desc-stream"})
	require.Equal(t, http.StatusOK, delRec.Code)

	descRec := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "del-desc-stream"})
	assert.Equal(t, http.StatusNotFound, descRec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}
