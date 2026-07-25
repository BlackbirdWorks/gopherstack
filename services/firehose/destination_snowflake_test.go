package firehose_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

// TestSnowflakeDestination_CreateAndDescribe verifies that CreateDeliveryStream accepts a
// SnowflakeDestinationConfiguration and that DescribeDeliveryStream returns it under the
// exact AWS wire key "SnowflakeDestinationDescription" nested in Destinations, and that
// write-only credential fields (PrivateKey/KeyPassphrase) are never echoed back — matching
// the real SDK's SnowflakeDestinationDescription, which has no such fields.
func TestSnowflakeDestination_CreateAndDescribe(t *testing.T) {
	t.Parallel()

	h, _ := auditHandler(t)

	streamName := "snowflake-desc-stream"
	auditCreateStream(t, h, streamName, map[string]any{
		"SnowflakeDestinationConfiguration": map[string]any{
			"AccountUrl":    "https://myaccount.snowflakecomputing.com",
			"Database":      "analytics_db",
			"Schema":        "public",
			"Table":         "events",
			"RoleARN":       "arn:aws:iam::000000000000:role/firehose",
			"User":          "firehose_user",
			"PrivateKey":    "-----BEGIN PRIVATE KEY-----super-secret-----END PRIVATE KEY-----",
			"KeyPassphrase": "shh-secret",
			"S3Configuration": map[string]any{
				"BucketARN": "arn:aws:s3:::snowflake-bucket",
				"RoleARN":   "arn:aws:iam::000000000000:role/firehose",
			},
			"SnowflakeVpcConfiguration": map[string]any{
				"PrivateLinkVpceId": "com.amazonaws.vpce.us-east-1.vpce-svc-abc123",
			},
		},
	})

	desc := auditDescribe(t, h, streamName)
	d := singleDestination(t, desc, "SnowflakeDestinationDescription")

	assert.Equal(t, "https://myaccount.snowflakecomputing.com", d["AccountUrl"])
	assert.Equal(t, "analytics_db", d["Database"])
	assert.Equal(t, "public", d["Schema"])
	assert.Equal(t, "events", d["Table"])
	assert.Equal(t, "firehose_user", d["User"])

	// Write-only Snowflake credentials must never round-trip in Describe output.
	assert.NotContains(t, d, "PrivateKey")
	assert.NotContains(t, d, "KeyPassphrase")

	s3cfg := d["S3DestinationDescription"].(map[string]any)
	assert.Equal(t, "arn:aws:s3:::snowflake-bucket", s3cfg["BucketARN"])

	vpc := d["SnowflakeVpcConfiguration"].(map[string]any)
	assert.Equal(t, "com.amazonaws.vpce.us-east-1.vpce-svc-abc123", vpc["PrivateLinkVpceId"])
}

// TestSnowflakeDestination_Delivery verifies that buffered records for a Snowflake
// destination are delivered (landed) into the destination's required S3Configuration
// bucket once the size-based flush threshold is reached.
func TestSnowflakeDestination_Delivery(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}
	b := firehose.NewInMemoryBackend("000000000000", flushRegion)
	b.SetS3Backend(s3mock)

	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
		Name: "snowflake-delivery-stream",
		SnowflakeDestination: &firehose.SnowflakeDestinationDescription{
			Database: "db",
			Schema:   "public",
			Table:    "events",
			BufferingHints: &firehose.SnowflakeBufferingHints{
				SizeInMBs:         1,
				IntervalInSeconds: 300,
			},
			S3Destination: &firehose.S3DestinationDescription{
				BucketARN: "arn:aws:s3:::snowflake-landing",
			},
		},
	})
	require.NoError(t, err)

	// Two ~600KB records push the stream past the 1MB size-based flush threshold
	// without exceeding the 1000KB per-record limit.
	chunk := make([]byte, 600*1024)
	require.NoError(t, b.PutRecord(context.TODO(), "snowflake-delivery-stream", chunk))
	require.NoError(t, b.PutRecord(context.TODO(), "snowflake-delivery-stream", chunk))

	require.Len(t, s3mock.calls, 1, "expected one Snowflake S3-landing PutObject call")
	assert.Equal(t, "snowflake-landing", s3mock.calls[0].bucket)
}

// TestSnowflakeDestination_Update verifies UpdateDestination can switch a stream to a
// Snowflake destination and that the change is reflected in a subsequent Describe.
func TestSnowflakeDestination_Update(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "snowflake-update-stream")

	rec := doFirehoseRequest(t, h, "UpdateDestination", map[string]any{
		"DeliveryStreamName":             "snowflake-update-stream",
		"CurrentDeliveryStreamVersionId": "1",
		"SnowflakeDestinationUpdate": map[string]any{
			"Database": "updated_db",
			"Table":    "updated_table",
			"S3Configuration": map[string]any{
				"BucketARN": "arn:aws:s3:::snowflake-updated",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	desc := auditDescribe(t, h, "snowflake-update-stream")
	d := singleDestination(t, desc, "SnowflakeDestinationDescription")
	assert.Equal(t, "updated_db", d["Database"])
	assert.Equal(t, "updated_table", d["Table"])
}
