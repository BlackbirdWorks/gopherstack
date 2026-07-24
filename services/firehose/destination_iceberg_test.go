package firehose_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

// TestIcebergDestination_CreateAndDescribe verifies that CreateDeliveryStream accepts an
// IcebergDestinationConfiguration and that DescribeDeliveryStream returns it under the
// exact AWS wire key "IcebergDestinationDescription" nested in Destinations, including
// the CatalogConfiguration/DestinationTableConfigurationList sub-shapes.
func TestIcebergDestination_CreateAndDescribe(t *testing.T) {
	t.Parallel()

	h, _ := auditHandler(t)

	streamName := "iceberg-desc-stream"
	auditCreateStream(t, h, streamName, map[string]any{
		"IcebergDestinationConfiguration": map[string]any{
			"RoleARN": "arn:aws:iam::000000000000:role/firehose",
			"CatalogConfiguration": map[string]any{
				"CatalogARN": "arn:aws:glue:us-east-1:000000000000:catalog",
			},
			"S3Configuration": map[string]any{
				"BucketARN": "arn:aws:s3:::iceberg-bucket",
				"RoleARN":   "arn:aws:iam::000000000000:role/firehose",
			},
			"DestinationTableConfigurationList": []map[string]any{
				{
					"DestinationDatabaseName": "analytics",
					"DestinationTableName":    "events",
					"UniqueKeys":              []string{"id"},
				},
			},
			"AppendOnly": true,
		},
	})

	desc := auditDescribe(t, h, streamName)
	d := singleDestination(t, desc, "IcebergDestinationDescription")

	assert.Equal(t, "arn:aws:iam::000000000000:role/firehose", d["RoleARN"])
	assert.Equal(t, true, d["AppendOnly"])

	catalog := d["CatalogConfiguration"].(map[string]any)
	assert.Equal(t, "arn:aws:glue:us-east-1:000000000000:catalog", catalog["CatalogARN"])

	s3cfg := d["S3DestinationDescription"].(map[string]any)
	assert.Equal(t, "arn:aws:s3:::iceberg-bucket", s3cfg["BucketARN"])

	tables := d["DestinationTableConfigurationList"].([]any)
	require.Len(t, tables, 1)
	table := tables[0].(map[string]any)
	assert.Equal(t, "analytics", table["DestinationDatabaseName"])
	assert.Equal(t, "events", table["DestinationTableName"])
}

// TestIcebergDestination_Delivery verifies that buffered records for an Iceberg
// destination are delivered (landed) into the destination's required S3Configuration
// bucket once the size-based flush threshold is reached.
func TestIcebergDestination_Delivery(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}
	b := firehose.NewInMemoryBackend("000000000000", flushRegion)
	b.SetS3Backend(s3mock)

	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
		Name: "iceberg-delivery-stream",
		IcebergDestination: &firehose.IcebergDestinationDescription{
			RoleARN: "arn:aws:iam::000000000000:role/firehose",
			BufferingHints: &firehose.BufferingHints{
				SizeInMBs:         1,
				IntervalInSeconds: 300,
			},
			S3Destination: &firehose.S3DestinationDescription{
				BucketARN: "arn:aws:s3:::iceberg-landing",
			},
		},
	})
	require.NoError(t, err)

	// Two ~600KB records push the stream past the 1MB size-based flush threshold
	// without exceeding the 1000KB per-record limit.
	chunk := make([]byte, 600*1024)
	require.NoError(t, b.PutRecord(context.TODO(), "iceberg-delivery-stream", chunk))
	require.NoError(t, b.PutRecord(context.TODO(), "iceberg-delivery-stream", chunk))

	require.Len(t, s3mock.calls, 1, "expected one Iceberg S3-landing PutObject call")
	assert.Equal(t, "iceberg-landing", s3mock.calls[0].bucket)
}

// TestIcebergDestination_Update verifies UpdateDestination can switch a stream to an
// Iceberg destination and that the change is reflected in a subsequent Describe.
func TestIcebergDestination_Update(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "iceberg-update-stream")

	rec := doFirehoseRequest(t, h, "UpdateDestination", map[string]any{
		"DeliveryStreamName":             "iceberg-update-stream",
		"CurrentDeliveryStreamVersionId": "1",
		"IcebergDestinationUpdate": map[string]any{
			"RoleARN": "arn:aws:iam::000000000000:role/firehose",
			"S3Configuration": map[string]any{
				"BucketARN": "arn:aws:s3:::iceberg-updated",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	desc := auditDescribe(t, h, "iceberg-update-stream")
	d := singleDestination(t, desc, "IcebergDestinationDescription")
	s3cfg := d["S3DestinationDescription"].(map[string]any)
	assert.Equal(t, "arn:aws:s3:::iceberg-updated", s3cfg["BucketARN"])
}
