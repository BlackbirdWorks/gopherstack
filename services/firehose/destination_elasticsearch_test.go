package firehose_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

// TestElasticsearchDestination_CreateAndDescribe verifies that CreateDeliveryStream
// accepts the legacy ElasticsearchDestinationConfiguration (wire-distinct from the newer
// AmazonopensearchserviceDestinationConfiguration family) and that DescribeDeliveryStream
// returns it under the exact AWS wire key "ElasticsearchDestinationDescription".
func TestElasticsearchDestination_CreateAndDescribe(t *testing.T) {
	t.Parallel()

	h, _ := auditHandler(t)

	streamName := "es-desc-stream"
	auditCreateStream(t, h, streamName, map[string]any{
		"ElasticsearchDestinationConfiguration": map[string]any{
			"DomainARN":           "arn:aws:es:us-east-1:000000000000:domain/legacy-domain",
			"IndexName":           "access-logs",
			"IndexRotationPeriod": "OneDay",
			"RoleARN":             "arn:aws:iam::000000000000:role/firehose",
			"S3Configuration": map[string]any{
				"BucketARN": "arn:aws:s3:::es-backup-bucket",
				"RoleARN":   "arn:aws:iam::000000000000:role/firehose",
			},
		},
	})

	desc := auditDescribe(t, h, streamName)
	d := singleDestination(t, desc, "ElasticsearchDestinationDescription")

	assert.Equal(t, "arn:aws:es:us-east-1:000000000000:domain/legacy-domain", d["DomainARN"])
	assert.Equal(t, "access-logs", d["IndexName"])
	assert.Equal(t, "OneDay", d["IndexRotationPeriod"])

	backup := d["S3BackupDescription"].(map[string]any)
	assert.Equal(t, "arn:aws:s3:::es-backup-bucket", backup["BucketARN"])
}

// TestElasticsearchDestination_Delivery verifies that records for a legacy Elasticsearch
// destination are bulk-indexed via the OpenSearch-compatible _bulk API, the same wire
// protocol used for the newer Amazonopensearchservice family.
func TestElasticsearchDestination_Delivery(t *testing.T) {
	t.Parallel()

	srv := newCaptureServer(t, http.StatusOK)
	b := newTestBackend(t)

	stream, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
		Name: "es-delivery-stream",
		ElasticsearchDestination: &firehose.ElasticsearchDestinationDescription{
			ClusterEndpoint: srv.srv.URL,
			IndexName:       "legacy-index",
			RetryOptions:    &firehose.RetryOptions{DurationInSeconds: 1},
			BufferingHints: &firehose.BufferingHints{
				SizeInMBs:         1,
				IntervalInSeconds: 0,
			},
		},
	})
	require.NoError(t, err)

	require.NoError(t, b.PutRecord(context.TODO(), stream.Name, []byte(`{"id":1}`)))
	b.FlushAll(context.Background())

	assert.Eventually(t, func() bool {
		return len(srv.captured()) > 0
	}, 3*time.Second, 50*time.Millisecond, "expected Elasticsearch bulk delivery")

	reqs := srv.captured()
	require.NotEmpty(t, reqs)
	assert.Contains(t, reqs[0].headers.Get("Content-Type"), "ndjson")
}

// TestElasticsearchDestination_Update verifies UpdateDestination can switch a stream to a
// legacy Elasticsearch destination and that the change is reflected in a subsequent
// Describe.
func TestElasticsearchDestination_Update(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "es-update-stream")

	rec := doFirehoseRequest(t, h, "UpdateDestination", map[string]any{
		"DeliveryStreamName":             "es-update-stream",
		"CurrentDeliveryStreamVersionId": "1",
		"ElasticsearchDestinationUpdate": map[string]any{
			"DomainARN": "arn:aws:es:us-east-1:000000000000:domain/updated-domain",
			"IndexName": "updated-index",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	desc := auditDescribe(t, h, "es-update-stream")
	d := singleDestination(t, desc, "ElasticsearchDestinationDescription")
	assert.Equal(t, "updated-index", d["IndexName"])
}
