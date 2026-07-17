package firehose

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// openSearchBulkTimeout is the HTTP timeout for an OpenSearch bulk index request.
const openSearchBulkTimeout = 30 * time.Second

// buildOpenSearchBulkBody assembles the NDJSON bulk payload for the OpenSearch _bulk API.
// Returns nil when there are no records to send.
func buildOpenSearchBulkBody(records [][]byte) []byte {
	var buf bytes.Buffer
	actionLine := []byte(`{"index":{}}` + "\n")
	for _, rec := range records {
		buf.Write(actionLine)
		buf.Write(rec)
		if len(rec) == 0 || rec[len(rec)-1] != '\n' {
			buf.WriteByte('\n')
		}
	}

	if buf.Len() == 0 {
		return nil
	}

	return buf.Bytes()
}

// deliverToOpenSearch bulk-indexes records into an OpenSearch / Elasticsearch cluster.
// Records are sent as NDJSON using the OpenSearch bulk API (_bulk endpoint).
// Each record becomes one "index" action; the document body is the raw record bytes
// decoded as JSON (or wrapped in {"data":"<base64>"} when the bytes are not valid JSON).
func (b *InMemoryBackend) deliverToOpenSearch(
	ctx context.Context,
	records [][]byte,
	dest *OpenSearchDestinationDescription,
	streamARN string,
) {
	endpoint := dest.ClusterEndpoint
	if endpoint == "" {
		// Derive endpoint from domain ARN: arn:aws:es:<region>:<account>:domain/<name>
		// Local OpenSearch is assumed at http://localhost:9200 in dev/test.
		endpoint = "http://localhost:9200"
	}

	endpoint = strings.TrimRight(endpoint, "/")
	indexName := dest.IndexName
	if indexName == "" {
		indexName = "firehose"
	}

	bulkURL := fmt.Sprintf("%s/%s/_bulk", endpoint, indexName)

	bodyBytes := buildOpenSearchBulkBody(records)
	if bodyBytes == nil {
		return
	}

	maxRetry := httpMaxRetryDuration
	if dest.RetryOptions != nil && dest.RetryOptions.DurationInSeconds > 0 {
		maxRetry = time.Duration(dest.RetryOptions.DurationInSeconds) * time.Second
	}

	deadline := time.Now().Add(maxRetry)
	backoff := 1 * time.Second
	client := &http.Client{Timeout: openSearchBulkTimeout}

	for {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, bulkURL, bytes.NewReader(bodyBytes))
		if reqErr != nil {
			logger.Load(ctx).WarnContext(ctx,
				"firehose: failed to build OpenSearch bulk request", "error", reqErr, "stream", streamARN)

			return
		}

		req.Header.Set("Content-Type", "application/x-ndjson")

		resp, doErr := client.Do(req)
		if checkHTTPDeliveryResponse(ctx, resp, doErr) {
			return
		}

		if time.Now().After(deadline) {
			logger.Load(ctx).WarnContext(ctx, "firehose: OpenSearch delivery failed after retries",
				"url", bulkURL, "stream", streamARN)

			return
		}

		if !httpDeliveryBackoff(ctx, deadline, &backoff) {
			return
		}
	}
}
