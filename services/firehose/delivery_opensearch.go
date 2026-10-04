package firehose

import (
	"bytes"
	"context"
	"encoding/json"
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

// OpenSearchIndexer is the in-process OpenSearch document store a domain-ARN destination
// delivers into; wired via SetOpenSearchBackend.
type OpenSearchIndexer interface {
	IndexDocument(domainName, indexName string, doc map[string]any) error
}

// RegionalOpenSearchIndexer is an OpenSearchIndexer that can target the domain of a specific region.
type RegionalOpenSearchIndexer interface {
	IndexDocumentInRegion(region, domainName, indexName string, doc map[string]any) error
}

// SetOpenSearchBackend wires the in-process OpenSearch document store.
func (b *InMemoryBackend) SetOpenSearchBackend(o OpenSearchIndexer) {
	b.mu.Lock("SetOpenSearchBackend")
	defer b.mu.Unlock()

	b.opensearch = o
}

func (b *InMemoryBackend) openSearchIndexer() OpenSearchIndexer {
	b.mu.RLock("openSearchIndexer")
	defer b.mu.RUnlock()

	return b.opensearch
}

// openSearchIndexName applies IndexRotationPeriod to the base index name.
func openSearchIndexName(base, rotation string, t time.Time) string {
	if base == "" {
		base = "firehose"
	}

	switch rotation {
	case "OneHour":
		return base + "-" + t.Format("2006-01-02-15")
	case "OneDay":
		return base + "-" + t.Format("2006-01-02")
	case "OneWeek":
		year, week := t.ISOWeek()

		return fmt.Sprintf("%s-%d-w%02d", base, year, week)
	case "OneMonth":
		return base + "-" + t.Format("2006-01")
	default:
		return base
	}
}

// arnRegionOf returns the region field of an ARN, or "" when it has none.
func arnRegionOf(arn string) string {
	const regionIdx, arnFields = 3, 6

	parts := strings.SplitN(arn, ":", arnFields)
	if len(parts) <= regionIdx {
		return ""
	}

	return parts[regionIdx]
}

// domainNameFromARN extracts the domain name from arn:aws:es:<region>:<acct>:domain/<name>.
func domainNameFromARN(arn string) string {
	_, name, ok := strings.Cut(arn, ":domain/")
	if !ok {
		return ""
	}

	return name
}

// deliverToOpenSearch indexes records into the in-process OpenSearch domain named by
// DomainARN, else bulk-posts them to ClusterEndpoint. It returns failure envelopes.
func (b *InMemoryBackend) deliverToOpenSearch(
	ctx context.Context,
	records [][]byte,
	dest *OpenSearchDestinationDescription,
	streamARN string,
) [][]byte {
	index := openSearchIndexName(dest.IndexName, dest.IndexRotationPeriod, time.Now().UTC())

	if dest.ClusterEndpoint == "" && dest.DomainARN != "" {
		if code := b.authorizeESWrite(dest.RoleARN, dest.DomainARN); code != "" {
			return failAllRecords(records, code, "delivery role denied")
		}

		if idx := b.openSearchIndexer(); idx != nil {
			return indexInProcess(idx, dest.DomainARN, index, records)
		}
	}

	return b.bulkToEndpoint(ctx, records, dest, index, streamARN)
}

func failAllRecords(records [][]byte, code, msg string) [][]byte {
	out := make([][]byte, 0, len(records))
	for _, rec := range records {
		out = append(out, failureRecord(rec, "", code, msg, 1))
	}

	return out
}

// indexInProcess indexes each JSON-object record as one document, failing the others.
func indexInProcess(idx OpenSearchIndexer, domainARN, index string, records [][]byte) [][]byte {
	var failed [][]byte

	domain := domainNameFromARN(domainARN)
	put := func(doc map[string]any) error { return idx.IndexDocument(domain, index, doc) }

	if ri, ok := idx.(RegionalOpenSearchIndexer); ok {
		region := arnRegionOf(domainARN)
		put = func(doc map[string]any) error { return ri.IndexDocumentInRegion(region, domain, index, doc) }
	}

	for _, rec := range records {
		var doc map[string]any
		if err := json.Unmarshal(rec, &doc); err != nil {
			failed = append(
				failed,
				failureRecord(rec, "", "ES.JsonProcessingException", "record is not a JSON object", 1),
			)

			continue
		}

		if err := put(doc); err != nil {
			failed = append(failed, failureRecord(rec, "", "ES.ServiceException", err.Error(), 1))
		}
	}

	return failed
}

func (b *InMemoryBackend) bulkToEndpoint(
	ctx context.Context,
	records [][]byte,
	dest *OpenSearchDestinationDescription,
	index, streamARN string,
) [][]byte {
	endpoint := dest.ClusterEndpoint
	if endpoint == "" {
		endpoint = "http://localhost:9200"
	}

	bulkURL := fmt.Sprintf("%s/%s/_bulk", strings.TrimRight(endpoint, "/"), index)

	bodyBytes := buildOpenSearchBulkBody(records)
	if bodyBytes == nil {
		return nil
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
				"firehose: failed to build OpenSearch bulk request", "stream", streamARN)

			return failAllRecords(records, "ES.ServiceException", "failed to build request")
		}

		req.Header.Set("Content-Type", "application/x-ndjson")

		resp, doErr := client.Do(req)
		if checkHTTPDeliveryResponse(ctx, resp, doErr) {
			return nil
		}

		if time.Now().After(deadline) || !httpDeliveryBackoff(ctx, deadline, &backoff) {
			logger.Load(ctx).WarnContext(ctx, "firehose: OpenSearch delivery failed after retries", "stream", streamARN)

			return failAllRecords(records, "ES.ServiceException", "bulk request failed after retries")
		}
	}
}
