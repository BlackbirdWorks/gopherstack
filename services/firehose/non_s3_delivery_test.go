package firehose_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

// newTestBackend returns a firehose backend suitable for unit tests.
func newTestBackend(t *testing.T) *firehose.InMemoryBackend {
	t.Helper()

	return firehose.NewInMemoryBackend("000000000000", "us-east-1")
}

// captureServer starts an httptest.Server that captures all requests.
type captureServer struct {
	requests []*capturedRequest
	srv      *httptest.Server
	mu       sync.Mutex
}

type capturedRequest struct {
	headers http.Header
	body    []byte
}

func newCaptureServer(t *testing.T, statusCode int) *captureServer {
	t.Helper()

	cs := &captureServer{}
	cs.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		cr := &capturedRequest{headers: r.Header.Clone(), body: body}
		cs.mu.Lock()
		cs.requests = append(cs.requests, cr)
		cs.mu.Unlock()
		w.WriteHeader(statusCode)
	}))

	t.Cleanup(cs.srv.Close)

	return cs
}

func (cs *captureServer) captured() []*capturedRequest {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	out := make([]*capturedRequest, len(cs.requests))
	copy(out, cs.requests)

	return out
}

// TestFirehose_HTTPEndpoint_Delivery verifies that Firehose delivers records to an HTTP
// endpoint in the AWS Firehose HTTP format: JSON body with requestId, timestamp, and
// base64-encoded records array.
func TestFirehose_HTTPEndpoint_Delivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		records        []string
		name           string
		accessKey      string
		wantRecordsLen int
		wantAccessKey  bool
	}{
		{
			name:           "SingleRecord_ValidPayload",
			records:        []string{"hello world"},
			wantRecordsLen: 1,
			wantAccessKey:  false,
		},
		{
			name:           "MultipleRecords_AllDelivered",
			records:        []string{"record-1", "record-2", "record-3"},
			wantRecordsLen: 3,
			wantAccessKey:  false,
		},
		{
			name:           "WithAccessKey_HeaderSet",
			records:        []string{"secure data"},
			accessKey:      "my-secret-key",
			wantRecordsLen: 1,
			wantAccessKey:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := newCaptureServer(t, http.StatusOK)
			b := newTestBackend(t)

			stream, err := b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{
				Name: "http-test-stream",
				HTTPEndpointDestination: &firehose.HTTPEndpointDestinationDescription{
					EndpointConfiguration: &firehose.HTTPEndpointConfiguration{
						URL:       srv.srv.URL,
						AccessKey: tt.accessKey,
					},
					RetryOptions: &firehose.RetryOptions{DurationInSeconds: 1},
					BufferingHints: &firehose.BufferingHints{
						SizeInMBs:         1,
						IntervalInSeconds: 0,
					},
				},
			})
			require.NoError(t, err)

			for _, rec := range tt.records {
				putErr := b.PutRecord(stream.Name, []byte(rec))
				require.NoError(t, putErr)
			}

			// Trigger flush.
			b.FlushAll(context.Background())

			// Allow delivery goroutine to complete.
			assert.Eventually(t, func() bool {
				return len(srv.captured()) > 0
			}, 3*time.Second, 50*time.Millisecond, "expected HTTP delivery")

			reqs := srv.captured()
			require.NotEmpty(t, reqs, "server received no requests")

			var payload struct {
				Records []struct {
					Data string `json:"data"`
				} `json:"records"`
				RequestID string `json:"requestId"`
				Timestamp int64  `json:"timestamp"`
			}
			require.NoError(t, json.Unmarshal(reqs[0].body, &payload))

			assert.NotEmpty(t, payload.RequestID)
			assert.Positive(t, payload.Timestamp)
			assert.Len(t, payload.Records, tt.wantRecordsLen)

			for i, rec := range tt.records {
				decoded, decErr := base64.StdEncoding.DecodeString(payload.Records[i].Data)
				require.NoError(t, decErr)
				assert.Equal(t, rec, string(decoded))
			}

			if tt.wantAccessKey {
				assert.Equal(t, tt.accessKey, reqs[0].headers.Get("X-Amz-Firehose-Access-Key"))
			} else {
				assert.Empty(t, reqs[0].headers.Get("X-Amz-Firehose-Access-Key"))
			}
		})
	}
}

// TestFirehose_OpenSearch_Delivery verifies that Firehose delivers records to an OpenSearch
// cluster using the bulk API (_bulk endpoint) with NDJSON format.
func TestFirehose_OpenSearch_Delivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		records    []string
		name       string
		indexName  string
		wantShards int
	}{
		{
			name:       "SingleRecord_BulkIndexed",
			records:    []string{`{"id":1}`},
			indexName:  "test-index",
			wantShards: 1,
		},
		{
			name:       "MultipleRecords_AllBulkIndexed",
			records:    []string{`{"id":1}`, `{"id":2}`, `{"id":3}`},
			indexName:  "events",
			wantShards: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := newCaptureServer(t, http.StatusOK)
			b := newTestBackend(t)

			stream, err := b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{
				Name: "opensearch-test-stream",
				OpenSearchDestination: &firehose.OpenSearchDestinationDescription{
					ClusterEndpoint: srv.srv.URL,
					IndexName:       tt.indexName,
					RetryOptions:    &firehose.RetryOptions{DurationInSeconds: 1},
					BufferingHints: &firehose.BufferingHints{
						SizeInMBs:         1,
						IntervalInSeconds: 0,
					},
				},
			})
			require.NoError(t, err)

			for _, rec := range tt.records {
				putErr := b.PutRecord(stream.Name, []byte(rec))
				require.NoError(t, putErr)
			}

			b.FlushAll(context.Background())

			assert.Eventually(t, func() bool {
				return len(srv.captured()) > 0
			}, 3*time.Second, 50*time.Millisecond, "expected OpenSearch delivery")

			reqs := srv.captured()
			require.NotEmpty(t, reqs)

			assert.Contains(t, reqs[0].headers.Get("Content-Type"), "ndjson",
				"expected ndjson content type for bulk request")

			// NDJSON: alternating action line and doc line — one action per record.
			ndjsonLines := strings.Split(strings.TrimSpace(string(reqs[0].body)), "\n")
			assert.Len(t, ndjsonLines, tt.wantShards*2,
				"expected action+doc line pairs for each record")
		})
	}
}

// TestFirehose_Splunk_Delivery verifies that Firehose delivers records to a Splunk HEC
// endpoint with the correct Authorization header and content body.
func TestFirehose_Splunk_Delivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		records     []string
		name        string
		hecToken    string
		hecType     string
		wantAuthHdr string
	}{
		{
			name:        "RawFormat_SingleRecord",
			records:     []string{"raw log line"},
			hecToken:    "my-splunk-token",
			hecType:     "Raw",
			wantAuthHdr: "Splunk my-splunk-token",
		},
		{
			name:        "EventFormat_SingleRecord_JSONWrapped",
			records:     []string{"event data"},
			hecToken:    "tok-abc",
			hecType:     "Event",
			wantAuthHdr: "Splunk tok-abc",
		},
		{
			name:        "RawFormat_MultipleRecords",
			records:     []string{"line-1", "line-2", "line-3"},
			hecToken:    "multi-tok",
			hecType:     "Raw",
			wantAuthHdr: "Splunk multi-tok",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := newCaptureServer(t, http.StatusOK)
			b := newTestBackend(t)

			stream, err := b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{
				Name: "splunk-test-stream",
				SplunkDestination: &firehose.SplunkDestinationDescription{
					HECEndpoint:     srv.srv.URL,
					HECEndpointType: tt.hecType,
					HECToken:        tt.hecToken,
					RetryOptions:    &firehose.RetryOptions{DurationInSeconds: 1},
				},
			})
			require.NoError(t, err)

			for _, rec := range tt.records {
				putErr := b.PutRecord(stream.Name, []byte(rec))
				require.NoError(t, putErr)
			}

			b.FlushAll(context.Background())

			assert.Eventually(t, func() bool {
				return len(srv.captured()) > 0
			}, 3*time.Second, 50*time.Millisecond, "expected Splunk HEC delivery")

			reqs := srv.captured()
			require.NotEmpty(t, reqs)

			assert.Equal(t, tt.wantAuthHdr, reqs[0].headers.Get("Authorization"))

			if strings.EqualFold(tt.hecType, "event") {
				for _, rec := range tt.records {
					var env struct {
						Event string `json:"event"`
					}
					assert.Contains(t, string(reqs[0].body), rec,
						"event body should contain original record data")
					_ = env
				}
			} else {
				for _, rec := range tt.records {
					assert.Contains(t, string(reqs[0].body), rec,
						"raw body should contain original record data")
				}
			}
		})
	}
}
