package firehose

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// httpDeliveryTimeout is how long an endpoint has to answer one request (3 minutes per the guide).
const httpDeliveryTimeout = 3 * time.Minute

// httpMaxRetryDuration is the default max retry window when RetryOptions is not set.
const httpMaxRetryDuration = 300 * time.Second

const (
	httpMaxBatchRecords  = 10000
	httpMaxResponseBytes = 1 << 20
	httpContentGZIP      = "GZIP"
	headerRequestID      = "X-Amz-Firehose-Request-Id"
)

var errHTTPResponse = errors.New("endpoint response does not follow the Firehose contract")

// httpAttemptResult classifies one delivery attempt.
type httpAttemptResult struct {
	code      string
	message   string
	ok        bool
	permanent bool
}

type httpRecord struct {
	Data string `json:"data"`
}

type httpPayload struct {
	RequestID string       `json:"requestId"`
	Records   []httpRecord `json:"records"`
	Timestamp int64        `json:"timestamp"`
}

// buildHTTPEndpointBody encodes records into the documented request body.
func buildHTTPEndpointBody(requestID string, records [][]byte) ([]byte, error) {
	httpRecords := make([]httpRecord, 0, len(records))
	for _, rec := range records {
		httpRecords = append(httpRecords, httpRecord{Data: base64.StdEncoding.EncodeToString(rec)})
	}

	return json.Marshal(httpPayload{RequestID: requestID, Timestamp: time.Now().UnixMilli(), Records: httpRecords})
}

// buildHTTPEndpointRequest constructs one POST with the documented Firehose request headers.
func buildHTTPEndpointRequest(
	ctx context.Context,
	dest *HTTPEndpointDestinationDescription,
	requestID, streamARN string,
	body []byte,
) (*http.Request, error) {
	cfg := dest.EndpointConfiguration

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amz-Firehose-Protocol-Version", "1.0")
	req.Header.Set(headerRequestID, requestID)
	req.Header.Set("X-Amz-Firehose-Source-Arn", streamARN)

	if cfg.AccessKey != "" {
		req.Header.Set("X-Amz-Firehose-Access-Key", cfg.AccessKey)
	}

	if rc := dest.RequestConfiguration; rc != nil {
		if strings.EqualFold(rc.ContentEncoding, httpContentGZIP) {
			req.Header.Set("Content-Encoding", "gzip")
		}

		if len(rc.CommonAttributes) > 0 {
			attrs := make(map[string]string, len(rc.CommonAttributes))
			for _, a := range rc.CommonAttributes {
				attrs[a.AttributeName] = a.AttributeValue
			}

			if raw, mErr := json.Marshal(map[string]any{"commonAttributes": attrs}); mErr == nil {
				req.Header.Set("X-Amz-Firehose-Common-Attributes", string(raw))
			}
		}
	}

	return req, nil
}

// deliverToHTTPEndpoint POSTs batches per the guide's contract, retrying within RetryOptions; it
// returns envelopes for batches that exhausted retries (a 413 is permanent and not written).
func (b *InMemoryBackend) deliverToHTTPEndpoint(
	ctx context.Context,
	records [][]byte,
	dest *HTTPEndpointDestinationDescription,
	streamARN string,
) [][]byte {
	if dest.EndpointConfiguration == nil || dest.EndpointConfiguration.URL == "" {
		return nil
	}

	var failed [][]byte

	for batch := range slices.Chunk(records, httpMaxBatchRecords) {
		res := deliverHTTPBatch(ctx, dest, batch, streamARN)
		if res.ok || res.permanent {
			continue
		}

		for _, rec := range batch {
			failed = append(failed, failureRecord(rec, "", res.code, res.message, 1))
		}
	}

	return failed
}

func deliverHTTPBatch(
	ctx context.Context, dest *HTTPEndpointDestinationDescription, batch [][]byte, streamARN string,
) httpAttemptResult {
	requestID := uuid.NewString()

	body, err := buildHTTPEndpointBody(requestID, batch)
	if err == nil && dest.RequestConfiguration != nil &&
		strings.EqualFold(dest.RequestConfiguration.ContentEncoding, httpContentGZIP) {
		body, err = gzipCompress(body)
	}

	if err != nil {
		return httpAttemptResult{code: "HttpEndpoint.MakeRequestFailure", message: "failed to build request"}
	}

	maxRetry := httpMaxRetryDuration
	if dest.RetryOptions != nil && dest.RetryOptions.DurationInSeconds > 0 {
		maxRetry = time.Duration(dest.RetryOptions.DurationInSeconds) * time.Second
	}

	deadline := time.Now().Add(maxRetry)
	backoff := time.Second
	client := &http.Client{
		Timeout:       httpDeliveryTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	for {
		res := attemptHTTPDelivery(ctx, client, dest, requestID, streamARN, body)
		if res.ok || res.permanent || time.Now().After(deadline) || !httpDeliveryBackoff(ctx, deadline, &backoff) {
			return res
		}
	}
}

func attemptHTTPDelivery(
	ctx context.Context,
	client *http.Client,
	dest *HTTPEndpointDestinationDescription,
	requestID, streamARN string,
	body []byte,
) httpAttemptResult {
	req, err := buildHTTPEndpointRequest(ctx, dest, requestID, streamARN, body)
	if err != nil {
		return httpAttemptResult{code: "HttpEndpoint.MakeRequestFailure", message: "failed to build request"}
	}

	resp, err := client.Do(req)
	if err != nil {
		return httpAttemptResult{code: "HttpEndpoint.ConnectionFailed", message: "request to the endpoint failed"}
	}

	defer func() { _ = resp.Body.Close() }()

	raw, _ := io.ReadAll(io.LimitReader(resp.Body, httpMaxResponseBytes+1))

	switch {
	case resp.StatusCode == http.StatusRequestEntityTooLarge:
		return httpAttemptResult{code: "HttpEndpoint.DestinationException", message: "413", permanent: true}
	case resp.StatusCode == http.StatusOK:
		if vErr := validateHTTPResponse(resp, raw, requestID); vErr != nil {
			return httpAttemptResult{code: "HttpEndpoint.InvalidResponseFromDestination", message: vErr.Error()}
		}

		return httpAttemptResult{ok: true}
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		return httpAttemptResult{code: "HttpEndpoint.InvalidStatusCode", message: "redirects are not followed"}
	default:
		return httpAttemptResult{
			code:    "HttpEndpoint.DestinationException",
			message: endpointErrorMessage(raw, resp.StatusCode),
		}
	}
}

// validateHTTPResponse enforces the documented 200 response: JSON content type, no content
// encoding, and a body carrying the request's requestId and a timestamp.
func validateHTTPResponse(resp *http.Response, raw []byte, requestID string) error {
	ctype, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if ctype != "application/json" || resp.Header.Get("Content-Encoding") != "" || len(raw) > httpMaxResponseBytes {
		return errHTTPResponse
	}

	var out struct {
		Timestamp *json.RawMessage `json:"timestamp"`
		RequestID string           `json:"requestId"`
	}
	if err := json.Unmarshal(raw, &out); err != nil || out.RequestID != requestID || out.Timestamp == nil {
		return errHTTPResponse
	}

	return nil
}

func endpointErrorMessage(raw []byte, status int) string {
	var out struct {
		ErrorMessage string `json:"errorMessage"`
	}
	if json.Unmarshal(raw, &out) == nil && out.ErrorMessage != "" {
		return out.ErrorMessage
	}

	return "endpoint returned status " + strconv.Itoa(status)
}

const (
	httpBackoffMaxInterval = 2 * time.Minute
	backoffJitter          = 0.15
	jitterBuckets          = 1000
	backoffMultiplier      = 2
)

// httpDeliveryBackoff waits the next jittered retry interval (doubling up to the 2 minute
// cap), never past the retry deadline so one final attempt lands on it. False when ctx is done.
func httpDeliveryBackoff(ctx context.Context, deadline time.Time, backoff *time.Duration) bool {
	jitter := 1 - backoffJitter + 2*backoffJitter*float64(time.Now().UnixNano()%jitterBuckets)/jitterBuckets
	wait := min(time.Duration(float64(*backoff)*jitter), max(time.Until(deadline), 0))
	t := time.NewTimer(wait)

	defer t.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		*backoff = min(*backoff*backoffMultiplier, httpBackoffMaxInterval)

		return true
	}
}

// checkHTTPDeliveryResponse closes the response body and reports whether it was a 2xx.
func checkHTTPDeliveryResponse(ctx context.Context, resp *http.Response, doErr error) bool {
	if doErr != nil {
		return false
	}
	if closeErr := resp.Body.Close(); closeErr != nil {
		logger.Load(ctx).WarnContext(ctx, "firehose: failed to close HTTP response body", "error", closeErr)
	}

	return resp.StatusCode >= 200 && resp.StatusCode < 300
}
