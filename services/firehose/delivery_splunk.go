package firehose

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// splunkHECTimeout is the HTTP timeout for a Splunk HEC request.
const splunkHECTimeout = 30 * time.Second

// buildSplunkBody assembles the request body and content-type for a Splunk HEC delivery.
// hecType should be the lower-cased HECEndpointType value.
// Returns (nil, "") when the resulting body is empty.
func buildSplunkBody(records [][]byte, hecType string) ([]byte, string) {
	if hecType == "event" {
		type hecEvent struct {
			Event string `json:"event"`
		}

		var buf bytes.Buffer
		for _, rec := range records {
			line, marshalErr := json.Marshal(hecEvent{Event: string(rec)})
			if marshalErr != nil {
				continue
			}
			buf.Write(line)
		}

		if buf.Len() == 0 {
			return nil, ""
		}

		return buf.Bytes(), "application/json"
	}

	var buf bytes.Buffer
	for _, rec := range records {
		buf.Write(rec)
		if len(rec) == 0 || rec[len(rec)-1] != '\n' {
			buf.WriteByte('\n')
		}
	}

	if buf.Len() == 0 {
		return nil, ""
	}

	return buf.Bytes(), "text/plain"
}

// deliverToSplunk POSTs records to a Splunk HTTP Event Collector (HEC) endpoint.
// Each record is sent as a separate JSON event in the HEC raw format, batched
// into a single POST when the HEC endpoint type is "Raw" (default).
// HECEndpointType "Event" wraps each record in the HEC event JSON envelope.
func (b *InMemoryBackend) deliverToSplunk(
	ctx context.Context,
	records [][]byte,
	dest *SplunkDestinationDescription,
	streamARN string,
) {
	if dest.HECEndpoint == "" {
		return
	}

	hecURL := strings.TrimRight(dest.HECEndpoint, "/")

	body, contentType := buildSplunkBody(records, strings.ToLower(dest.HECEndpointType))
	if len(body) == 0 {
		return
	}

	maxRetry := httpMaxRetryDuration
	if dest.RetryOptions != nil && dest.RetryOptions.DurationInSeconds > 0 {
		maxRetry = time.Duration(dest.RetryOptions.DurationInSeconds) * time.Second
	}

	deadline := time.Now().Add(maxRetry)
	backoff := 1 * time.Second
	client := &http.Client{Timeout: splunkHECTimeout}

	for {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, hecURL, bytes.NewReader(body))
		if reqErr != nil {
			logger.Load(ctx).
				WarnContext(ctx, "firehose: failed to build Splunk HEC request", "error", reqErr, "stream", streamARN)

			return
		}

		req.Header.Set("Content-Type", contentType)
		if dest.HECToken != "" {
			req.Header.Set("Authorization", "Splunk "+dest.HECToken)
		}

		resp, doErr := client.Do(req)
		if checkHTTPDeliveryResponse(ctx, resp, doErr) {
			return
		}

		if time.Now().After(deadline) {
			logger.Load(ctx).WarnContext(ctx, "firehose: Splunk HEC delivery failed after retries",
				"url", hecURL, "stream", streamARN)

			return
		}

		if !httpDeliveryBackoff(ctx, deadline, &backoff) {
			return
		}
	}
}
