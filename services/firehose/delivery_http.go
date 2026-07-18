package firehose

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// httpDeliveryTimeout is the maximum time allowed for a single HTTP endpoint delivery attempt.
const httpDeliveryTimeout = 30 * time.Second

// httpMaxRetryDuration is the default max retry window when RetryOptions is not set.
const httpMaxRetryDuration = 300 * time.Second

// buildHTTPEndpointBody encodes records into the AWS Firehose HTTP endpoint JSON payload.
func buildHTTPEndpointBody(records [][]byte) ([]byte, error) {
	type httpRecord struct {
		Data string `json:"data"`
	}
	type httpPayload struct {
		RequestID string       `json:"requestId"`
		Records   []httpRecord `json:"records"`
		Timestamp int64        `json:"timestamp"`
	}

	httpRecords := make([]httpRecord, 0, len(records))
	for _, rec := range records {
		httpRecords = append(httpRecords, httpRecord{Data: base64.StdEncoding.EncodeToString(rec)})
	}

	return json.Marshal(httpPayload{
		RequestID: uuid.NewString(),
		Timestamp: time.Now().UnixMilli(),
		Records:   httpRecords,
	})
}

// buildHTTPEndpointRequest constructs a single POST request for the HTTP endpoint delivery loop.
func buildHTTPEndpointRequest(
	ctx context.Context,
	endpointURL, accessKey string,
	dest *HTTPEndpointDestinationDescription,
	body []byte,
) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	if accessKey != "" {
		req.Header.Set("X-Amz-Firehose-Access-Key", accessKey)
	}

	if dest.RequestConfiguration != nil {
		for _, attr := range dest.RequestConfiguration.CommonAttributes {
			req.Header.Set(attr.AttributeName, attr.AttributeValue)
		}
	}

	return req, nil
}

// deliverToHTTPEndpoint POSTs records to a Firehose HTTP endpoint destination using the
// AWS Firehose HTTP endpoint delivery format. Retries are attempted within the configured
// RetryOptions.DurationInSeconds window (default 300s) with exponential back-off.
func (b *InMemoryBackend) deliverToHTTPEndpoint(
	ctx context.Context,
	records [][]byte,
	dest *HTTPEndpointDestinationDescription,
	streamARN string,
) {
	if dest.EndpointConfiguration == nil || dest.EndpointConfiguration.URL == "" {
		return
	}

	endpointURL := dest.EndpointConfiguration.URL
	accessKey := dest.EndpointConfiguration.AccessKey

	body, err := buildHTTPEndpointBody(records)
	if err != nil {
		logger.Load(ctx).
			WarnContext(ctx, "firehose: failed to marshal HTTP endpoint payload", "error", err, "stream", streamARN)

		return
	}

	maxRetry := httpMaxRetryDuration
	if dest.RetryOptions != nil && dest.RetryOptions.DurationInSeconds > 0 {
		maxRetry = time.Duration(dest.RetryOptions.DurationInSeconds) * time.Second
	}

	deadline := time.Now().Add(maxRetry)
	backoff := 1 * time.Second
	client := &http.Client{Timeout: httpDeliveryTimeout}

	for {
		req, reqErr := buildHTTPEndpointRequest(ctx, endpointURL, accessKey, dest, body)
		if reqErr != nil {
			logger.Load(ctx).
				WarnContext(ctx, "firehose: failed to build HTTP endpoint request", "error", reqErr, "stream", streamARN)

			return
		}

		resp, doErr := client.Do(req)
		if checkHTTPDeliveryResponse(ctx, resp, doErr) {
			return
		}

		if time.Now().After(deadline) {
			logger.Load(ctx).WarnContext(ctx, "firehose: HTTP endpoint delivery failed after retries",
				"url", endpointURL, "stream", streamARN)

			return
		}

		if !httpDeliveryBackoff(ctx, deadline, &backoff) {
			return
		}
	}
}

const httpBackoffMaxInterval = 30 * time.Second

// httpDeliveryBackoff waits for the next retry interval or returns false if the context
// is cancelled. backoff is doubled on each call and capped at httpBackoffMaxInterval.
// Returns true when the caller should proceed with the next attempt, false when it should stop.
func httpDeliveryBackoff(ctx context.Context, deadline time.Time, backoff *time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(*backoff):
		*backoff *= 2
		if *backoff > httpBackoffMaxInterval {
			*backoff = httpBackoffMaxInterval
		}

		return time.Now().Before(deadline)
	}
}

// checkHTTPDeliveryResponse closes the response body and returns true when the
// response indicates success (2xx status). When doErr is non-nil or the status
// is outside 2xx it returns false so the caller retries.
func checkHTTPDeliveryResponse(ctx context.Context, resp *http.Response, doErr error) bool {
	if doErr != nil {
		return false
	}
	if closeErr := resp.Body.Close(); closeErr != nil {
		logger.Load(ctx).WarnContext(ctx, "firehose: failed to close HTTP response body", "error", closeErr)
	}

	return resp.StatusCode >= 200 && resp.StatusCode < 300
}
