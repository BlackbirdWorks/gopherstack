package gcpcloudtasks

import (
	"net/http"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
)

// SetClock replaces the backend clock.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// SetHTTPClient replaces the dispatch client.
func (b *InMemoryBackend) SetHTTPClient(c *http.Client) {
	b.mu.Lock("SetHTTPClient")
	defer b.mu.Unlock()

	b.client = c
}

// Backoff exposes the retry delay computation.
func Backoff(rc *cloudtaskspb.RetryConfig, failures int32) time.Duration {
	return backoff(rc, failures)
}
