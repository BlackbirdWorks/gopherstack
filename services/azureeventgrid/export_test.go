package azureeventgrid

import (
	"net/http"
	"net/netip"
	"time"
)

// SetNow replaces the backend clock.
func SetNow(b *InMemoryBackend, fn func() time.Time) { b.now = fn }

// SetHTTPClient replaces the outbound HTTP client.
func SetHTTPClient(b *InMemoryBackend, c *http.Client) { b.httpc = c }

// PendingCount reports undelivered events of an event subscription.
func PendingCount(b *InMemoryBackend, topicName, sub string) int {
	b.mu.RLock("PendingCount")
	defer b.mu.RUnlock()

	_, s, err := b.subLocked(topicName, sub)
	if err != nil {
		return -1
	}

	return len(s.Pending)
}

// NextDue returns when the first pending event of a subscription is next attempted.
func NextDue(b *InMemoryBackend, topicName, sub string) time.Time {
	b.mu.RLock("NextDue")
	defer b.mu.RUnlock()

	_, s, err := b.subLocked(topicName, sub)
	if err != nil || len(s.Pending) == 0 {
		return time.Time{}
	}

	return s.Pending[0].Due
}

// SubscriptionState returns the provisioning state of an event subscription.
func SubscriptionState(b *InMemoryBackend, topicName, sub string) string {
	b.mu.RLock("SubscriptionState")
	defer b.mu.RUnlock()

	_, s, err := b.subLocked(topicName, sub)
	if err != nil {
		return ""
	}

	return s.State
}

// Backoff exposes the retry schedule.
func Backoff(failed int) time.Duration { return backoff(failed) }

// WebhookTargetErr returns the error for a blocked dial target.
func WebhookTargetErr() error { return errWebhookTarget }

// ParseWebhookURL exposes parseWebhookURL.
func ParseWebhookURL(raw string) error {
	_, err := parseWebhookURL(raw)

	return err
}

// BlockedWebhookAddr exposes blockedWebhookAddr.
func BlockedWebhookAddr(a netip.Addr) bool { return blockedWebhookAddr(a) }

// NewWebhookClient exposes newWebhookClient.
func NewWebhookClient(d time.Duration) *http.Client { return newWebhookClient(d) }
