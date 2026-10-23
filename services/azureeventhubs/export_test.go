package azureeventhubs

import "time"

// SetNow replaces the backend clock for retention tests.
func SetNow(b *InMemoryBackend, fn func() time.Time) { b.now = fn }

// ClaimRights exposes SAS token validation.
func ClaimRights(b *InMemoryBackend, token string) ([]string, bool) { return b.claimRights(token) }

// ReceiverCount reports registered receivers across all partitions.
func ReceiverCount(b *InMemoryBackend) int {
	b.mu.RLock("ReceiverCount")
	defer b.mu.RUnlock()

	n := 0
	for _, r := range b.receivers {
		n += len(r)
	}

	return n
}
