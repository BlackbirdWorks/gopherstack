package sqs

import "time"

// DedupMapLen returns the number of entries currently in the deduplication map
// for the named FIFO queue. Used only in tests.
func (b *InMemoryBackend) DedupMapLen(queueName string) int {
	b.mu.RLock("DedupMapLen")
	defer b.mu.RUnlock()

	q, ok := b.queues[queueName]
	if !ok {
		return -1
	}

	return len(q.DeduplicationIDs)
}

// InjectExpiredDedupID inserts a deduplication entry into the named FIFO queue
// with an expiry in the past so that the next call to pruneDedup will remove it.
// Used only in tests to simulate deduplication window expiry.
func (b *InMemoryBackend) InjectExpiredDedupID(queueName, dedupID string) {
	b.mu.Lock("InjectExpiredDedupID")
	defer b.mu.Unlock()

	q, ok := b.queues[queueName]
	if !ok {
		return
	}

	expired := time.Now().Add(-deduplicationWindowSecs * time.Second * 2)
	q.DeduplicationIDs[dedupID] = expired
	q.deduplicationMsgIDs[dedupID] = "injected-" + dedupID
}
