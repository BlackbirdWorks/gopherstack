package sqs

import (
	"strconv"
	"time"
)

// DedupMapLen returns the number of entries currently in the deduplication map
// for the named FIFO queue. Used only in tests.
func (b *InMemoryBackend) DedupMapLen(queueName string) int {
	b.mu.RLock("DedupMapLen")
	defer b.mu.RUnlock()

	q, ok := b.lookupQueueByName("", queueName)
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

	q, ok := b.lookupQueueByName("", queueName)
	if !ok {
		return
	}

	expired := time.Now().Add(-deduplicationWindowSecs * time.Second * 2)
	q.DeduplicationIDs[dedupID] = expired
	q.deduplicationMsgIDs[dedupID] = "injected-" + dedupID
}

func (b *InMemoryBackend) RunJanitorOnceForTest(now time.Time) {
	b.pruneState(now)
}

func (b *InMemoryBackend) InjectMoveTaskForTest(
	taskHandle string,
	status MoveTaskStatus,
	startedAt int64,
) {
	b.mu.Lock("InjectMoveTaskForTest")
	defer b.mu.Unlock()

	b.moveTasks.Put(&moveTaskState{
		cancel:     func() {},
		taskHandle: taskHandle,
		status:     status,
		startedAt:  startedAt,
	})
}

func (b *InMemoryBackend) MoveTaskCountForTest() int {
	b.mu.RLock("MoveTaskCountForTest")
	defer b.mu.RUnlock()

	return b.moveTasks.Len()
}

// SetRetentionForTest directly overrides the MessageRetentionPeriod attribute
// for the queue at queueURL without going through attribute range validation.
// This exists solely to allow unit tests to use sub-60-second retention windows
// for fast testing of message expiry behaviour.
func (b *InMemoryBackend) SetRetentionForTest(queueURL string, seconds int) {
	b.mu.Lock("SetRetentionForTest")
	defer b.mu.Unlock()

	if q, ok := b.lookupQueueByURL("", queueURL); ok {
		q.Attributes[attrMessageRetentionPeriod] = strconv.Itoa(seconds)
	}
}
