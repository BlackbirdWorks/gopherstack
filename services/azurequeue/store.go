package azurequeue

import (
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// Default and bound values for Put/Get Messages query parameters, matching
// real Azure Queue Storage's documented limits.
const (
	// DefaultVisibilityTimeout is applied when a caller omits
	// visibilitytimeout on Put Message / Get Messages.
	DefaultVisibilityTimeout = 30 * time.Second
	// DefaultMessageTTL is applied when a caller omits messagettl on Put
	// Message (or passes -1, Azure's "infinite" sentinel is not modeled --
	// see PARITY.md known gaps).
	DefaultMessageTTL = 7 * 24 * time.Hour
	// MaxNumOfMessages is the largest numofmessages Get/Peek Messages
	// accepts per call.
	MaxNumOfMessages = 32
	// MinNumOfMessages is the smallest numofmessages Get/Peek Messages
	// accepts per call.
	MinNumOfMessages = 1
)

// InMemoryBackend implements StorageBackend using in-memory maps guarded by
// a single RWMutex. Shaped after services/azureblob's InMemoryBackend.
type InMemoryBackend struct {
	mu     *lockmetrics.RWMutex
	queues map[string]*storedQueue
	// nowFunc is the backend's time source, overridable in tests (see
	// export_test.go's SetNowFunc) so visibility-timeout/TTL logic can be
	// exercised deterministically instead of via real sleeps.
	nowFunc func() time.Time
	// idFunc generates message IDs and pop receipts, overridable in tests
	// for deterministic assertions.
	idFunc func() string
}

// NewInMemoryBackend creates a new empty InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		mu:      lockmetrics.New("azurequeue"),
		queues:  make(map[string]*storedQueue),
		nowFunc: time.Now,
		idFunc:  uuid.NewString,
	}
}

func (b *InMemoryBackend) now() time.Time { return b.nowFunc().UTC() }

// CreateQueue creates a new, empty queue. If a queue with the same name
// already exists, created is false and err is nil -- this backend has no
// queue metadata (see PARITY.md known gaps), so any pre-existing queue is by
// definition "the same metadata" and Create is idempotent (204), matching
// real Azure Queue Storage's semantics for a metadata-identical retry.
// ErrQueueAlreadyExists is reserved for a future metadata-bearing Create.
func (b *InMemoryBackend) CreateQueue(name string) (bool, error) {
	b.mu.Lock("CreateQueue")
	defer b.mu.Unlock()

	if _, ok := b.queues[name]; ok {
		return false, nil
	}

	b.queues[name] = &storedQueue{
		Name:      name,
		CreatedAt: b.now(),
	}

	return true, nil
}

// DeleteQueue removes a queue and all of its messages. Returns
// ErrQueueNotFound if the queue does not exist.
func (b *InMemoryBackend) DeleteQueue(name string) error {
	b.mu.Lock("DeleteQueue")
	defer b.mu.Unlock()

	if _, ok := b.queues[name]; !ok {
		return ErrQueueNotFound
	}

	delete(b.queues, name)

	return nil
}

// ListQueues returns a snapshot of all queues, sorted by name (the order
// Azure's List Queues returns them in).
func (b *InMemoryBackend) ListQueues() []QueueInfo {
	b.mu.RLock("ListQueues")
	defer b.mu.RUnlock()

	out := make([]QueueInfo, 0, len(b.queues))
	for _, q := range b.queues {
		out = append(out, QueueInfo{Name: q.Name, CreatedAt: q.CreatedAt})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// PutMessage enqueues a new message on queue. visibilityTimeout delays the
// message's initial visibility (0 means immediately visible); ttl bounds how
// long the message survives before the janitor sweeps it (0 uses
// DefaultMessageTTL). Returns ErrQueueNotFound if the queue does not exist.
func (b *InMemoryBackend) PutMessage(queue, text string, visibilityTimeout, ttl time.Duration) (MessageInfo, error) {
	b.mu.Lock("PutMessage")
	defer b.mu.Unlock()

	q, ok := b.queues[queue]
	if !ok {
		return MessageInfo{}, ErrQueueNotFound
	}

	if ttl <= 0 {
		ttl = DefaultMessageTTL
	}

	now := b.now()
	msg := &storedMessage{
		ID:              b.idFunc(),
		Text:            text,
		InsertionTime:   now,
		ExpirationTime:  now.Add(ttl),
		NextVisibleTime: now.Add(visibilityTimeout),
		PopReceipt:      b.idFunc(),
	}
	q.Messages = append(q.Messages, msg)

	return msg.info(true), nil
}

// GetMessages dequeues up to numOfMessages visible, non-expired messages
// from queue: each returned message is hidden for visibilityTimeout (its
// NextVisibleTime advances, so subsequent Get/Peek calls skip it until that
// timeout elapses), assigned a fresh PopReceipt, and has its DequeueCount
// incremented. Returns ErrQueueNotFound if the queue does not exist.
func (b *InMemoryBackend) GetMessages(
	queue string, numOfMessages int, visibilityTimeout time.Duration,
) ([]MessageInfo, error) {
	b.mu.Lock("GetMessages")
	defer b.mu.Unlock()

	q, ok := b.queues[queue]
	if !ok {
		return nil, ErrQueueNotFound
	}

	if numOfMessages < MinNumOfMessages || numOfMessages > MaxNumOfMessages {
		return nil, ErrOutOfRangeQueryParam
	}

	now := b.now()
	nextVisible := now.Add(visibilityTimeout)

	out := make([]MessageInfo, 0, numOfMessages)

	for _, msg := range q.Messages {
		if len(out) >= numOfMessages {
			break
		}

		if msg.isExpired(now) || !msg.isVisible(now) {
			continue
		}

		msg.DequeueCount++
		msg.PopReceipt = b.idFunc()
		msg.NextVisibleTime = nextVisible
		out = append(out, msg.info(true))
	}

	return out, nil
}

// PeekMessages returns up to numOfMessages visible, non-expired messages
// from queue without changing their visibility, PopReceipt, or DequeueCount.
// Returns ErrQueueNotFound if the queue does not exist.
func (b *InMemoryBackend) PeekMessages(queue string, numOfMessages int) ([]MessageInfo, error) {
	b.mu.RLock("PeekMessages")
	defer b.mu.RUnlock()

	q, ok := b.queues[queue]
	if !ok {
		return nil, ErrQueueNotFound
	}

	if numOfMessages < MinNumOfMessages || numOfMessages > MaxNumOfMessages {
		return nil, ErrOutOfRangeQueryParam
	}

	now := b.now()

	out := make([]MessageInfo, 0, numOfMessages)

	for _, msg := range q.Messages {
		if len(out) >= numOfMessages {
			break
		}

		if msg.isExpired(now) || !msg.isVisible(now) {
			continue
		}

		out = append(out, msg.info(false))
	}

	return out, nil
}

// DeleteMessage removes a message identified by messageID from queue, after
// verifying popReceipt matches its current value. Returns ErrQueueNotFound,
// ErrMessageNotFound, or ErrPopReceiptMismatch as appropriate.
func (b *InMemoryBackend) DeleteMessage(queue, messageID, popReceipt string) error {
	b.mu.Lock("DeleteMessage")
	defer b.mu.Unlock()

	q, ok := b.queues[queue]
	if !ok {
		return ErrQueueNotFound
	}

	idx, msg, err := findMessageLocked(q, messageID, b.now())
	if err != nil {
		return err
	}

	if msg.PopReceipt != popReceipt {
		return ErrPopReceiptMismatch
	}

	q.Messages = append(q.Messages[:idx], q.Messages[idx+1:]...)

	return nil
}

// UpdateMessage sets a new visibility timeout (and, if text is non-nil,
// replaces the message body) for messageID after verifying popReceipt
// matches, then rotates the PopReceipt. Returns ErrQueueNotFound,
// ErrMessageNotFound, or ErrPopReceiptMismatch as appropriate.
func (b *InMemoryBackend) UpdateMessage(
	queue, messageID, popReceipt string, visibilityTimeout time.Duration, text *string,
) (MessageInfo, error) {
	b.mu.Lock("UpdateMessage")
	defer b.mu.Unlock()

	q, ok := b.queues[queue]
	if !ok {
		return MessageInfo{}, ErrQueueNotFound
	}

	_, msg, err := findMessageLocked(q, messageID, b.now())
	if err != nil {
		return MessageInfo{}, err
	}

	if msg.PopReceipt != popReceipt {
		return MessageInfo{}, ErrPopReceiptMismatch
	}

	msg.NextVisibleTime = b.now().Add(visibilityTimeout)
	msg.PopReceipt = b.idFunc()

	if text != nil {
		msg.Text = *text
	}

	return msg.info(true), nil
}

// ClearMessages removes every message from queue. Returns ErrQueueNotFound
// if the queue does not exist.
func (b *InMemoryBackend) ClearMessages(queue string) error {
	b.mu.Lock("ClearMessages")
	defer b.mu.Unlock()

	q, ok := b.queues[queue]
	if !ok {
		return ErrQueueNotFound
	}

	q.Messages = nil

	return nil
}

// Reset clears all in-memory state. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.queues = make(map[string]*storedQueue)
}

// findMessageLocked resolves a message by ID within q, as of now. A message
// that has reached its expiration instant (see storedMessage.isExpired) is
// treated as not found even if the Janitor has not yet swept it: real Azure
// Queue Storage deletes a message once its TTL elapses and rejects
// Delete/Update against an expired one, so DeleteMessage/UpdateMessage must
// not be able to observe or mutate it in the gap between expiry and sweep.
// Callers must hold b.mu (either read or write).
func findMessageLocked(q *storedQueue, messageID string, now time.Time) (int, *storedMessage, error) {
	for i, msg := range q.Messages {
		if msg.ID == messageID && !msg.isExpired(now) {
			return i, msg, nil
		}
	}

	return 0, nil, ErrMessageNotFound
}

// sweepExpired removes every expired message (see storedMessage.isExpired)
// from every queue, as of now. Returns the number of messages removed.
// Called by Janitor.
func (b *InMemoryBackend) sweepExpired(now time.Time) int {
	b.mu.Lock("sweepExpired")
	defer b.mu.Unlock()

	removed := 0

	for _, q := range b.queues {
		kept := q.Messages[:0]

		for _, msg := range q.Messages {
			if msg.isExpired(now) {
				removed++

				continue
			}

			kept = append(kept, msg)
		}

		q.Messages = kept
	}

	return removed
}
