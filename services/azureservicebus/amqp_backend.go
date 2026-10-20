package azureservicebus

import (
	"slices"
	"time"
)

// AMQPBackend is the backend surface the AMQP plane needs on top of the REST
// StorageBackend.
type AMQPBackend interface {
	StorageBackend
	Defer(ref EntityRef, messageID, lockToken string) error
	ReceiveDeferred(ref EntityRef, seqs []int64, lock bool) ([]MessageInfo, error)
	RenewLock(ref EntityRef, lockToken string) (time.Time, error)
	CancelScheduled(ref EntityRef, seqs []int64) error
	FindByLockToken(ref EntityRef, lockToken string) (MessageInfo, bool, error)
}

var _ AMQPBackend = (*InMemoryBackend)(nil)

// findTokenLocked locates a currently locked message by lock token in the
// live list, then the dead-letter list.
func (mq *messageQueue) findTokenLocked(token string, now time.Time) (*storedMessage, bool) {
	if token == "" {
		return nil, false
	}

	for _, m := range mq.Messages {
		if m.LockToken == token && m.isLocked(now) && !m.isExpired(now) {
			return m, false
		}
	}

	for _, m := range mq.DeadLetter {
		if m.LockToken == token && m.isLocked(now) && !m.isExpired(now) {
			return m, true
		}
	}

	return nil, false
}

// FindByLockToken returns the locked message holding token and whether it
// sits in the dead-letter sub-queue.
func (b *InMemoryBackend) FindByLockToken(ref EntityRef, token string) (MessageInfo, bool, error) {
	b.mu.RLock("FindByLockToken")
	defer b.mu.RUnlock()

	mq, err := b.resolveMessageQueueLocked(ref)
	if err != nil {
		return MessageInfo{}, false, err
	}

	m, dl := mq.findTokenLocked(token, b.now())
	if m == nil {
		return MessageInfo{}, false, ErrMessageNotFound
	}

	return m.info(), dl, nil
}

// Defer parks a locked message; it is then retrievable only by sequence
// number via ReceiveDeferred.
func (b *InMemoryBackend) Defer(ref EntityRef, messageID, lockToken string) error {
	b.mu.Lock("Defer")
	defer b.mu.Unlock()

	mq, err := b.resolveMessageQueueLocked(ref)
	if err != nil {
		return err
	}

	_, msg, err := mq.findLocked(false, messageID, b.now())
	if err != nil {
		return err
	}

	if msg.LockToken != lockToken {
		return ErrLockTokenMismatch
	}

	msg.Deferred = true
	msg.LockToken = ""
	msg.LockedUntil = time.Time{}
	mq.idx.hintEnd = 0
	mq.broadcastLocked()

	return nil
}

// ReceiveDeferred returns deferred messages by sequence number, locking them
// (lock=true) or removing them (lock=false). Unknown sequence numbers are
// skipped.
func (b *InMemoryBackend) ReceiveDeferred(ref EntityRef, seqs []int64, lock bool) ([]MessageInfo, error) {
	b.mu.Lock("ReceiveDeferred")
	defer b.mu.Unlock()

	mq, cfg, err := b.resolveEntityLocked(ref)
	if err != nil {
		return nil, err
	}

	now := b.now()
	out := make([]MessageInfo, 0, len(seqs))

	for _, seq := range seqs {
		i := slices.IndexFunc(mq.Messages, func(m *storedMessage) bool {
			return m.SequenceNumber == seq && m.Deferred && !m.isExpired(now)
		})
		if i < 0 || (lock && mq.Messages[i].isLocked(now)) {
			continue
		}

		m := mq.Messages[i]
		m.DeliveryCount++

		if lock {
			m.LockToken = b.idFunc()
			m.LockedUntil = now.Add(cfg.lockDuration())
			out = append(out, m.info())

			continue
		}

		out = append(out, m.info())
		mq.removeAt(false, i)
	}

	return out, nil
}

// RenewLock extends the lock identified by lockToken and returns the new
// expiry. ErrMessageNotFound means the lock was lost.
func (b *InMemoryBackend) RenewLock(ref EntityRef, lockToken string) (time.Time, error) {
	b.mu.Lock("RenewLock")
	defer b.mu.Unlock()

	mq, cfg, err := b.resolveEntityLocked(ref)
	if err != nil {
		return time.Time{}, err
	}

	now := b.now()

	m, _ := mq.findTokenLocked(lockToken, now)
	if m == nil {
		return time.Time{}, ErrMessageNotFound
	}

	m.LockedUntil = now.Add(cfg.lockDuration())
	mq.idx.hintEnd = 0

	return m.LockedUntil, nil
}

// CancelScheduled removes still-pending scheduled messages. For a topic ref
// every subscription copy of the schedule is removed. Any unknown sequence
// number yields ErrMessageNotFound after the known ones were cancelled.
func (b *InMemoryBackend) CancelScheduled(ref EntityRef, seqs []int64) error {
	b.mu.Lock("CancelScheduled")
	defer b.mu.Unlock()

	queues, err := b.scheduleTargetsLocked(ref)
	if err != nil {
		return err
	}

	now := b.now()
	missing := false

	for _, seq := range seqs {
		if !cancelOneLocked(queues, seq, now) {
			missing = true
		}
	}

	if missing {
		return ErrMessageNotFound
	}

	return nil
}

func (b *InMemoryBackend) scheduleTargetsLocked(ref EntityRef) ([]*messageQueue, error) {
	if ref.IsQueue() || ref.Subscription != "" {
		mq, err := b.resolveMessageQueueLocked(ref)
		if err != nil {
			return nil, err
		}

		return []*messageQueue{mq}, nil
	}

	t, ok := b.topics[ref.Topic]
	if !ok {
		return nil, ErrTopicNotFound
	}

	out := make([]*messageQueue, 0, len(t.Subscriptions))
	for _, sub := range sortedSubscriptions(t) {
		out = append(out, &sub.messageQueue)
	}

	return out, nil
}

func cancelOneLocked(queues []*messageQueue, seq int64, now time.Time) bool {
	found := false

	for _, mq := range queues {
		for i, m := range slices.Backward(mq.Messages) {
			pending := !m.ScheduledFor.IsZero() && m.LockToken == "" && m.LockedUntil.After(now)

			if pending && (m.SequenceNumber == seq || m.SchedGroup == seq) {
				mq.removeAt(false, i)

				found = true
			}
		}
	}

	return found
}
