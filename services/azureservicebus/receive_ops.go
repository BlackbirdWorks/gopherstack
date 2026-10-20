package azureservicebus

import "time"

// Peek implements StorageBackend.Peek.
func (b *InMemoryBackend) Peek(ref EntityRef, deadLetter bool, fromSeq int64, maxCount int) ([]MessageInfo, error) {
	if maxCount < 0 {
		maxCount = 0
	}

	if maxCount > maxPeekCount {
		maxCount = maxPeekCount
	}

	b.mu.RLock("Peek")
	defer b.mu.RUnlock()

	mq, err := b.resolveMessageQueueLocked(ref)
	if err != nil {
		return nil, err
	}

	now := b.now()
	out := []MessageInfo{}

	for _, msg := range mq.liveList(deadLetter) {
		if len(out) >= maxCount {
			break
		}

		if msg.SequenceNumber < fromSeq || msg.isExpired(now) {
			continue
		}

		info := msg.info()
		info.LockToken = ""
		info.LockedUntil = time.Time{}
		out = append(out, info)
	}

	return out, nil
}

// ReceiveAndDelete implements StorageBackend.ReceiveAndDelete.
func (b *InMemoryBackend) ReceiveAndDelete(ref EntityRef, deadLetter bool) (MessageInfo, error) {
	b.mu.Lock("ReceiveAndDelete")
	defer b.mu.Unlock()

	mq, err := b.resolveMessageQueueLocked(ref)
	if err != nil {
		return MessageInfo{}, err
	}

	now := b.now()

	for i, msg := range mq.liveList(deadLetter) {
		if msg.isExpired(now) || msg.isLocked(now) {
			continue
		}

		msg.DeliveryCount++
		info := msg.info()
		mq.removeAt(deadLetter, i)

		return info, nil
	}

	return MessageInfo{}, ErrMessageNotFound
}

// DeadLetter implements StorageBackend.DeadLetter.
func (b *InMemoryBackend) DeadLetter(ref EntityRef, messageID, lockToken, reason, description string) error {
	b.mu.Lock("DeadLetter")
	defer b.mu.Unlock()

	mq, err := b.resolveMessageQueueLocked(ref)
	if err != nil {
		return err
	}

	now := b.now()

	idx, msg, err := mq.findLocked(false, messageID, now)
	if err != nil {
		return err
	}

	if msg.LockToken != lockToken {
		return ErrLockTokenMismatch
	}

	mq.removeAt(false, idx)

	msg.LockToken = ""
	msg.LockedUntil = time.Time{}
	msg.ExpiresAt = now.Add(DefaultMessageTTL)
	msg.DeadLetterReason = reason
	msg.DeadLetterErrorDescription = description
	mq.DeadLetter = append(mq.DeadLetter, msg)
	mq.broadcastLocked()

	return nil
}
