package iotdataplane

import (
	"fmt"
	"time"
)

// StoreRetainedMessage saves a retained MQTT message for the given topic.
// Calling this with an empty payload removes the retained message for that topic.
// When the cap is reached, the oldest entry (by LastModifiedTime) is evicted to
// make room, matching AWS LRU behaviour and preventing silent publish failures.
func (b *InMemoryBackend) StoreRetainedMessage(topic string, payload []byte, qos int32) error {
	if len(payload) > 0 && len(payload) > maxPublishBodyBytes {
		return fmt.Errorf("%w: retained payload exceeds %d bytes", ErrValidation, maxPublishBodyBytes)
	}

	b.mu.Lock("StoreRetainedMessage")
	defer b.mu.Unlock()

	if len(payload) == 0 {
		b.retainedMessages.Delete(topic)

		return nil
	}

	// LRU eviction: when the cap is reached and the topic is new, evict the oldest entry.
	if !b.retainedMessages.Has(topic) && b.retainedMessages.Len() >= maxRetainedMessages {
		b.evictOldestRetained()
	}

	cp := make([]byte, len(payload))
	copy(cp, payload)

	b.retainedMessages.Put(&RetainedMessage{
		Topic:            topic,
		Payload:          cp,
		Qos:              qos,
		LastModifiedTime: time.Now().UnixMilli(),
	})

	return nil
}

// evictOldestRetained removes the retained message with the oldest LastModifiedTime.
// Must be called with b.mu held for writing.
func (b *InMemoryBackend) evictOldestRetained() {
	var oldestTopic string

	var oldestTime int64 = -1

	for _, msg := range b.retainedMessages.All() {
		if oldestTime < 0 || msg.LastModifiedTime < oldestTime {
			oldestTime = msg.LastModifiedTime
			oldestTopic = msg.Topic
		}
	}

	if oldestTopic != "" {
		b.retainedMessages.Delete(oldestTopic)
	}
}

// GetRetainedMessage returns the retained message stored for the given topic.
// ErrRetainedMessageNotFound is returned when no retained message exists for the topic.
func (b *InMemoryBackend) GetRetainedMessage(topic string) (*RetainedMessage, error) {
	b.mu.RLock("GetRetainedMessage")
	defer b.mu.RUnlock()

	msg, ok := b.retainedMessages.Get(topic)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRetainedMessageNotFound, topic)
	}

	cp := *msg
	if len(msg.Payload) > 0 {
		cp.Payload = make([]byte, len(msg.Payload))
		copy(cp.Payload, msg.Payload)
	}

	return &cp, nil
}

// ListRetainedMessages returns summaries of all retained messages, sorted by topic.
func (b *InMemoryBackend) ListRetainedMessages() ([]*RetainedMessage, error) {
	b.mu.RLock("ListRetainedMessages")
	defer b.mu.RUnlock()

	msgs := b.retainedMessages.Snapshot()
	result := make([]*RetainedMessage, 0, len(msgs))

	for _, msg := range msgs {
		cp := *msg
		if len(msg.Payload) > 0 {
			cp.Payload = make([]byte, len(msg.Payload))
			copy(cp.Payload, msg.Payload)
		}

		result = append(result, &cp)
	}

	return result, nil
}
