package ociqueue

import (
	"slices"
	"time"
)

// PeekedMessage is a read-only view of one stored message; it carries no receipt.
type PeekedMessage struct {
	CreatedAt     time.Time         `json:"createdAt"`
	VisibleAfter  time.Time         `json:"visibleAfter"`
	ExpireAfter   time.Time         `json:"expireAfter"`
	Properties    map[string]string `json:"customProperties,omitempty"`
	Content       string            `json:"content"`
	Channel       string            `json:"channelId,omitempty"`
	ID            int64             `json:"id"`
	DeliveryCount int               `json:"deliveryCount"`
	InFlight      bool              `json:"inFlight"`
}

// Compartments lists the distinct compartments that hold live queues.
func (b *InMemoryBackend) Compartments() []string {
	b.mu.RLock("Compartments")
	defer b.mu.RUnlock()

	seen := map[string]struct{}{}

	for _, q := range b.queues {
		if q.State != stateDeleted {
			seen[q.CompartmentID] = struct{}{}
		}
	}

	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}

	slices.Sort(out)

	return out
}

// Peek returns up to limit messages of a queue or its dead letter queue without delivering them: no receipt is
// issued, delivery counts and visibility stay untouched.
func (b *InMemoryBackend) Peek(queueID string, dlq bool, limit int) ([]PeekedMessage, error) {
	if aerr := inRange("limit", &limit, 1, maxGetLimit); aerr != nil {
		return nil, aerr
	}

	b.mu.Lock("Peek")
	defer b.mu.Unlock()

	q, aerr := b.dataQueue(queueID)
	if aerr != nil {
		return nil, aerr
	}

	src := q.Messages
	if dlq {
		src = q.DLQ
	}

	now := b.nowUTC()
	out := make([]PeekedMessage, 0, maxGetLimit)

	for _, m := range src {
		if len(out) >= limit {
			break
		}

		out = append(out, PeekedMessage{
			ID: m.ID, Content: m.Content, Channel: m.Channel, Properties: m.Props, DeliveryCount: m.Deliveries,
			CreatedAt: m.Created, VisibleAfter: m.VisibleAfter, ExpireAfter: m.Expire,
			InFlight: m.VisibleAfter.After(now),
		})
	}

	return out, nil
}
