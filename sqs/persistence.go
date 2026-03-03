package sqs

import (
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// queueSnapshot captures all serialisable fields of a Queue.
type queueSnapshot struct {
	DeduplicationIDs    map[string]time.Time `json:"deduplicationIDs"`
	Attributes          map[string]string    `json:"attributes"`
	Tags                *tags.Tags           `json:"tags,omitempty"`
	DeduplicationMsgIDs map[string]string    `json:"deduplicationMsgIDs"`
	Name                string               `json:"name"`
	URL                 string               `json:"url"`
	Messages            []*Message           `json:"messages"`
	InFlightMessages    []*InFlightMessage   `json:"inFlightMessages"`
	MaxReceiveCount     int                  `json:"maxReceiveCount"`
	IsFIFO              bool                 `json:"isFIFO"`
}

type backendSnapshot struct {
	Queues    map[string]*queueSnapshot `json:"queues"`
	AccountID string                    `json:"accountID"`
	Region    string                    `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	queues := make(map[string]*queueSnapshot, len(b.queues))
	for k, q := range b.queues {
		queues[k] = &queueSnapshot{
			DeduplicationIDs:    q.DeduplicationIDs,
			Attributes:          q.Attributes,
			Tags:                q.Tags,
			Messages:            q.messages,
			InFlightMessages:    q.inFlightMessages,
			DeduplicationMsgIDs: q.deduplicationMsgIDs,
			Name:                q.Name,
			URL:                 q.URL,
			MaxReceiveCount:     q.MaxReceiveCount,
			IsFIFO:              q.IsFIFO,
		}
	}

	snap := backendSnapshot{
		Queues:    queues,
		AccountID: b.accountID,
		Region:    b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// The dlq pointer is not restored — DLQ wiring must be re-established after restore.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Queues == nil {
		snap.Queues = make(map[string]*queueSnapshot)
	}

	b.queues = make(map[string]*Queue, len(snap.Queues))

	for k, qs := range snap.Queues {
		if qs.DeduplicationIDs == nil {
			qs.DeduplicationIDs = make(map[string]time.Time)
		}

		if qs.Attributes == nil {
			qs.Attributes = make(map[string]string)
		}

		if qs.DeduplicationMsgIDs == nil {
			qs.DeduplicationMsgIDs = make(map[string]string)
		}

		b.queues[k] = &Queue{
			DeduplicationIDs:    qs.DeduplicationIDs,
			Attributes:          qs.Attributes,
			Tags:                qs.Tags,
			messages:            qs.Messages,
			inFlightMessages:    qs.InFlightMessages,
			deduplicationMsgIDs: qs.DeduplicationMsgIDs,
			Name:                qs.Name,
			URL:                 qs.URL,
			MaxReceiveCount:     qs.MaxReceiveCount,
			IsFIFO:              qs.IsFIFO,
		}
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}
