package sns

import (
	"encoding/json"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	Topics        map[string]*Topic        `json:"topics"`
	Subscriptions map[string]*Subscription `json:"subscriptions"`
	TopicTags     map[string]*svcTags.Tags `json:"topicTags"`
	AccountID     string                   `json:"accountID"`
	Region        string                   `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Topics:        b.topics,
		Subscriptions: b.subscriptions,
		TopicTags:     b.topicTags,
		AccountID:     b.accountID,
		Region:        b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// The event emitter is not restored — it is re-wired by the CLI after restore.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Topics == nil {
		snap.Topics = make(map[string]*Topic)
	}

	if snap.Subscriptions == nil {
		snap.Subscriptions = make(map[string]*Subscription)
	}

	if snap.TopicTags == nil {
		snap.TopicTags = make(map[string]*svcTags.Tags)
	}

	b.topics = snap.Topics
	b.subscriptions = snap.Subscriptions
	b.topicTags = snap.TopicTags
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}
