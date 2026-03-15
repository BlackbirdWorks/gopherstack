package ses

import (
	"encoding/json"
	"maps"
	"time"
)

type backendSnapshot struct {
	Identities map[string]bool          `json:"identities"`
	Templates  map[string]EmailTemplate `json:"templates"`
	ConfigSets map[string]struct{}      `json:"configSets"`
	Emails     []Email                  `json:"emails"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	ids := make(map[string]bool, len(b.identities))
	maps.Copy(ids, b.identities)

	emails := make([]Email, len(b.emails))
	copy(emails, b.emails)

	tmpls := make(map[string]EmailTemplate, len(b.templates))
	maps.Copy(tmpls, b.templates)

	cfgs := make(map[string]struct{}, len(b.configSets))
	maps.Copy(cfgs, b.configSets)

	snap := backendSnapshot{
		Identities: ids,
		Emails:     emails,
		Templates:  tmpls,
		ConfigSets: cfgs,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Identities == nil {
		snap.Identities = make(map[string]bool)
	}

	if snap.Emails == nil {
		snap.Emails = []Email{}
	}

	if snap.Templates == nil {
		snap.Templates = make(map[string]EmailTemplate)
	}

	if snap.ConfigSets == nil {
		snap.ConfigSets = make(map[string]struct{})
	}

	b.identities = snap.Identities
	b.templates = snap.Templates
	b.configSets = snap.ConfigSets

	// Drop emails outside the current TTL window and cap to maxRetainedEmails
	// so that memory is bounded immediately after restore.
	cutoff := time.Now().Add(-b.emailTTL)

	start := 0

	for start < len(snap.Emails) && snap.Emails[start].Timestamp.Before(cutoff) {
		start++
	}

	valid := snap.Emails[start:]

	if len(valid) > maxRetainedEmails {
		valid = valid[len(valid)-maxRetainedEmails:]
	}

	b.emails = make([]Email, len(valid))
	copy(b.emails, valid)

	// Rebuild O(1) lookup map from the pruned slice.
	b.emailsByID = make(map[string]Email, len(b.emails))
	for _, e := range b.emails {
		b.emailsByID[e.MessageID] = e
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
