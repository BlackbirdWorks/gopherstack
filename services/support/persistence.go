package support

import (
	"encoding/json"
	"log/slog"
	"time"
)

type backendSnapshot struct {
	Cases                map[string]*Case                             `json:"cases"`
	Communications       map[string][]Communication                   `json:"communications"`
	AttachmentSets       map[string]time.Time                         `json:"attachmentSets"`
	Attachments          map[string]*Attachment                       `json:"attachments"`
	CheckRefreshStatuses map[string]*TrustedAdvisorCheckRefreshStatus `json:"checkRefreshStatuses"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Cases:                b.cases,
		Communications:       b.communications,
		AttachmentSets:       b.attachmentSets,
		Attachments:          b.attachments,
		CheckRefreshStatuses: b.checkRefreshStatuses,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("support: snapshot marshal failed", "error", err)

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

	ensureNonNilMaps(&snap)

	b.cases = snap.Cases
	b.communications = snap.Communications
	b.attachmentSets = snap.AttachmentSets
	b.attachments = snap.Attachments
	b.checkRefreshStatuses = snap.CheckRefreshStatuses

	return nil
}

func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Cases == nil {
		snap.Cases = make(map[string]*Case)
	}

	if snap.Communications == nil {
		snap.Communications = make(map[string][]Communication)
	}

	if snap.AttachmentSets == nil {
		snap.AttachmentSets = make(map[string]time.Time)
	}

	if snap.Attachments == nil {
		snap.Attachments = make(map[string]*Attachment)
	}

	if snap.CheckRefreshStatuses == nil {
		snap.CheckRefreshStatuses = make(map[string]*TrustedAdvisorCheckRefreshStatus)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
