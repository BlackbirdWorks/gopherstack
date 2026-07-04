package support

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	Cases                map[string]*Case                             `json:"cases"`
	Communications       map[string][]Communication                   `json:"communications"`
	AttachmentSets       map[string]*AttachmentSet                    `json:"attachmentSets"`
	Attachments          map[string]*Attachment                       `json:"attachments"`
	CheckRefreshStatuses map[string]*TrustedAdvisorCheckRefreshStatus `json:"checkRefreshStatuses"`
	CheckResults         map[string]*TrustedAdvisorCheckResult        `json:"checkResults,omitempty"`
	NextDisplayID        uint64                                       `json:"nextDisplayId"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Cases:                b.cases,
		Communications:       b.communications,
		AttachmentSets:       b.attachmentSets,
		Attachments:          b.attachments,
		CheckRefreshStatuses: b.checkRefreshStatuses,
		CheckResults:         b.checkResults,
		NextDisplayID:        b.nextDisplayID,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "support: snapshot marshal failed", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "support", data, &snap); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	ensureNonNilMaps(&snap)

	b.cases = snap.Cases
	b.communications = snap.Communications
	b.attachmentSets = snap.AttachmentSets
	b.attachments = snap.Attachments
	b.checkRefreshStatuses = snap.CheckRefreshStatuses
	b.checkResults = snap.CheckResults
	b.nextDisplayID = snap.NextDisplayID

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
		snap.AttachmentSets = make(map[string]*AttachmentSet)
	}

	if snap.Attachments == nil {
		snap.Attachments = make(map[string]*Attachment)
	}

	if snap.CheckRefreshStatuses == nil {
		snap.CheckRefreshStatuses = make(map[string]*TrustedAdvisorCheckRefreshStatus)
	}

	if snap.CheckResults == nil {
		snap.CheckResults = make(map[string]*TrustedAdvisorCheckResult)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
