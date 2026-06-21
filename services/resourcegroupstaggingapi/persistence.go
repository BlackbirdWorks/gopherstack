package resourcegroupstaggingapi

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// backendSnapshot is the serializable form of InMemoryBackend state.
type backendSnapshot struct {
	ReportStates map[string]*reportCreationState `json:"reportStates,omitempty"`
	AccountID    string                          `json:"accountID"`
	Region       string                          `json:"region"`
}

// Snapshot serializes the backend state to JSON.
// Providers, taggers, and untaggers are runtime callbacks and cannot be
// serialized; they must be re-registered after a Restore.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		ReportStates: b.reportStates,
		AccountID:    b.accountID,
		Region:       b.defaultRegion,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "resourcegroupstaggingapi: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot produced by Snapshot.
// Providers, taggers, and untaggers are runtime callbacks that cannot be serialized;
// they are cleared by this call and must be re-registered (e.g., via wireResourceGroupsTagging)
// after restore to re-enable cross-service tag operations.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.ReportStates != nil {
		b.reportStates = snap.ReportStates
	} else {
		b.reportStates = make(map[string]*reportCreationState)
	}

	b.accountID = snap.AccountID
	b.defaultRegion = snap.Region
	b.providers = nil
	b.filteredProviders = nil
	b.taggers = nil
	b.untaggers = nil
	clear(b.caches)

	return nil
}
