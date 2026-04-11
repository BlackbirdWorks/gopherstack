package resourcegroups

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Groups              map[string]*Group                   `json:"groups"`
	GroupConfigurations map[string][]GroupConfigurationItem `json:"groupConfigurations"`
	GroupResources      map[string][]string                 `json:"groupResources"`
	GroupingStatuses    map[string][]GroupingStatusItem     `json:"groupingStatuses"`
	TagSyncTasks        map[string]*TagSyncTask             `json:"tagSyncTasks"`
	AccountSettings     AccountSettings                     `json:"accountSettings"`
	AccountID           string                              `json:"accountID"`
	Region              string                              `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Groups:              b.groups,
		GroupConfigurations: b.groupConfigurations,
		GroupResources:      b.groupResources,
		GroupingStatuses:    b.groupingStatuses,
		TagSyncTasks:        b.tagSyncTasks,
		AccountSettings:     b.accountSettings,
		AccountID:           b.accountID,
		Region:              b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("resourcegroups: failed to snapshot backend", "error", err)

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

	if snap.Groups == nil {
		snap.Groups = make(map[string]*Group)
	}

	if snap.GroupConfigurations == nil {
		snap.GroupConfigurations = make(map[string][]GroupConfigurationItem)
	}

	if snap.GroupResources == nil {
		snap.GroupResources = make(map[string][]string)
	}

	if snap.GroupingStatuses == nil {
		snap.GroupingStatuses = make(map[string][]GroupingStatusItem)
	}

	if snap.TagSyncTasks == nil {
		snap.TagSyncTasks = make(map[string]*TagSyncTask)
	}

	// Close existing Tags to release Prometheus metrics before replacing state.
	for _, g := range b.groups {
		g.Tags.Close()
	}

	b.groups = snap.Groups
	b.groupConfigurations = snap.GroupConfigurations
	b.groupResources = snap.GroupResources
	b.groupingStatuses = snap.GroupingStatuses
	b.tagSyncTasks = snap.TagSyncTasks
	b.accountSettings = snap.AccountSettings
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild ARN index.
	b.arnIndex = make(map[string]string, len(b.groups))
	for name, g := range b.groups {
		b.arnIndex[g.ARN] = name
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

// Reset implements service.Resettable by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
