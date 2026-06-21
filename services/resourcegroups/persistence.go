package resourcegroups

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	Groups              map[string]map[string]*Group                   `json:"groups"`
	GroupConfigurations map[string]map[string][]GroupConfigurationItem `json:"groupConfigurations"`
	GroupResources      map[string]map[string][]string                 `json:"groupResources"`
	GroupingStatuses    map[string]map[string][]GroupingStatusItem     `json:"groupingStatuses"`
	TagSyncTasks        map[string]map[string]*TagSyncTask             `json:"tagSyncTasks"`
	AccountSettings     AccountSettings                                `json:"accountSettings"`
	AccountID           string                                         `json:"accountID"`
	Region              string                                         `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
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
		logger.Load(ctx).WarnContext(ctx, "resourcegroups: failed to snapshot backend", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	ensureSnapMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.closeAllGroupTags()

	b.groups = snap.Groups
	b.groupConfigurations = snap.GroupConfigurations
	b.groupResources = snap.GroupResources
	b.groupingStatuses = snap.GroupingStatuses
	b.tagSyncTasks = snap.TagSyncTasks
	b.accountSettings = snap.AccountSettings
	b.accountID = snap.AccountID
	b.region = snap.Region

	b.reinitGroupTags()
	b.rebuildARNIndex()

	return nil
}

// ensureSnapMaps initialises any nil region-nested maps in a freshly decoded snapshot.
func ensureSnapMaps(snap *backendSnapshot) {
	if snap.Groups == nil {
		snap.Groups = make(map[string]map[string]*Group)
	}

	if snap.GroupConfigurations == nil {
		snap.GroupConfigurations = make(map[string]map[string][]GroupConfigurationItem)
	}

	if snap.GroupResources == nil {
		snap.GroupResources = make(map[string]map[string][]string)
	}

	if snap.GroupingStatuses == nil {
		snap.GroupingStatuses = make(map[string]map[string][]GroupingStatusItem)
	}

	if snap.TagSyncTasks == nil {
		snap.TagSyncTasks = make(map[string]map[string]*TagSyncTask)
	}
}

// closeAllGroupTags releases Prometheus metrics for every group before replacing state.
// Must be called under b.mu (write lock).
func (b *InMemoryBackend) closeAllGroupTags() {
	for _, regionGroups := range b.groups {
		for _, g := range regionGroups {
			g.Tags.Close()
		}
	}
}

// reinitGroupTags replaces deserialized Tags with properly named instances to avoid
// Prometheus label collisions from the generic "json.tags" name used during JSON
// deserialization. Must be called under b.mu (write lock).
func (b *InMemoryBackend) reinitGroupTags() {
	for _, regionGroups := range b.groups {
		for name, g := range regionGroups {
			if g.Tags != nil {
				tagMap := g.Tags.Clone()
				g.Tags.Close()
				g.Tags = tags.FromMap("rg."+name+".tags", tagMap)
			} else {
				g.Tags = tags.New("rg." + name + ".tags")
			}
		}
	}
}

// rebuildARNIndex reconstructs the region-nested ARN → group name lookup from b.groups.
// Must be called under b.mu (write lock).
func (b *InMemoryBackend) rebuildARNIndex() {
	b.arnIndex = make(map[string]map[string]string, len(b.groups))

	for region, regionGroups := range b.groups {
		idx := make(map[string]string, len(regionGroups))

		for name, g := range regionGroups {
			idx[g.ARN] = name
		}

		b.arnIndex[region] = idx
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

// Reset implements service.Resettable by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
