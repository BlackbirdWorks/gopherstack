package kafka

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// backendSnapshot is the persisted form of the backend state. All resource maps
// are nested by region (outer key = region) to mirror the in-memory layout and
// keep same-named resources in different regions fully isolated across restarts.
type backendSnapshot struct {
	Clusters          map[string]map[string]*Cluster          `json:"clusters"`
	Configurations    map[string]map[string]*Configuration    `json:"configurations"`
	ScramSecrets      map[string]map[string][]string          `json:"scramSecrets"`
	Replicators       map[string]map[string]*Replicator       `json:"replicators"`
	Topics            map[string]map[string]*Topic            `json:"topics"`
	VpcConnections    map[string]map[string]*VpcConnection    `json:"vpcConnections"`
	ClusterPolicies   map[string]map[string]string            `json:"clusterPolicies"`
	ClusterOperations map[string]map[string]*ClusterOperation `json:"clusterOperations"`
	AccountID         string                                  `json:"accountID"`
	Region            string                                  `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Clusters:          b.clusters,
		Configurations:    b.configurations,
		ScramSecrets:      b.scramSecrets,
		Replicators:       b.replicators,
		Topics:            b.topics,
		VpcConnections:    b.vpcConnections,
		ClusterPolicies:   b.clusterPolicies,
		ClusterOperations: b.clusterOperations,
		AccountID:         b.accountID,
		Region:            b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "kafka: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "kafka", data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)
	fixNilTags(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.clusters = snap.Clusters
	b.configurations = snap.Configurations
	b.scramSecrets = snap.ScramSecrets
	b.replicators = snap.Replicators
	b.topics = snap.Topics
	b.vpcConnections = snap.VpcConnections
	b.clusterPolicies = snap.ClusterPolicies
	b.clusterOperations = snap.ClusterOperations
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// ensureNonNilMaps initialises nil region maps in the snapshot to empty maps.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Clusters == nil {
		snap.Clusters = make(map[string]map[string]*Cluster)
	}

	if snap.Configurations == nil {
		snap.Configurations = make(map[string]map[string]*Configuration)
	}

	if snap.ScramSecrets == nil {
		snap.ScramSecrets = make(map[string]map[string][]string)
	}

	if snap.Replicators == nil {
		snap.Replicators = make(map[string]map[string]*Replicator)
	}

	if snap.Topics == nil {
		snap.Topics = make(map[string]map[string]*Topic)
	}

	if snap.VpcConnections == nil {
		snap.VpcConnections = make(map[string]map[string]*VpcConnection)
	}

	if snap.ClusterPolicies == nil {
		snap.ClusterPolicies = make(map[string]map[string]string)
	}

	if snap.ClusterOperations == nil {
		snap.ClusterOperations = make(map[string]map[string]*ClusterOperation)
	}
}

// fixNilTags ensures restored resources have non-nil tag maps, across every region.
func fixNilTags(snap *backendSnapshot) {
	fixRegionTags(snap.Clusters, func(c *Cluster) *map[string]string { return &c.Tags })
	fixRegionTags(snap.Configurations, func(c *Configuration) *map[string]string { return &c.Tags })
	fixRegionTags(snap.Replicators, func(r *Replicator) *map[string]string { return &r.Tags })
	fixRegionTags(snap.VpcConnections, func(v *VpcConnection) *map[string]string { return &v.Tags })
}

// fixRegionTags walks a region-nested resource map and replaces any nil tag map
// (located via tagsOf) with an empty map so restored resources are tag-safe.
func fixRegionTags[T any](byRegion map[string]map[string]*T, tagsOf func(*T) *map[string]string) {
	for _, byKey := range byRegion {
		for _, item := range byKey {
			tags := tagsOf(item)
			if *tags == nil {
				*tags = make(map[string]string)
			}
		}
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
