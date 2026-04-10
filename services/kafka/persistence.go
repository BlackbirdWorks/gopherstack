package kafka

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Clusters          map[string]*Cluster          `json:"clusters"`
	Configurations    map[string]*Configuration    `json:"configurations"`
	ScramSecrets      map[string][]string          `json:"scramSecrets"`
	Replicators       map[string]*Replicator       `json:"replicators"`
	Topics            map[string]*Topic            `json:"topics"`
	VpcConnections    map[string]*VpcConnection    `json:"vpcConnections"`
	ClusterPolicies   map[string]string            `json:"clusterPolicies"`
	ClusterOperations map[string]*ClusterOperation `json:"clusterOperations"`
	AccountID         string                       `json:"accountID"`
	Region            string                       `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
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
		slog.Default().Warn("kafka: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
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

// ensureNonNilMaps initialises nil maps in the snapshot to empty maps.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Clusters == nil {
		snap.Clusters = make(map[string]*Cluster)
	}

	if snap.Configurations == nil {
		snap.Configurations = make(map[string]*Configuration)
	}

	if snap.ScramSecrets == nil {
		snap.ScramSecrets = make(map[string][]string)
	}

	if snap.Replicators == nil {
		snap.Replicators = make(map[string]*Replicator)
	}

	if snap.Topics == nil {
		snap.Topics = make(map[string]*Topic)
	}

	if snap.VpcConnections == nil {
		snap.VpcConnections = make(map[string]*VpcConnection)
	}

	if snap.ClusterPolicies == nil {
		snap.ClusterPolicies = make(map[string]string)
	}

	if snap.ClusterOperations == nil {
		snap.ClusterOperations = make(map[string]*ClusterOperation)
	}
}

// fixNilTags ensures restored resources have non-nil tag maps.
func fixNilTags(snap *backendSnapshot) {
	for _, c := range snap.Clusters {
		if c.Tags == nil {
			c.Tags = make(map[string]string)
		}
	}

	for _, c := range snap.Configurations {
		if c.Tags == nil {
			c.Tags = make(map[string]string)
		}
	}

	for _, r := range snap.Replicators {
		if r.Tags == nil {
			r.Tags = make(map[string]string)
		}
	}

	for _, v := range snap.VpcConnections {
		if v.Tags == nil {
			v.Tags = make(map[string]string)
		}
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
