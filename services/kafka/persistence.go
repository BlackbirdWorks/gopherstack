package kafka

import (
	"context"
	"encoding/json"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// kafkaSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to backendSnapshot (or the shape of any table it
// carries) would make an older snapshot unsafe to decode as the current
// shape. Restore compares this against the persisted value and discards
// (ResetAll, not a partial decode) any mismatch -- see Restore. The
// pre-Phase-3.3 snapshot format had no version field at all, so an old
// snapshot decodes with Version == 0, which is guaranteed to mismatch
// kafkaSnapshotVersion and is discarded the same way any other incompatible
// snapshot is.
const kafkaSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the kafka (MSK) backend.
//
// Tables holds one JSON-encoded array per store.Table registered on
// b.registry (clusters, configurations, replicators, topics, vpcConnections,
// clusterOperations -- see store_setup.go), produced by
// b.registry.SnapshotAll(). Every kafka resource's identity field already
// carries a normal JSON tag, so none of these need a DTO layer to round-trip
// (contrast services/ses's "dirty" tables).
//
// ScramSecrets and ClusterPolicies persist the two fields left as plain maps
// (their values are not *T, so they don't fit store.Table) alongside Tables.
type backendSnapshot struct {
	Tables          map[string]json.RawMessage `json:"tables"`
	ScramSecrets    map[string][]string        `json:"scramSecrets"`
	ClusterPolicies map[string]string          `json:"clusterPolicies"`
	AccountID       string                     `json:"accountID"`
	Region          string                     `json:"region"`
	Version         int                        `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "kafka: snapshot table marshal failed", "error", err)

		return nil
	}

	scramSecrets := make(map[string][]string, len(b.scramSecrets))
	for clusterArn, secrets := range b.scramSecrets {
		scramSecrets[clusterArn] = append([]string(nil), secrets...)
	}

	clusterPolicies := make(map[string]string, len(b.clusterPolicies))
	maps.Copy(clusterPolicies, b.clusterPolicies)

	snap := backendSnapshot{
		Version:         kafkaSnapshotVersion,
		Tables:          tables,
		ScramSecrets:    scramSecrets,
		ClusterPolicies: clusterPolicies,
		AccountID:       b.accountID,
		Region:          b.region,
	}

	return persistence.MarshalSnapshot(ctx, "kafka", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "kafka", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != kafkaSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"kafka: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", kafkaSnapshotVersion)

		b.registry.ResetAll()
		b.scramSecrets = make(map[string][]string)
		b.clusterPolicies = make(map[string]string)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return err
	}

	b.scramSecrets = ensureNonNilScramSecrets(snap.ScramSecrets)
	b.clusterPolicies = ensureNonNilClusterPolicies(snap.ClusterPolicies)
	fixNilTags(b)
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// ensureNonNilScramSecrets returns m unchanged if non-nil, else an empty map,
// so a restored backend never carries a nil scramSecrets field.
func ensureNonNilScramSecrets(m map[string][]string) map[string][]string {
	if m == nil {
		return make(map[string][]string)
	}

	return m
}

// ensureNonNilClusterPolicies returns m unchanged if non-nil, else an empty
// map, so a restored backend never carries a nil clusterPolicies field.
func ensureNonNilClusterPolicies(m map[string]string) map[string]string {
	if m == nil {
		return make(map[string]string)
	}

	return m
}

// fixNilTags ensures every restored Cluster/Configuration/Replicator/
// VpcConnection/Channel has a non-nil Tags map. The first four are tagged
// json:"-" (tags are fetched separately via ListTagsForResource/GetTags), so
// Restore's JSON unmarshal never populates them. Channel's Tags has a normal
// JSON tag (see the Channel doc comment in models.go) and round-trips fine
// when non-empty, but still needs this guard for a channel created with zero
// tags: an empty map marshals as an omitted key under omitempty.
func fixNilTags(b *InMemoryBackend) {
	for _, c := range b.clusters.All() {
		if c.Tags == nil {
			c.Tags = make(map[string]string)
		}
	}

	for _, c := range b.configurations.All() {
		if c.Tags == nil {
			c.Tags = make(map[string]string)
		}
	}

	for _, r := range b.replicators.All() {
		if r.Tags == nil {
			r.Tags = make(map[string]string)
		}
	}

	for _, v := range b.vpcConnections.All() {
		if v.Tags == nil {
			v.Tags = make(map[string]string)
		}
	}

	for _, ch := range b.channels.All() {
		if ch.Tags == nil {
			ch.Tags = make(map[string]string)
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
