package dax

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// errSnapshotIntegrity is the sentinel error for snapshot referential integrity failures.
var errSnapshotIntegrity = errors.New("snapshot integrity violation")

// daxSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to backendSnapshot (or a value type held by one of
// the registered tables) would make an older snapshot unsafe to decode as the
// current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll, not a partial decode) any mismatch -- see
// Restore. The pre-Phase-3.3 snapshot format had no version field at all, so
// an old snapshot decodes with Version == 0, which is guaranteed to mismatch
// daxSnapshotVersion and is discarded the same way any other incompatible
// snapshot is.
const daxSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the DAX backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() (clusters, paramGroups, subnetGroups -- see
// store_setup.go). Tags and Events are left as plain fields: tags is a
// non-*T value map (map[string]map[string]string, keyed by ARN) and events
// is an ordered ring-buffer slice, neither of which fits store.Table's keyed
// shape. Version guards against decoding a snapshot from an incompatible
// (older or newer) build of this backend as though it were the current
// shape; see Restore.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage   `json:"tables"`
	Tags      map[string]map[string]string `json:"tags"`
	AccountID string                       `json:"accountID"`
	Region    string                       `json:"region"`
	Events    []*Event                     `json:"events"`
	Version   int                          `json:"version"`
}

func ensureNonNilMaps(s *backendSnapshot) {
	if s.Tags == nil {
		s.Tags = make(map[string]map[string]string)
	}

	if s.Events == nil {
		s.Events = make([]*Event, 0)
	}
}

// validateSnapshot checks referential integrity of the decoded clusters
// table against the decoded parameter/subnet group tables, before any of it
// is committed to the live backend.
func validateSnapshot(
	clusters *store.Table[Cluster],
	paramGroups *store.Table[ParameterGroup],
	subnetGroups *store.Table[SubnetGroup],
) error {
	var firstErr error

	clusters.Range(func(c *Cluster) bool {
		if pgName := c.ParameterGroup.ParameterGroupName; pgName != "" {
			if !paramGroups.Has(pgName) {
				firstErr = fmt.Errorf(
					"%w: cluster %q references missing parameter group %q",
					errSnapshotIntegrity, c.ClusterName, pgName,
				)

				return false
			}
		}

		if c.SubnetGroupName != "" {
			if !subnetGroups.Has(c.SubnetGroupName) {
				firstErr = fmt.Errorf(
					"%w: cluster %q references missing subnet group %q",
					errSnapshotIntegrity, c.ClusterName, c.SubnetGroupName,
				)

				return false
			}
		}

		return true
	})

	return firstErr
}

// rebuildTagIndex rebuilds the tag index from cluster.Tags as the source of truth.
// This corrects any desync that can occur between the tag index and cluster.Tags.
func rebuildTagIndex(clusters *store.Table[Cluster], tagsMap map[string]map[string]string) {
	clusters.Range(func(cluster *Cluster) bool {
		if len(cluster.Tags) == 0 {
			return true
		}

		arn := cluster.ClusterArn
		if tagsMap[arn] == nil {
			tagsMap[arn] = make(map[string]string)
		}

		maps.Copy(tagsMap[arn], cluster.Tags)

		return true
	})
}

// Snapshot serializes the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	var (
		snap   backendSnapshot
		tblErr error
	)

	func() {
		b.mu.RLock("Snapshot")
		defer b.mu.RUnlock()

		var tables map[string]json.RawMessage

		tables, tblErr = b.registry.SnapshotAll()

		snap = backendSnapshot{
			Version:   daxSnapshotVersion,
			Tables:    tables,
			Tags:      b.tags,
			Events:    b.events,
			AccountID: b.AccountID,
			Region:    b.Region,
		}
	}()

	if tblErr != nil {
		logger.Load(ctx).ErrorContext(ctx, "dax: failed to marshal snapshot", "error", tblErr)

		return nil
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).ErrorContext(ctx, "dax: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore deserializes backend state from JSON.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "dax", data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != daxSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"dax: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", daxSnapshotVersion)

		b.registry.ResetAll()
		b.tags = make(map[string]map[string]string)
		b.events = make([]*Event, 0)
		b.AccountID = snap.AccountID
		b.Region = snap.Region

		return nil
	}

	// Decode into a scratch registry first so a referential-integrity failure
	// (below) leaves the live backend state untouched.
	scratch := store.NewRegistry()
	clusters := store.Register(scratch, "clusters", store.New(clusterKeyFn))
	paramGroups := store.Register(scratch, "paramGroups", store.New(paramGroupKeyFn))
	subnetGroups := store.Register(scratch, "subnetGroups", store.New(subnetGroupKeyFn))

	if err := scratch.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("dax: restore snapshot tables: %w", err)
	}

	if err := validateSnapshot(clusters, paramGroups, subnetGroups); err != nil {
		return fmt.Errorf("snapshot integrity check failed: %w", err)
	}

	rebuildTagIndex(clusters, snap.Tags)

	b.clusters.Restore(clusters.Snapshot())
	b.paramGroups.Restore(paramGroups.Snapshot())
	b.subnetGroups.Restore(subnetGroups.Snapshot())
	b.tags = snap.Tags
	b.events = snap.Events
	b.AccountID = snap.AccountID
	b.Region = snap.Region

	b.recoverAsyncTransitions()

	return nil
}

func (b *InMemoryBackend) recoverAsyncTransitions() {
	for _, c := range b.clusters.All() {
		b.recoverClusterState(c.ClusterName, c)

		for i := range c.Nodes {
			if c.Nodes[i].NodeStatus == StatusRebooting {
				b.recoverNodeState(c.ClusterName, c.Nodes[i].NodeID)
			}
		}
	}
}

func (b *InMemoryBackend) recoverClusterState(name string, c *Cluster) {
	switch c.Status {
	case StatusCreating, StatusModifying:
		go func(cName string) {
			b.mu.Lock("Restore:cluster-recovery")
			defer b.mu.Unlock()
			if cl, ok := b.clusters.Get(cName); ok {
				cl.Status = StatusAvailable
			}
		}(name)
	case StatusDeleting:
		go func(cName, cArn string) {
			b.mu.Lock("Restore:delete-recovery")
			defer b.mu.Unlock()
			if cl, ok := b.clusters.Get(cName); ok && cl.Status == StatusDeleting {
				b.clusters.Delete(cName)
				delete(b.tags, cArn)
				b.emitEventLocked(cName, EventSourceTypeCluster,
					fmt.Sprintf("Cluster %s has been deleted.", cName))
			}
		}(name, c.ClusterArn)
	}
}

func (b *InMemoryBackend) recoverNodeState(cName, nodeID string) {
	go func() {
		b.mu.Lock("Restore:node-recovery")
		defer b.mu.Unlock()
		if cl, ok := b.clusters.Get(cName); ok {
			for j := range cl.Nodes {
				if cl.Nodes[j].NodeID == nodeID {
					cl.Nodes[j].NodeStatus = StatusAvailable
					b.emitEventLocked(cName, EventSourceTypeNode,
						fmt.Sprintf("Node %s reboot complete.", nodeID))

					break
				}
			}
		}
	}()
}
