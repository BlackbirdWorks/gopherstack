package firehose

// Code in this file supports Phase 3.3 of the datalayer refactor: the
// map[string]map[string]*DeliveryStream (region -> stream name -> stream)
// resource field on InMemoryBackend is replaced by a single flat
// *store.Table[DeliveryStream], composite-keyed by "region|name" (see
// regionKey/deliveryStreamKeyFn below) so that same-named streams stay
// isolated across regions exactly as the old per-region nested map did. A
// companion "byRegion" *store.Index groups entries by region for the
// per-region scans ListDeliveryStreamsByType and FlushAll need, matching the
// old nested map's O(region size) lookup rather than an O(all regions) scan.
// This follows pkgs/store's package doc and the services/dms (commit
// 1b7f0f5f-style region-qualified conversion) pilot: DeliveryStream already
// carries an exported, JSON-tagged Region field (used in Describe/List
// responses), so -- unlike services/ses or services/neptune, whose
// region-nested value types needed a hidden `region` field or a DTO wrapper
// to survive Table.Snapshot's plain json.Marshal -- no DTO layer is needed
// here; persistence.go routes b.streams straight through
// store.Registry.SnapshotAll()/RestoreAll().
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionKey builds the composite store.Table/store.Index key "<region>|<name>"
// used by the streams table and its byRegion index, mirroring the outer
// (region) / inner (stream name) nesting of the map it replaces.
func regionKey(region, name string) string { return region + "|" + name }

// deliveryStreamKeyFn is the store.Table primary key function for a delivery stream.
func deliveryStreamKeyFn(v *DeliveryStream) string { return regionKey(v.Region, v.Name) }

// deliveryStreamRegionIndexKeyFn is the store.Index key function grouping every
// delivery stream that belongs to one region (mirrors the old streams[region] map).
func deliveryStreamRegionIndexKeyFn(v *DeliveryStream) string { return v.Region }

// registerAllTables registers the streams table on b.registry exactly once.
// It must be called during construction only (immediately after b.registry is
// created), never on every Reset() -- store.Register panics on a duplicate
// name, so runtime resets go through b.registry.ResetAll() instead (see
// InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.streams = store.Register(b.registry, "streams", store.New(deliveryStreamKeyFn))
	b.streamsByRegion = b.streams.AddIndex("byRegion", deliveryStreamRegionIndexKeyFn)
}
