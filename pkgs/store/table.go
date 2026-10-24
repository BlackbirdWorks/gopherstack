// Package store provides a generic, type-safe keyed collection (Table[V]) that
// replaces the per-backend map + Init/Reset/Snapshot/Restore boilerplate that
// every service.InMemoryBackend hand-rolls today (EC2 alone repeats it for
// ~180 maps). It keeps the existing O(1) partitioned-map design services
// already use — this package does not introduce a database, a memdb, or a
// pointer graph; those were evaluated and rejected (14-79x slower, and a
// referential-integrity burden respectively). It only collapses the
// boilerplate around a plain map[string]*V into one generic type.
//
// # Locking
//
// [Table] and [Index] perform NO internal locking. This is deliberate and
// documented loudly: gopherstack's locking rule is that lock granularity
// follows INVARIANT granularity, not data-structure granularity (see
// .claude/memories/pkgs-catalog.md). A service backend's operations are
// cross-map transactions (create validates foreign keys + writes a resource +
// updates secondary indexes atomically; Snapshot needs one consistent view),
// so the single coarse [github.com/blackbirdworks/gopherstack/pkgs/lockmetrics.RWMutex]
// already held by the owning backend is the correct lock boundary. A [Table]
// is meant to be a field inside that backend, guarded by the backend's
// existing mutex exactly as the raw map it replaces was. Using a [Table] (or
// [Index]) concurrently without an external lock is a data race, just as it
// would be with a bare map.
//
// # Typical usage
//
// A backend declares one Table per resource collection and registers each one
// with a [Registry] exactly once at construction time:
//
//	type Queue struct {
//		Name string
//		// ...
//	}
//
//	type InMemoryBackend struct {
//		mu       *lockmetrics.RWMutex
//		queues   *store.Table[Queue]
//		registry *store.Registry
//	}
//
//	func NewInMemoryBackend() *InMemoryBackend {
//		b := &InMemoryBackend{
//			mu:       lockmetrics.New("sqs"),
//			registry: store.NewRegistry(),
//		}
//		b.queues = store.Register(b.registry, "queues", store.New(func(q *Queue) string { return q.Name }))
//
//		return b
//	}
//
// Every backend operation still takes b.mu itself (store performs no locking),
// but Reset/Snapshot/Restore across every registered table — regardless of how
// many different value types they hold — collapse to one [Registry] call
// instead of one hand-written block per map.
package store

import (
	"reflect"
	"unsafe"
)

// Table is a generic keyed collection over map[string]*V. The string key is
// the universal primary-key shape for AWS resources (names, ARNs, IDs, ...).
//
// The zero value is not usable; always create via [New].
//
// Table performs no locking of its own — see the package doc for why. Every
// method below assumes the caller (typically a service backend) already holds
// whatever lock protects the surrounding invariant.
//
// All behaviour lives in the non-generic tableCore; the methods here are thin
// typed casts so the binary carries one body per operation, not one per V.
type Table[V any] struct {
	tableCore
}

// New creates an empty [Table]. keyFn extracts the primary key from a value;
// it is called on every [Table.Put] and [Table.Restore] to place the value in
// the underlying map, so it must be a pure function of the value's identity
// (e.g. a resource name or ARN) and must not change across the value's
// lifetime — see [Table.AddIndex] for the same rule applied to secondary keys.
func New[V any](keyFn func(*V) string) *Table[V] {
	return &Table[V]{tableCore{
		data:  make(map[string]unsafe.Pointer),
		keyFn: eraseKeyFn(keyFn),
		elem:  reflect.TypeFor[V](),
	}}
}

// Get returns the value stored under id and whether it was found.
func (t *Table[V]) Get(id string) (*V, bool) {
	p, ok := t.get(id)

	return (*V)(p), ok
}

// All returns every value in the table. Iteration order is UNSPECIFIED (Go
// map order); callers that need a stable order for an AWS list operation must
// sort the result themselves (e.g. by name/ARN, matching how the real AWS API
// they are emulating orders its own responses).
func (t *Table[V]) All() []*V {
	return typedSlice[V](t.all())
}

// Range calls f for each value in the table in unspecified order, stopping
// early if f returns false. Unlike [Table.All] it performs no allocation,
// making it the preferred form for a read-only scan over a large table.
func (t *Table[V]) Range(f func(*V) bool) {
	t.rangeAll(eraseVisitor(f))
}

// Put inserts or replaces the value under the key derived from v via the
// table's keyFn. A nil v is a documented no-op.
//
// If an entry already exists at that key, it is first removed from every
// registered [Index] so the index reflects only the new value; the new value
// is then added to each index under its (possibly different) index key.
func (t *Table[V]) Put(v *V) {
	t.put(unsafe.Pointer(v))
}

// Snapshot returns every value in the table ordered by key, ascending. The
// fixed order (rather than [Table.All]'s unspecified map order) is what makes
// [Table.Restore] round-trips and JSON-marshaled snapshots byte-for-byte
// deterministic across runs.
func (t *Table[V]) Snapshot() []*V {
	return typedSlice[V](t.snapshot())
}

// Restore replaces the table's contents with items, rebuilding the primary
// map and every registered [Index] from scratch via keyFn. It is the inverse
// of [Table.Snapshot]. A nil or empty items leaves the table empty, matching
// [Table.Reset].
func (t *Table[V]) Restore(items []*V) {
	t.restore(untypedSlice(items))
}

// Reset removes every entry from the table and clears every registered
// [Index], returning it to the state it was in immediately after [New].
func (t *Table[V]) Reset() {
	t.reset()
}

