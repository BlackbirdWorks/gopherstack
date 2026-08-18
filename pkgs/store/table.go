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
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
)

// Table is a generic keyed collection over map[string]*V. The string key is
// the universal primary-key shape for AWS resources (names, ARNs, IDs, ...).
//
// The zero value is not usable; always create via [New].
//
// Table performs no locking of its own — see the package doc for why. Every
// method below assumes the caller (typically a service backend) already holds
// whatever lock protects the surrounding invariant.
type Table[V any] struct {
	data    map[string]*V
	keyFn   func(*V) string
	indexes []*Index[V]
}

// New creates an empty [Table]. keyFn extracts the primary key from a value;
// it is called on every [Table.Put] and [Table.Restore] to place the value in
// the underlying map, so it must be a pure function of the value's identity
// (e.g. a resource name or ARN) and must not change across the value's
// lifetime — see [Table.AddIndex] for the same rule applied to secondary keys.
func New[V any](keyFn func(*V) string) *Table[V] {
	return &Table[V]{
		data:  make(map[string]*V),
		keyFn: keyFn,
	}
}

// Get returns the value stored under id and whether it was found.
func (t *Table[V]) Get(id string) (*V, bool) {
	v, ok := t.data[id]

	return v, ok
}

// Has reports whether id is present in the table.
func (t *Table[V]) Has(id string) bool {
	_, ok := t.data[id]

	return ok
}

// Len returns the number of entries in the table.
func (t *Table[V]) Len() int {
	return len(t.data)
}

// Put inserts or replaces the value under the key derived from v via the
// table's keyFn. A nil v is a documented no-op (nil-safe): there is no
// well-formed key to derive from a nil value, so Put silently ignores it
// rather than panicking inside a caller-supplied keyFn.
//
// If an entry already exists at that key, it is first removed from every
// registered [Index] so the index reflects only the new value; the new value
// is then added to each index under its (possibly different) index key.
func (t *Table[V]) Put(v *V) {
	if v == nil {
		return
	}

	id := t.keyFn(v)

	if old, exists := t.data[id]; exists {
		for _, idx := range t.indexes {
			idx.remove(old)
		}
	}

	t.data[id] = v

	for _, idx := range t.indexes {
		idx.add(v)
	}
}

// Delete removes the entry stored under id, reporting whether it existed.
// Any registered [Index] is kept consistent by removing the deleted value.
func (t *Table[V]) Delete(id string) bool {
	old, exists := t.data[id]
	if !exists {
		return false
	}

	delete(t.data, id)

	for _, idx := range t.indexes {
		idx.remove(old)
	}

	return true
}

// All returns every value in the table. Iteration order is UNSPECIFIED (Go
// map order); callers that need a stable order for an AWS list operation must
// sort the result themselves (e.g. by name/ARN, matching how the real AWS API
// they are emulating orders its own responses).
func (t *Table[V]) All() []*V {
	out := make([]*V, 0, len(t.data))
	for _, v := range t.data {
		out = append(out, v)
	}

	return out
}

// Range calls f for each value in the table in unspecified order, stopping
// early if f returns false. Unlike [Table.All] it performs no allocation,
// making it the preferred form for a read-only scan over a large table.
func (t *Table[V]) Range(f func(*V) bool) {
	for _, v := range t.data {
		if !f(v) {
			return
		}
	}
}

// Reset removes every entry from the table and clears every registered
// [Index], returning it to the state it was in immediately after [New].
func (t *Table[V]) Reset() {
	t.reset()
}

// Snapshot returns every value in the table ordered by key, ascending. The
// fixed order (rather than [Table.All]'s unspecified map order) is what makes
// [Table.Restore] round-trips and JSON-marshaled snapshots byte-for-byte
// deterministic across runs, which keeps snapshot-based tests and diffs stable.
func (t *Table[V]) Snapshot() []*V {
	if len(t.data) == 0 {
		return nil
	}

	out := make([]*V, 0, len(t.data))
	for _, v := range t.data {
		out = append(out, v)
	}

	slices.SortFunc(out, func(a, b *V) int {
		return cmp.Compare(t.keyFn(a), t.keyFn(b))
	})

	return out
}

// Restore replaces the table's contents with items, rebuilding the primary
// map and every registered [Index] from scratch via keyFn. It is the inverse
// of [Table.Snapshot]. A nil or empty items leaves the table empty, matching
// [Table.Reset].
func (t *Table[V]) Restore(items []*V) {
	t.reset()

	for _, v := range items {
		t.insertFresh(v)
	}
}

// reset clears the primary map and every index without reallocating them,
// so existing capacity is reused by subsequent inserts.
func (t *Table[V]) reset() {
	clear(t.data)

	for _, idx := range t.indexes {
		idx.clear()
	}
}

// insertFresh adds v to the table and every index without checking for (or
// evicting) an existing entry at the same key. It is only safe to call when
// the table is known to be empty at that key, e.g. immediately after reset,
// which is why [Table.Restore] uses it instead of [Table.Put].
func (t *Table[V]) insertFresh(v *V) {
	if v == nil {
		return
	}

	id := t.keyFn(v)
	t.data[id] = v

	for _, idx := range t.indexes {
		idx.add(v)
	}
}

// tableSnapshotter is the type-erased contract a [Table][V] satisfies so that
// a [Registry] can hold tables of heterogeneous value types in one collection.
// It is unexported and implemented only by [Table]; callers never name it —
// [Register] accepts a fully-typed *Table[V] and the compiler checks the
// erasure at the call site, so no interface{}/any ever appears in user code.
type tableSnapshotter interface {
	reset()
	snapshotJSON() (json.RawMessage, error)
	restoreJSON(data json.RawMessage) error
}

// snapshotJSON marshals [Table.Snapshot]'s deterministic, key-sorted output to
// JSON. It implements tableSnapshotter for [Registry.SnapshotAll].
func (t *Table[V]) snapshotJSON() (json.RawMessage, error) {
	data, err := json.Marshal(t.Snapshot())
	if err != nil {
		return nil, fmt.Errorf("store: marshal table snapshot: %w", err)
	}

	return data, nil
}

// restoreJSON unmarshals data produced by snapshotJSON and loads it via
// [Table.Restore]. It implements tableSnapshotter for [Registry.RestoreAll].
func (t *Table[V]) restoreJSON(data json.RawMessage) error {
	var items []*V

	if err := json.Unmarshal(data, &items); err != nil {
		return fmt.Errorf("store: unmarshal table snapshot: %w", err)
	}

	t.Restore(items)

	return nil
}
