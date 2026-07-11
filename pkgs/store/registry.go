package store

import (
	"encoding/json"
	"fmt"
)

// Registry is a lifecycle registry for a backend's [Table]s. A backend
// registers each of its tables once at construction time (via [Register]);
// afterward the whole backend's lifecycle — reset, snapshot, restore —
// collapses to one [Registry] call each, regardless of how many differently
// typed tables the backend owns. This is what eliminates the
// per-map Init/Reset/Snapshot/Restore boilerplate described in the package doc.
//
// Registry performs no locking of its own, matching [Table] and [Index]: it
// is meant to be driven from within the backend's own coarse lock.
//
// The zero value is not usable; always create via [NewRegistry].
type Registry struct {
	tables map[string]tableSnapshotter
}

// NewRegistry creates an empty [Registry].
func NewRegistry() *Registry {
	return &Registry{tables: make(map[string]tableSnapshotter)}
}

// Register adds t to r under name and returns t unchanged, so it can be used
// inline in a field initializer:
//
//	b.queues = store.Register(b.registry, "queues", store.New(queueKeyFn))
//
// Register is a free function rather than a [Registry] method because Go does
// not allow a generic method (Table's value type V) on a non-generic receiver
// (Registry); the erasure this requires — storing *Table[V] behind the
// unexported tableSnapshotter interface — happens entirely inside this
// function. Every call site remains fully typed: t keeps its concrete
// *Table[V] type both before and after this call, so no interface{}/any is
// ever visible to the caller.
//
// name must be unique within r; registering a second table under a name
// already in use panics, since that always indicates a construction-time bug
// (e.g. a copy-pasted registration) rather than a runtime condition a backend
// could reasonably recover from.
func Register[V any](r *Registry, name string, t *Table[V]) *Table[V] {
	if _, exists := r.tables[name]; exists {
		panic(fmt.Sprintf("store: table %q already registered", name))
	}

	r.tables[name] = t

	return t
}

// ResetAll clears every registered table (and every index on it), returning
// each to the empty state it was in immediately after [New].
func (r *Registry) ResetAll() {
	for _, t := range r.tables {
		t.reset()
	}
}

// SnapshotAll captures every registered table's contents as JSON, keyed by the
// name each was registered under. Each table's own encoding is already
// deterministic (see [Table.Snapshot]); marshaling a Go map with string keys
// additionally sorts those keys, so the overall result — and any JSON built
// from it — is byte-for-byte stable across calls for the same backend state.
func (r *Registry) SnapshotAll() (map[string]json.RawMessage, error) {
	out := make(map[string]json.RawMessage, len(r.tables))

	for name, t := range r.tables {
		data, err := t.snapshotJSON()
		if err != nil {
			return nil, fmt.Errorf("store: snapshot table %q: %w", name, err)
		}

		out[name] = data
	}

	return out, nil
}

// RestoreAll loads every registered table from data, keyed the same way
// [Registry.SnapshotAll] produced it. A registered table whose name is absent
// from data is reset to empty rather than left untouched, so RestoreAll always
// produces the same backend state a fresh [NewRegistry] plus SnapshotAll's
// input would — never a stale mix of old and new state.
func (r *Registry) RestoreAll(data map[string]json.RawMessage) error {
	for name, t := range r.tables {
		raw, ok := data[name]
		if !ok {
			t.reset()

			continue
		}

		if err := t.restoreJSON(raw); err != nil {
			return fmt.Errorf("store: restore table %q: %w", name, err)
		}
	}

	return nil
}
