package store

import (
	"cmp"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"unsafe"
)

// tableCore is the non-generic engine behind every Table[V]. Values are held
// as unsafe.Pointer so each operation compiles once rather than once per V
// (Go only shares generic bodies across pointer-shaped type arguments, and V
// is a struct). The typed wrappers guarantee every pointer is a *V.
type tableCore struct {
	data    map[string]unsafe.Pointer
	keyFn   func(unsafe.Pointer) string
	indexes []*indexCore
	elem    reflect.Type
}

// eraseKeyFn reinterprets func(*V) as func(unsafe.Pointer): identical ABI,
// and it avoids a per-V adapter closure.
func eraseKeyFn[V any](f func(*V) string) func(unsafe.Pointer) string {
	return *(*func(unsafe.Pointer) string)(unsafe.Pointer(&f))
}

func eraseVisitor[V any](f func(*V) bool) func(unsafe.Pointer) bool {
	return *(*func(unsafe.Pointer) bool)(unsafe.Pointer(&f))
}

// typedSlice and untypedSlice reinterpret []*V <-> []unsafe.Pointer (same
// layout); a plain pointer cast compiles far smaller than unsafe.Slice.
func typedSlice[V any](s []unsafe.Pointer) []*V {
	return *(*[]*V)(unsafe.Pointer(&s))
}

func untypedSlice[V any](s []*V) []unsafe.Pointer {
	return *(*[]unsafe.Pointer)(unsafe.Pointer(&s))
}

func (t *tableCore) get(id string) (unsafe.Pointer, bool) {
	p, ok := t.data[id]

	return p, ok
}

func (t *tableCore) addIndex(idx *indexCore, name string, keyFn func(unsafe.Pointer) string) {
	idx.init(name, keyFn)

	for _, v := range t.data {
		idx.add(v)
	}

	t.indexes = append(t.indexes, idx)
}

// Has reports whether id is present in the table.
func (t *tableCore) Has(id string) bool {
	_, ok := t.data[id]

	return ok
}

// Len returns the number of entries in the table.
func (t *tableCore) Len() int {
	return len(t.data)
}

// Delete removes the entry stored under id, reporting whether it existed.
// Any registered [Index] is kept consistent by removing the deleted value.
func (t *tableCore) Delete(id string) bool {
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

func (t *tableCore) put(v unsafe.Pointer) {
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

func (t *tableCore) all() []unsafe.Pointer {
	out := make([]unsafe.Pointer, 0, len(t.data))
	for _, v := range t.data {
		out = append(out, v)
	}

	return out
}

func (t *tableCore) rangeAll(f func(unsafe.Pointer) bool) {
	for _, v := range t.data {
		if !f(v) {
			return
		}
	}
}

func (t *tableCore) snapshot() []unsafe.Pointer {
	if len(t.data) == 0 {
		return nil
	}

	out := t.all()

	slices.SortFunc(out, func(a, b unsafe.Pointer) int {
		return cmp.Compare(t.keyFn(a), t.keyFn(b))
	})

	return out
}

func (t *tableCore) restore(items []unsafe.Pointer) {
	t.reset()

	for _, v := range items {
		t.insertFresh(v)
	}
}

// reset clears the primary map and every index without reallocating them,
// so existing capacity is reused by subsequent inserts.
func (t *tableCore) reset() {
	clear(t.data)

	for _, idx := range t.indexes {
		idx.clear()
	}
}

// insertFresh skips the existing-entry eviction; only safe right after reset.
func (t *tableCore) insertFresh(v unsafe.Pointer) {
	if v == nil {
		return
	}

	t.data[t.keyFn(v)] = v

	for _, idx := range t.indexes {
		idx.add(v)
	}
}

// snapshotJSON marshals the key-sorted contents as a JSON array of *V. The
// slice is built with reflect so no per-V code (and no *Table[V] interface
// conversion, which would keep every Table method alive in the binary) is
// needed.
func (t *tableCore) snapshotJSON() (json.RawMessage, error) {
	items := t.snapshot()
	sliceType := reflect.SliceOf(reflect.PointerTo(t.elem))
	s := reflect.Zero(sliceType) // nil marshals as null, as the typed original did

	if len(items) > 0 {
		s = reflect.MakeSlice(sliceType, len(items), len(items))
	}

	for i, p := range items {
		s.Index(i).Set(reflect.NewAt(t.elem, p))
	}

	data, err := json.Marshal(s.Interface())
	if err != nil {
		return nil, fmt.Errorf("store: marshal table snapshot: %w", err)
	}

	return data, nil
}

// restoreJSON loads data produced by snapshotJSON.
func (t *tableCore) restoreJSON(data json.RawMessage) error {
	sp := reflect.New(reflect.SliceOf(reflect.PointerTo(t.elem)))

	if err := json.Unmarshal(data, sp.Interface()); err != nil {
		return fmt.Errorf("store: unmarshal table snapshot: %w", err)
	}

	s := sp.Elem()
	items := make([]unsafe.Pointer, s.Len())

	for i := range items {
		items[i] = s.Index(i).UnsafePointer()
	}

	t.restore(items)

	return nil
}

// tableSnapshotter is the type-erased contract a [Registry] holds per table.
type tableSnapshotter interface {
	reset()
	snapshotJSON() (json.RawMessage, error)
	restoreJSON(data json.RawMessage) error
}

// indexCore is the non-generic engine behind every Index[V].
type indexCore struct {
	groups map[string][]unsafe.Pointer
	// keys records the group key each pointer was filed under, so remove still
	// finds the right bucket after the caller mutated the value in place.
	keys  map[unsafe.Pointer]string
	keyFn func(unsafe.Pointer) string
	name  string
}

func (idx *indexCore) init(name string, keyFn func(unsafe.Pointer) string) {
	idx.groups = make(map[string][]unsafe.Pointer)
	idx.keys = make(map[unsafe.Pointer]string)
	idx.keyFn = keyFn
	idx.name = name
}

// Name returns the identifier passed to [Table.AddIndex] when this index was
// created.
func (idx *indexCore) Name() string {
	return idx.name
}

// Len returns the number of distinct group keys currently populated.
func (idx *indexCore) Len() int {
	return len(idx.groups)
}

func (idx *indexCore) add(v unsafe.Pointer) {
	key := idx.keyFn(v)
	idx.groups[key] = append(idx.groups[key], v)
	idx.keys[v] = key
}

// remove drops v from its group by pointer identity, using the key recorded
// at add time. Empty groups are deleted so key churn does not leak buckets.
func (idx *indexCore) remove(v unsafe.Pointer) {
	key, ok := idx.keys[v]
	if !ok {
		return
	}

	delete(idx.keys, v)

	group := idx.groups[key]
	for i, item := range group {
		if item == v {
			group[i] = group[len(group)-1]
			group = group[:len(group)-1]

			break
		}
	}

	if len(group) == 0 {
		delete(idx.groups, key)
	} else {
		idx.groups[key] = group
	}
}

// clear empties the index without reallocating its maps. Stale pointer keys
// must go too: a reused address would inherit a bogus recorded key.
func (idx *indexCore) clear() {
	clear(idx.groups)
	clear(idx.keys)
}
