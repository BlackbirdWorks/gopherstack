package store_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// widget is a small value type used throughout the table-driven tests below.
type widget struct {
	ID    string `json:"id"`
	Group string `json:"group"`
	Count int    `json:"count"`
}

func widgetKey(w *widget) string { return w.ID }

func newWidgetTable(items ...*widget) *store.Table[widget] {
	tbl := store.New(widgetKey)
	for _, w := range items {
		tbl.Put(w)
	}

	return tbl
}

func Test_TablePutGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantSelf *widget
		name     string
		lookup   string
		seed     []*widget
		wantOK   bool
	}{
		{
			name:     "found",
			seed:     []*widget{{ID: "a", Count: 1}},
			lookup:   "a",
			wantOK:   true,
			wantSelf: &widget{ID: "a", Count: 1},
		},
		{
			name:   "missing_from_empty",
			seed:   nil,
			lookup: "a",
			wantOK: false,
		},
		{
			name:   "missing_wrong_key",
			seed:   []*widget{{ID: "a", Count: 1}},
			lookup: "b",
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tbl := newWidgetTable(tt.seed...)

			got, ok := tbl.Get(tt.lookup)
			require.Equal(t, tt.wantOK, ok)

			if tt.wantOK {
				assert.Equal(t, tt.wantSelf.ID, got.ID)
				assert.Equal(t, tt.wantSelf.Count, got.Count)
			} else {
				assert.Nil(t, got)
			}
		})
	}
}

func Test_TablePutOverwrite(t *testing.T) {
	t.Parallel()

	tbl := newWidgetTable(&widget{ID: "a", Count: 1})
	tbl.Put(&widget{ID: "a", Count: 2})

	got, ok := tbl.Get("a")
	require.True(t, ok)
	assert.Equal(t, 2, got.Count)
	assert.Equal(t, 1, tbl.Len())
}

func Test_TablePutNilIsNoOp(t *testing.T) {
	t.Parallel()

	tbl := store.New(widgetKey)

	assert.NotPanics(t, func() { tbl.Put(nil) })
	assert.Equal(t, 0, tbl.Len())
}

func Test_TableHas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		lookup string
		want   bool
	}{
		{name: "present", lookup: "a", want: true},
		{name: "absent", lookup: "z", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tbl := newWidgetTable(&widget{ID: "a"})
			assert.Equal(t, tt.want, tbl.Has(tt.lookup))
		})
	}
}

func Test_TableDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		lookup     string
		wantExists bool
	}{
		{name: "existing", lookup: "a", wantExists: true},
		{name: "missing", lookup: "z", wantExists: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tbl := newWidgetTable(&widget{ID: "a"})

			got := tbl.Delete(tt.lookup)
			require.Equal(t, tt.wantExists, got)

			if tt.wantExists {
				assert.Equal(t, 0, tbl.Len())
				assert.False(t, tbl.Has(tt.lookup))
			}
		})
	}
}

func Test_TableLen(t *testing.T) {
	t.Parallel()

	tbl := store.New(widgetKey)
	assert.Equal(t, 0, tbl.Len())

	tbl.Put(&widget{ID: "a"})
	assert.Equal(t, 1, tbl.Len())

	tbl.Put(&widget{ID: "b"})
	assert.Equal(t, 2, tbl.Len())

	tbl.Delete("a")
	assert.Equal(t, 1, tbl.Len())
}

func Test_TableAll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		seed []*widget
		want []string
	}{
		{name: "empty", seed: nil, want: []string{}},
		{
			name: "multiple",
			seed: []*widget{{ID: "a"}, {ID: "b"}, {ID: "c"}},
			want: []string{"a", "b", "c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tbl := newWidgetTable(tt.seed...)
			got := idsOf(tbl.All())
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func Test_TableRange(t *testing.T) {
	t.Parallel()

	t.Run("visits_every_entry", func(t *testing.T) {
		t.Parallel()

		tbl := newWidgetTable(&widget{ID: "a"}, &widget{ID: "b"}, &widget{ID: "c"})

		seen := make(map[string]bool)
		tbl.Range(func(w *widget) bool {
			seen[w.ID] = true

			return true
		})

		assert.Len(t, seen, 3)
	})

	t.Run("early_stop", func(t *testing.T) {
		t.Parallel()

		tbl := newWidgetTable(&widget{ID: "a"}, &widget{ID: "b"}, &widget{ID: "c"})

		count := 0
		tbl.Range(func(_ *widget) bool {
			count++

			return false
		})

		assert.Equal(t, 1, count)
	})

	t.Run("empty_table", func(t *testing.T) {
		t.Parallel()

		tbl := store.New(widgetKey)

		called := false
		tbl.Range(func(_ *widget) bool {
			called = true

			return true
		})

		assert.False(t, called)
	})
}

func Test_TableReset(t *testing.T) {
	t.Parallel()

	tbl := newWidgetTable(&widget{ID: "a"}, &widget{ID: "b"})
	idx := tbl.AddIndex("group", func(w *widget) string { return w.Group })

	tbl.Reset()

	assert.Equal(t, 0, tbl.Len())
	assert.Empty(t, tbl.All())
	assert.Equal(t, 0, idx.Len())
}

func Test_TableSnapshotDeterministicOrder(t *testing.T) {
	t.Parallel()

	tbl := newWidgetTable(&widget{ID: "c"}, &widget{ID: "a"}, &widget{ID: "b"})

	snap := tbl.Snapshot()
	require.Len(t, snap, 3)
	assert.Equal(t, []string{"a", "b", "c"}, idsOf(snap))

	// Repeated snapshots of unchanged state must produce identical order.
	again := tbl.Snapshot()
	assert.Equal(t, idsOf(snap), idsOf(again))
}

func Test_TableSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		seed []*widget
	}{
		{name: "empty", seed: nil},
		{
			name: "populated",
			seed: []*widget{
				{ID: "a", Group: "g1", Count: 1},
				{ID: "b", Group: "g2", Count: 2},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := newWidgetTable(tt.seed...)
			snap := src.Snapshot()

			dst := store.New(widgetKey)
			dst.Restore(snap)

			assert.Equal(t, src.Len(), dst.Len())
			assert.Equal(t, idsOf(src.Snapshot()), idsOf(dst.Snapshot()))

			for _, w := range tt.seed {
				got, ok := dst.Get(w.ID)
				require.True(t, ok)
				assert.Equal(t, w.Group, got.Group)
				assert.Equal(t, w.Count, got.Count)
			}
		})
	}
}

func Test_TableRestoreReplacesExistingContents(t *testing.T) {
	t.Parallel()

	tbl := newWidgetTable(&widget{ID: "old"})
	tbl.Restore([]*widget{{ID: "new"}})

	assert.False(t, tbl.Has("old"))
	assert.True(t, tbl.Has("new"))
	assert.Equal(t, 1, tbl.Len())
}

func Test_TableRestoreNilIsEmpty(t *testing.T) {
	t.Parallel()

	tbl := newWidgetTable(&widget{ID: "a"})
	tbl.Restore(nil)

	assert.Equal(t, 0, tbl.Len())
}

func Test_TableSnapshotJSONRoundTrip(t *testing.T) {
	t.Parallel()

	src := newWidgetTable(&widget{ID: "a", Group: "g", Count: 1})

	data, err := json.Marshal(src.Snapshot())
	require.NoError(t, err)

	var items []*widget
	require.NoError(t, json.Unmarshal(data, &items))

	dst := store.New(widgetKey)
	dst.Restore(items)

	got, ok := dst.Get("a")
	require.True(t, ok)
	assert.Equal(t, "g", got.Group)
	assert.Equal(t, 1, got.Count)
}

func Test_IndexConsistencyAcrossPut(t *testing.T) {
	t.Parallel()

	tbl := store.New(widgetKey)
	idx := tbl.AddIndex("group", func(w *widget) string { return w.Group })

	tbl.Put(&widget{ID: "a", Group: "g1"})
	tbl.Put(&widget{ID: "b", Group: "g1"})
	tbl.Put(&widget{ID: "c", Group: "g2"})

	assert.ElementsMatch(t, []string{"a", "b"}, idsOf(idx.Get("g1")))
	assert.ElementsMatch(t, []string{"c"}, idsOf(idx.Get("g2")))
	assert.Empty(t, idx.Get("no-such-group"))

	// Moving "a" from g1 to g2 via Put must relocate it in the index.
	tbl.Put(&widget{ID: "a", Group: "g2"})

	assert.ElementsMatch(t, []string{"b"}, idsOf(idx.Get("g1")))
	assert.ElementsMatch(t, []string{"a", "c"}, idsOf(idx.Get("g2")))
}

// Test_IndexConsistencyAcrossInPlaceMutateThenPut reproduces gopherstack-vho:
// a caller that does Get, mutates the returned pointer's indexed field in
// place, then calls Put with that same pointer must not leave the entry
// filed under its old index key. Put's old/new value are the same object at
// that point, so a remove() that recomputes its key from the passed-in value
// (rather than the key it was originally filed under) looks in the *new*
// bucket instead of the old one and never clears the stale entry.
func Test_IndexConsistencyAcrossInPlaceMutateThenPut(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mutate     func(w *widget)
		wantOldKey string
		wantNewKey string
	}{
		{
			name:       "group_field_changes",
			mutate:     func(w *widget) { w.Group = "g2" },
			wantOldKey: "g1",
			wantNewKey: "g2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tbl := store.New(widgetKey)
			idx := tbl.AddIndex("group", func(w *widget) string { return w.Group })

			tbl.Put(&widget{ID: "a", Group: "g1"})

			got, ok := tbl.Get("a")
			require.True(t, ok)

			tt.mutate(got)
			tbl.Put(got)

			assert.Empty(t, idsOf(idx.Get(tt.wantOldKey)),
				"old index bucket must not retain the entry after in-place mutate+Put")
			assert.ElementsMatch(t, []string{"a"}, idsOf(idx.Get(tt.wantNewKey)))
		})
	}
}

func Test_IndexConsistencyAcrossDelete(t *testing.T) {
	t.Parallel()

	tbl := store.New(widgetKey)
	idx := tbl.AddIndex("group", func(w *widget) string { return w.Group })

	tbl.Put(&widget{ID: "a", Group: "g1"})
	tbl.Put(&widget{ID: "b", Group: "g1"})

	require.True(t, tbl.Delete("a"))

	assert.ElementsMatch(t, []string{"b"}, idsOf(idx.Get("g1")))

	require.True(t, tbl.Delete("b"))

	// The group must be gone entirely, not left as an empty slice, once the
	// last member is removed.
	assert.Equal(t, 0, idx.Len())
	assert.Empty(t, idx.Get("g1"))
}

// Test_IndexConsistencyAcrossInPlaceMutateThenDelete covers the same
// gopherstack-vho bug class as Test_IndexConsistencyAcrossInPlaceMutateThenPut,
// but for Delete: a caller that Gets a pointer, mutates its indexed field in
// place, and then Deletes it by id must not leave the (now-dangling) entry
// behind in the index under its stale key.
func Test_IndexConsistencyAcrossInPlaceMutateThenDelete(t *testing.T) {
	t.Parallel()

	tbl := store.New(widgetKey)
	idx := tbl.AddIndex("group", func(w *widget) string { return w.Group })

	tbl.Put(&widget{ID: "a", Group: "g1"})

	got, ok := tbl.Get("a")
	require.True(t, ok)

	got.Group = "g2"

	require.True(t, tbl.Delete("a"))

	assert.Equal(t, 0, idx.Len())
	assert.Empty(t, idx.Get("g1"))
	assert.Empty(t, idx.Get("g2"))
}

func Test_IndexConsistencyAcrossRestore(t *testing.T) {
	t.Parallel()

	tbl := store.New(widgetKey)
	idx := tbl.AddIndex("group", func(w *widget) string { return w.Group })

	tbl.Put(&widget{ID: "a", Group: "g1"})
	tbl.Put(&widget{ID: "b", Group: "g2"})

	tbl.Restore([]*widget{
		{ID: "c", Group: "g3"},
		{ID: "d", Group: "g3"},
	})

	assert.Empty(t, idx.Get("g1"))
	assert.Empty(t, idx.Get("g2"))
	assert.ElementsMatch(t, []string{"c", "d"}, idsOf(idx.Get("g3")))
}

func Test_IndexAddedAfterDataAlreadyPresent(t *testing.T) {
	t.Parallel()

	tbl := newWidgetTable(&widget{ID: "a", Group: "g1"}, &widget{ID: "b", Group: "g1"})
	idx := tbl.AddIndex("group", func(w *widget) string { return w.Group })

	assert.ElementsMatch(t, []string{"a", "b"}, idsOf(idx.Get("g1")))
}

func Test_IndexName(t *testing.T) {
	t.Parallel()

	tbl := store.New(widgetKey)
	idx := tbl.AddIndex("group", func(w *widget) string { return w.Group })

	assert.Equal(t, "group", idx.Name())
}

// gadget is a second, differently-shaped value type used to prove the
// Registry can hold heterogeneous tables consistently.
type gadget struct {
	SKU   string `json:"sku"`
	Price int    `json:"price"`
}

func gadgetKey(g *gadget) string { return g.SKU }

func Test_RegistryResetAll(t *testing.T) {
	t.Parallel()

	reg := store.NewRegistry()
	widgets := store.Register(reg, "widgets", newWidgetTable(&widget{ID: "a"}))
	gadgets := store.Register(reg, "gadgets", store.New(gadgetKey))
	gadgets.Put(&gadget{SKU: "g1", Price: 100})

	reg.ResetAll()

	assert.Equal(t, 0, widgets.Len())
	assert.Equal(t, 0, gadgets.Len())
}

func Test_RegistrySnapshotRestoreAllRoundTrip(t *testing.T) {
	t.Parallel()

	reg := store.NewRegistry()
	widgets := store.Register(reg, "widgets", store.New(widgetKey))
	gadgets := store.Register(reg, "gadgets", store.New(gadgetKey))

	widgets.Put(&widget{ID: "a", Group: "g1", Count: 1})
	widgets.Put(&widget{ID: "b", Group: "g2", Count: 2})
	gadgets.Put(&gadget{SKU: "sku-1", Price: 100})

	snap, err := reg.SnapshotAll()
	require.NoError(t, err)
	require.Contains(t, snap, "widgets")
	require.Contains(t, snap, "gadgets")

	// Simulate a fresh backend restart: new registry, new (empty) tables
	// registered under the same names, then restore from the captured bytes.
	reg2 := store.NewRegistry()
	widgets2 := store.Register(reg2, "widgets", store.New(widgetKey))
	gadgets2 := store.Register(reg2, "gadgets", store.New(gadgetKey))

	require.NoError(t, reg2.RestoreAll(snap))

	assert.Equal(t, 2, widgets2.Len())
	assert.Equal(t, 1, gadgets2.Len())

	w, ok := widgets2.Get("a")
	require.True(t, ok)
	assert.Equal(t, "g1", w.Group)
	assert.Equal(t, 1, w.Count)

	g, ok := gadgets2.Get("sku-1")
	require.True(t, ok)
	assert.Equal(t, 100, g.Price)
}

func Test_RegistrySnapshotIsDeterministic(t *testing.T) {
	t.Parallel()

	build := func() *store.Registry {
		reg := store.NewRegistry()
		widgets := store.Register(reg, "widgets", store.New(widgetKey))
		widgets.Put(&widget{ID: "z"})
		widgets.Put(&widget{ID: "a"})
		widgets.Put(&widget{ID: "m"})

		return reg
	}

	snap1, err := build().SnapshotAll()
	require.NoError(t, err)
	snap2, err := build().SnapshotAll()
	require.NoError(t, err)

	assert.Equal(t, snap1["widgets"], snap2["widgets"])
}

func Test_RegistryRestoreAllResetsTablesMissingFromData(t *testing.T) {
	t.Parallel()

	reg := store.NewRegistry()
	widgets := store.Register(reg, "widgets", store.New(widgetKey))
	widgets.Put(&widget{ID: "stale"})

	// data intentionally omits "widgets" entirely.
	require.NoError(t, reg.RestoreAll(map[string]json.RawMessage{}))

	assert.Equal(t, 0, widgets.Len())
}

func Test_RegisterDuplicateNamePanics(t *testing.T) {
	t.Parallel()

	reg := store.NewRegistry()
	store.Register(reg, "widgets", store.New(widgetKey))

	assert.Panics(t, func() {
		store.Register(reg, "widgets", store.New(widgetKey))
	})
}

// idsOf extracts the ID field from a slice of *widget for order-insensitive
// comparisons in tests above.
func idsOf(ws []*widget) []string {
	out := make([]string, len(ws))
	for i, w := range ws {
		out[i] = w.ID
	}

	return out
}
