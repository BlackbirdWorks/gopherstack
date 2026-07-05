package store_test

import (
	"fmt"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// lookupTableSize is the fixed table size used by the Get/Put micro-benchmarks
// below (both the raw-map baseline and the Table[V] variant), chosen large
// enough that key rotation exercises real hashing/bucket behaviour rather
// than fitting entirely in a couple of cache lines.
const lookupTableSize = 1000

// buildBenchKeys returns n deterministic, distinct string keys.
func buildBenchKeys(n int) []string {
	keys := make([]string, n)
	for i := range n {
		keys[i] = fmt.Sprintf("key-%06d", i)
	}

	return keys
}

// --- Get -------------------------------------------------------------------

func BenchmarkRawMapGet(b *testing.B) {
	keys := buildBenchKeys(lookupTableSize)
	m := make(map[string]*widget, lookupTableSize)

	for i, k := range keys {
		m[k] = &widget{ID: k, Count: i}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		_ = m[keys[i%lookupTableSize]]
	}
}

func BenchmarkTableGet(b *testing.B) {
	keys := buildBenchKeys(lookupTableSize)
	tbl := store.New(widgetKey)

	for i, k := range keys {
		tbl.Put(&widget{ID: k, Count: i})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		_, _ = tbl.Get(keys[i%lookupTableSize])
	}
}

// --- Put (steady-state overwrite of an existing key) -----------------------

func BenchmarkRawMapPut(b *testing.B) {
	keys := buildBenchKeys(lookupTableSize)
	m := make(map[string]*widget, lookupTableSize)

	for i, k := range keys {
		m[k] = &widget{ID: k, Count: i}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		k := keys[i%lookupTableSize]
		m[k] = &widget{ID: k, Count: i}
	}
}

func BenchmarkTablePut(b *testing.B) {
	keys := buildBenchKeys(lookupTableSize)
	tbl := store.New(widgetKey)

	for i, k := range keys {
		tbl.Put(&widget{ID: k, Count: i})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		k := keys[i%lookupTableSize]
		tbl.Put(&widget{ID: k, Count: i})
	}
}

// --- Delete (each op deletes a distinct, never-before-deleted key) ---------

func BenchmarkRawMapDelete(b *testing.B) {
	keys := buildBenchKeys(b.N)
	m := make(map[string]*widget, b.N)

	for i, k := range keys {
		m[k] = &widget{ID: k, Count: i}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for _, k := range keys {
		delete(m, k)
	}
}

func BenchmarkTableDelete(b *testing.B) {
	keys := buildBenchKeys(b.N)
	tbl := store.New(widgetKey)

	for i, k := range keys {
		tbl.Put(&widget{ID: k, Count: i})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for _, k := range keys {
		tbl.Delete(k)
	}
}

// --- Range over 10k entries -------------------------------------------------

const rangeTableSize = 10_000

func BenchmarkRawMapRangeAll(b *testing.B) {
	keys := buildBenchKeys(rangeTableSize)
	m := make(map[string]*widget, rangeTableSize)

	for i, k := range keys {
		m[k] = &widget{ID: k, Count: i}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		sum := 0
		for _, v := range m {
			sum += v.Count
		}

		if sum < 0 {
			b.Fatal("impossible negative sum")
		}
	}
}

func BenchmarkTableRange(b *testing.B) {
	keys := buildBenchKeys(rangeTableSize)
	tbl := store.New(widgetKey)

	for i, k := range keys {
		tbl.Put(&widget{ID: k, Count: i})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		sum := 0
		tbl.Range(func(w *widget) bool {
			sum += w.Count

			return true
		})

		if sum < 0 {
			b.Fatal("impossible negative sum")
		}
	}
}

// --- Snapshot/Restore round trip over 10k entries ---------------------------

const snapshotTableSize = 10_000

func BenchmarkSnapshotRestore(b *testing.B) {
	keys := buildBenchKeys(snapshotTableSize)
	src := store.New(widgetKey)

	for i, k := range keys {
		src.Put(&widget{ID: k, Group: k, Count: i})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		snap := src.Snapshot()
		dst := store.New(widgetKey)
		dst.Restore(snap)
	}
}

// --- Indexed lookup vs. linear scan over All (100 groups x 1k items) -------

const (
	indexGroupCount    = 100
	indexItemsPerGroup = 1_000
)

// buildIndexedTable returns a table of indexGroupCount*indexItemsPerGroup
// widgets evenly spread across indexGroupCount distinct "group" values, plus
// the secondary index over that field and the list of group keys in order.
func buildIndexedTable() (*store.Table[widget], *store.Index[widget], []string) {
	tbl := store.New(widgetKey)
	idx := tbl.AddIndex("group", func(w *widget) string { return w.Group })

	groupKeys := make([]string, indexGroupCount)
	for g := range indexGroupCount {
		groupKeys[g] = fmt.Sprintf("group-%03d", g)
	}

	for g, gk := range groupKeys {
		for i := range indexItemsPerGroup {
			tbl.Put(&widget{ID: fmt.Sprintf("g%03d-i%04d", g, i), Group: gk, Count: i})
		}
	}

	return tbl, idx, groupKeys
}

func BenchmarkIndexedLookup(b *testing.B) {
	_, idx, groupKeys := buildIndexedTable()

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		got := idx.Get(groupKeys[i%indexGroupCount])
		if len(got) != indexItemsPerGroup {
			b.Fatalf("expected %d members, got %d", indexItemsPerGroup, len(got))
		}
	}
}

func BenchmarkLinearScanLookup(b *testing.B) {
	tbl, _, groupKeys := buildIndexedTable()

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		want := groupKeys[i%indexGroupCount]

		matches := make([]*widget, 0, indexItemsPerGroup)

		for _, w := range tbl.All() {
			if w.Group == want {
				matches = append(matches, w)
			}
		}

		if len(matches) != indexItemsPerGroup {
			b.Fatalf("expected %d members, got %d", indexItemsPerGroup, len(matches))
		}
	}
}
