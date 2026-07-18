package translate

import (
	"maps"
	"sort"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend stores Translate state for concurrent requests.
//
// terminologies, parallelData, and jobs are *store.Table[T]-backed (see
// store_setup.go and pkgs/store's package doc); tags remains a plain map
// since its values are map[string]string, not *T, so there is nothing for
// store.Table to key on.
type InMemoryBackend struct {
	terminologies *store.Table[Terminology]
	parallelData  *store.Table[ParallelData]
	jobs          *store.Table[TranslationJob]
	tags          map[string]map[string]string
	registry      *store.Registry
	accountID     string
	region        string
	mu            sync.RWMutex
}

// NewInMemoryBackend returns an initialized InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID: accountID,
		region:    region,
		registry:  store.NewRegistry(),
		tags:      make(map[string]map[string]string),
	}
	registerAllTables(b)

	return b
}

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.tags = make(map[string]map[string]string)
}

// sortedNames extracts a key from every item via keyFn and returns the
// resulting names in ascending sorted order, matching the deterministic
// ordering collections.SortedKeys previously gave callers iterating the raw
// map[string]*V this table replaced.
func sortedNames[V any](items []*V, keyFn func(*V) string) []string {
	names := make([]string, 0, len(items))
	for _, v := range items {
		names = append(names, keyFn(v))
	}

	sort.Strings(names)

	return names
}

// tableGet looks up id in t, returning nil if absent. It adapts
// [store.Table.Get]'s (value, ok) result to the single-value getter shape
// [paginate] expects.
func tableGet[V any](t *store.Table[V], id string) *V {
	v, _ := t.Get(id)

	return v
}

func copyMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}

	out := make(map[string]string, len(m))
	maps.Copy(out, m)

	return out
}

func paginate[T any](keys []string, get func(string) T, maxResults int, nextToken string) ([]T, string) {
	const defaultMaxResults = 100

	if maxResults <= 0 {
		maxResults = defaultMaxResults
	}

	start := 0

	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := start + maxResults

	var outToken string

	if end < len(keys) {
		outToken = keys[end]
	} else {
		end = len(keys)
	}

	items := make([]T, 0, end-start)

	for _, k := range keys[start:end] {
		items = append(items, get(k))
	}

	return items, outToken
}
