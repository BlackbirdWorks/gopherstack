package mediastoredata

import (
	"context"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// MediaStore Data objects are isolated per region: every backend operation resolves
// the caller's region from the request context and operates only on that region's
// nested store. Object paths carry no region component, so the region is always
// taken from the request context (falling back to the backend default).
// Cross-region references never occur and isolation is always safe.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// InMemoryBackend is the in-memory store for MediaStore Data objects, nested per region.
//
// states is nested per-region (map[string]*store.Table[Object] -- outer key
// is region) because MediaStore Data objects are isolated per region (see
// getRegion): the set of regions is only known at runtime, so states is NOT
// registered on a *store.Registry (Registry's SnapshotAll/RestoreAll require
// a fixed, construction-time-known table-name set) and is
// captured/restored directly in persistence.go instead, matching
// services/mediastore's region-nested containers field.
type InMemoryBackend struct {
	states        map[string]*store.Table[Object] // region → objects table
	mu            *lockmetrics.RWMutex
	defaultRegion string
}

// NewInMemoryBackend creates a new in-memory MediaStore Data backend.
func NewInMemoryBackend(region string) *InMemoryBackend {
	return &InMemoryBackend{
		states:        make(map[string]*store.Table[Object]),
		mu:            lockmetrics.New("mediastoredata"),
		defaultRegion: region,
	}
}

// Region returns the backend's default region.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }

// state returns the per-region objects [store.Table] for region, creating it
// on first use. Because creation mutates b.states, callers must already hold
// b.mu for writing (Lock) -- read paths that only need to look up an
// already-existing region must use [InMemoryBackend.stateRO] instead so a
// lazy allocation never races under a shared RLock.
func (b *InMemoryBackend) state(region string) *store.Table[Object] {
	if b.states[region] == nil {
		b.states[region] = store.New(objectKeyFn)
	}

	return b.states[region]
}

// stateRO returns the per-region objects [store.Table] for read-only access.
// Returns nil if the region has no state yet. Must be called while holding
// at least a read lock.
func (b *InMemoryBackend) stateRO(region string) *store.Table[Object] {
	return b.states[region]
}

// Stats holds aggregate metrics for the store.
type Stats struct {
	ObjectCount int
	TotalBytes  int64
}

// Stats returns aggregate object count and total stored bytes for the request region.
func (b *InMemoryBackend) Stats(ctx context.Context) Stats {
	b.mu.RLock("Stats")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	tbl := b.stateRO(region)

	var s Stats
	if tbl == nil {
		return s
	}

	s.ObjectCount = tbl.Len()

	tbl.Range(func(obj *Object) bool {
		s.TotalBytes += obj.ContentLength

		return true
	})

	return s
}

// ListAllObjects returns all stored objects for the request region for dashboard display.
func (b *InMemoryBackend) ListAllObjects(ctx context.Context, prefix string) []*Item {
	b.mu.RLock("ListAllObjects")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	tbl := b.stateRO(region)

	if tbl == nil {
		return nil
	}

	objects := tbl.All()
	items := make([]*Item, 0, len(objects))

	for _, obj := range objects {
		key := obj.Path
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			continue
		}

		items = append(items, &Item{
			Name:          key,
			Type:          "OBJECT",
			ETag:          obj.ETag,
			SHA256:        obj.SHA256,
			ContentType:   obj.ContentType,
			CacheControl:  obj.CacheControl,
			StorageClass:  obj.StorageClass,
			ContentLength: obj.ContentLength,
			LastModified:  obj.LastModified,
		})
	}

	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })

	return items
}
