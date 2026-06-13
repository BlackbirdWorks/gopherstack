package mediastoredata

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	// maxPathLength is the maximum allowed byte-length of an object path,
	// matching the AWS MediaStore Data limit.
	maxPathLength = 900

	// defaultMaxResults is the page size applied when MaxResults is zero.
	defaultMaxResults = 1000
)

var (
	// ErrNotFound is returned when a requested object does not exist.
	ErrNotFound = awserr.New("ObjectNotFoundException", awserr.ErrNotFound)

	// ErrInvalidPath is returned when a path fails validation.
	ErrInvalidPath = awserr.New("InvalidPathException", awserr.ErrInvalidParameter)

	// ErrInvalidStorageClass is returned when an unknown storage class is supplied.
	ErrInvalidStorageClass = awserr.New("InvalidStorageClassException", awserr.ErrInvalidParameter)
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

// isValidStorageClass reports whether sc is a known MediaStore Data storage class.
func isValidStorageClass(sc string) bool {
	return sc == "TEMPORAL" || sc == "STANDARD"
}

// Object represents a stored media object.
type Object struct {
	LastModified       time.Time
	ETag               string
	SHA256             string // cached hex-encoded SHA-256 of Body
	ContentType        string
	CacheControl       string
	StorageClass       string
	UploadAvailability string
	Body               []byte
	ContentLength      int64
}

// regionState holds all objects for a single AWS region.
type regionState struct {
	objects map[string]*Object
}

func newRegionState() *regionState {
	return &regionState{
		objects: make(map[string]*Object),
	}
}

// InMemoryBackend is the in-memory store for MediaStore Data objects, nested per region.
type InMemoryBackend struct {
	states        map[string]*regionState // region → state
	mu            *lockmetrics.RWMutex
	defaultRegion string
}

// NewInMemoryBackend creates a new in-memory MediaStore Data backend.
func NewInMemoryBackend(region string) *InMemoryBackend {
	return &InMemoryBackend{
		states:        make(map[string]*regionState),
		mu:            lockmetrics.New("mediastoredata"),
		defaultRegion: region,
	}
}

// Region returns the backend's default region.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }

// state returns the per-region state for region, lazily creating it.
// Must be called while holding a write lock.
func (b *InMemoryBackend) state(region string) *regionState {
	st, ok := b.states[region]
	if !ok {
		st = newRegionState()
		b.states[region] = st
	}

	return st
}

// stateRO returns the per-region state for read-only access.
// Returns nil if the region has no state yet.
// Must be called while holding at least a read lock.
func (b *InMemoryBackend) stateRO(region string) *regionState {
	return b.states[region]
}

// normalizePath normalises an object path (strips leading slash).
func normalizePath(p string) string {
	return strings.TrimPrefix(p, "/")
}

// ValidatePath checks that path is a legal MediaStore object path.
func ValidatePath(p string) error {
	key := normalizePath(p)
	if key == "" {
		return fmt.Errorf("%w: path cannot be empty", ErrInvalidPath)
	}
	if len(key) > maxPathLength {
		return fmt.Errorf("%w: path exceeds %d characters", ErrInvalidPath, maxPathLength)
	}
	if strings.Contains(key, "..") {
		return fmt.Errorf("%w: path cannot contain '..'", ErrInvalidPath)
	}
	if strings.ContainsRune(key, 0) {
		return fmt.Errorf("%w: path contains null byte", ErrInvalidPath)
	}

	return nil
}

// contentSHA256 returns the hex-encoded SHA-256 digest of body.
func contentSHA256(body []byte) string {
	sum := sha256.Sum256(body)

	return hex.EncodeToString(sum[:])
}

// cloneObject returns a shallow copy of obj. Body is shared (CoW: objects are
// immutable after storage, so callers only read the body, never mutate it).
func cloneObject(obj *Object) *Object {
	cp := *obj

	return &cp
}

// PutObject stores an object at the given path.
// Returns ErrInvalidPath if path is malformed or ErrInvalidStorageClass if
// storageClass is unrecognised.
func (b *InMemoryBackend) PutObject(
	ctx context.Context,
	path string, body []byte, contentType, cacheControl, storageClass, uploadAvailability string,
) (*Object, error) {
	if err := ValidatePath(path); err != nil {
		return nil, err
	}

	if storageClass == "" {
		storageClass = "TEMPORAL"
	} else if !isValidStorageClass(storageClass) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidStorageClass, storageClass)
	}

	b.mu.Lock("PutObject")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)
	key := normalizePath(path)

	// Clone the input body to prevent callers mutating the stored slice.
	stored := append([]byte(nil), body...)
	sha := contentSHA256(stored)
	obj := &Object{
		Body:               stored,
		SHA256:             sha,
		ETag:               fmt.Sprintf(`"%s"`, sha),
		ContentType:        contentType,
		CacheControl:       cacheControl,
		StorageClass:       storageClass,
		LastModified:       time.Now().UTC(),
		ContentLength:      int64(len(stored)),
		UploadAvailability: uploadAvailability,
	}
	b.state(region).objects[key] = obj

	return cloneObject(obj), nil
}

// GetObject retrieves an object by path.
func (b *InMemoryBackend) GetObject(ctx context.Context, path string) (*Object, error) {
	if err := ValidatePath(path); err != nil {
		return nil, err
	}

	b.mu.RLock("GetObject")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	st := b.stateRO(region)

	if st == nil {
		return nil, fmt.Errorf("%w: object %q not found", ErrNotFound, path)
	}

	key := normalizePath(path)
	obj, ok := st.objects[key]

	if !ok {
		return nil, fmt.Errorf("%w: object %q not found", ErrNotFound, path)
	}

	return cloneObject(obj), nil
}

// DeleteObject removes an object by path.
func (b *InMemoryBackend) DeleteObject(ctx context.Context, path string) error {
	if err := ValidatePath(path); err != nil {
		return err
	}

	b.mu.Lock("DeleteObject")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)
	st := b.stateRO(region)

	if st == nil {
		return fmt.Errorf("%w: object %q not found", ErrNotFound, path)
	}

	key := normalizePath(path)
	if _, ok := st.objects[key]; !ok {
		return fmt.Errorf("%w: object %q not found", ErrNotFound, path)
	}

	delete(st.objects, key)

	return nil
}

// UpdateObjectMetadata updates content-type and cache-control on an existing
// object without re-uploading the body. Returns ErrNotFound if path is absent.
func (b *InMemoryBackend) UpdateObjectMetadata(ctx context.Context, path, contentType, cacheControl string) error {
	if err := ValidatePath(path); err != nil {
		return err
	}

	b.mu.Lock("UpdateObjectMetadata")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)
	st := b.stateRO(region)

	if st == nil {
		return fmt.Errorf("%w: object %q not found", ErrNotFound, path)
	}

	key := normalizePath(path)
	obj, ok := st.objects[key]

	if !ok {
		return fmt.Errorf("%w: object %q not found", ErrNotFound, path)
	}

	obj.ContentType = contentType
	obj.CacheControl = cacheControl
	obj.LastModified = time.Now().UTC()

	return nil
}

// Item is a metadata entry for a folder or object returned by ListItems.
type Item struct {
	LastModified  time.Time
	Name          string
	Type          string
	ETag          string
	SHA256        string
	ContentType   string
	CacheControl  string
	StorageClass  string
	ContentLength int64
}

// ListItemsInput parameterises a ListItems call.
type ListItemsInput struct {
	FolderPath string
	NextToken  string // opaque pagination cursor (item Name)
	MaxResults int    // 0 → defaultMaxResults
}

// ListItemsOutput is returned by ListItems.
type ListItemsOutput struct {
	NextToken string // empty when no further pages remain
	Items     []*Item
}

// ListItems returns items at the given folder path with optional pagination.
func (b *InMemoryBackend) ListItems(ctx context.Context, in ListItemsInput) *ListItemsOutput {
	b.mu.RLock("ListItems")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	st := b.stateRO(region)

	prefix := normalizePath(in.FolderPath)
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	var objects map[string]*Object
	if st != nil {
		objects = st.objects
	}

	seen := make(map[string]bool)
	all := make([]*Item, 0, len(objects))

	for key, obj := range objects {
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		rest := strings.TrimPrefix(key, prefix)
		before, _, isNested := strings.Cut(rest, "/")

		if !isNested {
			if !seen[rest] {
				// Direct object.
				seen[rest] = true
				all = append(all, &Item{
					Name:          rest,
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
		} else if !seen[before] {
			// Folder – deduplicate.
			seen[before] = true
			all = append(all, &Item{
				Name: before,
				Type: "FOLDER",
			})
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	// Apply cursor.
	if in.NextToken != "" {
		cut := 0
		for cut < len(all) && all[cut].Name <= in.NextToken {
			cut++
		}
		all = all[cut:]
	}

	// Apply page limit.
	limit := in.MaxResults
	if limit <= 0 {
		limit = defaultMaxResults
	}

	out := &ListItemsOutput{}

	if len(all) > limit {
		out.Items = all[:limit]
		out.NextToken = all[limit-1].Name
	} else {
		out.Items = all
	}

	return out
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
	st := b.stateRO(region)

	var s Stats
	if st == nil {
		return s
	}

	s.ObjectCount = len(st.objects)

	for _, obj := range st.objects {
		s.TotalBytes += obj.ContentLength
	}

	return s
}

// ListAllObjects returns all stored objects for the request region for dashboard display.
func (b *InMemoryBackend) ListAllObjects(ctx context.Context, prefix string) []*Item {
	b.mu.RLock("ListAllObjects")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	st := b.stateRO(region)

	if st == nil {
		return nil
	}

	items := make([]*Item, 0, len(st.objects))

	for key, obj := range st.objects {
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
