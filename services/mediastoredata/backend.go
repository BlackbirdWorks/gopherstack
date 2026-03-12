package mediastoredata

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested object does not exist.
	ErrNotFound = awserr.New("ObjectNotFoundException", awserr.ErrNotFound)
)

// Object represents a stored media object.
type Object struct {
	LastModified  time.Time
	ETag          string
	ContentType   string
	CacheControl  string
	StorageClass  string
	Body          []byte
	ContentLength int64
}

// InMemoryBackend is the in-memory store for MediaStore Data objects.
type InMemoryBackend struct {
	objects map[string]*Object
	mu      *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new in-memory MediaStore Data backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		objects: make(map[string]*Object),
		mu:      lockmetrics.New("mediastoredata"),
	}
}

// normalizePath normalises an object path (strips leading slash).
func normalizePath(p string) string {
	return strings.TrimPrefix(p, "/")
}

// PutObject stores an object at the given path.
func (b *InMemoryBackend) PutObject(path string, body []byte, contentType, cacheControl, storageClass string) *Object {
	b.mu.Lock("PutObject")
	defer b.mu.Unlock()

	key := normalizePath(path)
	if storageClass == "" {
		storageClass = "TEMPORAL"
	}

	etag := fmt.Sprintf("%x", len(body))
	obj := &Object{
		Body:          body,
		ETag:          etag,
		ContentType:   contentType,
		CacheControl:  cacheControl,
		StorageClass:  storageClass,
		LastModified:  time.Now().UTC(),
		ContentLength: int64(len(body)),
	}
	b.objects[key] = obj
	cp := *obj

	return &cp
}

// GetObject retrieves an object by path.
func (b *InMemoryBackend) GetObject(path string) (*Object, error) {
	b.mu.RLock("GetObject")
	defer b.mu.RUnlock()

	key := normalizePath(path)
	obj, ok := b.objects[key]

	if !ok {
		return nil, fmt.Errorf("%w: object %q not found", ErrNotFound, path)
	}

	cp := *obj

	return &cp, nil
}

// DeleteObject removes an object by path.
func (b *InMemoryBackend) DeleteObject(path string) error {
	b.mu.Lock("DeleteObject")
	defer b.mu.Unlock()

	key := normalizePath(path)
	if _, ok := b.objects[key]; !ok {
		return fmt.Errorf("%w: object %q not found", ErrNotFound, path)
	}

	delete(b.objects, key)

	return nil
}

// Item is a metadata entry for a folder or object returned by ListItems.
type Item struct {
	LastModified  time.Time
	Name          string
	Type          string
	ETag          string
	ContentType   string
	ContentLength int64
}

// ListItems returns items at the given folder path.
// An empty path lists all top-level items.
func (b *InMemoryBackend) ListItems(folderPath string) []*Item {
	b.mu.RLock("ListItems")
	defer b.mu.RUnlock()

	prefix := normalizePath(folderPath)
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	seen := make(map[string]bool)
	items := make([]*Item, 0)

	for key, obj := range b.objects {
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		rest := strings.TrimPrefix(key, prefix)
		before, _, ok := strings.Cut(rest, "/")

		if !ok {
			// Direct object.
			items = append(items, &Item{
				Name:          rest,
				Type:          "OBJECT",
				ETag:          obj.ETag,
				ContentType:   obj.ContentType,
				ContentLength: obj.ContentLength,
				LastModified:  obj.LastModified,
			})
		} else {
			// Folder.
			folder := before
			if !seen[folder] {
				seen[folder] = true
				items = append(items, &Item{
					Name: folder,
					Type: "FOLDER",
				})
			}
		}
	}

	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })

	return items
}

// ListAllObjects returns all stored objects for dashboard display.
func (b *InMemoryBackend) ListAllObjects() []*Item {
	b.mu.RLock("ListAllObjects")
	defer b.mu.RUnlock()

	items := make([]*Item, 0, len(b.objects))
	for key, obj := range b.objects {
		items = append(items, &Item{
			Name:          key,
			Type:          "OBJECT",
			ETag:          obj.ETag,
			ContentType:   obj.ContentType,
			ContentLength: obj.ContentLength,
			LastModified:  obj.LastModified,
		})
	}

	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })

	return items
}
