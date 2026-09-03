package azureblob

import (
	"crypto/md5" //nolint:gosec // ETag generation only, not a security use of MD5
	"encoding/hex"
	"sort"
	"sync"
	"time"
)

// InMemoryBackend implements StorageBackend using in-memory maps guarded by a
// single RWMutex. Shaped after services/sqs's InMemoryBackend, but simpler:
// Azure Blob's MVP surface (see AZURE.md/PARITY.md) has no janitor, no
// metrics emitter, and no cross-resource relationships to track, so a single
// coarse lock over one map of containers is sufficient.
type InMemoryBackend struct {
	mu         sync.RWMutex
	containers map[string]*storedContainer
}

// NewInMemoryBackend creates a new empty InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		containers: make(map[string]*storedContainer),
	}
}

// CreateContainer creates a new, empty container. Returns
// ErrContainerAlreadyExists if a container with the same name already exists.
func (b *InMemoryBackend) CreateContainer(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.containers[name]; ok {
		return ErrContainerAlreadyExists
	}

	b.containers[name] = &storedContainer{
		Name:      name,
		CreatedAt: time.Now().UTC(),
		Blobs:     make(map[string]*storedBlob),
	}

	return nil
}

// DeleteContainer removes a container and all of its blobs. Returns
// ErrContainerNotFound if the container does not exist.
func (b *InMemoryBackend) DeleteContainer(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.containers[name]; !ok {
		return ErrContainerNotFound
	}

	delete(b.containers, name)

	return nil
}

// ListContainers returns a snapshot of all containers, sorted by name (the
// order Azure's List Containers returns them in).
func (b *InMemoryBackend) ListContainers() []ContainerInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]ContainerInfo, 0, len(b.containers))
	for _, c := range b.containers {
		out = append(out, ContainerInfo{Name: c.Name, CreatedAt: c.CreatedAt})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// PutBlob stores data as a block blob named blob within container. Returns
// ErrContainerNotFound if the container does not exist. Overwrites any
// existing blob with the same name (Azure's Put Blob semantics -- no
// conditional headers are enforced, see PARITY.md known gaps).
func (b *InMemoryBackend) PutBlob(container, blob string, data []byte, contentType string) (BlobInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	c, ok := b.containers[container]
	if !ok {
		return BlobInfo{}, ErrContainerNotFound
	}

	stored := &storedBlob{
		Name:         blob,
		ContentType:  contentType,
		Data:         append([]byte(nil), data...),
		LastModified: time.Now().UTC(),
		ETag:         computeETag(data),
	}
	c.Blobs[blob] = stored

	return stored.info(), nil
}

// GetBlob returns a blob's metadata and full body. Returns ErrContainerNotFound
// or ErrBlobNotFound as appropriate.
func (b *InMemoryBackend) GetBlob(container, blob string) (BlobInfo, []byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stored, err := b.lookupBlobLocked(container, blob)
	if err != nil {
		return BlobInfo{}, nil, err
	}

	return stored.info(), append([]byte(nil), stored.Data...), nil
}

// HeadBlob returns a blob's metadata without its body. Returns
// ErrContainerNotFound or ErrBlobNotFound as appropriate.
func (b *InMemoryBackend) HeadBlob(container, blob string) (BlobInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stored, err := b.lookupBlobLocked(container, blob)
	if err != nil {
		return BlobInfo{}, err
	}

	return stored.info(), nil
}

// DeleteBlob removes a blob. Returns ErrContainerNotFound or ErrBlobNotFound
// as appropriate.
func (b *InMemoryBackend) DeleteBlob(container, blob string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	c, ok := b.containers[container]
	if !ok {
		return ErrContainerNotFound
	}

	if _, ok := c.Blobs[blob]; !ok {
		return ErrBlobNotFound
	}

	delete(c.Blobs, blob)

	return nil
}

// ListBlobs returns a snapshot of all blobs in container, sorted by name.
// Returns ErrContainerNotFound if the container does not exist.
func (b *InMemoryBackend) ListBlobs(container string) ([]BlobInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	c, ok := b.containers[container]
	if !ok {
		return nil, ErrContainerNotFound
	}

	out := make([]BlobInfo, 0, len(c.Blobs))
	for _, stored := range c.Blobs {
		out = append(out, stored.info())
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

// Reset clears all in-memory state. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.containers = make(map[string]*storedContainer)
}

// lookupBlobLocked resolves a (container, blob) pair. Callers must hold
// b.mu (either read or write).
func (b *InMemoryBackend) lookupBlobLocked(container, blob string) (*storedBlob, error) {
	c, ok := b.containers[container]
	if !ok {
		return nil, ErrContainerNotFound
	}

	stored, ok := c.Blobs[blob]
	if !ok {
		return nil, ErrBlobNotFound
	}

	return stored, nil
}

// computeETag derives a quoted ETag from the blob body, matching the shape
// (a quoted opaque token) real Azure Storage ETags take, without attempting
// to replicate Azure's actual internal ETag algorithm.
func computeETag(data []byte) string {
	sum := md5.Sum(data) //nolint:gosec // content fingerprint only, not a security use of MD5

	return `"` + hex.EncodeToString(sum[:]) + `"`
}
