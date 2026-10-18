package azureblob

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// InMemoryBackend implements StorageBackend using in-memory maps guarded by a
// single RWMutex. Shaped after services/sqs's InMemoryBackend, but simpler:
// Azure Blob's MVP surface (see AZURE.md/PARITY.md) has no janitor, no
// metrics emitter, and no cross-resource relationships to track, so a single
// coarse lock over one map of containers is sufficient.
type InMemoryBackend struct {
	mu         *lockmetrics.RWMutex
	containers map[string]*storedContainer
	// etagSeq is a monotonically increasing counter mixed into every ETag
	// (see computeETag) so that overwriting a blob with byte-identical
	// content still produces a new ETag, matching real Azure Blob semantics
	// (an ETag changes on every mutation, not just on content changes) and
	// keeping If-Match/If-None-Match concurrency checks meaningful.
	etagSeq uint64
}

// NewInMemoryBackend creates a new empty InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		mu:         lockmetrics.New("azureblob"),
		containers: make(map[string]*storedContainer),
	}
}

// CreateContainer creates a new, empty container with default settings.
func (b *InMemoryBackend) CreateContainer(name string) error {
	_, err := b.CreateContainerWith(name, ContainerOptions{})

	return err
}

// CreateContainerWith creates a container with metadata and a public-access
// level. Returns ErrContainerAlreadyExists if the name is taken.
func (b *InMemoryBackend) CreateContainerWith(name string, opts ContainerOptions) (ContainerInfo, error) {
	b.mu.Lock("CreateContainer")
	defer b.mu.Unlock()

	if _, ok := b.containers[name]; ok {
		return ContainerInfo{}, ErrContainerAlreadyExists
	}

	now := time.Now().UTC()
	b.etagSeq++

	c := &storedContainer{
		Name:         name,
		CreatedAt:    now,
		LastModified: now,
		ETag:         computeBlobETag([]byte(name), b.etagSeq),
		Blobs:        make(map[string]*storedBlob),
		Metadata:     maps.Clone(opts.Metadata),
		PublicAccess: opts.PublicAccess,
	}
	b.containers[name] = c

	return c.info(), nil
}

// DeleteContainer removes a container and all of its blobs. Returns
// ErrContainerNotFound if the container does not exist.
func (b *InMemoryBackend) DeleteContainer(name string) error {
	b.mu.Lock("DeleteContainer")
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
	b.mu.RLock("ListContainers")
	defer b.mu.RUnlock()

	out := make([]ContainerInfo, 0, len(b.containers))
	for _, c := range b.containers {
		out = append(out, c.info())
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// PutBlob stores data as a block blob with only a content type set.
func (b *InMemoryBackend) PutBlob(container, blob string, data []byte, contentType string) (BlobInfo, error) {
	return b.PutBlobWith(container, blob, data, BlobAttrs{ContentType: contentType})
}

// PutBlobWith stores data as a block blob named blob within container,
// replacing any existing blob but keeping its snapshots, as Azure does.
// Returns ErrContainerNotFound if the container does not exist.
func (b *InMemoryBackend) PutBlobWith(container, blob string, data []byte, attrs BlobAttrs) (BlobInfo, error) {
	b.mu.Lock("PutBlob")
	defer b.mu.Unlock()

	c, ok := b.containers[container]
	if !ok {
		return BlobInfo{}, ErrContainerNotFound
	}

	b.etagSeq++

	stored := &storedBlob{
		Name:         blob,
		Data:         append([]byte(nil), data...),
		LastModified: time.Now().UTC(),
		ETag:         computeBlobETag(data, b.etagSeq),
	}
	stored.applyAttrs(attrs)
	stored.Metadata = maps.Clone(attrs.Metadata)

	if attrs.AccessTier != "" {
		stored.AccessTier = attrs.AccessTier
		stored.AccessTierChanged = stored.LastModified
	}

	if old, exists := c.Blobs[blob]; exists {
		stored.Snapshots = old.Snapshots
	}

	c.Blobs[blob] = stored

	return stored.info(), nil
}

// GetBlob returns a blob's metadata and full body; the slice is immutable
// backend storage and must not be modified.
func (b *InMemoryBackend) GetBlob(container, blob string) (BlobInfo, []byte, error) {
	return b.GetBlobAt(container, blob, "")
}

// GetBlobAt is GetBlob for the base blob (snapshot == "") or one snapshot.
func (b *InMemoryBackend) GetBlobAt(container, blob, snapshot string) (BlobInfo, []byte, error) {
	b.mu.RLock("GetBlob")
	defer b.mu.RUnlock()

	stored, err := b.lookupBlobLocked(container, blob, snapshot)
	if err != nil {
		return BlobInfo{}, nil, err
	}

	return stored.info(), stored.Data, nil
}

// HeadBlob returns a blob's metadata without its body.
func (b *InMemoryBackend) HeadBlob(container, blob string) (BlobInfo, error) {
	return b.HeadBlobAt(container, blob, "")
}

// HeadBlobAt is HeadBlob for the base blob (snapshot == "") or one snapshot.
func (b *InMemoryBackend) HeadBlobAt(container, blob, snapshot string) (BlobInfo, error) {
	b.mu.RLock("HeadBlob")
	defer b.mu.RUnlock()

	stored, err := b.lookupBlobLocked(container, blob, snapshot)
	if err != nil {
		return BlobInfo{}, err
	}

	return stored.info(), nil
}

// DeleteBlob removes a blob that has no snapshots.
func (b *InMemoryBackend) DeleteBlob(container, blob string) error {
	return b.DeleteBlobAt(container, blob, "", "")
}

// ListBlobs returns every base blob in container, sorted by name. Returns
// ErrContainerNotFound if the container does not exist.
func (b *InMemoryBackend) ListBlobs(container string) ([]BlobInfo, error) {
	b.mu.RLock("ListBlobs")
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
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.containers = make(map[string]*storedContainer)
	b.etagSeq = 0
}

// lookupBlobLocked resolves a (container, blob[, snapshot]) triple. Callers
// must hold b.mu (either read or write).
func (b *InMemoryBackend) lookupBlobLocked(container, blob, snapshot string) (*storedBlob, error) {
	c, ok := b.containers[container]
	if !ok {
		return nil, ErrContainerNotFound
	}

	stored, ok := c.Blobs[blob]
	if !ok {
		return nil, ErrBlobNotFound
	}

	if snapshot == "" {
		return stored, nil
	}

	snap, ok := stored.Snapshots[snapshot]
	if !ok {
		return nil, ErrBlobNotFound
	}

	return snap, nil
}

// computeBlobETag derives a quoted, opaque ETag from a blob's body and seq, a
// per-backend monotonically increasing counter incremented on every mutation
// (see InMemoryBackend.etagSeq). Mixing in seq -- rather than hashing the
// body alone -- ensures overwriting a blob with byte-identical content still
// produces a new ETag, matching real Azure Blob semantics and keeping
// If-Match/If-None-Match concurrency checks meaningful. This only needs to
// match the shape (a quoted opaque token) real Azure Storage ETags take, not
// Azure's actual internal ETag algorithm, so SHA-256 (not a
// weak-hash-guarded algorithm) is used instead of MD5; MD5 is reserved for
// an actual Content-MD5 header if one is added later.
func computeBlobETag(data []byte, seq uint64) string {
	var seqBuf [8]byte
	binary.BigEndian.PutUint64(seqBuf[:], seq)

	return quotedHash(seqBuf[:], data)
}

// computeContainerETag derives a quoted, opaque ETag for a container's List
// Containers listing entry from its name and creation time. Containers in
// this MVP have no mutable properties and no If-Match semantics (see
// PARITY.md known gaps), so unlike computeBlobETag this does not need a
// per-mutation sequence number -- it only needs to be a stable, plausible
// ETag shape for azure-sdk-for-go to parse.
func computeContainerETag(name string, createdAt time.Time) string {
	return quotedHash([]byte(name), []byte(createdAt.String()))
}

// quotedHash returns a quoted, opaque SHA-256-derived token over the
// concatenation of parts. Shared by computeBlobETag (store.go) and
// computeContainerETag (handler.go's List Containers response) -- neither
// needs to replicate Azure's actual internal ETag algorithm, only its shape.
func quotedHash(parts ...[]byte) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write(p)
	}

	return `"` + hex.EncodeToString(h.Sum(nil)[:16]) + `"`
}
