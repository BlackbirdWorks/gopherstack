package azureblob

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"maps"
	"slices"
	"sort"
	"strings"
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
	clock      func() time.Time
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
	return NewInMemoryBackendWithClock(time.Now)
}

// NewInMemoryBackendWithClock is NewInMemoryBackend with an injectable clock
// driving timestamps, lease expiry and uncommitted-block expiry.
func NewInMemoryBackendWithClock(clock func() time.Time) *InMemoryBackend {
	return &InMemoryBackend{
		mu:         lockmetrics.New("azureblob"),
		clock:      clock,
		containers: make(map[string]*storedContainer),
	}
}

func (b *InMemoryBackend) now() time.Time { return b.clock().UTC() }

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

	now := b.now()
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

	return c.info(now), nil
}

// DeleteContainer removes a container and all of its blobs. Returns
// ErrContainerNotFound if the container does not exist. An active container
// lease requires the matching lease id.
func (b *InMemoryBackend) DeleteContainer(name string, pre ...Precond) error {
	b.mu.Lock("DeleteContainer")
	defer b.mu.Unlock()

	c, ok := b.containers[name]
	if !ok {
		return ErrContainerNotFound
	}

	now := b.now()
	p := firstPre(pre)

	if err := c.Lease.gate(now, p.LeaseID, true, "Container"); err != nil {
		return err
	}

	if err := p.checkContainer(c.info(now).LastModified); err != nil {
		return err
	}

	delete(b.containers, name)

	return nil
}

// ListContainers returns a snapshot of all containers, sorted by name (the
// order Azure's List Containers returns them in).
func (b *InMemoryBackend) ListContainers() []ContainerInfo {
	page, _ := b.ListContainersPage(ListContainersOptions{})

	return page.Containers
}

// ListContainersPage lists containers with prefix filtering and marker
// paging. Returns ErrInvalidMarker for a marker it did not issue.
func (b *InMemoryBackend) ListContainersPage(opts ListContainersOptions) (ContainerPage, error) {
	b.mu.RLock("ListContainers")
	defer b.mu.RUnlock()

	now := b.now()
	names := make([]string, 0, len(b.containers))

	for name := range b.containers {
		if strings.HasPrefix(name, opts.Prefix) {
			names = append(names, name)
		}
	}

	sort.Strings(names)

	if opts.Marker != "" {
		raw, err := base64.RawURLEncoding.DecodeString(opts.Marker)
		if err != nil {
			return ContainerPage{}, ErrInvalidMarker
		}

		names = names[sort.SearchStrings(names, string(raw)):]
	}

	var page ContainerPage

	if opts.MaxResults > 0 && len(names) > opts.MaxResults {
		page.NextMarker = base64.RawURLEncoding.EncodeToString([]byte(names[opts.MaxResults]))
		names = names[:opts.MaxResults]
	}

	page.Containers = make([]ContainerInfo, len(names))
	for i, name := range names {
		page.Containers[i] = b.containers[name].info(now)
	}

	return page, nil
}

// PutBlob stores data as a block blob with only a content type set.
func (b *InMemoryBackend) PutBlob(container, blob string, data []byte, contentType string) (BlobInfo, error) {
	return b.PutBlobWith(container, blob, data, BlobAttrs{ContentType: contentType})
}

// PutBlobWith stores data as a block blob named blob within container,
// replacing any existing blob but keeping its snapshots and lease, as Azure
// does. Returns ErrContainerNotFound if the container does not exist.
func (b *InMemoryBackend) PutBlobWith(
	container, blob string, data []byte, attrs BlobAttrs, pre ...Precond,
) (BlobInfo, error) {
	b.mu.Lock("PutBlob")
	defer b.mu.Unlock()

	c, ok := b.containers[container]
	if !ok {
		return BlobInfo{}, ErrContainerNotFound
	}

	if err := b.guardWriteLocked(c, blob, firstPre(pre)); err != nil {
		return BlobInfo{}, err
	}

	stored := b.storeBlobLocked(c, blob, slices.Clone(data), attrs)

	return stored.info(b.now()), nil
}

// guardWriteLocked applies the lease and conditional checks for a write that
// creates or replaces blob.
func (b *InMemoryBackend) guardWriteLocked(c *storedContainer, blob string, p Precond) error {
	old, exists := c.Blobs[blob]
	if exists {
		if err := old.Lease.gate(b.now(), p.LeaseID, true, "Blob"); err != nil {
			return err
		}

		return p.check(condCreate, true, old.ETag, old.LastModified, old.Tags)
	}

	if p.LeaseID != "" {
		return (*leaseRecord)(nil).gate(b.now(), p.LeaseID, true, "Blob")
	}

	return p.check(condCreate, false, "", time.Time{}, nil)
}

// storeBlobLocked replaces c.Blobs[blob] with a new blob holding data,
// carrying over the old blob's snapshots and lease.
func (b *InMemoryBackend) storeBlobLocked(c *storedContainer, blob string, data []byte, attrs BlobAttrs) *storedBlob {
	b.etagSeq++

	stored := &storedBlob{
		Name:         blob,
		Data:         data,
		LastModified: b.now(),
		ETag:         computeBlobETag(data, b.etagSeq),
	}
	stored.applyAttrs(attrs)
	stored.Metadata = maps.Clone(attrs.Metadata)
	stored.Tags = maps.Clone(attrs.Tags)

	if attrs.AccessTier != "" {
		stored.AccessTier = attrs.AccessTier
		stored.AccessTierChanged = stored.LastModified
	}

	if old, exists := c.Blobs[blob]; exists {
		stored.Snapshots = old.Snapshots
		stored.Lease = old.Lease
	}

	c.Blobs[blob] = stored

	return stored
}

// GetBlob returns a blob's metadata and full body; the slice is immutable
// backend storage and must not be modified.
func (b *InMemoryBackend) GetBlob(container, blob string) (BlobInfo, []byte, error) {
	return b.GetBlobAt(container, blob, "")
}

// GetBlobAt is GetBlob for the base blob (snapshot == "") or one snapshot.
// Conditional headers and an optional lease id are evaluated.
func (b *InMemoryBackend) GetBlobAt(container, blob, snapshot string, pre ...Precond) (BlobInfo, []byte, error) {
	b.mu.RLock("GetBlob")
	defer b.mu.RUnlock()

	stored, err := b.readableLocked(container, blob, snapshot, firstPre(pre))
	if err != nil {
		return BlobInfo{}, nil, err
	}

	return stored.info(b.now()), stored.Data, nil
}

// readableLocked resolves a blob for a read and applies lease and
// conditional checks.
func (b *InMemoryBackend) readableLocked(container, blob, snapshot string, p Precond) (*storedBlob, error) {
	stored, err := b.lookupBlobLocked(container, blob, snapshot)
	if err != nil {
		return nil, err
	}

	if snapshot == "" {
		base := b.containers[container].Blobs[blob]
		if err = base.Lease.gate(b.now(), p.LeaseID, false, "Blob"); err != nil {
			return nil, err
		}
	}

	if err = p.check(condRead, true, stored.ETag, stored.LastModified, stored.Tags); err != nil {
		return nil, err
	}

	return stored, nil
}

// HeadBlob returns a blob's metadata without its body.
func (b *InMemoryBackend) HeadBlob(container, blob string) (BlobInfo, error) {
	return b.HeadBlobAt(container, blob, "")
}

// HeadBlobAt is HeadBlob for the base blob (snapshot == "") or one snapshot.
func (b *InMemoryBackend) HeadBlobAt(container, blob, snapshot string, pre ...Precond) (BlobInfo, error) {
	b.mu.RLock("HeadBlob")
	defer b.mu.RUnlock()

	stored, err := b.readableLocked(container, blob, snapshot, firstPre(pre))
	if err != nil {
		return BlobInfo{}, err
	}

	return stored.info(b.now()), nil
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

	now := b.now()
	out := make([]BlobInfo, 0, len(c.Blobs))
	for _, stored := range c.Blobs {
		out = append(out, stored.info(now))
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
