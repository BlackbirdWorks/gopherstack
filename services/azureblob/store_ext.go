package azureblob

import (
	"encoding/base64"
	"errors"
	"net/http"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"
)

// DeleteSnapshots values of the x-ms-delete-snapshots header.
const (
	DeleteSnapshotsInclude = "include"
	DeleteSnapshotsOnly    = "only"
)

// snapshotTimeFormat is Azure's snapshot id format (7 fractional digits).
const snapshotTimeFormat = "2006-01-02T15:04:05.0000000Z"

// ListBlobsOptions are the List Blobs query parameters.
type ListBlobsOptions struct {
	Prefix           string
	Delimiter        string
	Marker           string
	MaxResults       int
	IncludeSnapshots bool
}

// ListEntry is one item of a List Blobs page: a blob or a BlobPrefix.
type ListEntry struct {
	Prefix string
	Blob   BlobInfo
	// IsPrefix selects Prefix over Blob.
	IsPrefix bool
}

// BlobPage is one page of a List Blobs response.
type BlobPage struct {
	NextMarker string
	Entries    []ListEntry
}

func (s *storedBlob) applyAttrs(a BlobAttrs) {
	s.ContentType = a.ContentType
	s.ContentEncoding = a.ContentEncoding
	s.ContentLanguage = a.ContentLanguage
	s.ContentDisposition = a.ContentDisposition
	s.CacheControl = a.CacheControl
	s.ContentMD5 = a.ContentMD5
}

func (b *InMemoryBackend) containerLocked(name string) (*storedContainer, error) {
	c, ok := b.containers[name]
	if !ok {
		return nil, ErrContainerNotFound
	}

	return c, nil
}

func (b *InMemoryBackend) touchContainerLocked(c *storedContainer) {
	b.etagSeq++
	c.LastModified = b.now()
	c.ETag = computeBlobETag([]byte(c.Name), b.etagSeq)
}

// GetContainer returns a container's properties, metadata and ACL. A lease
// id, when given, must match an active lease.
func (b *InMemoryBackend) GetContainer(name string, pre ...Precond) (ContainerInfo, error) {
	b.mu.RLock("GetContainer")
	defer b.mu.RUnlock()

	c, err := b.containerLocked(name)
	if err != nil {
		return ContainerInfo{}, err
	}

	now := b.now()
	if err = c.Lease.gate(now, firstPre(pre).LeaseID, false, "Container"); err != nil {
		return ContainerInfo{}, err
	}

	return c.info(now), nil
}

// writableContainerLocked resolves a container for a write guarded by an
// optional lease id and the time conditions.
func (b *InMemoryBackend) writableContainerLocked(name string, p Precond) (*storedContainer, error) {
	c, err := b.containerLocked(name)
	if err != nil {
		return nil, err
	}

	now := b.now()
	if err = c.Lease.gate(now, p.LeaseID, false, "Container"); err != nil {
		return nil, err
	}

	if err = p.checkContainer(c.info(now).LastModified); err != nil {
		return nil, err
	}

	return c, nil
}

// SetContainerMetadata replaces a container's metadata.
func (b *InMemoryBackend) SetContainerMetadata(
	name string, md map[string]string, pre ...Precond,
) (ContainerInfo, error) {
	b.mu.Lock("SetContainerMetadata")
	defer b.mu.Unlock()

	c, err := b.writableContainerLocked(name, firstPre(pre))
	if err != nil {
		return ContainerInfo{}, err
	}

	c.Metadata = maps.Clone(md)
	b.touchContainerLocked(c)

	return c.info(b.now()), nil
}

// SetContainerACL replaces a container's public-access level and stored
// access policies.
func (b *InMemoryBackend) SetContainerACL(
	name, publicAccess string, ids []SignedIdentifier, pre ...Precond,
) (ContainerInfo, error) {
	b.mu.Lock("SetContainerACL")
	defer b.mu.Unlock()

	c, err := b.writableContainerLocked(name, firstPre(pre))
	if err != nil {
		return ContainerInfo{}, err
	}

	c.PublicAccess = publicAccess
	c.ACL = slices.Clone(ids)
	b.touchContainerLocked(c)

	return c.info(b.now()), nil
}

func (b *InMemoryBackend) touchBlobLocked(s *storedBlob) {
	b.etagSeq++
	s.LastModified = b.now()
	s.ETag = computeBlobETag(s.Data, b.etagSeq)
}

// writableBlobLocked resolves a base blob for a write: the lease gate
// (require: an active lease demands its id) and conditional headers apply.
func (b *InMemoryBackend) writableBlobLocked(container, blob string, p Precond, require bool) (*storedBlob, error) {
	s, err := b.lookupBlobLocked(container, blob, "")
	if err != nil {
		return nil, err
	}

	if err = s.Lease.gate(b.now(), p.LeaseID, require, "Blob"); err != nil {
		return nil, err
	}

	if err = p.check(condWrite, true, s.ETag, s.LastModified); err != nil {
		return nil, err
	}

	return s, nil
}

// SetBlobProperties replaces the content properties of a blob. Properties
// absent from attrs are cleared, as in Set Blob Properties.
func (b *InMemoryBackend) SetBlobProperties(
	container, blob string, attrs BlobAttrs, pre ...Precond,
) (BlobInfo, error) {
	b.mu.Lock("SetBlobProperties")
	defer b.mu.Unlock()

	s, err := b.writableBlobLocked(container, blob, firstPre(pre), true)
	if err != nil {
		return BlobInfo{}, err
	}

	s.applyAttrs(attrs)
	b.touchBlobLocked(s)

	return s.info(b.now()), nil
}

// SetBlobMetadata replaces a blob's metadata.
func (b *InMemoryBackend) SetBlobMetadata(
	container, blob string, md map[string]string, pre ...Precond,
) (BlobInfo, error) {
	b.mu.Lock("SetBlobMetadata")
	defer b.mu.Unlock()

	s, err := b.writableBlobLocked(container, blob, firstPre(pre), true)
	if err != nil {
		return BlobInfo{}, err
	}

	s.Metadata = maps.Clone(md)
	b.touchBlobLocked(s)

	return s.info(b.now()), nil
}

// SetBlobTier sets the access tier and returns the tier it replaced.
func (b *InMemoryBackend) SetBlobTier(container, blob, tier string, pre ...Precond) (string, BlobInfo, error) {
	b.mu.Lock("SetBlobTier")
	defer b.mu.Unlock()

	s, err := b.writableBlobLocked(container, blob, firstPre(pre), false)
	if err != nil {
		return "", BlobInfo{}, err
	}

	prev := s.AccessTier
	if prev == "" {
		prev = TierHot
	}

	s.AccessTier = tier
	s.AccessTierChanged = b.now()

	return prev, s.info(b.now()), nil
}

// uniqueSnapshotID returns a snapshot timestamp not already used by s.
func uniqueSnapshotID(s *storedBlob, t time.Time) string {
	for {
		id := t.Format(snapshotTimeFormat)
		if _, taken := s.Snapshots[id]; !taken {
			return id
		}

		t = t.Add(time.Microsecond)
	}
}

// SnapshotBlob freezes a copy of the blob. A nil md inherits the blob's
// metadata; a non-nil md (even empty) replaces it.
func (b *InMemoryBackend) SnapshotBlob(container, blob string, md map[string]string, pre ...Precond) (BlobInfo, error) {
	b.mu.Lock("SnapshotBlob")
	defer b.mu.Unlock()

	s, err := b.writableBlobLocked(container, blob, firstPre(pre), false)
	if err != nil {
		return BlobInfo{}, err
	}

	snap := *s
	snap.Snapshots = nil
	snap.Lease = nil
	snap.Data = slices.Clone(s.Data)
	snap.Blocks = slices.Clone(s.Blocks)
	snap.Snapshot = uniqueSnapshotID(s, b.now())

	if md != nil {
		snap.Metadata = maps.Clone(md)
	} else {
		snap.Metadata = maps.Clone(s.Metadata)
	}

	if s.Snapshots == nil {
		s.Snapshots = make(map[string]*storedBlob)
	}

	s.Snapshots[snap.Snapshot] = &snap

	return snap.info(b.now()), nil
}

// CopyBlob copies a blob (or snapshot) within the account. A nil md inherits
// the source metadata.
func (b *InMemoryBackend) CopyBlob(
	srcContainer, srcBlob, srcSnapshot, dstContainer, dstBlob string, md map[string]string, pre ...Precond,
) (BlobInfo, error) {
	b.mu.Lock("CopyBlob")
	defer b.mu.Unlock()

	src, err := b.lookupBlobLocked(srcContainer, srcBlob, srcSnapshot)
	if err != nil {
		return BlobInfo{}, err
	}

	dstC, err := b.containerLocked(dstContainer)
	if err != nil {
		return BlobInfo{}, err
	}

	p := firstPre(pre)
	if p.Source != nil {
		if err = p.Source.check(condWrite, true, src.ETag, src.LastModified); err != nil {
			return BlobInfo{}, asSourceCondition(err)
		}
	}

	if err = b.guardWriteLocked(dstC, dstBlob, p); err != nil {
		return BlobInfo{}, err
	}

	b.etagSeq++

	dst := &storedBlob{
		Name:         dstBlob,
		Blocks:       slices.Clone(src.Blocks),
		Data:         slices.Clone(src.Data),
		LastModified: b.now(),
		ETag:         computeBlobETag(src.Data, b.etagSeq),
	}
	dst.applyAttrs(BlobAttrs{
		ContentType:        src.ContentType,
		ContentEncoding:    src.ContentEncoding,
		ContentLanguage:    src.ContentLanguage,
		ContentDisposition: src.ContentDisposition,
		CacheControl:       src.CacheControl,
		ContentMD5:         src.ContentMD5,
	})

	if md != nil {
		dst.Metadata = maps.Clone(md)
	} else {
		dst.Metadata = maps.Clone(src.Metadata)
	}

	if old, exists := dstC.Blobs[dstBlob]; exists {
		dst.Snapshots = old.Snapshots
		dst.Lease = old.Lease
	}

	dstC.Blobs[dstBlob] = dst

	return dst.info(b.now()), nil
}

// DeleteBlobAt deletes a snapshot (snapshot != ""), or the base blob subject
// to the x-ms-delete-snapshots mode: with snapshots present, "" fails with
// ErrSnapshotsPresent, "include" removes all, "only" removes just snapshots.
func (b *InMemoryBackend) DeleteBlobAt(container, blob, snapshot, mode string, pre ...Precond) error {
	b.mu.Lock("DeleteBlob")
	defer b.mu.Unlock()

	s, err := b.writableBlobLocked(container, blob, firstPre(pre), snapshot == "")
	if err != nil {
		return err
	}

	if snapshot != "" {
		if _, ok := s.Snapshots[snapshot]; !ok {
			return ErrBlobNotFound
		}

		delete(s.Snapshots, snapshot)

		return nil
	}

	if mode == DeleteSnapshotsOnly {
		s.Snapshots = nil

		return nil
	}

	if len(s.Snapshots) > 0 && mode != DeleteSnapshotsInclude {
		return ErrSnapshotsPresent
	}

	delete(b.containers[container].Blobs, blob)

	return nil
}

type listItem struct {
	key   string
	entry ListEntry
}

// listKey orders a snapshot before its base blob, both after any shorter name.
func listKey(name, snapshot string) string {
	if snapshot == "" {
		return name + "\x00~"
	}

	return name + "\x00" + snapshot
}

func (c *storedContainer) listItems(opts ListBlobsOptions, now time.Time) []listItem {
	items := make([]listItem, 0, len(c.Blobs))
	seen := make(map[string]struct{})

	add := func(info BlobInfo, key string) {
		rest := strings.TrimPrefix(info.Name, opts.Prefix)
		if opts.Delimiter != "" {
			if idx := strings.Index(rest, opts.Delimiter); idx >= 0 {
				p := opts.Prefix + rest[:idx+len(opts.Delimiter)]
				if _, dup := seen[p]; !dup {
					seen[p] = struct{}{}
					items = append(items, listItem{key: p, entry: ListEntry{IsPrefix: true, Prefix: p}})
				}

				return
			}
		}

		items = append(items, listItem{key: key, entry: ListEntry{Blob: info}})
	}

	for name, s := range c.Blobs {
		if !strings.HasPrefix(name, opts.Prefix) {
			continue
		}

		if opts.IncludeSnapshots {
			for id, snap := range s.Snapshots {
				add(snap.info(now), listKey(name, id))
			}
		}

		add(s.info(now), listKey(name, ""))
	}

	sort.Slice(items, func(i, j int) bool { return items[i].key < items[j].key })

	return items
}

// ListBlobsPage lists blobs with prefix/delimiter collapsing and marker
// paging. Returns ErrContainerNotFound or ErrInvalidMarker.
func (b *InMemoryBackend) ListBlobsPage(container string, opts ListBlobsOptions) (BlobPage, error) {
	b.mu.RLock("ListBlobsPage")
	defer b.mu.RUnlock()

	c, err := b.containerLocked(container)
	if err != nil {
		return BlobPage{}, err
	}

	items := c.listItems(opts, b.now())

	if opts.Marker != "" {
		raw, decErr := base64.RawURLEncoding.DecodeString(opts.Marker)
		if decErr != nil {
			return BlobPage{}, ErrInvalidMarker
		}

		from := string(raw)
		items = items[sort.Search(len(items), func(i int) bool { return items[i].key >= from }):]
	}

	var page BlobPage

	if opts.MaxResults > 0 && len(items) > opts.MaxResults {
		page.NextMarker = base64.RawURLEncoding.EncodeToString([]byte(items[opts.MaxResults].key))
		items = items[:opts.MaxResults]
	}

	page.Entries = make([]ListEntry, len(items))
	for i := range items {
		page.Entries[i] = items[i].entry
	}

	return page, nil
}

// asSourceCondition relabels a failed copy-source condition.
func asSourceCondition(err error) error {
	var se *StorageError
	if errors.As(err, &se) && se.Code == "ConditionNotMet" {
		return storageErr(http.StatusPreconditionFailed, "SourceConditionNotMet",
			"The source condition specified using HTTP conditional header(s) is not met.")
	}

	return err
}

// ListContainersOptions are the List Containers query parameters.
type ListContainersOptions struct {
	Prefix     string
	Marker     string
	MaxResults int
}

// ContainerPage is one page of a List Containers response.
type ContainerPage struct {
	NextMarker string
	Containers []ContainerInfo
}
