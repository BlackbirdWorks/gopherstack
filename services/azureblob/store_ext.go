package azureblob

import (
	"encoding/base64"
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
	c.LastModified = time.Now().UTC()
	c.ETag = computeBlobETag([]byte(c.Name), b.etagSeq)
}

// GetContainer returns a container's properties, metadata and ACL.
func (b *InMemoryBackend) GetContainer(name string) (ContainerInfo, error) {
	b.mu.RLock("GetContainer")
	defer b.mu.RUnlock()

	c, err := b.containerLocked(name)
	if err != nil {
		return ContainerInfo{}, err
	}

	return c.info(), nil
}

// SetContainerMetadata replaces a container's metadata.
func (b *InMemoryBackend) SetContainerMetadata(name string, md map[string]string) (ContainerInfo, error) {
	b.mu.Lock("SetContainerMetadata")
	defer b.mu.Unlock()

	c, err := b.containerLocked(name)
	if err != nil {
		return ContainerInfo{}, err
	}

	c.Metadata = maps.Clone(md)
	b.touchContainerLocked(c)

	return c.info(), nil
}

// SetContainerACL replaces a container's public-access level and stored
// access policies.
func (b *InMemoryBackend) SetContainerACL(name, publicAccess string, ids []SignedIdentifier) (ContainerInfo, error) {
	b.mu.Lock("SetContainerACL")
	defer b.mu.Unlock()

	c, err := b.containerLocked(name)
	if err != nil {
		return ContainerInfo{}, err
	}

	c.PublicAccess = publicAccess
	c.ACL = slices.Clone(ids)
	b.touchContainerLocked(c)

	return c.info(), nil
}

func (b *InMemoryBackend) touchBlobLocked(s *storedBlob) {
	b.etagSeq++
	s.LastModified = time.Now().UTC()
	s.ETag = computeBlobETag(s.Data, b.etagSeq)
}

// SetBlobProperties replaces the content properties of a blob. Properties
// absent from attrs are cleared, as in Set Blob Properties.
func (b *InMemoryBackend) SetBlobProperties(container, blob string, attrs BlobAttrs) (BlobInfo, error) {
	b.mu.Lock("SetBlobProperties")
	defer b.mu.Unlock()

	s, err := b.lookupBlobLocked(container, blob, "")
	if err != nil {
		return BlobInfo{}, err
	}

	s.applyAttrs(attrs)
	b.touchBlobLocked(s)

	return s.info(), nil
}

// SetBlobMetadata replaces a blob's metadata.
func (b *InMemoryBackend) SetBlobMetadata(container, blob string, md map[string]string) (BlobInfo, error) {
	b.mu.Lock("SetBlobMetadata")
	defer b.mu.Unlock()

	s, err := b.lookupBlobLocked(container, blob, "")
	if err != nil {
		return BlobInfo{}, err
	}

	s.Metadata = maps.Clone(md)
	b.touchBlobLocked(s)

	return s.info(), nil
}

// SetBlobTier sets the access tier and returns the tier it replaced.
func (b *InMemoryBackend) SetBlobTier(container, blob, tier string) (string, BlobInfo, error) {
	b.mu.Lock("SetBlobTier")
	defer b.mu.Unlock()

	s, err := b.lookupBlobLocked(container, blob, "")
	if err != nil {
		return "", BlobInfo{}, err
	}

	prev := s.AccessTier
	if prev == "" {
		prev = TierHot
	}

	s.AccessTier = tier
	s.AccessTierChanged = time.Now().UTC()

	return prev, s.info(), nil
}

// uniqueSnapshotID returns a snapshot timestamp not already used by s.
func uniqueSnapshotID(s *storedBlob) string {
	t := time.Now().UTC()

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
func (b *InMemoryBackend) SnapshotBlob(container, blob string, md map[string]string) (BlobInfo, error) {
	b.mu.Lock("SnapshotBlob")
	defer b.mu.Unlock()

	s, err := b.lookupBlobLocked(container, blob, "")
	if err != nil {
		return BlobInfo{}, err
	}

	snap := *s
	snap.Snapshots = nil
	snap.Data = slices.Clone(s.Data)
	snap.Snapshot = uniqueSnapshotID(s)

	if md != nil {
		snap.Metadata = maps.Clone(md)
	} else {
		snap.Metadata = maps.Clone(s.Metadata)
	}

	if s.Snapshots == nil {
		s.Snapshots = make(map[string]*storedBlob)
	}

	s.Snapshots[snap.Snapshot] = &snap

	return snap.info(), nil
}

// CopyBlob copies a blob (or snapshot) within the account. A nil md inherits
// the source metadata.
func (b *InMemoryBackend) CopyBlob(
	srcContainer, srcBlob, srcSnapshot, dstContainer, dstBlob string, md map[string]string,
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

	b.etagSeq++

	dst := &storedBlob{
		Name:         dstBlob,
		Data:         slices.Clone(src.Data),
		LastModified: time.Now().UTC(),
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
	}

	dstC.Blobs[dstBlob] = dst

	return dst.info(), nil
}

// DeleteBlobAt deletes a snapshot (snapshot != ""), or the base blob subject
// to the x-ms-delete-snapshots mode: with snapshots present, "" fails with
// ErrSnapshotsPresent, "include" removes all, "only" removes just snapshots.
func (b *InMemoryBackend) DeleteBlobAt(container, blob, snapshot, mode string) error {
	b.mu.Lock("DeleteBlob")
	defer b.mu.Unlock()

	s, err := b.lookupBlobLocked(container, blob, "")
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

func (c *storedContainer) listItems(opts ListBlobsOptions) []listItem {
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
				add(snap.info(), listKey(name, id))
			}
		}

		add(s.info(), listKey(name, ""))
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

	items := c.listItems(opts)

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
