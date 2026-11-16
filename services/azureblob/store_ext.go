package azureblob

import (
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"net/http"
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
	IncludeTags      bool
	IncludeDeleted   bool
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

	if err = p.check(condWrite, true, s.ETag, s.LastModified, s.Tags); err != nil {
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
	s.clearCopy()
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

// Rehydration times of an Archive blob moved to an online tier, by x-ms-rehydrate-priority (the upper bounds the
// Blob rehydration documentation gives for Standard and High priority).
const (
	RehydratePriorityStandard = "Standard"
	RehydratePriorityHigh     = "High"
	RehydrateStandardAfter    = 15 * time.Hour
	RehydrateHighAfter        = time.Hour
)

// TierRequest is a Set Blob Tier request.
type TierRequest struct {
	Tier     string
	Priority string
}

// SetBlobTier sets the access tier at standard rehydration priority and returns the tier it replaced.
func (b *InMemoryBackend) SetBlobTier(container, blob, tier string, pre ...Precond) (string, BlobInfo, error) {
	return b.SetBlobTierWith(container, blob, TierRequest{Tier: tier}, pre...)
}

// SetBlobTierWith sets the access tier. Leaving Archive starts an asynchronous rehydration that completes once the
// priority's duration has elapsed; until then the blob stays Archive with x-ms-archive-status set.
func (b *InMemoryBackend) SetBlobTierWith(
	container, blob string, req TierRequest, pre ...Precond,
) (string, BlobInfo, error) {
	b.mu.Lock("SetBlobTier")
	defer b.mu.Unlock()

	s, err := b.writableBlobLocked(container, blob, firstPre(pre), false)
	if err != nil {
		return "", BlobInfo{}, err
	}

	now := b.now()
	s.settleRehydration(now)

	prev := s.AccessTier
	if prev == "" {
		prev = TierHot
	}

	switch {
	case prev == TierArchive && req.Tier == TierArchive:
		s.RehydrateTo, s.RehydrateDue = "", time.Time{}
	case prev == TierArchive:
		after := RehydrateStandardAfter
		if req.Priority == RehydratePriorityHigh {
			after = RehydrateHighAfter
		}

		s.RehydrateTo, s.RehydrateDue = req.Tier, now.Add(after)
	default:
		s.AccessTier = req.Tier
		s.AccessTierChanged = now
	}

	return prev, s.info(now), nil
}

func (s *storedBlob) settleRehydration(now time.Time) {
	if s.RehydrateTo == "" || now.Before(s.RehydrateDue) {
		return
	}

	s.AccessTier, s.AccessTierChanged = s.RehydrateTo, s.RehydrateDue
	s.RehydrateTo, s.RehydrateDue = "", time.Time{}
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
	snap.cloneKind()
	snap.Blocks = slices.Clone(s.Blocks)
	snap.Tags = maps.Clone(s.Tags)
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

// CopyRequest carries the destination settings of a Copy Blob. A nil Metadata inherits the source's; tags are
// never inherited. SourceURL is recorded as x-ms-copy-source with any SAS removed.
type CopyRequest struct {
	Metadata  map[string]string
	Tags      map[string]string
	SourceURL string
}

// ExternalBlob is the content and properties of a blob fetched from a source URL.
type ExternalBlob struct {
	Metadata map[string]string
	Data     []byte
	Attrs    BlobAttrs
}

// CopyBlob copies a blob (or snapshot) within the account and records the x-ms-copy-* properties on the destination.
func (b *InMemoryBackend) CopyBlob(
	srcContainer, srcBlob, srcSnapshot, dstContainer, dstBlob string, req CopyRequest, pre ...Precond,
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
		if err = p.Source.check(condWrite, true, src.ETag, src.LastModified, src.Tags); err != nil {
			return BlobInfo{}, asSourceCondition(err)
		}
	}

	if err = b.guardWriteLocked(dstC, dstBlob, p); err != nil {
		return BlobInfo{}, err
	}

	dst := &storedBlob{
		Name:         dstBlob,
		Blocks:       slices.Clone(src.Blocks),
		Data:         slices.Clone(src.Data),
		BlobType:     src.BlobType,
		PageLength:   src.PageLength,
		AppendBlocks: src.AppendBlocks,
		Pages:        slices.Clone(src.Pages),
	}
	dst.cloneKind()
	dst.applyAttrs(BlobAttrs{
		ContentType:        src.ContentType,
		ContentEncoding:    src.ContentEncoding,
		ContentLanguage:    src.ContentLanguage,
		ContentDisposition: src.ContentDisposition,
		CacheControl:       src.CacheControl,
		ContentMD5:         src.ContentMD5,
	})

	md := req.Metadata
	if md == nil {
		md = src.Metadata
	}

	return b.finishCopyLocked(dstC, dst, md, req), nil
}

// CopyBlobData stores content fetched from a source URL as a block blob and records the copy properties.
func (b *InMemoryBackend) CopyBlobData(
	dstContainer, dstBlob string, src ExternalBlob, req CopyRequest, pre ...Precond,
) (BlobInfo, error) {
	b.mu.Lock("CopyBlobData")
	defer b.mu.Unlock()

	dstC, err := b.containerLocked(dstContainer)
	if err != nil {
		return BlobInfo{}, err
	}

	if err = b.guardWriteLocked(dstC, dstBlob, firstPre(pre)); err != nil {
		return BlobInfo{}, err
	}

	dst := &storedBlob{Name: dstBlob, Data: slices.Clone(src.Data)}
	dst.applyAttrs(src.Attrs)

	md := req.Metadata
	if md == nil {
		md = src.Metadata
	}

	return b.finishCopyLocked(dstC, dst, md, req), nil
}

func (b *InMemoryBackend) finishCopyLocked(
	dstC *storedContainer, dst *storedBlob, md map[string]string, req CopyRequest,
) BlobInfo {
	b.etagSeq++
	now := b.now()
	dst.Tags = maps.Clone(req.Tags)
	dst.Metadata = maps.Clone(md)
	dst.LastModified = now
	dst.ETag = computeBlobETag(dst.Data, b.etagSeq)
	dst.CopyID = newRequestID()
	dst.CopyStatus = CopyStatusSuccess
	dst.CopySource = req.SourceURL
	dst.CopyCompletion = now
	dst.CopyProgress = fmt.Sprintf("%d/%d", dst.length(), dst.length())

	if old, exists := dstC.Blobs[dst.Name]; exists {
		dst.Snapshots = old.Snapshots
		dst.Lease = old.Lease
	}

	dstC.Blobs[dst.Name] = dst

	return dst.info(now)
}

// AbortCopy aborts a pending copy. Copies complete synchronously, so a destination never has one pending.
func (b *InMemoryBackend) AbortCopy(container, blob, copyID string, pre ...Precond) error {
	b.mu.RLock("AbortCopy")
	defer b.mu.RUnlock()

	s, err := b.lookupBlobLocked(container, blob, "")
	if err != nil {
		return err
	}

	if err = s.Lease.gate(b.now(), firstPre(pre).LeaseID, false, "Blob"); err != nil {
		return err
	}

	if s.CopyID == "" || s.CopyStatus != CopyStatusPending {
		return storageErr(http.StatusConflict, "NoPendingCopyOperation",
			"There is currently no pending copy operation.")
	}

	if s.CopyID != copyID {
		return storageErr(http.StatusConflict, "CopyIdMismatch",
			"The specified copy ID does not match the copy ID of the pending copy operation.")
	}

	return nil
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

	c := b.containers[container]
	delete(c.Blobs, blob)
	b.softDeleteLocked(c, s)

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
		if p, ok := collapsePrefix(info.Name, opts, seen); ok {
			if p != "" {
				items = append(items, listItem{key: p, entry: ListEntry{IsPrefix: true, Prefix: p}})
			}

			return
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

	if opts.IncludeDeleted {
		for name, s := range c.Deleted {
			if strings.HasPrefix(name, opts.Prefix) && !s.purgeDue(now) {
				add(s.info(now), name+"\x00~d")
			}
		}
	}

	sort.Slice(items, func(i, j int) bool { return items[i].key < items[j].key })

	return items
}

// collapsePrefix reports whether name folds into a delimiter prefix; the returned prefix is empty when that
// prefix was already emitted.
func collapsePrefix(name string, opts ListBlobsOptions, seen map[string]struct{}) (string, bool) {
	if opts.Delimiter == "" {
		return "", false
	}

	rest := strings.TrimPrefix(name, opts.Prefix)

	idx := strings.Index(rest, opts.Delimiter)
	if idx < 0 {
		return "", false
	}

	p := opts.Prefix + rest[:idx+len(opts.Delimiter)]
	if _, dup := seen[p]; dup {
		return "", true
	}

	seen[p] = struct{}{}

	return p, true
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
