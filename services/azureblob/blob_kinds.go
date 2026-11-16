package azureblob

import (
	"net/http"
	"slices"
	"sort"
	"time"
)

// Blob types of the x-ms-blob-type header.
const (
	BlobTypeBlock  = "BlockBlob"
	BlobTypeAppend = "AppendBlob"
	BlobTypePage   = "PageBlob"
)

// Page and append blob limits (Put Page, Append Block, Put Blob references).
const (
	PageSize           = 512
	MaxPageWrite       = 4 << 20
	MaxAppendBlockSize = 4 << 20
	// MaxAppendBlocks is the committed-block limit of an append blob.
	MaxAppendBlocks = 50000
	// MaxPageBlobLength is the emulator's size cap for a page blob (the service allows 8 TiB).
	MaxPageBlobLength = 256 << 20
)

type pageExtent struct {
	Data   []byte
	Offset int64
}

// PageRange is an inclusive byte range of valid pages.
type PageRange struct {
	Start int64
	End   int64
}

// AppendOptions are the conditional headers of Append Block.
type AppendOptions struct {
	MaxSize   *int64
	AppendPos *int64
}

// AppendResult reports where an Append Block landed.
type AppendResult struct {
	Info   BlobInfo
	Offset int64
}

func (b *storedBlob) blobType() string {
	if b.BlobType == "" {
		return BlobTypeBlock
	}

	return b.BlobType
}

func (b *storedBlob) length() int64 {
	if b.BlobType == BlobTypePage {
		return b.PageLength
	}

	return int64(len(b.Data))
}

// body returns the blob content; page blobs are assembled with zeros for unwritten pages.
func (b *storedBlob) body() []byte {
	if b.BlobType != BlobTypePage {
		return b.Data
	}

	out := make([]byte, b.PageLength)
	for _, e := range b.Pages {
		copy(out[e.Offset:], e.Data)
	}

	return out
}

func (b *storedBlob) cloneKind() {
	b.Pages = slices.Clone(b.Pages)
	for i := range b.Pages {
		b.Pages[i].Data = slices.Clone(b.Pages[i].Data)
	}
}

// carve removes [start,end) from the extents.
func (b *storedBlob) carve(start, end int64) {
	out := make([]pageExtent, 0, len(b.Pages)+1)

	for _, e := range b.Pages {
		eEnd := e.Offset + int64(len(e.Data))
		if eEnd <= start || e.Offset >= end {
			out = append(out, e)

			continue
		}

		if e.Offset < start {
			out = append(out, pageExtent{Offset: e.Offset, Data: e.Data[:start-e.Offset]})
		}

		if eEnd > end {
			out = append(out, pageExtent{Offset: end, Data: e.Data[end-e.Offset:]})
		}
	}

	b.Pages = out
}

func (b *storedBlob) ranges() []PageRange {
	exts := slices.Clone(b.Pages)
	sort.Slice(exts, func(i, j int) bool { return exts[i].Offset < exts[j].Offset })

	var out []PageRange

	for _, e := range exts {
		end := e.Offset + int64(len(e.Data)) - 1
		if n := len(out); n > 0 && out[n-1].End+1 == e.Offset {
			out[n-1].End = end

			continue
		}

		out = append(out, PageRange{Start: e.Offset, End: end})
	}

	return out
}

func errInvalidBlobType() *StorageError {
	return storageErr(http.StatusConflict, "InvalidBlobType",
		"The blob type is invalid for this operation.")
}

func errInvalidPageRange() *StorageError {
	return storageErr(http.StatusRequestedRangeNotSatisfiable, "InvalidPageRange",
		"The page range specified is invalid.")
}

// AppendBlock appends data to an append blob.
func (b *InMemoryBackend) AppendBlock(
	container, blob string, data []byte, opts AppendOptions, pre ...Precond,
) (AppendResult, error) {
	b.mu.Lock("AppendBlock")
	defer b.mu.Unlock()

	s, err := b.writableBlobLocked(container, blob, firstPre(pre), true)
	if err != nil {
		return AppendResult{}, err
	}

	if s.BlobType != BlobTypeAppend {
		return AppendResult{}, errInvalidBlobType()
	}

	off := int64(len(s.Data))

	if opts.AppendPos != nil && *opts.AppendPos != off {
		return AppendResult{}, storageErr(http.StatusPreconditionFailed, "AppendPositionConditionNotMet",
			"The append position condition specified was not met.")
	}

	if opts.MaxSize != nil && off+int64(len(data)) > *opts.MaxSize {
		return AppendResult{}, storageErr(http.StatusPreconditionFailed, "MaxBlobSizeConditionNotMet",
			"The max blob size condition specified was not met.")
	}

	if s.AppendBlocks >= MaxAppendBlocks {
		return AppendResult{}, storageErr(http.StatusConflict, "BlockCountExceedsLimit",
			"The committed block count cannot exceed the maximum limit of 50,000 blocks.")
	}

	s.Data = append(s.Data, data...)
	s.AppendBlocks++
	b.touchBlobLocked(s)

	return AppendResult{Info: s.info(b.now()), Offset: off}, nil
}

// PutPage writes or clears the page range [start,end] of a page blob.
func (b *InMemoryBackend) PutPage(
	container, blob string, start, end int64, data []byte, wipe bool, pre ...Precond,
) (BlobInfo, error) {
	b.mu.Lock("PutPage")
	defer b.mu.Unlock()

	s, err := b.writableBlobLocked(container, blob, firstPre(pre), true)
	if err != nil {
		return BlobInfo{}, err
	}

	if s.BlobType != BlobTypePage {
		return BlobInfo{}, errInvalidBlobType()
	}

	if start < 0 || end < start || start%PageSize != 0 || (end+1)%PageSize != 0 || end >= s.PageLength {
		return BlobInfo{}, errInvalidPageRange()
	}

	s.carve(start, end+1)

	if !wipe {
		s.Pages = append(s.Pages, pageExtent{Offset: start, Data: slices.Clone(data)})
	}

	b.touchBlobLocked(s)

	return s.info(b.now()), nil
}

// GetPageRanges lists the valid page ranges of a page blob or snapshot, optionally within [from,to].
func (b *InMemoryBackend) GetPageRanges(
	container, blob, snapshot string, from, to *int64, pre ...Precond,
) ([]PageRange, BlobInfo, error) {
	b.mu.RLock("GetPageRanges")
	defer b.mu.RUnlock()

	s, err := b.readableLocked(container, blob, snapshot, firstPre(pre))
	if err != nil {
		return nil, BlobInfo{}, err
	}

	if s.BlobType != BlobTypePage {
		return nil, BlobInfo{}, errInvalidBlobType()
	}

	lo, hi := int64(0), s.PageLength-1
	if from != nil {
		lo = *from
	}

	if to != nil {
		hi = *to
	}

	if lo < 0 || hi < lo || hi >= s.PageLength {
		return nil, BlobInfo{}, errInvalidPageRange()
	}

	var out []PageRange

	for _, r := range s.ranges() {
		if r.End < lo || r.Start > hi {
			continue
		}

		out = append(out, PageRange{Start: max(r.Start, lo), End: min(r.End, hi)})
	}

	return out, s.info(b.now()), nil
}

// ResizePageBlob sets the length of a page blob; shrinking discards pages beyond it.
func (b *InMemoryBackend) ResizePageBlob(container, blob string, length int64, pre ...Precond) (BlobInfo, error) {
	b.mu.Lock("ResizePageBlob")
	defer b.mu.Unlock()

	s, err := b.writableBlobLocked(container, blob, firstPre(pre), true)
	if err != nil {
		return BlobInfo{}, err
	}

	if s.BlobType != BlobTypePage {
		return BlobInfo{}, errInvalidBlobType()
	}

	if length < 0 || length%PageSize != 0 || length > MaxPageBlobLength {
		return BlobInfo{}, errInvalidPageRange()
	}

	if length < s.PageLength {
		s.carve(length, s.PageLength)
	}

	s.PageLength = length
	b.touchBlobLocked(s)

	return s.info(b.now()), nil
}

// Values of x-ms-copy-status.
const (
	CopyStatusSuccess = "success"
	CopyStatusPending = "pending"
)

func (b *storedBlob) clearCopy() {
	b.CopyID, b.CopyStatus, b.CopySource, b.CopyProgress = "", "", "", ""
	b.CopyCompletion = time.Time{}
}

const hoursPerDay = 24

func (b *storedBlob) purgeDue(now time.Time) bool {
	return !b.DeletedAt.IsZero() && !now.Before(b.DeletedAt.AddDate(0, 0, b.RetainDays))
}

func (b *storedBlob) remainingDays(now time.Time) int {
	if b.DeletedAt.IsZero() {
		return 0
	}

	left := b.DeletedAt.AddDate(0, 0, b.RetainDays).Sub(now)

	return max(int((left+hoursPerDay*time.Hour-1)/(hoursPerDay*time.Hour)), 0)
}

// softDeleteLocked keeps a deleted blob recoverable when the delete retention policy is enabled.
func (b *InMemoryBackend) softDeleteLocked(c *storedContainer, s *storedBlob) {
	pol := b.serviceLocked().DeleteRetentionPolicy
	if pol == nil || !pol.Enabled || pol.Days == nil {
		return
	}

	s.DeletedAt, s.RetainDays = b.now(), *pol.Days
	s.Lease = nil

	if c.Deleted == nil {
		c.Deleted = make(map[string]*storedBlob)
	}

	c.Deleted[s.Name] = s
}

// UndeleteBlob restores a soft-deleted blob with its snapshots; a blob that is not deleted is left as it is.
func (b *InMemoryBackend) UndeleteBlob(container, blob string) error {
	b.mu.Lock("UndeleteBlob")
	defer b.mu.Unlock()

	c, err := b.containerLocked(container)
	if err != nil {
		return err
	}

	if _, live := c.Blobs[blob]; live {
		return nil
	}

	s, ok := c.Deleted[blob]
	if !ok || s.purgeDue(b.now()) {
		delete(c.Deleted, blob)

		return ErrBlobNotFound
	}

	s.DeletedAt, s.RetainDays = time.Time{}, 0
	c.Blobs[blob] = s
	delete(c.Deleted, blob)

	return nil
}
