package azureblob

import (
	"encoding/base64"
	"errors"
	"net/http"
	"slices"
	"sort"
	"time"
)

const (
	// UncommittedBlockTTL is how long a staged block lives without a commit.
	UncommittedBlockTTL = 7 * 24 * time.Hour
	// MaxBlocksPerBlob is the Put Block List block-count limit.
	MaxBlocksPerBlob = 50000
	// MaxBlockSize is the largest Put Block body (4000 MiB).
	MaxBlockSize = 4000 << 20

	maxBlockIDBytes = 64
)

// Block kinds in a Put Block List request and values of blocklisttype.
const (
	BlockKindCommitted   = "Committed"
	BlockKindUncommitted = "Uncommitted"
	BlockKindLatest      = "Latest"

	BlockListCommitted   = "committed"
	BlockListUncommitted = "uncommitted"
	BlockListAll         = "all"
)

type committedBlock struct {
	ID   string
	Size int64
}

type stagedBlock struct {
	At   time.Time
	Data []byte
}

// BlockRef is one entry of a Put Block List request.
type BlockRef struct {
	ID   string
	Kind string
}

// BlockEntry is one block of a Get Block List response.
type BlockEntry struct {
	Name string
	Size int64
}

// BlockList is the Get Block List result. HasBlob is false when only
// uncommitted blocks exist.
type BlockList struct {
	Committed   []BlockEntry
	Uncommitted []BlockEntry
	Blob        BlobInfo
	HasBlob     bool
}

func errInvalidBlock(code, msg string) *StorageError {
	return storageErr(http.StatusBadRequest, code, msg)
}

func decodedBlockIDLen(id string) (int, *StorageError) {
	raw, err := base64.StdEncoding.DecodeString(id)
	if err != nil || len(raw) == 0 || len(raw) > maxBlockIDBytes {
		return 0, errInvalidBlock("InvalidQueryParameterValue",
			"Value for one of the query parameters specified in the request URI is invalid.")
	}

	return len(raw), nil
}

// gcStagedLocked drops staged blocks older than UncommittedBlockTTL.
func (c *storedContainer) gcStagedLocked(now time.Time) {
	for name, blocks := range c.Staged {
		for id, blk := range blocks {
			if now.Sub(blk.At) >= UncommittedBlockTTL {
				delete(blocks, id)
			}
		}

		if len(blocks) == 0 {
			delete(c.Staged, name)
		}
	}
}

// PutBlock stages an uncommitted block for blob. All block ids of a blob must
// decode to the same length. The blob need not exist yet.
func (b *InMemoryBackend) PutBlock(container, blob, blockID string, data []byte, pre ...Precond) error {
	b.mu.Lock("PutBlock")
	defer b.mu.Unlock()

	c, err := b.containerLocked(container)
	if err != nil {
		return err
	}

	n, idErr := decodedBlockIDLen(blockID)
	if idErr != nil {
		return idErr
	}

	now := b.now()
	c.gcStagedLocked(now)

	p := firstPre(pre)

	if old, exists := c.Blobs[blob]; exists {
		if err = old.Lease.gate(now, p.LeaseID, true, "Blob"); err != nil {
			return err
		}

		for _, cb := range old.Blocks {
			if l, _ := decodedBlockIDLen(cb.ID); l != n {
				return errInvalidBlock("InvalidBlobOrBlock", "The specified blob or block content is invalid.")
			}
		}
	} else if p.LeaseID != "" {
		return (*leaseRecord)(nil).gate(now, p.LeaseID, true, "Blob")
	}

	for id := range c.Staged[blob] {
		if l, _ := decodedBlockIDLen(id); id != blockID && l != n {
			return errInvalidBlock("InvalidBlobOrBlock", "The specified blob or block content is invalid.")
		}
	}

	if c.Staged == nil {
		c.Staged = make(map[string]map[string]*stagedBlock)
	}

	if c.Staged[blob] == nil {
		c.Staged[blob] = make(map[string]*stagedBlock)
	}

	c.Staged[blob][blockID] = &stagedBlock{At: now, Data: slices.Clone(data)}

	return nil
}

// blockSource resolves one Put Block List entry to its bytes and size.
func resolveBlock(
	ref BlockRef,
	staged map[string]*stagedBlock,
	old *storedBlob,
	offsets map[string][2]int64,
) ([]byte, bool) {
	if ref.Kind != BlockKindCommitted {
		if blk, ok := staged[ref.ID]; ok {
			return blk.Data, true
		}
	}

	if ref.Kind != BlockKindUncommitted && old != nil {
		if off, ok := offsets[ref.ID]; ok {
			return old.Data[off[0]:off[1]], true
		}
	}

	return nil, false
}

func committedOffsets(old *storedBlob) map[string][2]int64 {
	offsets := make(map[string][2]int64)
	if old == nil {
		return offsets
	}

	var pos int64

	for _, cb := range old.Blocks {
		if _, dup := offsets[cb.ID]; !dup {
			offsets[cb.ID] = [2]int64{pos, pos + cb.Size}
		}

		pos += cb.Size
	}

	return offsets
}

// CommitBlockList assembles blob from staged and committed blocks, then
// discards every uncommitted block of the blob.
func (b *InMemoryBackend) CommitBlockList(
	container, blob string, refs []BlockRef, attrs BlobAttrs, pre ...Precond,
) (BlobInfo, error) {
	b.mu.Lock("CommitBlockList")
	defer b.mu.Unlock()

	c, err := b.containerLocked(container)
	if err != nil {
		return BlobInfo{}, err
	}

	if len(refs) > MaxBlocksPerBlob {
		return BlobInfo{}, storageErr(http.StatusConflict, "BlockCountExceedsLimit",
			"The committed block count cannot exceed the maximum limit of 50,000 blocks.")
	}

	c.gcStagedLocked(b.now())

	if err = b.guardWriteLocked(c, blob, firstPre(pre)); err != nil {
		return BlobInfo{}, err
	}

	old := c.Blobs[blob]
	offsets := committedOffsets(old)

	var (
		data   []byte
		blocks = make([]committedBlock, 0, len(refs))
	)

	for _, ref := range refs {
		part, ok := resolveBlock(ref, c.Staged[blob], old, offsets)
		if !ok {
			return BlobInfo{}, errInvalidBlock("InvalidBlockList", "The specified block list is invalid.")
		}

		data = append(data, part...)
		blocks = append(blocks, committedBlock{ID: ref.ID, Size: int64(len(part))})
	}

	stored := b.storeBlobLocked(c, blob, data, attrs)
	stored.Blocks = blocks
	delete(c.Staged, blob)

	return stored.info(b.now()), nil
}

// GetBlockList lists committed and/or uncommitted blocks. listType is
// "committed", "uncommitted" or "all".
func (b *InMemoryBackend) GetBlockList(
	container, blob, snapshot, listType string, pre ...Precond,
) (BlockList, error) {
	b.mu.RLock("GetBlockList")
	defer b.mu.RUnlock()

	c, err := b.containerLocked(container)
	if err != nil {
		return BlockList{}, err
	}

	now := b.now()
	p := firstPre(pre)

	var out BlockList

	if listType != BlockListUncommitted {
		out.Committed = []BlockEntry{}
	}

	if listType != BlockListCommitted {
		out.Uncommitted = liveStaged(c.Staged[blob], now)
	}

	stored, err := b.readableLocked(container, blob, snapshot, p)
	if err != nil {
		if errors.Is(err, ErrBlobNotFound) && snapshot == "" && listType != BlockListCommitted &&
			len(out.Uncommitted) > 0 {
			return out, nil
		}

		return BlockList{}, err
	}

	out.HasBlob = true
	out.Blob = stored.info(now)

	if listType != BlockListUncommitted {
		for _, cb := range stored.Blocks {
			out.Committed = append(out.Committed, BlockEntry{Name: cb.ID, Size: cb.Size})
		}
	}

	return out, nil
}

func liveStaged(staged map[string]*stagedBlock, now time.Time) []BlockEntry {
	out := make([]BlockEntry, 0, len(staged))

	for id, blk := range staged {
		if now.Sub(blk.At) < UncommittedBlockTTL {
			out = append(out, BlockEntry{Name: id, Size: int64(len(blk.Data))})
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}
