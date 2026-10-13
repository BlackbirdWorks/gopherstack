package s3

import (
	"slices"
	"sort"
)

const (
	keyChunkMax  = 512
	keyChunkInit = 16
)

// keyIndex is a sorted (UTF-8 binary) key set stored as bounded chunks, so an
// insert or delete shifts at most keyChunkMax keys instead of the whole set.
type keyIndex struct {
	chunks [][]string
}

// keyCursor addresses one key in a keyIndex; it is valid while ci < len(chunks).
type keyCursor struct{ ci, off int }

func (x *keyIndex) len() int {
	n := 0
	for _, c := range x.chunks {
		n += len(c)
	}

	return n
}

func (x *keyIndex) key(c keyCursor) string { return x.chunks[c.ci][c.off] }

func (x *keyIndex) valid(c keyCursor) bool { return c.ci < len(x.chunks) }

func (x *keyIndex) next(c keyCursor) keyCursor {
	c.off++
	if c.off >= len(x.chunks[c.ci]) {
		c.ci++
		c.off = 0
	}

	return c
}

// seek returns the first key satisfying pred, which must be monotone (false then true).
func (x *keyIndex) seek(pred func(string) bool) keyCursor {
	return x.seekFrom(keyCursor{}, pred)
}

// seekFrom is seek restricted to keys at or after from; pred must be monotone from there.
func (x *keyIndex) seekFrom(from keyCursor, pred func(string) bool) keyCursor {
	if !x.valid(from) {
		return from
	}

	lo := from.ci + sort.Search(len(x.chunks)-from.ci, func(i int) bool {
		c := x.chunks[from.ci+i]

		return pred(c[len(c)-1])
	})
	if lo >= len(x.chunks) {
		return keyCursor{ci: len(x.chunks)}
	}

	start := 0
	if lo == from.ci {
		start = from.off
	}

	c := x.chunks[lo]
	off := start + sort.Search(len(c)-start, func(i int) bool { return pred(c[start+i]) })

	return keyCursor{ci: lo, off: off}
}

// remaining counts keys at or after c.
func (x *keyIndex) remaining(c keyCursor) int {
	if !x.valid(c) {
		return 0
	}

	r := len(x.chunks[c.ci]) - c.off
	for _, ch := range x.chunks[c.ci+1:] {
		r += len(ch)
	}

	return r
}

func (x *keyIndex) chunkFor(key string) int {
	ci := sort.Search(len(x.chunks), func(i int) bool {
		c := x.chunks[i]

		return c[len(c)-1] >= key
	})

	return ci
}

func (x *keyIndex) insert(key string) {
	if len(x.chunks) == 0 {
		x.chunks = [][]string{make([]string, 1, keyChunkInit)}
		x.chunks[0][0] = key

		return
	}

	ci := x.chunkFor(key)
	if ci == len(x.chunks) {
		ci--
	}

	c := x.chunks[ci]
	i, found := slices.BinarySearch(c, key)
	if found {
		return
	}

	c = slices.Insert(c, i, key)

	if len(c) <= keyChunkMax {
		x.chunks[ci] = c

		return
	}

	half := len(c) >> 1
	right := make([]string, len(c)-half, keyChunkMax)
	copy(right, c[half:])
	clear(c[half:])
	x.chunks[ci] = c[:half]
	x.chunks = slices.Insert(x.chunks, ci+1, right)
}

func (x *keyIndex) remove(key string) {
	ci := x.chunkFor(key)
	if ci == len(x.chunks) {
		return
	}

	c := x.chunks[ci]
	i, found := slices.BinarySearch(c, key)
	if !found {
		return
	}

	if len(c) == 1 {
		x.chunks = slices.Delete(x.chunks, ci, ci+1)

		return
	}

	x.chunks[ci] = slices.Delete(c, i, i+1)
}

// indexInsert adds key to the bucket's sorted key index. Callers must hold
// bucket.mu for writing and must call this only when key is genuinely new to
// bucket.Objects -- an overwrite of an existing key must not call it.
func (bucket *StoredBucket) indexInsert(key string) { bucket.keyIndex.insert(key) }

// indexRemove removes key from the bucket's sorted key index. Callers must
// hold bucket.mu for writing and must call this only when key is actually
// removed from bucket.Objects.
func (bucket *StoredBucket) indexRemove(key string) { bucket.keyIndex.remove(key) }

// rebuildKeyIndex derives bucket.keyIndex from bucket.Objects. keyIndex is
// never persisted (it is an unexported field, skipped by encoding/json), so
// this must run after every snapshot restore.
func rebuildKeyIndex(bucket *StoredBucket) {
	keys := make([]string, 0, len(bucket.Objects))
	for key := range bucket.Objects {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	const fill = keyChunkMax >> 1

	var idx keyIndex
	for len(keys) > 0 {
		take := min(fill, len(keys))
		chunk := make([]string, take, keyChunkMax)
		copy(chunk, keys[:take])
		idx.chunks = append(idx.chunks, chunk)
		keys = keys[take:]
	}

	bucket.keyIndex = idx
}
