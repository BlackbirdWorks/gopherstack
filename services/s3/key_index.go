package s3

import "slices"

// indexInsert adds key to the bucket's sorted key index. Callers must hold
// bucket.mu for writing and must call this only when key is genuinely new to
// bucket.Objects -- an overwrite of an existing key must not call it.
func (bucket *StoredBucket) indexInsert(key string) {
	i, found := slices.BinarySearch(bucket.keyIndex, key)
	if found {
		return
	}

	bucket.keyIndex = slices.Insert(bucket.keyIndex, i, key)
}

// indexRemove removes key from the bucket's sorted key index. Callers must
// hold bucket.mu for writing and must call this only when key is actually
// removed from bucket.Objects.
func (bucket *StoredBucket) indexRemove(key string) {
	i, found := slices.BinarySearch(bucket.keyIndex, key)
	if !found {
		return
	}

	bucket.keyIndex = slices.Delete(bucket.keyIndex, i, i+1)
}

// rebuildKeyIndex derives bucket.keyIndex from bucket.Objects. keyIndex is
// never persisted (it is an unexported field, skipped by encoding/json), so
// this must run after every snapshot restore.
func rebuildKeyIndex(bucket *StoredBucket) {
	bucket.keyIndex = make([]string, 0, len(bucket.Objects))
	for key := range bucket.Objects {
		bucket.keyIndex = append(bucket.keyIndex, key)
	}

	slices.Sort(bucket.keyIndex)
}
