package s3

import (
	"encoding/json"
	"maps"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

type backendSnapshot struct {
	Buckets       map[string]map[string]*StoredBucket          `json:"buckets"`
	Tags          map[string][]types.Tag                       `json:"tags"`
	Uploads       map[string]map[string]*StoredMultipartUpload `json:"uploads"`
	DefaultRegion string                                       `json:"defaultRegion"`
}

// UnmarshalJSON implements [json.Unmarshaler] so that backendSnapshot can decode
// both the current nested uploads format introduced in issue #620 and the
// legacy flat format used by older snapshots:
//
//	legacy:  {"uploads": {"<uploadID>": {uploadID, bucket, key, …}}}
//	current: {"uploads": {"<bucket>":   {"<uploadID>": {…}}}}
//
// Detection: if a top-level uploads value has a non-empty "uploadID" field it
// is a StoredMultipartUpload (legacy); otherwise it is a bucket-level map.
func (s *backendSnapshot) UnmarshalJSON(data []byte) error {
	type snapshotRaw struct {
		Buckets       map[string]map[string]*StoredBucket `json:"buckets"`
		Tags          map[string][]types.Tag              `json:"tags"`
		Uploads       map[string]json.RawMessage          `json:"uploads"`
		DefaultRegion string                              `json:"defaultRegion"`
	}

	var raw snapshotRaw
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	s.Buckets = raw.Buckets
	s.Tags = raw.Tags
	s.DefaultRegion = raw.DefaultRegion
	s.Uploads = migrateUploads(raw.Uploads)

	return nil
}

// migrateUploads converts the raw uploads JSON into the nested
// map[bucket]map[uploadID]*StoredMultipartUpload format, transparently
// upgrading the legacy flat map[uploadID]*StoredMultipartUpload shape.
func migrateUploads(raw map[string]json.RawMessage) map[string]map[string]*StoredMultipartUpload {
	nested := make(map[string]map[string]*StoredMultipartUpload, len(raw))

	for topKey, value := range raw {
		// Probe: does this value look like a StoredMultipartUpload (legacy flat entry)?
		var probe struct {
			UploadID string `json:"uploadID"`
		}

		if err := json.Unmarshal(value, &probe); err == nil && probe.UploadID != "" {
			// Legacy flat format — top key is the upload ID.
			var upload StoredMultipartUpload
			if unmarshalErr := json.Unmarshal(value, &upload); unmarshalErr == nil {
				bkt := upload.Bucket
				if nested[bkt] == nil {
					nested[bkt] = make(map[string]*StoredMultipartUpload)
				}
				nested[bkt][upload.UploadID] = &upload
			}

			continue
		}

		// Current nested format — top key is the bucket name.
		var bucketUploads map[string]*StoredMultipartUpload
		if err := json.Unmarshal(value, &bucketUploads); err == nil {
			nested[topKey] = bucketUploads
		}
	}

	return nested
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Buckets:       b.buckets,
		Tags:          b.tags,
		Uploads:       b.uploads,
		DefaultRegion: b.defaultRegion,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	normalizeSnapshot(&snap)
	reinitBucketMutexes(snap.Buckets)
	reinitUploadMutexes(snap.Uploads)

	b.buckets = snap.Buckets
	b.tags = snap.Tags
	b.uploads = snap.Uploads
	b.defaultRegion = snap.DefaultRegion
	b.bucketIndex = buildBucketIndex(snap.Buckets)

	return nil
}

// normalizeSnapshot ensures nil maps in the snapshot are replaced with empty maps.
func normalizeSnapshot(snap *backendSnapshot) {
	if snap.Buckets == nil {
		snap.Buckets = make(map[string]map[string]*StoredBucket)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string][]types.Tag)
	}

	if snap.Uploads == nil {
		snap.Uploads = make(map[string]map[string]*StoredMultipartUpload)
	}
}

// reinitBucketMutexes reinitialises per-bucket and per-object mutexes after
// deserialisation, since [sync.Mutex] values cannot be serialised.
func reinitBucketMutexes(buckets map[string]map[string]*StoredBucket) {
	for _, regionBuckets := range buckets {
		for _, bucket := range regionBuckets {
			if bucket.mu == nil {
				bucket.mu = lockmetrics.New("s3-bucket")
			}

			if bucket.Objects == nil {
				bucket.Objects = make(map[string]*StoredObject)
			}

			for _, obj := range bucket.Objects {
				if obj.mu == nil {
					obj.mu = lockmetrics.New("s3-object")
				}
			}
		}
	}
}

// reinitUploadMutexes reinitialises per-upload mutexes after deserialisation.
func reinitUploadMutexes(uploads map[string]map[string]*StoredMultipartUpload) {
	for _, bucketUploads := range uploads {
		for _, u := range bucketUploads {
			if u.mu == nil {
				u.mu = lockmetrics.New("s3.upload")
			}
		}
	}
}

// buildBucketIndex constructs the name→region index from the bucket map.
// Active (non-pending) buckets take precedence over pending-delete entries
// to ensure getBucket resolves to the live bucket after a Restore. Pending
// buckets are included only when no active entry exists for that name, so
// that idempotent DeleteBucket calls still work after a Restore.
func buildBucketIndex(buckets map[string]map[string]*StoredBucket) map[string]string {
	index := make(map[string]string)

	// Two-pass approach: first register active buckets, then fill in any
	// pending-only names. This makes the result deterministic regardless of
	// map iteration order.
	pendingOnly := make(map[string]string)

	for region, regionBuckets := range buckets {
		for bucketName, bucket := range regionBuckets {
			if bucket.DeletePending {
				// Record as pending-only candidate; active entry wins.
				if _, activeExists := index[bucketName]; !activeExists {
					pendingOnly[bucketName] = region
				}
			} else {
				index[bucketName] = region
				// Remove any pending-only candidate now that active is known.
				delete(pendingOnly, bucketName)
			}
		}
	}

	maps.Copy(index, pendingOnly)

	return index
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *S3Handler) Snapshot() []byte {
	type snapshotter interface{ Snapshot() []byte }
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *S3Handler) Restore(data []byte) error {
	type restorer interface{ Restore([]byte) error }
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(data)
	}

	return nil
}
