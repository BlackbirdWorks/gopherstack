package s3

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

type backendSnapshot struct {
	Buckets       map[string]map[string]*StoredBucket `json:"buckets"`
	Tags          map[string][]types.Tag              `json:"tags"`
	Uploads       map[string]*StoredMultipartUpload   `json:"uploads"`
	DefaultRegion string                              `json:"defaultRegion"`
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

	if snap.Buckets == nil {
		snap.Buckets = make(map[string]map[string]*StoredBucket)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string][]types.Tag)
	}

	if snap.Uploads == nil {
		snap.Uploads = make(map[string]*StoredMultipartUpload)
	}

	// Reinitialise per-bucket and per-object mutexes after deserialisation.
	for _, regionBuckets := range snap.Buckets {
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

	b.buckets = snap.Buckets
	b.tags = snap.Tags
	b.uploads = snap.Uploads
	b.defaultRegion = snap.DefaultRegion

	return nil
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
