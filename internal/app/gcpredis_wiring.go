package app

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpredisbackend "github.com/blackbirdworks/gopherstack/services/gcpredis"
	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

// gcsRedisStore lets Memorystore import and export RDB files through the in-process GCS emulator.
type gcsRedisStore struct {
	b *gcpstoragebackend.InMemoryBackend
}

func (s gcsRedisStore) ReadObject(bucket, object string) ([]byte, error) {
	o, err := s.b.GetObject(bucket, object, 0, gcpstoragebackend.Preconds{})
	if err != nil {
		return nil, fmt.Errorf("read gs://%s/%s: %w", bucket, object, err)
	}

	return o.Data, nil
}

func (s gcsRedisStore) WriteObject(bucket, object string, data []byte) error {
	if _, err := s.b.PutObject(
		bucket,
		object,
		data,
		gcpstoragebackend.Patch{},
		gcpstoragebackend.Preconds{},
	); err != nil {
		return fmt.Errorf("write gs://%s/%s: %w", bucket, object, err)
	}

	return nil
}

// wireGCPRedis connects Memorystore import and export to the GCS emulator.
func wireGCPRedis(byName map[string]service.Registerable) {
	redisH, ok := byName["GCPRedis"].(*gcpredisbackend.Handler)
	if !ok {
		return
	}

	if gcs, isGCS := byName["GCPStorage"].(*gcpstoragebackend.Handler); isGCS {
		redisH.Backend.SetObjectStore(gcsRedisStore{b: gcs.Backend})
	}
}
