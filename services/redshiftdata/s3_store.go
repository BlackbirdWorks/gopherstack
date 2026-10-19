package redshiftdata

import "context"

// ObjectStore is the S3 surface COPY FROM and UNLOAD TO run against; the emulator's S3 backs it.
type ObjectStore interface {
	GetObject(ctx context.Context, bucket, key string) ([]byte, error)
	PutObject(ctx context.Context, bucket, key string, body []byte) error
	DeleteObject(ctx context.Context, bucket, key string) error
	// ListKeys returns every key under prefix in ascending order.
	ListKeys(ctx context.Context, bucket, prefix string) ([]string, error)
}

type storeHolder struct{ store ObjectStore }

// SetObjectStore wires the S3 that COPY FROM 's3://...' and UNLOAD TO 's3://...' read and write.
func (b *InMemoryBackend) SetObjectStore(s ObjectStore) {
	b.mu.Lock("SetObjectStore")
	defer b.mu.Unlock()

	if b.engineState == nil {
		b.engineState = &engineState{exec: NewPGXExecutor()}
	}

	if x, ok := b.engineState.exec.(*pgxExecutor); ok {
		x.store.Store(&storeHolder{store: s})
	}
}

// SetObjectStore wires the S3 behind COPY and UNLOAD into the Data API backend.
func (h *Handler) SetObjectStore(s ObjectStore) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.SetObjectStore(s)
	}
}
