package persistence

// NullStore is a no-op Store that preserves zero-persistence behaviour.
// It is the default when PERSIST is not set.
type NullStore struct{}

// Save is a no-op.
func (NullStore) Save(_, _ string, _ []byte) error { return nil }

// Load always returns ErrKeyNotFound.
func (NullStore) Load(_, _ string) ([]byte, error) { return nil, ErrKeyNotFound }

// Delete is a no-op.
func (NullStore) Delete(_, _ string) error { return nil }

// ListKeys always returns an empty slice.
func (NullStore) ListKeys(_ string) ([]string, error) { return nil, nil }
