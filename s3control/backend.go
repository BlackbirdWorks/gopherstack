package s3control

import (
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrNotFound is returned when public access block config is not found.
	ErrNotFound = awserr.New("NoSuchPublicAccessBlockConfiguration", awserr.ErrNotFound)
)

// PublicAccessBlock represents the S3 Control public access block configuration.
type PublicAccessBlock struct {
	AccountID             string
	BlockPublicAcls       bool
	IgnorePublicAcls      bool
	BlockPublicPolicy     bool
	RestrictPublicBuckets bool
}

// InMemoryBackend is the in-memory store for S3 Control resources.
type InMemoryBackend struct {
	configs map[string]*PublicAccessBlock
	mu      *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		configs: make(map[string]*PublicAccessBlock),
		mu:      lockmetrics.New("s3control"),
	}
}

// PutPublicAccessBlock creates or updates the public access block configuration for an account.
func (b *InMemoryBackend) PutPublicAccessBlock(cfg PublicAccessBlock) {
	b.mu.Lock("PutPublicAccessBlock")
	defer b.mu.Unlock()

	cp := cfg
	b.configs[cfg.AccountID] = &cp
}

// GetPublicAccessBlock retrieves the public access block configuration for an account.
func (b *InMemoryBackend) GetPublicAccessBlock(accountID string) (*PublicAccessBlock, error) {
	b.mu.RLock("GetPublicAccessBlock")
	defer b.mu.RUnlock()

	cfg, ok := b.configs[accountID]
	if !ok {
		return nil, ErrNotFound
	}

	cp := *cfg

	return &cp, nil
}

// ListAll returns all stored public access block configurations.
func (b *InMemoryBackend) ListAll() []PublicAccessBlock {
	b.mu.RLock("ListAll")
	defer b.mu.RUnlock()

	out := make([]PublicAccessBlock, 0, len(b.configs))
	for _, cfg := range b.configs {
		out = append(out, *cfg)
	}

	return out
}

// DeletePublicAccessBlock deletes the public access block configuration for an account.
func (b *InMemoryBackend) DeletePublicAccessBlock(accountID string) error {
	b.mu.Lock("DeletePublicAccessBlock")
	defer b.mu.Unlock()

	if _, ok := b.configs[accountID]; !ok {
		return ErrNotFound
	}

	delete(b.configs, accountID)

	return nil
}
