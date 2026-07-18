package s3control

// PutPublicAccessBlock creates or updates the public access block configuration for an account.
func (b *InMemoryBackend) PutPublicAccessBlock(cfg PublicAccessBlock) {
	b.mu.Lock("PutPublicAccessBlock")
	defer b.mu.Unlock()

	cp := cfg
	b.configs.Put(&cp)
}

// GetPublicAccessBlock retrieves the public access block configuration for an account.
func (b *InMemoryBackend) GetPublicAccessBlock(accountID string) (*PublicAccessBlock, error) {
	b.mu.RLock("GetPublicAccessBlock")
	defer b.mu.RUnlock()

	cfg, ok := b.configs.Get(accountID)
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

	all := b.configs.All()
	out := make([]PublicAccessBlock, 0, len(all))

	for _, cfg := range all {
		out = append(out, *cfg)
	}

	return out
}

// DeletePublicAccessBlock deletes the public access block configuration for an account.
func (b *InMemoryBackend) DeletePublicAccessBlock(accountID string) error {
	b.mu.Lock("DeletePublicAccessBlock")
	defer b.mu.Unlock()

	if !b.configs.Delete(accountID) {
		return ErrNotFound
	}

	return nil
}

// AddPublicAccessBlockInternal stores a public access block directly, for seeding test data.
func (b *InMemoryBackend) AddPublicAccessBlockInternal(accountID string, block *PublicAccessBlock) {
	b.mu.Lock("AddPublicAccessBlockInternal")
	defer b.mu.Unlock()

	cp := *block
	cp.AccountID = accountID
	b.configs.Put(&cp)
}
