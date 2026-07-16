package cognitoidp

import "fmt"

// CreateTerms sets the terms and conditions text for a user pool.
func (b *InMemoryBackend) CreateTerms(userPoolID, text string) (*Terms, error) {
	b.mu.Lock("CreateTerms")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	t := &Terms{UserPoolID: userPoolID, Text: text}
	b.terms.Put(t)
	cp := *t

	return &cp, nil
}

// DescribeTerms returns the terms and conditions for a user pool.
func (b *InMemoryBackend) DescribeTerms(userPoolID string) (*Terms, error) {
	b.mu.RLock("DescribeTerms")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	t, _ := b.terms.Get(userPoolID)
	if t == nil {
		return &Terms{UserPoolID: userPoolID}, nil
	}

	cp := *t

	return &cp, nil
}

// ListTerms returns terms for a pool (returns slice of at most one element).
func (b *InMemoryBackend) ListTerms(userPoolID string) ([]*Terms, error) {
	b.mu.RLock("ListTerms")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	t, _ := b.terms.Get(userPoolID)
	if t == nil {
		return []*Terms{}, nil
	}

	cp := *t

	return []*Terms{&cp}, nil
}

// UpdateTerms replaces the terms text for a user pool.
func (b *InMemoryBackend) UpdateTerms(userPoolID, text string) (*Terms, error) {
	b.mu.Lock("UpdateTerms")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	t := &Terms{UserPoolID: userPoolID, Text: text}
	b.terms.Put(t)
	cp := *t

	return &cp, nil
}

// DeleteTerms removes the terms and conditions for a user pool.
func (b *InMemoryBackend) DeleteTerms(userPoolID string) error {
	b.mu.Lock("DeleteTerms")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	b.terms.Delete(userPoolID)

	return nil
}
