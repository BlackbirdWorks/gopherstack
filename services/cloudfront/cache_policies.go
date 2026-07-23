package cloudfront

import (
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// CreateCachePolicy creates a new cache policy.
// Names must be unique. TTLs must satisfy: 0 ≤ MinTTL ≤ DefaultTTL ≤ MaxTTL.
func (b *InMemoryBackend) CreateCachePolicy(
	name, comment string,
	defaultTTL, maxTTL, minTTL int64,
	params ...*CachePolicyParams,
) (*CachePolicy, error) {
	b.mu.Lock("CreateCachePolicy")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if minTTL < 0 {
		return nil, fmt.Errorf("%w: MinTTL must be >= 0", ErrValidation)
	}

	if defaultTTL < minTTL {
		return nil, fmt.Errorf("%w: DefaultTTL must be >= MinTTL", ErrValidation)
	}

	if maxTTL > maxCachePolicyTTL {
		return nil, fmt.Errorf("%w: MaxTTL must be <= %d, got %d", ErrValidation, maxCachePolicyTTL, maxTTL)
	}

	if maxTTL < defaultTTL {
		return nil, fmt.Errorf("%w: MaxTTL must be >= DefaultTTL", ErrValidation)
	}

	if _, exists := b.cachePolicyByName[name]; exists {
		return nil, fmt.Errorf(
			"%w: cache policy with name %q already exists",
			ErrCachePolicyAlreadyExists,
			name,
		)
	}

	var p *CachePolicyParams
	if len(params) > 0 {
		p = params[0]
	}

	id := generateID()
	policy := &CachePolicy{
		ID:         id,
		ETag:       uuid.NewString(),
		Name:       name,
		Comment:    comment,
		DefaultTTL: defaultTTL,
		MaxTTL:     maxTTL,
		MinTTL:     minTTL,
		Params:     p,
	}
	b.cachePolicies.Put(policy)
	b.cachePolicyByName[name] = id
	cp := *policy

	return &cp, nil
}

// GetCachePolicy returns a cache policy by ID.
func (b *InMemoryBackend) GetCachePolicy(id string) (*CachePolicy, error) {
	b.mu.RLock("GetCachePolicy")
	defer b.mu.RUnlock()

	p, ok := b.cachePolicies.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: cache policy %s not found", ErrCachePolicyNotFound, id)
	}

	cp := *p

	return &cp, nil
}

// ListCachePolicies returns all cache policies sorted by ID.
func (b *InMemoryBackend) ListCachePolicies() []*CachePolicy {
	b.mu.RLock("ListCachePolicies")
	defer b.mu.RUnlock()

	list := make([]*CachePolicy, 0, b.cachePolicies.Len())
	for _, p := range b.cachePolicies.All() {
		cp := *p
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdateCachePolicy updates an existing cache policy.
func (b *InMemoryBackend) UpdateCachePolicy(
	id, name, comment string,
	defaultTTL, maxTTL, minTTL int64,
	params ...*CachePolicyParams,
) (*CachePolicy, error) {
	b.mu.Lock("UpdateCachePolicy")
	defer b.mu.Unlock()

	p, ok := b.cachePolicies.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: cache policy %s not found", ErrCachePolicyNotFound, id)
	}

	if p.Managed {
		return nil, fmt.Errorf(
			"%w: cache policy %s is an AWS-managed policy and cannot be updated", ErrIllegalUpdate, id,
		)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if minTTL < 0 {
		return nil, fmt.Errorf("%w: MinTTL must be >= 0", ErrValidation)
	}

	if defaultTTL < minTTL {
		return nil, fmt.Errorf("%w: DefaultTTL must be >= MinTTL", ErrValidation)
	}

	if maxTTL > maxCachePolicyTTL {
		return nil, fmt.Errorf("%w: MaxTTL must be <= %d, got %d", ErrValidation, maxCachePolicyTTL, maxTTL)
	}

	if maxTTL < defaultTTL {
		return nil, fmt.Errorf("%w: MaxTTL must be >= DefaultTTL", ErrValidation)
	}

	// If name changed, ensure uniqueness and update index.
	if name != p.Name {
		if _, exists := b.cachePolicyByName[name]; exists {
			return nil, fmt.Errorf(
				"%w: cache policy with name %q already exists",
				ErrCachePolicyAlreadyExists,
				name,
			)
		}

		delete(b.cachePolicyByName, p.Name)
		b.cachePolicyByName[name] = id
	}

	p.Name = name
	p.Comment = comment
	p.DefaultTTL = defaultTTL
	p.MaxTTL = maxTTL
	p.MinTTL = minTTL
	p.ETag = uuid.NewString()
	if len(params) > 0 {
		p.Params = params[0]
	}

	cp := *p

	return &cp, nil
}

// DeleteCachePolicy deletes a cache policy by ID.
func (b *InMemoryBackend) DeleteCachePolicy(id string) error {
	b.mu.Lock("DeleteCachePolicy")
	defer b.mu.Unlock()

	p, ok := b.cachePolicies.Get(id)
	if !ok {
		return fmt.Errorf("%w: cache policy %s not found", ErrCachePolicyNotFound, id)
	}

	if p.Managed {
		return fmt.Errorf("%w: cache policy %s is an AWS-managed policy and cannot be deleted", ErrIllegalDelete, id)
	}

	if b.tokenReferencedByAnyDistribution(id) {
		return fmt.Errorf("%w: cache policy %s is attached to a distribution", ErrCachePolicyInUse, id)
	}

	delete(b.cachePolicyByName, p.Name)
	b.cachePolicies.Delete(id)

	return nil
}

// --- Origin Access Control CRUD ---
