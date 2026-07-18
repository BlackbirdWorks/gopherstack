package cognitoidp

import (
	"fmt"
	"sort"
)

// CreateResourceServer creates an OAuth 2.0 resource server for a user pool.
func (b *InMemoryBackend) CreateResourceServer(
	userPoolID, identifier, name string,
	scopes []ResourceServerScope,
) (*ResourceServer, error) {
	b.mu.Lock("CreateResourceServer")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, exists := b.resourceServers.Get(resourceServerKey(userPoolID, identifier)); exists {
		return nil, fmt.Errorf(
			"%w: resource server %q already exists in pool %q",
			ErrAlreadyExists,
			identifier,
			userPoolID,
		)
	}

	scopesCopy := make([]ResourceServerScope, len(scopes))
	copy(scopesCopy, scopes)

	rs := &ResourceServer{
		UserPoolID: userPoolID,
		Identifier: identifier,
		Name:       name,
		Scopes:     scopesCopy,
	}
	b.resourceServers.Put(rs)

	cp := *rs

	return &cp, nil
}

// DescribeResourceServer returns a resource server by pool ID and identifier.
func (b *InMemoryBackend) DescribeResourceServer(userPoolID, identifier string) (*ResourceServer, error) {
	b.mu.RLock("DescribeResourceServer")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	rs, ok := b.resourceServers.Get(resourceServerKey(userPoolID, identifier))
	if !ok {
		return nil, fmt.Errorf(
			"%w: resource server %q not found in pool %q",
			ErrUserPoolNotFound,
			identifier,
			userPoolID,
		)
	}

	cp := *rs

	return &cp, nil
}

// ListResourceServers returns all resource servers for a user pool sorted by identifier.
func (b *InMemoryBackend) ListResourceServers(userPoolID string) ([]*ResourceServer, error) {
	b.mu.RLock("ListResourceServers")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	poolServers := b.resourceServersByPool.Get(userPoolID)
	out := make([]*ResourceServer, 0, len(poolServers))

	for _, rs := range poolServers {
		cp := *rs
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Identifier < out[j].Identifier })

	return out, nil
}

// UpdateResourceServer updates the name and scopes of an existing resource server.
func (b *InMemoryBackend) UpdateResourceServer(
	userPoolID, identifier, name string,
	scopes []ResourceServerScope,
) (*ResourceServer, error) {
	b.mu.Lock("UpdateResourceServer")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	rs, ok := b.resourceServers.Get(resourceServerKey(userPoolID, identifier))
	if !ok {
		return nil, fmt.Errorf(
			"%w: resource server %q not found in pool %q",
			ErrUserPoolNotFound,
			identifier,
			userPoolID,
		)
	}

	if name != "" {
		rs.Name = name
	}

	if scopes != nil {
		scopesCopy := make([]ResourceServerScope, len(scopes))
		copy(scopesCopy, scopes)
		rs.Scopes = scopesCopy
	}

	cp := *rs

	return &cp, nil
}

// DeleteResourceServer removes a resource server from a user pool.
func (b *InMemoryBackend) DeleteResourceServer(userPoolID, identifier string) error {
	b.mu.Lock("DeleteResourceServer")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.resourceServers.Get(resourceServerKey(userPoolID, identifier)); !ok {
		return fmt.Errorf(
			"%w: resource server %q not found in pool %q",
			ErrUserPoolNotFound,
			identifier,
			userPoolID,
		)
	}

	b.resourceServers.Delete(resourceServerKey(userPoolID, identifier))

	return nil
}
