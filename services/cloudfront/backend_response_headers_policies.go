package cloudfront

import (
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// CreateResponseHeadersPolicy creates a new Response Headers Policy.
func (b *InMemoryBackend) CreateResponseHeadersPolicy(
	name, comment string,
	opts ...*ResponseHeadersPolicyConfig,
) (*ResponseHeadersPolicy, error) {
	b.mu.Lock("CreateResponseHeadersPolicy")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.responseHeadersPolicyByName[name]; exists {
		return nil, fmt.Errorf(
			"%w: response headers policy with name %q already exists",
			ErrResponseHeadersPolicyAlreadyExists,
			name,
		)
	}

	id := generateID()
	p := &ResponseHeadersPolicy{
		ID:      id,
		Name:    name,
		Comment: comment,
		ETag:    uuid.NewString(),
	}

	if len(opts) > 0 && opts[0] != nil {
		cfg := opts[0]
		p.CorsConfig = cfg.CorsConfig
		p.SecurityHeaders = cfg.SecurityHeaders
		p.CustomHeaders = cfg.CustomHeaders
		p.RemoveHeaders = cfg.RemoveHeaders
	}

	b.responseHeadersPolicies.Put(p)
	b.responseHeadersPolicyByName[name] = id
	cp := *p

	return &cp, nil
}

// GetResponseHeadersPolicy returns a Response Headers Policy by ID.
func (b *InMemoryBackend) GetResponseHeadersPolicy(id string) (*ResponseHeadersPolicy, error) {
	b.mu.RLock("GetResponseHeadersPolicy")
	defer b.mu.RUnlock()

	p, ok := b.responseHeadersPolicies.Get(id)
	if !ok {
		return nil, fmt.Errorf(
			"%w: response headers policy %s not found",
			ErrResponseHeadersPolicyNotFound,
			id,
		)
	}

	cp := *p

	return &cp, nil
}

// ListResponseHeadersPolicies returns all Response Headers Policies sorted by ID.
func (b *InMemoryBackend) ListResponseHeadersPolicies() []*ResponseHeadersPolicy {
	b.mu.RLock("ListResponseHeadersPolicies")
	defer b.mu.RUnlock()

	list := make([]*ResponseHeadersPolicy, 0, b.responseHeadersPolicies.Len())
	for _, p := range b.responseHeadersPolicies.All() {
		cp := *p
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdateResponseHeadersPolicy updates an existing Response Headers Policy.
func (b *InMemoryBackend) UpdateResponseHeadersPolicy(
	id, name, comment string,
	opts ...*ResponseHeadersPolicyConfig,
) (*ResponseHeadersPolicy, error) {
	b.mu.Lock("UpdateResponseHeadersPolicy")
	defer b.mu.Unlock()

	p, ok := b.responseHeadersPolicies.Get(id)
	if !ok {
		return nil, fmt.Errorf(
			"%w: response headers policy %s not found",
			ErrResponseHeadersPolicyNotFound,
			id,
		)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if name != p.Name {
		if _, exists := b.responseHeadersPolicyByName[name]; exists {
			return nil, fmt.Errorf(
				"%w: response headers policy with name %q already exists",
				ErrResponseHeadersPolicyAlreadyExists,
				name,
			)
		}

		delete(b.responseHeadersPolicyByName, p.Name)
		b.responseHeadersPolicyByName[name] = id
	}

	p.Name = name
	p.Comment = comment
	p.ETag = uuid.NewString()
	if len(opts) > 0 && opts[0] != nil {
		cfg := opts[0]
		p.CorsConfig = cfg.CorsConfig
		p.SecurityHeaders = cfg.SecurityHeaders
		p.CustomHeaders = cfg.CustomHeaders
		p.RemoveHeaders = cfg.RemoveHeaders
	}

	cp := *p

	return &cp, nil
}

// DeleteResponseHeadersPolicy deletes a Response Headers Policy by ID.
func (b *InMemoryBackend) DeleteResponseHeadersPolicy(id string) error {
	b.mu.Lock("DeleteResponseHeadersPolicy")
	defer b.mu.Unlock()

	p, ok := b.responseHeadersPolicies.Get(id)
	if !ok {
		return fmt.Errorf(
			"%w: response headers policy %s not found",
			ErrResponseHeadersPolicyNotFound,
			id,
		)
	}

	if b.tokenReferencedByAnyDistribution(id) {
		return fmt.Errorf(
			"%w: response headers policy %s is attached to a distribution",
			ErrResponseHeadersPolicyInUse, id,
		)
	}

	delete(b.responseHeadersPolicyByName, p.Name)
	b.responseHeadersPolicies.Delete(id)

	return nil
}

// --- CloudFront Function CRUD ---
