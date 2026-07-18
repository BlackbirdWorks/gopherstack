package mediastore

import "context"

// PutCorsPolicy stores a CORS policy for a container.
func (b *InMemoryBackend) PutCorsPolicy(ctx context.Context, name string, rules []CorsRule) error {
	for _, r := range rules {
		if len(r.AllowedOrigins) == 0 || len(r.AllowedHeaders) == 0 {
			return ErrCorsRuleInvalid
		}
	}

	region := regionFromContext(ctx)

	b.mu.Lock("PutCorsPolicy")
	defer b.mu.Unlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return ErrContainerNotFound
	}

	ptrs := make([]*CorsRule, len(rules))
	for i := range rules {
		r := rules[i]
		ptrs[i] = &r
	}

	c.CorsPolicy = ptrs

	return nil
}

// GetCorsPolicy retrieves the CORS policy for a container.
func (b *InMemoryBackend) GetCorsPolicy(ctx context.Context, name string) ([]CorsRule, error) {
	region := regionFromContext(ctx)

	b.mu.RLock("GetCorsPolicy")
	defer b.mu.RUnlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return nil, ErrContainerNotFound
	}

	if c.CorsPolicy == nil {
		return nil, ErrCorsPolicyNotFound
	}

	rules := make([]CorsRule, len(c.CorsPolicy))
	for i, p := range c.CorsPolicy {
		rules[i] = *p
	}

	return rules, nil
}

// DeleteCorsPolicy removes the CORS policy from a container.
func (b *InMemoryBackend) DeleteCorsPolicy(ctx context.Context, name string) error {
	region := regionFromContext(ctx)

	b.mu.Lock("DeleteCorsPolicy")
	defer b.mu.Unlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return ErrContainerNotFound
	}

	if c.CorsPolicy == nil {
		return ErrCorsPolicyNotFound
	}

	c.CorsPolicy = nil

	return nil
}
