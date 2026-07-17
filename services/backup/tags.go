package backup

import "fmt"

// TagResource adds tags to a resource by ARN.
// Supported resource types: backup vaults, backup plans, frameworks, report plans.
func (b *InMemoryBackend) TagResource(resourceArn string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if name, ok := b.vaultARNIndex[resourceArn]; ok {
		v, _ := b.vaults.Get(name)
		v.Tags.Merge(kv)

		return nil
	}

	if name, ok := b.planARNIndex[resourceArn]; ok {
		p, _ := b.plans.Get(name)
		p.Tags.Merge(kv)

		return nil
	}

	if name, ok := b.frameworkARNIndex[resourceArn]; ok {
		f, _ := b.frameworks.Get(name)
		f.Tags.Merge(kv)

		return nil
	}

	if name, ok := b.reportPlanARNIndex[resourceArn]; ok {
		rp, _ := b.reportPlans.Get(name)
		rp.Tags.Merge(kv)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
}

// ListTags returns tags for a resource by ARN.
// Supported resource types: backup vaults, backup plans, frameworks, report plans.
func (b *InMemoryBackend) ListTags(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	if name, ok := b.vaultARNIndex[resourceArn]; ok {
		v, _ := b.vaults.Get(name)

		return v.Tags.Clone(), nil
	}

	if name, ok := b.planARNIndex[resourceArn]; ok {
		p, _ := b.plans.Get(name)

		return p.Tags.Clone(), nil
	}

	if name, ok := b.frameworkARNIndex[resourceArn]; ok {
		f, _ := b.frameworks.Get(name)

		return f.Tags.Clone(), nil
	}

	if name, ok := b.reportPlanARNIndex[resourceArn]; ok {
		rp, _ := b.reportPlans.Get(name)

		return rp.Tags.Clone(), nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
}

// UntagResource removes the given tag keys from a resource identified by ARN.
// Supported resource types: backup vaults, backup plans, frameworks, report plans.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if name, ok := b.vaultARNIndex[resourceArn]; ok {
		v, _ := b.vaults.Get(name)
		v.Tags.DeleteKeys(tagKeys)

		return nil
	}

	if name, ok := b.planARNIndex[resourceArn]; ok {
		p, _ := b.plans.Get(name)
		p.Tags.DeleteKeys(tagKeys)

		return nil
	}

	if name, ok := b.frameworkARNIndex[resourceArn]; ok {
		f, _ := b.frameworks.Get(name)
		f.Tags.DeleteKeys(tagKeys)

		return nil
	}

	if name, ok := b.reportPlanARNIndex[resourceArn]; ok {
		rp, _ := b.reportPlans.Get(name)
		rp.Tags.DeleteKeys(tagKeys)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
}
