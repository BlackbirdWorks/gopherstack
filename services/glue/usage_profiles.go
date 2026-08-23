package glue

import (
	"fmt"
	"sort"
	"time"
)

var ErrUsageProfileNotFound = fmt.Errorf("usage profile not found: %w", ErrNotFound)

// CreateUsageProfile creates a new usage profile.
func (b *InMemoryBackend) CreateUsageProfile(name, description string, tags map[string]string) (*UsageProfile, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: UsageProfile Name is required", ErrValidation)
	}

	b.mu.Lock("CreateUsageProfile")
	defer b.mu.Unlock()

	if b.usageProfiles.Has(name) {
		return nil, fmt.Errorf("usage profile %q already exists: %w", name, ErrAlreadyExists)
	}

	now := time.Now().UTC()
	p := &UsageProfile{
		Name:           name,
		Description:    description,
		Tags:           tags,
		CreatedOn:      now,
		LastModifiedOn: now,
	}
	b.usageProfiles.Put(p)
	cp := *p

	return &cp, nil
}

// GetUsageProfile returns a usage profile by name.
func (b *InMemoryBackend) GetUsageProfile(name string) (*UsageProfile, error) {
	b.mu.RLock("GetUsageProfile")
	defer b.mu.RUnlock()

	p, ok := b.usageProfiles.Get(name)
	if !ok {
		return nil, ErrUsageProfileNotFound
	}

	cp := *p

	return &cp, nil
}

// DeleteUsageProfile removes a usage profile.
func (b *InMemoryBackend) DeleteUsageProfile(name string) error {
	b.mu.Lock("DeleteUsageProfile")
	defer b.mu.Unlock()

	if !b.usageProfiles.Has(name) {
		return ErrUsageProfileNotFound
	}

	b.usageProfiles.Delete(name)

	return nil
}

// ListUsageProfiles returns all usage profiles.
func (b *InMemoryBackend) ListUsageProfiles() []*UsageProfile {
	b.mu.RLock("ListUsageProfiles")
	defer b.mu.RUnlock()

	src := b.usageProfiles.All()
	profiles := make([]*UsageProfile, 0, len(src))
	for _, p := range src {
		cp := *p
		profiles = append(profiles, &cp)
	}

	sort.Slice(profiles, func(i, k int) bool {
		return profiles[i].Name < profiles[k].Name
	})

	return profiles
}

// UpdateUsageProfile updates a usage profile.
func (b *InMemoryBackend) UpdateUsageProfile(name, description string) (*UsageProfile, error) {
	b.mu.Lock("UpdateUsageProfile")
	defer b.mu.Unlock()

	p, ok := b.usageProfiles.Get(name)
	if !ok {
		return nil, ErrUsageProfileNotFound
	}

	if description != "" {
		p.Description = description
	}

	p.LastModifiedOn = time.Now().UTC()

	cp := *p

	return &cp, nil
}
