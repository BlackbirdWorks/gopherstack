package shield

import (
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// protectionARN builds a Shield protection ARN.
// Shield ARNs are global (no region component).
func protectionARN(accountID, protectionID string) string {
	return arn.Build("shield", "", accountID, fmt.Sprintf("protection/%s", protectionID))
}

// CreateProtection creates a new Shield protection for the given resource ARN.
func (b *InMemoryBackend) CreateProtection(name, resourceARN string, tags map[string]string) (*Protection, error) {
	id := newShieldID()

	b.mu.Lock("CreateProtection")
	defer b.mu.Unlock()

	if b.subscription == nil {
		return nil, fmt.Errorf(
			"%w: Shield Advanced subscription is required to create protections",
			ErrSubscriptionRequired,
		)
	}

	if matches := b.protectionsByName.Get(name); len(matches) > 0 {
		return nil, fmt.Errorf("%w: protection %q already exists", ErrProtectionAlreadyExists, name)
	}

	if matches := b.protectionsByResourceARN.Get(resourceARN); len(matches) > 0 {
		return nil, fmt.Errorf("%w: protection for resource %s already exists", ErrProtectionAlreadyExists, resourceARN)
	}

	pArn := protectionARN(b.accountID, id)

	p := &Protection{
		ID:            id,
		ProtectionArn: pArn,
		Name:          name,
		ResourceARN:   resourceARN,
		CreationTime:  time.Now(),
		Tags:          cloneTags(tags),
	}
	b.protections.Put(p)

	return cloneProtection(p), nil
}

// DescribeProtection returns a protection by ID or resource ARN.
func (b *InMemoryBackend) DescribeProtection(protectionID, resourceARN string) (*Protection, error) {
	b.mu.RLock("DescribeProtection")
	defer b.mu.RUnlock()

	if protectionID != "" {
		p, ok := b.protections.Get(protectionID)
		if !ok {
			return nil, fmt.Errorf("%w: protection %q not found", ErrProtectionNotFound, protectionID)
		}

		return cloneProtection(p), nil
	}

	if matches := b.protectionsByResourceARN.Get(resourceARN); len(matches) > 0 {
		return cloneProtection(matches[0]), nil
	}

	return nil, fmt.Errorf("%w: no protection for resource %q", ErrProtectionNotFound, resourceARN)
}

// DeleteProtection deletes a protection by ID.
func (b *InMemoryBackend) DeleteProtection(protectionID string) error {
	b.mu.Lock("DeleteProtection")
	defer b.mu.Unlock()

	if !b.protections.Delete(protectionID) {
		return fmt.Errorf("%w: protection %q not found", ErrProtectionNotFound, protectionID)
	}

	return nil
}

// ListProtections returns all protections sorted by name.
// Clones are built under RLock; sorting happens after the lock is released.
func (b *InMemoryBackend) ListProtections() []*Protection {
	b.mu.RLock("ListProtections")

	items := b.protections.All()
	list := make([]*Protection, 0, len(items))

	for _, p := range items {
		list = append(list, cloneProtection(p))
	}

	b.mu.RUnlock()

	slices.SortFunc(list, func(a, b *Protection) int {
		if a.Name < b.Name {
			return -1
		}

		if a.Name > b.Name {
			return 1
		}

		return 0
	})

	return list
}

// AddProtectionInternal creates a protection directly (for tests).
func (b *InMemoryBackend) AddProtectionInternal(name, resourceARN string) *Protection {
	id := newShieldID()

	b.mu.Lock("AddProtectionInternal")
	defer b.mu.Unlock()

	pArn := protectionARN(b.accountID, id)

	p := &Protection{
		ID:            id,
		ProtectionArn: pArn,
		Name:          name,
		ResourceARN:   resourceARN,
		CreationTime:  time.Now(),
		Tags:          make(map[string]string),
	}
	b.protections.Put(p)

	return cloneProtection(p)
}
