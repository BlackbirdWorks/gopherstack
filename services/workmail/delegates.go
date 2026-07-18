package workmail

import (
	"fmt"
	"sort"
)

// AssociateDelegateToResource adds a delegate to a resource.
func (b *InMemoryBackend) AssociateDelegateToResource(orgID, resourceID, entityID string) error {
	b.mu.Lock("AssociateDelegateToResource")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	r := b.findResource(orgID, resourceID)
	if r == nil {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceID)
	}

	if b.delegates[orgID][r.ResourceID] == nil {
		b.delegates[orgID][r.ResourceID] = make(map[string]bool)
	}
	b.delegates[orgID][r.ResourceID][entityID] = true

	return nil
}

// DisassociateDelegateFromResource removes a delegate from a resource.
func (b *InMemoryBackend) DisassociateDelegateFromResource(
	orgID, resourceID, entityID string,
) error {
	b.mu.Lock("DisassociateDelegateFromResource")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	r := b.findResource(orgID, resourceID)
	if r == nil {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceID)
	}

	delegates := b.delegates[orgID][r.ResourceID]
	if delegates == nil || !delegates[entityID] {
		return fmt.Errorf("%w: delegate %q not found on resource", ErrNotFound, entityID)
	}
	delete(delegates, entityID)

	return nil
}

// ListResourceDelegates returns delegates of a resource.
func (b *InMemoryBackend) ListResourceDelegates(
	orgID, resourceID string,
	maxResults int32,
	nextToken string,
) ([]*Delegate, string, error) {
	b.mu.RLock("ListResourceDelegates")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	r := b.findResource(orgID, resourceID)
	if r == nil {
		return nil, "", fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceID)
	}

	delegates := make([]*Delegate, 0)
	for entityID := range b.delegates[orgID][r.ResourceID] {
		dt := memberTypeUser
		if b.findGroup(orgID, entityID) != nil {
			dt = memberTypeGroup
		}
		delegates = append(delegates, &Delegate{DelegateID: entityID, DelegateType: dt})
	}
	sort.Slice(
		delegates,
		func(i, j int) bool { return delegates[i].DelegateID < delegates[j].DelegateID },
	)

	items, next := paginate(delegates, maxResults, nextToken)

	return items, next, nil
}
