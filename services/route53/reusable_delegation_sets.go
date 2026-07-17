package route53

import (
	"fmt"
	"sort"
	"time"
)

const (
	delegationSetIDChars  = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	delegationSetIDLength = 13
)

func randomDelegationSetID() string {
	return randomID(delegationSetIDChars, delegationSetIDLength)
}

// CreateReusableDelegationSet creates a new reusable delegation set.
func (b *InMemoryBackend) CreateReusableDelegationSet(
	callerRef, _ /* hostedZoneID */ string,
) (*ReusableDelegationSet, error) {
	if callerRef == "" {
		return nil, fmt.Errorf("%w: callerReference is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateReusableDelegationSet")
	defer b.mu.Unlock()

	id := "/delegationset/N" + randomDelegationSetID()
	ds := &ReusableDelegationSet{
		ID:              id,
		CallerReference: callerRef,
		NameServers:     []string{dnsNS1Default, dnsNS2Default},
		CreatedAt:       time.Now(),
	}

	b.reusableDelegationSets.Put(ds)

	cp := *ds

	return &cp, nil
}

// GetReusableDelegationSet returns a reusable delegation set by ID.
func (b *InMemoryBackend) GetReusableDelegationSet(id string) (*ReusableDelegationSet, error) {
	b.mu.RLock("GetReusableDelegationSet")
	defer b.mu.RUnlock()

	ds, ok := b.reusableDelegationSets.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: delegation set %s not found", ErrDelegationSetNotFound, id)
	}

	cp := *ds

	return &cp, nil
}

// DeleteReusableDelegationSet deletes a reusable delegation set. It returns
// ErrDelegationSetInUse if any hosted zone is still linked to it — real AWS
// requires all zones created with the set to be deleted first.
func (b *InMemoryBackend) DeleteReusableDelegationSet(id string) error {
	b.mu.Lock("DeleteReusableDelegationSet")
	defer b.mu.Unlock()

	if !b.reusableDelegationSets.Has(id) {
		return fmt.Errorf("%w: delegation set %s not found", ErrDelegationSetNotFound, id)
	}

	for _, zd := range b.zones.All() {
		if zd.zone.DelegationSetID == id {
			return fmt.Errorf(
				"%w: reusable delegation set %s is still in use by hosted zone %s",
				ErrDelegationSetInUse,
				id, zd.zone.ID,
			)
		}
	}

	b.reusableDelegationSets.Delete(id)

	return nil
}

// ListReusableDelegationSets returns all reusable delegation sets.
func (b *InMemoryBackend) ListReusableDelegationSets() ([]*ReusableDelegationSet, error) {
	b.mu.RLock("ListReusableDelegationSets")
	defer b.mu.RUnlock()

	all := b.reusableDelegationSets.All()
	result := make([]*ReusableDelegationSet, 0, len(all))
	for _, ds := range all {
		cp := *ds
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return result, nil
}

// CountZonesByReusableDelegationSet returns the number of hosted zones that use
// the given reusable delegation set. It returns ErrDelegationSetNotFound if the
// delegation set does not exist.
func (b *InMemoryBackend) CountZonesByReusableDelegationSet(id string) (int, error) {
	b.mu.RLock("CountZonesByReusableDelegationSet")
	defer b.mu.RUnlock()

	if !b.reusableDelegationSets.Has(id) {
		return 0, fmt.Errorf("%w: delegation set %s not found", ErrDelegationSetNotFound, id)
	}

	count := 0

	for _, zd := range b.zones.All() {
		if zd.zone.DelegationSetID == id {
			count++
		}
	}

	return count, nil
}
