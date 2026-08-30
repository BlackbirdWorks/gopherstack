package route53

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	delegationSetIDChars  = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	delegationSetIDLength = 13
)

func randomDelegationSetID() string {
	return randomID(delegationSetIDChars, delegationSetIDLength)
}

// CreateReusableDelegationSet creates a new reusable delegation set.
//
// When hostedZoneID is non-empty, this is real AWS's "mark an existing
// hosted zone's delegation set as reusable" mode (see the migration steps
// documented on CreateReusableDelegationSet): the new set reuses that zone's
// own current name servers instead of the default pair. The referenced zone
// must exist (ErrHostedZoneNotFoundForDelegationSet — a distinct wire code
// from the NoSuchHostedZone every other hosted-zone lookup returns), must be
// a public zone (a reusable delegation set can't be associated with a
// private hosted zone), and must not have already had its delegation set
// extracted this way (ErrDelegationSetAlreadyReusable).
//
// Reusing a CallerReference that already identifies a reusable delegation
// set always fails with ErrDelegationSetAlreadyCreated — unlike
// CreateHostedZone/CreateHealthCheck, real AWS does not treat this as an
// idempotent retry for this operation.
func (b *InMemoryBackend) CreateReusableDelegationSet(
	callerRef, hostedZoneID string,
) (*ReusableDelegationSet, error) {
	if callerRef == "" {
		return nil, fmt.Errorf("%w: callerReference is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateReusableDelegationSet")
	defer b.mu.Unlock()

	for _, existing := range b.reusableDelegationSets.All() {
		if existing.CallerReference == callerRef {
			return nil, fmt.Errorf(
				"%w: a reusable delegation set already exists for CallerReference %s",
				ErrDelegationSetAlreadyCreated,
				callerRef,
			)
		}
	}

	nameServers, err := b.resolveSourceZoneNameServers(hostedZoneID)
	if err != nil {
		return nil, err
	}

	id := "/delegationset/N" + randomDelegationSetID()
	ds := &ReusableDelegationSet{
		ID:              id,
		CallerReference: callerRef,
		NameServers:     nameServers,
		CreatedAt:       time.Now(),
	}

	b.reusableDelegationSets.Put(ds)

	cp := *ds

	return &cp, nil
}

// resolveSourceZoneNameServers implements CreateReusableDelegationSet's
// optional HostedZoneId mode: when hostedZoneID is empty it returns the
// default name-server pair; otherwise it validates the zone (existence,
// public, not already extracted), marks it as extracted, and returns its
// real name servers. Caller must hold b.mu.
func (b *InMemoryBackend) resolveSourceZoneNameServers(hostedZoneID string) ([]string, error) {
	if hostedZoneID == "" {
		return []string{dnsNS1Default, dnsNS2Default}, nil
	}

	zd, ok := b.zones.Get(hostedZoneID)
	if !ok {
		return nil, fmt.Errorf(
			"%w: hosted zone %s not found",
			ErrHostedZoneNotFoundForDelegationSet,
			hostedZoneID,
		)
	}

	if zd.zone.PrivateZone {
		return nil, fmt.Errorf(
			"%w: a reusable delegation set can't be associated with private hosted zone %s",
			ErrInvalidInput,
			hostedZoneID,
		)
	}

	if zd.zone.DelegationSetSourceUsed {
		return nil, fmt.Errorf(
			"%w: hosted zone %s's delegation set has already been marked reusable",
			ErrDelegationSetAlreadyReusable,
			hostedZoneID,
		)
	}

	nameServers := []string{dnsNS1Default, dnsNS2Default}
	if len(zd.zone.NameServers) > 0 {
		nameServers = append([]string(nil), zd.zone.NameServers...)
	}

	zd.zone.DelegationSetSourceUsed = true

	return nameServers, nil
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

// ListReusableDelegationSets returns a page of reusable delegation sets,
// paginated by Marker/NextMarker (route53@v1.65.6
// api_op_ListReusableDelegationSets.go). Sorted by ID, which is unique, so
// the sort admits no ties despite b.reusableDelegationSets.All() being an
// unordered map walk.
func (b *InMemoryBackend) ListReusableDelegationSets(
	marker string,
	maxItems int,
) (page.Page[*ReusableDelegationSet], error) {
	b.mu.RLock("ListReusableDelegationSets")
	defer b.mu.RUnlock()

	all := b.reusableDelegationSets.All()
	result := make([]*ReusableDelegationSet, 0, len(all))
	for _, ds := range all {
		cp := *ds
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return page.New(result, marker, maxItems, route53DefaultMaxItems), nil
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
