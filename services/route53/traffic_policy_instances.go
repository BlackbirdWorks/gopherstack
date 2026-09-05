package route53

import (
	"fmt"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	tpiIDChars  = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz"
	tpiIDLength = 36
)

func randomTPIID() string { return randomID(tpiIDChars, tpiIDLength) }

// CreateTrafficPolicyInstance creates a new traffic policy instance.
func (b *InMemoryBackend) CreateTrafficPolicyInstance(
	hostedZoneID, name, tpID string,
	tpVersion int32,
	ttl int64,
) (*TrafficPolicyInstance, error) {
	if hostedZoneID == "" {
		return nil, fmt.Errorf("%w: hostedZoneId is required", ErrInvalidInput)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}

	if tpID == "" {
		return nil, fmt.Errorf("%w: trafficPolicyId is required", ErrInvalidInput)
	}

	if ttl < 1 {
		return nil, fmt.Errorf("%w: TTL must be >= 1 for traffic policy instances", ErrInvalidInput)
	}

	b.mu.Lock("CreateTrafficPolicyInstance")
	defer b.mu.Unlock()

	if _, ok := b.zones.Get(hostedZoneID); !ok {
		return nil, fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, hostedZoneID)
	}

	versions, ok := b.trafficPolicies[tpID]
	if !ok || len(versions) == 0 {
		return nil, fmt.Errorf("%w: traffic policy %s not found", ErrTrafficPolicyNotFound, tpID)
	}

	tpType := "DNS"
	for _, v := range versions {
		if v.Version == tpVersion {
			tpType = v.Type

			break
		}
	}

	normalisedName := normaliseName(name)

	// AWS allows only one traffic policy instance per (hosted zone, name):
	// a second CreateTrafficPolicyInstance for the same pair returns
	// TrafficPolicyInstanceAlreadyExists (409) rather than creating a
	// duplicate or silently overwriting the existing instance.
	for _, existing := range b.trafficPolicyInstancesByZone.Get(hostedZoneID) {
		if strings.EqualFold(existing.Name, normalisedName) {
			return nil, fmt.Errorf(
				"%w: a traffic policy instance already exists for name %s in zone %s",
				ErrTrafficPolicyInstanceAlreadyExists,
				name,
				hostedZoneID,
			)
		}
	}

	id := randomTPIID()
	inst := &TrafficPolicyInstance{
		ID:                   id,
		HostedZoneID:         hostedZoneID,
		Name:                 normalisedName,
		TrafficPolicyID:      tpID,
		TrafficPolicyVersion: tpVersion,
		TrafficPolicyType:    tpType,
		TTL:                  ttl,
		State:                tpiStateApplied,
	}

	b.trafficPolicyInstances.Put(inst)

	cp := *inst

	return &cp, nil
}

// DeleteTrafficPolicyInstance deletes a traffic policy instance.
func (b *InMemoryBackend) DeleteTrafficPolicyInstance(id string) error {
	b.mu.Lock("DeleteTrafficPolicyInstance")
	defer b.mu.Unlock()

	if !b.trafficPolicyInstances.Has(id) {
		return fmt.Errorf(
			"%w: traffic policy instance %s not found",
			ErrTrafficPolicyInstNotFound,
			id,
		)
	}

	b.trafficPolicyInstances.Delete(id)

	return nil
}

// GetTrafficPolicyInstance returns a traffic policy instance by ID.
func (b *InMemoryBackend) GetTrafficPolicyInstance(id string) (*TrafficPolicyInstance, error) {
	b.mu.RLock("GetTrafficPolicyInstance")
	defer b.mu.RUnlock()

	inst, ok := b.trafficPolicyInstances.Get(id)
	if !ok {
		return nil, fmt.Errorf(
			"%w: traffic policy instance %s not found",
			ErrTrafficPolicyInstNotFound,
			id,
		)
	}

	cp := *inst

	return &cp, nil
}

// ListTrafficPolicyInstances returns a page of traffic policy instances,
// paginated by marker (route53@v1.65.6 api_op_ListTrafficPolicyInstances.go
// echoes the cursor across HostedZoneIdMarker/TrafficPolicyInstanceNameMarker/
// TrafficPolicyInstanceTypeMarker; this backend carries it as a single
// opaque token, same simplification ListHostedZonesByVPC already makes).
// Sorted by ID, which is unique, so the sort admits no ties despite
// b.trafficPolicyInstances.All() being an unordered map walk.
func (b *InMemoryBackend) ListTrafficPolicyInstances(
	marker string,
	maxItems int,
) (page.Page[*TrafficPolicyInstance], error) {
	b.mu.RLock("ListTrafficPolicyInstances")
	defer b.mu.RUnlock()

	all := b.trafficPolicyInstances.All()
	result := make([]*TrafficPolicyInstance, 0, len(all))
	for _, inst := range all {
		cp := *inst
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return page.New(result, marker, maxItems, route53DefaultMaxItems), nil
}

// UpdateTrafficPolicyInstance updates the TTL of a traffic policy instance.
func (b *InMemoryBackend) UpdateTrafficPolicyInstance(
	id, tpID string,
	tpVersion int32,
	ttl int64,
) (*TrafficPolicyInstance, error) {
	b.mu.Lock("UpdateTrafficPolicyInstance")
	defer b.mu.Unlock()

	inst, ok := b.trafficPolicyInstances.Get(id)
	if !ok {
		return nil, fmt.Errorf(
			"%w: traffic policy instance %s not found",
			ErrTrafficPolicyInstNotFound,
			id,
		)
	}

	// inst.HostedZoneID (the only field trafficPolicyInstancesByZone indexes
	// on) is never modified here, so the index stays consistent without a
	// Put — see trafficPolicyInstanceZoneKeyFn's doc comment.
	if tpID != "" {
		inst.TrafficPolicyID = tpID
		inst.TrafficPolicyVersion = tpVersion
	}

	if ttl > 0 {
		inst.TTL = ttl
	}

	cp := *inst

	return &cp, nil
}

// ListTrafficPolicyInstancesByHostedZone returns a page of instances
// filtered by hosted zone ID, paginated by marker (route53@v1.65.6
// api_op_ListTrafficPolicyInstancesByHostedZone.go; see
// ListTrafficPolicyInstances's doc comment for the single-opaque-token
// simplification). Sorted by ID, which is unique, so the sort admits no
// ties despite trafficPolicyInstancesByZone.Get being an unordered map walk.
func (b *InMemoryBackend) ListTrafficPolicyInstancesByHostedZone(
	hostedZoneID, marker string,
	maxItems int,
) (page.Page[*TrafficPolicyInstance], error) {
	b.mu.RLock("ListTrafficPolicyInstancesByHostedZone")
	defer b.mu.RUnlock()

	zoneInstances := b.trafficPolicyInstancesByZone.Get(hostedZoneID)
	result := make([]*TrafficPolicyInstance, 0, len(zoneInstances))

	for _, inst := range zoneInstances {
		cp := *inst
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return page.New(result, marker, maxItems, route53DefaultMaxItems), nil
}

// ListTrafficPolicyInstancesByPolicy returns a page of instances filtered by
// traffic policy ID and version, paginated by marker (route53@v1.65.6
// api_op_ListTrafficPolicyInstancesByPolicy.go; see
// ListTrafficPolicyInstances's doc comment for the single-opaque-token
// simplification).
func (b *InMemoryBackend) ListTrafficPolicyInstancesByPolicy(
	tpID string,
	tpVersion int32,
	marker string,
	maxItems int,
) (page.Page[*TrafficPolicyInstance], error) {
	b.mu.RLock("ListTrafficPolicyInstancesByPolicy")
	defer b.mu.RUnlock()

	var result []*TrafficPolicyInstance

	// No secondary index on TrafficPolicyID: UpdateTrafficPolicyInstance can
	// change it in place above, which would make such an index stale (see
	// store.Index.AddIndex's doc comment on mutable index keys). A linear
	// scan matches the original map-iteration behaviour exactly.
	for _, inst := range b.trafficPolicyInstances.All() {
		if inst.TrafficPolicyID == tpID &&
			(tpVersion == 0 || inst.TrafficPolicyVersion == tpVersion) {
			cp := *inst
			result = append(result, &cp)
		}
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return page.New(result, marker, maxItems, route53DefaultMaxItems), nil
}
