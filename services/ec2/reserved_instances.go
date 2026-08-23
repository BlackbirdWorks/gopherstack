package ec2

import (
	"fmt"
	"slices"
	"sort"

	"github.com/google/uuid"
)

func (b *InMemoryBackend) DescribeReservedInstances(ids []string) []*ReservedInstance {
	b.mu.RLock("DescribeReservedInstances")
	defer b.mu.RUnlock()

	var result []*ReservedInstance

	for _, ri := range b.reservedInstances.All() {
		if len(ids) > 0 && !slices.Contains(ids, ri.ReservedInstancesID) {
			continue
		}

		cp := *ri
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ReservedInstancesID < result[j].ReservedInstancesID
	})

	return result
}

func (b *InMemoryBackend) DescribeReservedInstancesOfferings(
	instanceType, az, productDesc string,
) []*ReservedInstancesOffering {
	b.mu.RLock("DescribeReservedInstancesOfferings")
	defer b.mu.RUnlock()

	var result []*ReservedInstancesOffering

	for _, o := range b.reservedInstancesOfferings.All() {
		if instanceType != "" && o.InstanceType != instanceType {
			continue
		}

		if az != "" && o.AvailabilityZone != az {
			continue
		}

		if productDesc != "" && o.ProductDescription != productDesc {
			continue
		}

		cp := *o
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ReservedInstancesOfferingID < result[j].ReservedInstancesOfferingID
	})

	return result
}

func (b *InMemoryBackend) PurchaseReservedInstancesOffering(
	offeringID string,
	instanceCount int,
) (*ReservedInstance, error) {
	b.mu.Lock("PurchaseReservedInstancesOffering")
	defer b.mu.Unlock()

	offering, ok := b.reservedInstancesOfferings.Get(offeringID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrReservedInstancesNotFound, offeringID)
	}

	id := "r-" + uuid.New().String()[:8]
	ri := &ReservedInstance{
		ReservedInstancesID: id,
		InstanceType:        offering.InstanceType,
		AvailabilityZone:    offering.AvailabilityZone,
		ProductDescription:  offering.ProductDescription,
		OfferingType:        offering.OfferingType,
		Duration:            offering.Duration,
		FixedPrice:          offering.FixedPrice,
		UsagePrice:          offering.UsagePrice,
		InstanceCount:       instanceCount,
		State:               SpotFleetStateActive,
	}
	b.reservedInstances.Put(ri)

	cp := *ri

	return &cp, nil
}

func (b *InMemoryBackend) CreateReservedInstancesListing(
	reservedInstancesID string,
	_ int,
) (*ReservedInstancesListing, error) {
	b.mu.Lock("CreateReservedInstancesListing")
	defer b.mu.Unlock()

	id := "rsl-" + uuid.New().String()[:8]
	l := &ReservedInstancesListing{
		ReservedInstancesListingID: id,
		ReservedInstancesID:        reservedInstancesID,
		Status:                     SpotFleetStateActive,
	}
	b.reservedInstancesListings.Put(l)

	cp := *l

	return &cp, nil
}

func (b *InMemoryBackend) CancelReservedInstancesListing(id string) (*ReservedInstancesListing, error) {
	b.mu.Lock("CancelReservedInstancesListing")
	defer b.mu.Unlock()

	l, ok := b.reservedInstancesListings.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrReservedInstancesListingNotFound, id)
	}

	l.Status = "cancelled"

	cp := *l

	return &cp, nil
}

func (b *InMemoryBackend) DescribeReservedInstancesListings(ids []string) []*ReservedInstancesListing {
	b.mu.RLock("DescribeReservedInstancesListings")
	defer b.mu.RUnlock()

	var result []*ReservedInstancesListing

	for _, l := range b.reservedInstancesListings.All() {
		if len(ids) > 0 && !slices.Contains(ids, l.ReservedInstancesListingID) {
			continue
		}

		cp := *l
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ReservedInstancesListingID < result[j].ReservedInstancesListingID
	})

	return result
}

func (b *InMemoryBackend) DescribeReservedInstancesModifications(ids []string) []*ReservedInstancesModification {
	b.mu.RLock("DescribeReservedInstancesModifications")
	defer b.mu.RUnlock()

	var result []*ReservedInstancesModification

	for _, m := range b.reservedInstancesModifications.All() {
		if len(ids) > 0 && !slices.Contains(ids, m.ReservedInstancesModificationID) {
			continue
		}

		cp := *m
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ReservedInstancesModificationID < result[j].ReservedInstancesModificationID
	})

	return result
}

func (b *InMemoryBackend) ModifyReservedInstances(
	_ []string,
	_ string,
	_ int,
) (*ReservedInstancesModification, error) {
	b.mu.Lock("ModifyReservedInstances")
	defer b.mu.Unlock()

	id := "rimod-" + uuid.New().String()[:8]
	m := &ReservedInstancesModification{
		ReservedInstancesModificationID: id,
		Status:                          "fulfilled",
		StatusMessage:                   "Modification fulfilled",
	}
	b.reservedInstancesModifications.Put(m)

	cp := *m

	return &cp, nil
}

// reservedInstanceStateQueued is the real ReservedInstanceState enum value
// ("queued") DeleteQueuedReservedInstances requires a target to be in; see
// QueuedPurchaseDeletionResult's doc comment for why no Reserved Instance in
// this backend is ever actually in that state.
const reservedInstanceStateQueued = "queued"

// Real DeleteQueuedReservedInstancesErrorCode enum values (types.go).
const (
	deleteQueuedRIErrCodeIDInvalid = "reserved-instances-id-invalid"
	deleteQueuedRIErrCodeNotQueued = "reserved-instances-not-in-queued-state"
)

// DeleteQueuedReservedInstances reports real per-ID success/failure
// (types.SuccessfulQueuedPurchaseDeletion / types.FailedQueuedPurchaseDeletion)
// instead of silently deleting whatever Reserved Instance IDs happen to match,
// which would incorrectly let this call delete an ACTIVE (non-queued)
// reservation -- something real AWS never does through this operation.
func (b *InMemoryBackend) DeleteQueuedReservedInstances(ids []string) []QueuedPurchaseDeletionResult {
	b.mu.Lock("DeleteQueuedReservedInstances")
	defer b.mu.Unlock()

	results := make([]QueuedPurchaseDeletionResult, 0, len(ids))

	for _, id := range ids {
		ri, ok := b.reservedInstances.Get(id)

		switch {
		case !ok:
			results = append(results, QueuedPurchaseDeletionResult{
				ReservedInstancesID: id,
				Failed:              true,
				ErrorCode:           deleteQueuedRIErrCodeIDInvalid,
				ErrorMessage:        fmt.Sprintf("The reserved instance ID '%s' does not exist", id),
			})
		case ri.State != reservedInstanceStateQueued:
			results = append(results, QueuedPurchaseDeletionResult{
				ReservedInstancesID: id,
				Failed:              true,
				ErrorCode:           deleteQueuedRIErrCodeNotQueued,
				ErrorMessage:        fmt.Sprintf("The reserved instance '%s' is not in the queued state", id),
			})
		default:
			b.reservedInstances.Delete(id)
			delete(b.tags, id)
			results = append(results, QueuedPurchaseDeletionResult{ReservedInstancesID: id})
		}
	}

	return results
}
