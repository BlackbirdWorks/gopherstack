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

func (b *InMemoryBackend) CancelReservedInstancesListing(id string) error {
	b.mu.Lock("CancelReservedInstancesListing")
	defer b.mu.Unlock()

	l, ok := b.reservedInstancesListings.Get(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrReservedInstancesListingNotFound, id)
	}

	l.Status = "cancelled"

	return nil
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

func (b *InMemoryBackend) DeleteQueuedReservedInstances(ids []string) {
	b.mu.Lock("DeleteQueuedReservedInstances")
	defer b.mu.Unlock()

	for _, id := range ids {
		b.reservedInstances.Delete(id)
		delete(b.tags, id)
	}
}
