package elasticsearch

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

// DescribeReservedElasticsearchInstanceOfferings returns available reserved instance offerings.
func (b *InMemoryBackend) DescribeReservedElasticsearchInstanceOfferings() []ReservedInstanceOffering {
	return []ReservedInstanceOffering{{
		OfferingID:    "offer-t3-small-1y",
		InstanceType:  defaultInstanceType,
		PaymentOption: "NO_UPFRONT",
		Currency:      "USD",
		Duration:      reservedDurationOneYearSeconds,
	}}
}

// DescribeReservedElasticsearchInstances returns purchased reserved instances for the request's region.
func (b *InMemoryBackend) DescribeReservedElasticsearchInstances(ctx context.Context) []ReservedInstance {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeReservedElasticsearchInstances")
	defer b.mu.RUnlock()

	reserved := b.reservedInstancesInRegion(region)
	instances := make([]ReservedInstance, 0, len(reserved))
	for _, instance := range reserved {
		instances = append(instances, *instance)
	}

	slices.SortFunc(instances, func(a, c ReservedInstance) int {
		return strings.Compare(a.ReservationID, c.ReservationID)
	})

	return instances
}

// PurchaseReservedElasticsearchInstanceOffering purchases a reserved instance offering.
func (b *InMemoryBackend) PurchaseReservedElasticsearchInstanceOffering(
	ctx context.Context, offeringID, name string, count int,
) (*ReservedInstance, error) {
	if offeringID == "" {
		return nil, fmt.Errorf("%w: ReservedElasticsearchInstanceOfferingId is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	b.mu.Lock("PurchaseReservedElasticsearchInstanceOffering")
	defer b.mu.Unlock()

	if count == 0 {
		count = 1
	}

	id := fmt.Sprintf("ri-%010d", b.nextIDLocked())
	instance := &ReservedInstance{
		ReservationID:   id,
		ReservationName: name,
		OfferingID:      offeringID,
		Count:           count,
		State:           statusActive,
		region:          region,
	}
	for _, offering := range b.DescribeReservedElasticsearchInstanceOfferings() {
		if offering.OfferingID == offeringID {
			instance.InstanceType = offering.InstanceType
			instance.FixedPrice = offering.FixedPrice
			instance.UsagePrice = offering.UsagePrice
			instance.Duration = offering.Duration

			break
		}
	}

	b.reservedInstancePut(instance)
	cp := *instance

	return &cp, nil
}
