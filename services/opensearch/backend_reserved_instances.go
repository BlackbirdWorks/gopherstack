package opensearch

import (
	"fmt"
	"time"
)

// staticReservedInstanceOfferings returns a fixed list of available offerings.
func staticReservedInstanceOfferings() []*ReservedInstanceOffering {
	return []*ReservedInstanceOffering{
		{
			ReservedInstanceOfferingID: "ri-offering-1",
			InstanceType:               instanceTypeT3Small,
			Duration:                   reservedDuration1Year,
			FixedPrice:                 reservedPrice1YearAllUpfront,
			UsagePrice:                 0.0,
			CurrencyCode:               currencyUSD,
			PaymentOption:              "ALL_UPFRONT",
		},
		{
			ReservedInstanceOfferingID: "ri-offering-2",
			InstanceType:               instanceTypeR6gLarge,
			Duration:                   reservedDuration1Year,
			FixedPrice:                 reservedPrice1YearPartialFixed,
			UsagePrice:                 reservedPrice1YearPartialHourly,
			CurrencyCode:               currencyUSD,
			PaymentOption:              "PARTIAL_UPFRONT",
		},
		{
			ReservedInstanceOfferingID: "ri-offering-3",
			InstanceType:               instanceTypeM6gLarge,
			Duration:                   reservedDuration3Year,
			FixedPrice:                 0.0,
			UsagePrice:                 reservedPrice3YearNoUpfrontHrly,
			CurrencyCode:               currencyUSD,
			PaymentOption:              "NO_UPFRONT",
		},
	}
}

// DescribeReservedInstanceOfferings returns available reserved instance offerings.
func (b *InMemoryBackend) DescribeReservedInstanceOfferings() []*ReservedInstanceOffering {
	return staticReservedInstanceOfferings()
}

// DescribeReservedInstances returns all purchased reserved instances.
func (b *InMemoryBackend) DescribeReservedInstances() []*ReservedInstance {
	b.mu.RLock("DescribeReservedInstances")
	defer b.mu.RUnlock()

	out := make([]*ReservedInstance, 0, b.reservedInstances.Len())
	for _, ri := range b.reservedInstances.All() {
		cp := *ri
		out = append(out, &cp)
	}

	return out
}

// PurchaseReservedInstanceOffering purchases a reserved instance offering.
func (b *InMemoryBackend) PurchaseReservedInstanceOffering(
	offeringID, name string,
	count int,
) (*ReservedInstance, error) {
	var offering *ReservedInstanceOffering

	for _, o := range staticReservedInstanceOfferings() {
		if o.ReservedInstanceOfferingID == offeringID {
			offering = o

			break
		}
	}

	if offering == nil {
		return nil, fmt.Errorf(
			"%w: reserved instance offering %s not found",
			ErrConnectionNotFound,
			offeringID,
		)
	}

	b.mu.Lock("PurchaseReservedInstanceOffering")
	defer b.mu.Unlock()

	b.reservedCounter++
	id := fmt.Sprintf("ri-%d", b.reservedCounter)

	ri := &ReservedInstance{
		ReservedInstanceID:         id,
		ReservedInstanceOfferingID: offeringID,
		InstanceType:               offering.InstanceType,
		ReservationName:            name,
		Duration:                   offering.Duration,
		FixedPrice:                 offering.FixedPrice,
		UsagePrice:                 offering.UsagePrice,
		InstanceCount:              count,
		CurrencyCode:               offering.CurrencyCode,
		PaymentOption:              offering.PaymentOption,
		State:                      pkgStateActive,
		StartTime:                  float64(time.Now().Unix()),
	}
	b.reservedInstances.Put(ri)

	cp := *ri

	return &cp, nil
}
