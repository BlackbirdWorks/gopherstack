package rds

import (
	"fmt"
	"time"
)

// PurchaseReservedDBInstancesOffering purchases a reserved DB instance offering.
func (b *InMemoryBackend) PurchaseReservedDBInstancesOffering(
	offeringID, reservedDBInstanceID string,
	dbInstanceCount int,
) (*ReservedDBInstance, error) {
	if dbInstanceCount < 1 {
		return nil, fmt.Errorf("%w: dbInstanceCount must be at least 1", ErrInvalidParameter)
	}
	var offering *ReservedDBInstancesOffering
	for _, o := range staticReservedOfferings() {
		if o.ReservedDBInstancesOfferingID == offeringID {
			cp := o
			offering = &cp

			break
		}
	}
	if offering == nil {
		offering = &ReservedDBInstancesOffering{
			ReservedDBInstancesOfferingID: offeringID,
			DBInstanceClass:               defaultInstanceClass,
			Duration:                      reservedDurationOneYear,
			FixedPrice:                    0,
			UsagePrice:                    0,
			ProductDescription:            engineMySQL,
			OfferingType:                  "No Upfront",
			MultiAZ:                       false,
			CurrencyCode:                  currencyUSD,
		}
	}
	if reservedDBInstanceID == "" {
		reservedDBInstanceID = fmt.Sprintf("ri-%s-%d", offeringID, time.Now().UnixNano())
	}
	b.mu.Lock("PurchaseReservedDBInstancesOffering")
	defer b.mu.Unlock()
	if _, exists := b.reservedInstances.Get(reservedDBInstanceID); exists {
		return nil, fmt.Errorf("%w: reserved instance %s already exists", ErrInvalidParameter, reservedDBInstanceID)
	}
	ri := &ReservedDBInstance{
		ReservedDBInstanceID:          reservedDBInstanceID,
		ReservedDBInstancesOfferingID: offeringID,
		DBInstanceClass:               offering.DBInstanceClass,
		StartTime:                     time.Now().UTC().Format(time.RFC3339),
		Duration:                      offering.Duration,
		FixedPrice:                    offering.FixedPrice,
		UsagePrice:                    offering.UsagePrice,
		DBInstanceCount:               dbInstanceCount,
		ProductDescription:            offering.ProductDescription,
		OfferingType:                  offering.OfferingType,
		MultiAZ:                       offering.MultiAZ,
		State:                         subscriptionStatusActive,
		CurrencyCode:                  offering.CurrencyCode,
	}
	b.reservedInstances.Put(ri)
	cp := *ri

	return &cp, nil
}

// DescribeReservedDBInstances returns reserved DB instances.
func (b *InMemoryBackend) DescribeReservedDBInstances(
	reservedDBInstanceID, dbInstanceClass string,
) []ReservedDBInstance {
	b.mu.RLock("DescribeReservedDBInstances")
	defer b.mu.RUnlock()
	result := make([]ReservedDBInstance, 0, b.reservedInstances.Len())
	for _, ri := range b.reservedInstances.All() {
		if reservedDBInstanceID != "" && ri.ReservedDBInstanceID != reservedDBInstanceID {
			continue
		}
		if dbInstanceClass != "" && ri.DBInstanceClass != dbInstanceClass {
			continue
		}
		result = append(result, *ri)
	}

	return result
}

// DescribeReservedDBInstancesOfferings returns available reserved DB instance offerings.
func (b *InMemoryBackend) DescribeReservedDBInstancesOfferings(
	offeringID, dbInstanceClass string,
) []ReservedDBInstancesOffering {
	offerings := staticReservedOfferings()
	if offeringID == "" && dbInstanceClass == "" {
		return offerings
	}
	result := make([]ReservedDBInstancesOffering, 0, len(offerings))
	for _, o := range offerings {
		if offeringID != "" && o.ReservedDBInstancesOfferingID != offeringID {
			continue
		}
		if dbInstanceClass != "" && o.DBInstanceClass != dbInstanceClass {
			continue
		}
		result = append(result, o)
	}

	return result
}

func staticReservedOfferings() []ReservedDBInstancesOffering {
	return []ReservedDBInstancesOffering{
		{
			ReservedDBInstancesOfferingID: "01f5e8a3-2f47-4f47-8a7f-1234567890ab",
			DBInstanceClass:               defaultInstanceClass,
			Duration:                      reservedDurationOneYear,
			FixedPrice:                    reservedFixedPriceMicro,
			UsagePrice:                    0.0,
			ProductDescription:            engineMySQL,
			OfferingType:                  reservedAllUpfront,
			MultiAZ:                       false,
			CurrencyCode:                  currencyUSD,
		},
		{
			ReservedDBInstancesOfferingID: "12a1e534-e8a3-4f47-8a7f-2345678901bc",
			DBInstanceClass:               "db.t3.small",
			Duration:                      reservedDurationOneYear,
			FixedPrice:                    reservedFixedPriceSmall,
			UsagePrice:                    0.0,
			ProductDescription:            engineMySQL,
			OfferingType:                  reservedAllUpfront,
			MultiAZ:                       false,
			CurrencyCode:                  currencyUSD,
		},
		{
			ReservedDBInstancesOfferingID: "23b2f645-f9b4-4f47-8a7f-3456789012cd",
			DBInstanceClass:               "db.m5.large",
			Duration:                      reservedDurationOneYear,
			FixedPrice:                    reservedFixedPriceMedium,
			UsagePrice:                    0.0,
			ProductDescription:            enginePostgres,
			OfferingType:                  reservedAllUpfront,
			MultiAZ:                       false,
			CurrencyCode:                  currencyUSD,
		},
		{
			ReservedDBInstancesOfferingID: "34c3g756-g0c5-4f47-8a7f-4567890123de",
			DBInstanceClass:               "db.m5.large",
			Duration:                      reservedDurationOneYear,
			FixedPrice:                    reservedFixedPriceMediumMulti,
			UsagePrice:                    0.0,
			ProductDescription:            enginePostgres,
			OfferingType:                  reservedAllUpfront,
			MultiAZ:                       true,
			CurrencyCode:                  currencyUSD,
		},
		{
			ReservedDBInstancesOfferingID: "45d4h867-h1d6-4f47-8a7f-5678901234ef",
			DBInstanceClass:               "db.r5.large",
			Duration:                      reservedDurationOneYear,
			FixedPrice:                    reservedFixedPriceLarge,
			UsagePrice:                    0.0,
			ProductDescription:            engineAuroraMySQL,
			OfferingType:                  reservedAllUpfront,
			MultiAZ:                       false,
			CurrencyCode:                  currencyUSD,
		},
	}
}

const (
	reservedDurationOneYear       = 31536000
	reservedFixedPriceMicro       = 51.0
	reservedFixedPriceSmall       = 102.0
	reservedFixedPriceMedium      = 540.0
	reservedFixedPriceMediumMulti = 1080.0
	reservedFixedPriceLarge       = 810.0
)
