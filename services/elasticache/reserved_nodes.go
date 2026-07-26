package elasticache

import (
	"context"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	engineRedisCap = "Redis"
	allUpfrontPlan = "All Upfront"
)

func (b *InMemoryBackend) reservedCacheNodeARN(region, id string) string {
	return arn.Build("elasticache", region, b.accountID, "reserved-instance:"+id)
}

// reservedOneYearSeconds is the duration in seconds for a 1-year reserved cache node.
const reservedOneYearSeconds int32 = 365 * 24 * 60 * 60

// Builtin offering prices (USD, fixed price, all upfront, 1 year).
const (
	reservedPriceLarge  float64 = 500
	reservedPriceXLarge float64 = 1000
	reservedPriceMicro  float64 = 50
)

func builtinReservedOfferings() []ReservedCacheNodesOffering {
	return []ReservedCacheNodesOffering{
		{
			OfferingID:         "31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa",
			CacheNodeType:      "cache.r6g.large",
			Duration:           reservedOneYearSeconds,
			FixedPrice:         reservedPriceLarge,
			UsagePrice:         0.0,
			ProductDescription: engineRedisCap,
			OfferingType:       allUpfrontPlan,
		},
		{
			OfferingID:         "649fd0c8-cf6d-47a0-bfa6-060f8e75e95f",
			CacheNodeType:      "cache.r6g.xlarge",
			Duration:           reservedOneYearSeconds,
			FixedPrice:         reservedPriceXLarge,
			UsagePrice:         0.0,
			ProductDescription: engineRedisCap,
			OfferingType:       allUpfrontPlan,
		},
		{
			OfferingID:         "a2b54b70-d5b3-4b96-a72e-afedbc16e70f",
			CacheNodeType:      nodeTypeT3Micro,
			Duration:           reservedOneYearSeconds,
			FixedPrice:         reservedPriceMicro,
			UsagePrice:         0.0,
			ProductDescription: engineRedisCap,
			OfferingType:       allUpfrontPlan,
		},
	}
}

// DescribeReservedCacheNodes returns a paginated list of reserved cache nodes.
func (b *InMemoryBackend) DescribeReservedCacheNodes(
	ctx context.Context,
	id, cacheNodeType, offeringType, marker string,
	maxRecords int,
) (page.Page[ReservedCacheNode], error) {
	b.mu.RLock("DescribeReservedCacheNodes")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return describePaged(
		b.reservedCacheNodesStoreRO(region),
		id,
		ErrReservedCacheNodeNotFound,
		func(rcn ReservedCacheNode) bool {
			return (cacheNodeType == "" || rcn.CacheNodeType == cacheNodeType) &&
				(offeringType == "" || rcn.OfferingType == offeringType)
		},
		func(rcn ReservedCacheNode) string { return rcn.ReservedCacheNodeID },
		marker,
		maxRecords,
	)
}

// DescribeReservedCacheNodesOfferings returns a paginated list of reserved cache node offerings.
func (b *InMemoryBackend) DescribeReservedCacheNodesOfferings(
	_ context.Context,
	offeringID, cacheNodeType, offeringType, marker string,
	maxRecords int,
) (page.Page[ReservedCacheNodesOffering], error) {
	b.mu.RLock("DescribeReservedCacheNodesOfferings")
	defer b.mu.RUnlock()

	all := builtinReservedOfferings()

	if offeringID != "" {
		for _, o := range all {
			if o.OfferingID == offeringID {
				return page.Page[ReservedCacheNodesOffering]{Data: []ReservedCacheNodesOffering{o}}, nil
			}
		}

		return page.Page[ReservedCacheNodesOffering]{}, ErrReservedCacheNodesOfferingNotFound
	}

	filtered := make([]ReservedCacheNodesOffering, 0, len(all))
	for _, o := range all {
		if cacheNodeType != "" && o.CacheNodeType != cacheNodeType {
			continue
		}
		if offeringType != "" && o.OfferingType != offeringType {
			continue
		}
		filtered = append(filtered, o)
	}

	return page.New(filtered, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// PurchaseReservedCacheNodesOffering purchases a reserved cache node offering.
func (b *InMemoryBackend) PurchaseReservedCacheNodesOffering(
	ctx context.Context,
	offeringID, reservedCacheNodeID string,
	cacheNodeCount int32,
) (*ReservedCacheNode, error) {
	b.mu.Lock("PurchaseReservedCacheNodesOffering")
	defer b.mu.Unlock()

	var found *ReservedCacheNodesOffering

	offerings := builtinReservedOfferings()
	for idx := range offerings {
		if offerings[idx].OfferingID == offeringID {
			found = &offerings[idx]

			break
		}
	}

	if found == nil {
		return nil, ErrReservedCacheNodesOfferingNotFound
	}

	if cacheNodeCount <= 0 {
		cacheNodeCount = 1
	}

	if reservedCacheNodeID == "" {
		reservedCacheNodeID = fmt.Sprintf("rcn-%s-%s", offeringID[:8], randomSuffix())
	}

	region := getRegion(ctx, b.region)
	tbl := b.reservedCacheNodesStore(region)
	if _, exists := tbl.Get(reservedCacheNodeID); exists {
		return nil, fmt.Errorf("reserved cache node %q: %w", reservedCacheNodeID, ErrReservedCacheNodeAlreadyExists)
	}

	rcn := &ReservedCacheNode{
		ReservedCacheNodeID: reservedCacheNodeID,
		ARN:                 b.reservedCacheNodeARN(region, reservedCacheNodeID),
		CacheNodeType:       found.CacheNodeType,
		Duration:            found.Duration,
		FixedPrice:          found.FixedPrice,
		UsagePrice:          found.UsagePrice,
		ProductDescription:  found.ProductDescription,
		OfferingType:        found.OfferingType,
		OfferingID:          found.OfferingID,
		State:               statusActive,
		CacheNodeCount:      cacheNodeCount,
		StartTime:           time.Now(),
	}
	tbl.Put(rcn)
	b.appendEventLocked(reservedCacheNodeID, "reserved-cache-node", "reserved cache node purchased")

	return rcn, nil
}
