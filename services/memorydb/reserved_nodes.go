package memorydb

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// defaultReservedNodesOfferings returns the built-in reserved node offerings.
func defaultReservedNodesOfferings() []*ReservedNodesOffering {
	return []*ReservedNodesOffering{
		{
			ReservedNodesOfferingID: "aaa00000-1111-2222-3333-444444444444",
			NodeType:                defaultNodeType,
			Duration:                reservedDuration1Year,
			FixedPrice:              reservedFixedPriceLarge1Y,
			OfferingType:            "No Upfront",
			RecurringCharges: []recurringChargeObject{
				{RecurringChargeAmount: reservedChargeRateLarge, RecurringChargeFrequency: "Hourly"},
			},
		},
		{
			ReservedNodesOfferingID: "bbb00000-1111-2222-3333-444444444444",
			NodeType:                defaultReservedNodeType,
			Duration:                reservedDuration1Year,
			FixedPrice:              reservedFixedPriceXLarge1Y,
			OfferingType:            "No Upfront",
			RecurringCharges: []recurringChargeObject{
				{RecurringChargeAmount: reservedChargeRateXLarge, RecurringChargeFrequency: "Hourly"},
			},
		},
		{
			ReservedNodesOfferingID: "ccc00000-1111-2222-3333-444444444444",
			NodeType:                defaultNodeType,
			Duration:                reservedDuration3Years,
			FixedPrice:              reservedFixedPriceLarge3Y,
			OfferingType:            "All Upfront",
			RecurringCharges:        []recurringChargeObject{},
		},
	}
}

// DescribeReservedNodes returns reserved nodes, optionally filtered by reservation ID or node type.
func (b *InMemoryBackend) DescribeReservedNodes(
	ctx context.Context,
	req *describeReservedNodesRequest,
) ([]*ReservedNode, error) {
	b.mu.RLock()

	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	all := tableAll(b.reservedNodes[region])

	result := make([]*ReservedNode, 0, len(all))
	for _, rn := range all {
		if req.ReservationID != "" && rn.ReservationID != req.ReservationID {
			continue
		}
		if req.NodeType != "" && rn.NodeType != req.NodeType {
			continue
		}
		if req.OfferingType != "" && rn.OfferingType != req.OfferingType {
			continue
		}
		if req.ReservedNodesOfferingID != "" && rn.ReservedNodesOfferingID != req.ReservedNodesOfferingID {
			continue
		}
		if req.Duration != "" {
			dSec := parseDurationToSeconds(req.Duration)
			if dSec > 0 && rn.Duration != dSec {
				continue
			}
		}
		cp := *rn
		result = append(result, &cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ReservationID < result[j].ReservationID
	})

	return result, nil
}

// DescribeReservedNodesOfferings returns available reserved node offerings.
func (b *InMemoryBackend) DescribeReservedNodesOfferings(
	_ context.Context,
	req *describeReservedNodesOfferingsRequest,
) ([]*ReservedNodesOffering, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	all := defaultReservedNodesOfferings()

	result := make([]*ReservedNodesOffering, 0, len(all))
	for _, o := range all {
		if req.ReservedNodesOfferingID != "" && o.ReservedNodesOfferingID != req.ReservedNodesOfferingID {
			continue
		}
		if req.NodeType != "" && o.NodeType != req.NodeType {
			continue
		}
		if req.OfferingType != "" && o.OfferingType != req.OfferingType {
			continue
		}
		if req.Duration != "" {
			dSec := parseDurationToSeconds(req.Duration)
			if dSec > 0 && o.Duration != dSec {
				continue
			}
		}
		result = append(result, o)
	}

	return result, nil
}

// parseDurationToSeconds converts a duration string to seconds for reserved node filtering.

func parseDurationToSeconds(d string) int32 {
	switch d {
	case "1", "31536000":
		return reservedDuration1Year
	case "3", "94608000":
		return reservedDuration3Years
	default:
		return 0
	}
}

// PurchaseReservedNodesOffering creates a new reserved node from an offering.
func (b *InMemoryBackend) PurchaseReservedNodesOffering(
	ctx context.Context,
	req *purchaseReservedNodesOfferingRequest,
) (*ReservedNode, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	if req.ReservedNodesOfferingID == "" {
		return nil, fmt.Errorf("ReservedNodesOfferingId is required: %w", ErrValidation)
	}

	var offering *ReservedNodesOffering
	for _, o := range defaultReservedNodesOfferings() {
		if o.ReservedNodesOfferingID == req.ReservedNodesOfferingID {
			offering = o

			break
		}
	}

	if offering == nil {
		return nil, fmt.Errorf("reserved nodes offering not found: %w", ErrValidation)
	}

	reservationID := req.ReservationID
	if reservationID == "" {
		reservationID = req.ReservedNodesOfferingID + "-reservation"
	}

	if _, exists := b.reservedNodesStore(region).Get(reservationID); exists {
		return nil, fmt.Errorf("reserved node %q already exists: %w", reservationID, ErrReservationAlreadyExists)
	}

	nodeCount := int32(1)
	if req.NodeCount != nil {
		nodeCount = *req.NodeCount
	}

	rnARN := arn.Build("memorydb", region, b.accountID, "reservednode/"+reservationID)

	rn := &ReservedNode{
		ReservationID:           reservationID,
		ReservedNodesOfferingID: req.ReservedNodesOfferingID,
		NodeType:                offering.NodeType,
		Duration:                offering.Duration,
		FixedPrice:              offering.FixedPrice,
		OfferingType:            offering.OfferingType,
		RecurringCharges:        offering.RecurringCharges,
		State:                   "active",
		StartTime:               awstime.Epoch(time.Now().UTC()),
		NodeCount:               nodeCount,
		ARN:                     rnARN,
	}

	b.reservedNodesStore(region).Put(rn)

	cp := *rn

	return &cp, nil
}

// -- DescribeMultiRegionParameters operation ------------------------------------
