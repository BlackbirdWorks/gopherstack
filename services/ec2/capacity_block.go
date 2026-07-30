package ec2

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ---- Capacity Block ----

// CapacityBlockReservationStatus mirrors the availability of one Capacity
// Reservation within a Capacity Block.
type CapacityBlockReservationStatus struct {
	CapacityReservationID    string `json:"capacityReservationId,omitempty"`
	TotalCapacity            int32  `json:"totalCapacity,omitempty"`
	TotalAvailableCapacity   int32  `json:"totalAvailableCapacity,omitempty"`
	TotalUnavailableCapacity int32  `json:"totalUnavailableCapacity,omitempty"`
}

// CapacityBlockStatus reports capacity/interconnect availability for a purchased
// Capacity Block.
type CapacityBlockStatus struct {
	CapacityBlockID             string                           `json:"capacityBlockId,omitempty"`
	InterconnectStatus          string                           `json:"interconnectStatus,omitempty"`
	CapacityReservationStatuses []CapacityBlockReservationStatus `json:"capacityReservationStatuses,omitempty"`
	TotalCapacity               int32                            `json:"totalCapacity,omitempty"`
	TotalAvailableCapacity      int32                            `json:"totalAvailableCapacity,omitempty"`
	TotalUnavailableCapacity    int32                            `json:"totalUnavailableCapacity,omitempty"`
}

// DescribeCapacityBlockOfferings generates a static, realistic catalog of
// purchasable Capacity Block offerings for the requested instance type and
// duration across the region's "a" and "b" Availability Zones, caching each
// generated offering so a subsequent PurchaseCapacityBlock call can reference it.
func (b *InMemoryBackend) DescribeCapacityBlockOfferings(
	instanceType string,
	durationHours, instanceCount int32,
) ([]*CapacityBlockOffering, error) {
	if instanceType == "" {
		return nil, fmt.Errorf("%w: InstanceType is required", ErrInvalidParameter)
	}

	if durationHours <= 0 {
		return nil, fmt.Errorf("%w: CapacityDurationHours must be a positive integer", ErrInvalidParameter)
	}

	if instanceCount <= 0 {
		instanceCount = 1
	}

	b.mu.Lock("DescribeCapacityBlockOfferings")
	defer b.mu.Unlock()

	hourlyRate := capacityBlockHourlyRate(instanceType)
	upfront := hourlyRate * float64(durationHours) * float64(instanceCount)
	start := time.Now().UTC().Add(capacityBlockOfferingLeadTime)

	var offerings []*CapacityBlockOffering

	for _, azSuffix := range []string{"a", "b"} {
		offering := &CapacityBlockOffering{
			CapacityBlockOfferingID:    "cbo-" + uuid.New().String()[:8],
			InstanceType:               instanceType,
			AvailabilityZone:           b.Region + azSuffix,
			Tenancy:                    crFleetTenancyDefault,
			CurrencyCode:               "USD",
			UpfrontPrice:               fmt.Sprintf("%.2f", upfront),
			InstanceCount:              instanceCount,
			CapacityBlockDurationHours: durationHours,
			StartDate:                  start,
			EndDate:                    start.Add(time.Duration(durationHours) * time.Hour),
		}
		b.capacityBlockOfferings.Put(offering)
		offerings = append(offerings, offering)
	}

	return offerings, nil
}

// PurchaseCapacityBlock purchases a previously described Capacity Block
// offering, creating a backing CapacityReservation and a CapacityBlock record.
func (b *InMemoryBackend) PurchaseCapacityBlock(
	offeringID, instancePlatform string,
	tags map[string]string,
) (*CapacityBlock, *CapacityReservation, error) {
	if offeringID == "" {
		return nil, nil, fmt.Errorf("%w: CapacityBlockOfferingId is required", ErrInvalidParameter)
	}

	b.mu.Lock("PurchaseCapacityBlock")
	defer b.mu.Unlock()

	offering, ok := b.capacityBlockOfferings.Get(offeringID)
	if !ok {
		return nil, nil, fmt.Errorf("%w: %s", ErrCapacityBlockOfferingNotFound, offeringID)
	}

	if instancePlatform == "" {
		instancePlatform = "Linux/UNIX"
	}

	crID := "cr-" + uuid.New().String()[:8]
	now := time.Now().UTC()
	cr := &CapacityReservation{
		CapacityReservationID:  crID,
		InstanceType:           offering.InstanceType,
		AvailabilityZone:       offering.AvailabilityZone,
		InstancePlatform:       instancePlatform,
		TotalInstanceCount:     int(offering.InstanceCount),
		AvailableInstanceCount: int(offering.InstanceCount),
		State:                  stateActive,
		CreateTime:             now,
		OwnedBy:                b.AccountID,
	}
	b.capacityReservations.Put(cr)

	block := &CapacityBlock{
		CapacityBlockID:        "cb-" + uuid.New().String()[:8],
		AvailabilityZone:       offering.AvailabilityZone,
		State:                  stateActive,
		CreateDate:             now,
		StartDate:              offering.StartDate,
		EndDate:                offering.EndDate,
		CapacityReservationIDs: []string{crID},
	}
	b.capacityBlocks.Put(block)
	b.setTagsLocked(block.CapacityBlockID, tags)
	b.capacityBlockOfferings.Delete(offeringID)

	blockCopy := *block
	crCopy := *cr

	return &blockCopy, &crCopy, nil
}

// findCapacityBlockByReservationIDLocked returns the CapacityBlock backing the
// given CapacityReservation, if any. Must be called with b.mu held.
func (b *InMemoryBackend) findCapacityBlockByReservationIDLocked(reservationID string) *CapacityBlock {
	for _, block := range b.capacityBlocks.All() {
		if slices.Contains(block.CapacityReservationIDs, reservationID) {
			return block
		}
	}

	return nil
}

// DescribeCapacityBlockExtensionOfferings generates a purchasable extension
// offering for the given Capacity Reservation, starting at the Capacity Block's
// current end date.
func (b *InMemoryBackend) DescribeCapacityBlockExtensionOfferings(
	reservationID string,
	durationHours int32,
) ([]*CapacityBlockExtensionOffering, error) {
	if reservationID == "" {
		return nil, fmt.Errorf("%w: CapacityReservationId is required", ErrInvalidParameter)
	}

	if durationHours <= 0 {
		return nil, fmt.Errorf(
			"%w: CapacityBlockExtensionDurationHours must be a positive integer",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("DescribeCapacityBlockExtensionOfferings")
	defer b.mu.Unlock()

	cr, ok := b.capacityReservations.Get(reservationID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCapacityReservationNotFound, reservationID)
	}

	block := b.findCapacityBlockByReservationIDLocked(reservationID)

	extStart := time.Now().UTC()
	if block != nil {
		extStart = block.EndDate
	}

	hourlyRate := capacityBlockHourlyRate(cr.InstanceType)
	upfront := hourlyRate * float64(durationHours) * float64(cr.TotalInstanceCount)

	offering := &CapacityBlockExtensionOffering{
		CapacityBlockExtensionOfferingID:    "cbeo-" + uuid.New().String()[:8],
		CapacityReservationID:               reservationID,
		InstanceType:                        cr.InstanceType,
		AvailabilityZone:                    cr.AvailabilityZone,
		CurrencyCode:                        "USD",
		UpfrontPrice:                        fmt.Sprintf("%.2f", upfront),
		InstanceCount:                       toInt32Clamped(cr.TotalInstanceCount),
		CapacityBlockExtensionDurationHours: durationHours,
		CapacityBlockExtensionStartDate:     extStart,
		CapacityBlockExtensionEndDate:       extStart.Add(time.Duration(durationHours) * time.Hour),
	}
	b.capacityBlockExtensionOfferings.Put(offering)

	return []*CapacityBlockExtensionOffering{offering}, nil
}

// PurchaseCapacityBlockExtension purchases a previously described Capacity
// Block extension offering, extending the backing Capacity Block's end date.
func (b *InMemoryBackend) PurchaseCapacityBlockExtension(
	extensionOfferingID, reservationID string,
) (*CapacityBlockExtension, error) {
	if extensionOfferingID == "" {
		return nil, fmt.Errorf("%w: CapacityBlockExtensionOfferingId is required", ErrInvalidParameter)
	}

	b.mu.Lock("PurchaseCapacityBlockExtension")
	defer b.mu.Unlock()

	offering, ok := b.capacityBlockExtensionOfferings.Get(extensionOfferingID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCapacityBlockExtensionOfferingNotFound, extensionOfferingID)
	}

	if reservationID != "" && reservationID != offering.CapacityReservationID {
		return nil, fmt.Errorf(
			"%w: CapacityReservationId does not match the offering's reservation",
			ErrInvalidParameter,
		)
	}

	now := time.Now().UTC()
	ext := &CapacityBlockExtension{
		CapacityBlockExtensionOfferingID:    offering.CapacityBlockExtensionOfferingID,
		CapacityReservationID:               offering.CapacityReservationID,
		AvailabilityZone:                    offering.AvailabilityZone,
		CapacityBlockExtensionStatus:        "payment-succeeded",
		CapacityBlockExtensionDurationHours: offering.CapacityBlockExtensionDurationHours,
		CapacityBlockExtensionStartDate:     offering.CapacityBlockExtensionStartDate,
		CapacityBlockExtensionEndDate:       offering.CapacityBlockExtensionEndDate,
		CapacityBlockExtensionPurchaseDate:  now,
	}
	b.capacityBlockExtensions.Put(ext)

	if block := b.findCapacityBlockByReservationIDLocked(offering.CapacityReservationID); block != nil {
		block.EndDate = offering.CapacityBlockExtensionEndDate
	}
	b.capacityBlockExtensionOfferings.Delete(extensionOfferingID)

	return ext, nil
}

// DescribeCapacityBlocks returns Capacity Blocks matching the given IDs (all, if
// empty) and filters (capacity-block-id, availability-zone, state), sorted by ID.
func (b *InMemoryBackend) DescribeCapacityBlocks(
	ids []string,
	filters map[string][]string,
) []*CapacityBlock {
	b.mu.RLock("DescribeCapacityBlocks")
	defer b.mu.RUnlock()

	idSet := toIDSet(ids)

	var result []*CapacityBlock

	for _, block := range b.capacityBlocks.All() {
		matched := idAndFiltersMatch(idSet, block.CapacityBlockID,
			matchesCapacityFilter(filters, "capacity-block-id", block.CapacityBlockID),
			matchesCapacityFilter(filters, "availability-zone", block.AvailabilityZone),
			matchesCapacityFilter(filters, "state", block.State),
		)
		if !matched {
			continue
		}

		cp := *block
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].CapacityBlockID < result[j].CapacityBlockID })

	return result
}

// DescribeCapacityBlockStatus returns the capacity/interconnect availability of
// the given Capacity Blocks (all, if ids is empty), filtered by interconnect-status.
func (b *InMemoryBackend) DescribeCapacityBlockStatus(
	ids []string,
	filters map[string][]string,
) []*CapacityBlockStatus {
	b.mu.RLock("DescribeCapacityBlockStatus")
	defer b.mu.RUnlock()

	idSet := toIDSet(ids)

	var result []*CapacityBlockStatus

	const interconnectOK = "ok"

	for _, block := range b.capacityBlocks.All() {
		matched := idAndFiltersMatch(idSet, block.CapacityBlockID,
			matchesCapacityFilter(filters, "interconnect-status", interconnectOK),
		)
		if !matched {
			continue
		}

		status := &CapacityBlockStatus{
			CapacityBlockID:    block.CapacityBlockID,
			InterconnectStatus: interconnectOK,
		}

		for _, crID := range block.CapacityReservationIDs {
			cr, ok := b.capacityReservations.Get(crID)
			if !ok {
				continue
			}

			total := toInt32Clamped(cr.TotalInstanceCount)
			available := toInt32Clamped(cr.AvailableInstanceCount)
			status.TotalCapacity += total
			status.TotalAvailableCapacity += available
			status.TotalUnavailableCapacity += total - available
			status.CapacityReservationStatuses = append(status.CapacityReservationStatuses,
				CapacityBlockReservationStatus{
					CapacityReservationID:    crID,
					TotalCapacity:            total,
					TotalAvailableCapacity:   available,
					TotalUnavailableCapacity: total - available,
				})
		}

		result = append(result, status)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].CapacityBlockID < result[j].CapacityBlockID })

	return result
}

// DescribeCapacityBlockExtensionHistory returns purchased Capacity Block
// extensions for the given Capacity Reservation IDs (all, if empty), filtered by
// capacity-reservation-id, availability-zone, and capacity-block-extension-status.
func (b *InMemoryBackend) DescribeCapacityBlockExtensionHistory(
	reservationIDs []string,
	filters map[string][]string,
) []*CapacityBlockExtension {
	b.mu.RLock("DescribeCapacityBlockExtensionHistory")
	defer b.mu.RUnlock()

	idSet := toIDSet(reservationIDs)

	var result []*CapacityBlockExtension

	for _, ext := range b.capacityBlockExtensions.All() {
		matched := idAndFiltersMatch(idSet, ext.CapacityReservationID,
			matchesCapacityFilter(filters, "capacity-reservation-id", ext.CapacityReservationID),
			matchesCapacityFilter(filters, "availability-zone", ext.AvailabilityZone),
			matchesCapacityFilter(filters, "capacity-block-extension-status", ext.CapacityBlockExtensionStatus),
		)
		if !matched {
			continue
		}

		cp := *ext
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CapacityBlockExtensionOfferingID < result[j].CapacityBlockExtensionOfferingID
	})

	return result
}
