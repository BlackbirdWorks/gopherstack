package ec2

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ---- Capacity Reservation splitting / instance movement ----

// CreateCapacityReservationBySplitting splits instanceCount instances off of
// the source Capacity Reservation into a brand-new destination reservation with
// the same instance type and Availability Zone.
func (b *InMemoryBackend) CreateCapacityReservationBySplitting(
	sourceID string,
	instanceCount int,
	tags map[string]string,
) (*CapacityReservation, *CapacityReservation, error) {
	if sourceID == "" {
		return nil, nil, fmt.Errorf("%w: SourceCapacityReservationId is required", ErrInvalidParameter)
	}

	if instanceCount <= 0 {
		return nil, nil, fmt.Errorf("%w: InstanceCount must be a positive integer", ErrInvalidParameter)
	}

	b.mu.Lock("CreateCapacityReservationBySplitting")
	defer b.mu.Unlock()

	src, ok := b.capacityReservations.Get(sourceID)
	if !ok {
		return nil, nil, fmt.Errorf("%w: %s", ErrCapacityReservationNotFound, sourceID)
	}

	if src.AvailableInstanceCount < instanceCount {
		return nil, nil, fmt.Errorf(
			"%w: source Capacity Reservation only has %d available instances",
			ErrCapacityReservationFull, src.AvailableInstanceCount,
		)
	}

	src.AvailableInstanceCount -= instanceCount
	src.TotalInstanceCount -= instanceCount

	dst := &CapacityReservation{
		CapacityReservationID:  "cr-" + uuid.New().String()[:8],
		InstanceType:           src.InstanceType,
		AvailabilityZone:       src.AvailabilityZone,
		InstancePlatform:       src.InstancePlatform,
		TotalInstanceCount:     instanceCount,
		AvailableInstanceCount: instanceCount,
		State:                  stateActive,
		CreateTime:             time.Now().UTC(),
		OwnedBy:                b.AccountID,
	}
	b.capacityReservations.Put(dst)

	if len(tags) > 0 {
		b.tags[dst.CapacityReservationID] = tags
	}

	srcCopy := *src
	dstCopy := *dst

	return &dstCopy, &srcCopy, nil
}

// MoveCapacityReservationInstances moves instanceCount instances from the
// source Capacity Reservation into the destination Capacity Reservation. Both
// reservations must already exist and share the same instance type.
func (b *InMemoryBackend) MoveCapacityReservationInstances(
	sourceID, destinationID string,
	instanceCount int,
) (*CapacityReservation, *CapacityReservation, error) {
	if sourceID == "" || destinationID == "" {
		return nil, nil, fmt.Errorf(
			"%w: SourceCapacityReservationId and DestinationCapacityReservationId are required",
			ErrInvalidParameter,
		)
	}

	if instanceCount <= 0 {
		return nil, nil, fmt.Errorf("%w: InstanceCount must be a positive integer", ErrInvalidParameter)
	}

	b.mu.Lock("MoveCapacityReservationInstances")
	defer b.mu.Unlock()

	src, ok := b.capacityReservations.Get(sourceID)
	if !ok {
		return nil, nil, fmt.Errorf("%w: %s", ErrCapacityReservationNotFound, sourceID)
	}

	dst, ok := b.capacityReservations.Get(destinationID)
	if !ok {
		return nil, nil, fmt.Errorf("%w: %s", ErrCapacityReservationNotFound, destinationID)
	}

	if src.AvailableInstanceCount < instanceCount {
		return nil, nil, fmt.Errorf(
			"%w: source Capacity Reservation only has %d available instances",
			ErrCapacityReservationFull, src.AvailableInstanceCount,
		)
	}

	src.AvailableInstanceCount -= instanceCount
	src.TotalInstanceCount -= instanceCount
	dst.AvailableInstanceCount += instanceCount
	dst.TotalInstanceCount += instanceCount

	srcCopy := *src
	dstCopy := *dst

	return &dstCopy, &srcCopy, nil
}

// ---- Capacity Reservation billing ownership ----

// AssociateCapacityReservationBillingOwner creates (or replaces) a pending
// request to assign billing for the given Capacity Reservation to another
// account.
func (b *InMemoryBackend) AssociateCapacityReservationBillingOwner(
	reservationID, billingOwnerID string,
) error {
	if reservationID == "" || billingOwnerID == "" {
		return fmt.Errorf(
			"%w: CapacityReservationId and UnusedReservationBillingOwnerId are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("AssociateCapacityReservationBillingOwner")
	defer b.mu.Unlock()

	if _, ok := b.capacityReservations.Get(reservationID); !ok {
		return fmt.Errorf("%w: %s", ErrCapacityReservationNotFound, reservationID)
	}
	b.capacityReservationBillingRequests.Put(&CapacityReservationBillingRequest{
		CapacityReservationID:           reservationID,
		RequestedBy:                     b.AccountID,
		UnusedReservationBillingOwnerID: billingOwnerID,
		Status:                          billingRequestPending,
		LastUpdateTime:                  time.Now().UTC(),
	})

	return nil
}

// DisassociateCapacityReservationBillingOwner revokes a pending or accepted
// billing ownership request for the given Capacity Reservation.
func (b *InMemoryBackend) DisassociateCapacityReservationBillingOwner(
	reservationID, billingOwnerID string,
) error {
	if reservationID == "" || billingOwnerID == "" {
		return fmt.Errorf(
			"%w: CapacityReservationId and UnusedReservationBillingOwnerId are required",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("DisassociateCapacityReservationBillingOwner")
	defer b.mu.Unlock()

	req, ok := b.capacityReservationBillingRequests.Get(reservationID)
	if !ok || req.UnusedReservationBillingOwnerID != billingOwnerID {
		return fmt.Errorf("%w: %s", ErrCapacityReservationBillingRequestNotFound, reservationID)
	}

	req.Status = billingRequestRevoked
	req.LastUpdateTime = time.Now().UTC()

	return nil
}

// RejectCapacityReservationBillingOwnership rejects a pending billing ownership
// request sent to this account for the given Capacity Reservation.
func (b *InMemoryBackend) RejectCapacityReservationBillingOwnership(
	reservationID string,
) error {
	if reservationID == "" {
		return fmt.Errorf("%w: CapacityReservationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("RejectCapacityReservationBillingOwnership")
	defer b.mu.Unlock()

	req, ok := b.capacityReservationBillingRequests.Get(reservationID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrCapacityReservationBillingRequestNotFound, reservationID)
	}

	req.Status = billingRequestRejected
	req.LastUpdateTime = time.Now().UTC()

	return nil
}

// DescribeCapacityReservationBillingRequests returns billing ownership requests
// matching the given Capacity Reservation IDs (all, if empty) and filters
// (status).
func (b *InMemoryBackend) DescribeCapacityReservationBillingRequests(
	ids []string,
	filters map[string][]string,
) []*CapacityReservationBillingRequest {
	b.mu.RLock("DescribeCapacityReservationBillingRequests")
	defer b.mu.RUnlock()

	idSet := toIDSet(ids)

	var result []*CapacityReservationBillingRequest

	for _, req := range b.capacityReservationBillingRequests.All() {
		if len(idSet) > 0 && !idSet[req.CapacityReservationID] {
			continue
		}

		if !matchesCapacityFilter(filters, "status", req.Status) {
			continue
		}

		cp := *req
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CapacityReservationID < result[j].CapacityReservationID
	})

	return result
}
