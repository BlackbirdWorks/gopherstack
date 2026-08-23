package ec2

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

const interruptibleAllocStatusActive = "active"

// InterruptibleCapacityReservationAllocation tracks an interruptible
// allocation carved out of an existing (source) Capacity Reservation's spare
// capacity. It overlays the source CapacityReservation rather than minting a
// new reservation resource: the real CreateInterruptibleCapacityReservationAllocation
// and UpdateInterruptibleCapacityReservationAllocation outputs identify the
// allocation by its source reservation, not a distinct ID.
type InterruptibleCapacityReservationAllocation struct {
	SourceCapacityReservationID string `json:"sourceCapacityReservationID,omitempty"`
	Status                      string `json:"status,omitempty"`
	TargetInstanceCount         int32  `json:"targetInstanceCount,omitempty"`
}

// CapacityReservationInstanceUsage is a per-account usage row within
// CapacityReservationUsage.
type CapacityReservationInstanceUsage struct {
	AccountID         string `json:"accountID,omitempty"`
	UsedInstanceCount int32  `json:"usedInstanceCount,omitempty"`
}

// CapacityReservationUsage is the real-usage view returned by
// GetCapacityReservationUsage, derived from the source CapacityReservation
// plus any instances currently targeting it and any interruptible allocation
// carved out of it.
type CapacityReservationUsage struct {
	InterruptibleAllocation *InterruptibleCapacityReservationAllocation
	CapacityReservationID   string
	InstanceType            string
	State                   string
	InstanceUsages          []CapacityReservationInstanceUsage
	AvailableInstanceCount  int
	TotalInstanceCount      int
	Interruptible           bool
}

// CapacityReservationTopologyEntry is a single row returned by
// DescribeCapacityReservationTopology, derived from an existing
// CapacityReservation.
type CapacityReservationTopologyEntry struct {
	CapacityReservationID string
	InstanceType          string
	AvailabilityZone      string
}

// CreateCapacityReservation creates a new capacity reservation.
func (b *InMemoryBackend) CreateCapacityReservation(
	instanceType, availabilityZone string,
	instanceCount int,
	tags map[string]string,
) (*CapacityReservation, error) {
	if instanceType == "" {
		return nil, fmt.Errorf("%w: InstanceType is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateCapacityReservation")
	defer b.mu.Unlock()

	cr := &CapacityReservation{
		CapacityReservationID:  "cr-" + uuid.New().String()[:8],
		InstanceType:           instanceType,
		AvailabilityZone:       availabilityZone,
		TotalInstanceCount:     instanceCount,
		AvailableInstanceCount: instanceCount,
		State:                  stateActive,
		CreateTime:             time.Now().UTC(),
		OwnedBy:                b.AccountID,
	}
	b.capacityReservations.Put(cr)
	b.setTagsLocked(cr.CapacityReservationID, tags)

	return cr, nil
}

// CancelCapacityReservation cancels an active capacity reservation.
func (b *InMemoryBackend) CancelCapacityReservation(reservationID string) error {
	if reservationID == "" {
		return fmt.Errorf("%w: CapacityReservationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CancelCapacityReservation")
	defer b.mu.Unlock()

	cr, ok := b.capacityReservations.Get(reservationID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, reservationID)
	}
	cr.State = "cancelled"

	return nil
}

// ModifyCapacityReservation updates instance count for a capacity reservation.
func (b *InMemoryBackend) ModifyCapacityReservation(reservationID string, instanceCount int) error {
	if reservationID == "" {
		return fmt.Errorf("%w: CapacityReservationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyCapacityReservation")
	defer b.mu.Unlock()

	cr, ok := b.capacityReservations.Get(reservationID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrInvalidParameter, reservationID)
	}
	cr.TotalInstanceCount = instanceCount
	cr.AvailableInstanceCount = instanceCount

	return nil
}

// GetGroupsForCapacityReservation returns placement groups associated with a reservation.
// In this implementation, returns an empty list (no group associations tracked).
func (b *InMemoryBackend) GetGroupsForCapacityReservation(reservationID string) ([]string, error) {
	if reservationID == "" {
		return nil, fmt.Errorf("%w: CapacityReservationId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetGroupsForCapacityReservation")
	defer b.mu.RUnlock()

	if _, ok := b.capacityReservations.Get(reservationID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, reservationID)
	}

	return []string{}, nil
}

// ---- Instance Connect Endpoint ----

// CreateInterruptibleCapacityReservationAllocation carves out an
// interruptible allocation from a source Capacity Reservation's spare
// capacity, reducing the source's AvailableInstanceCount.
func (b *InMemoryBackend) CreateInterruptibleCapacityReservationAllocation(
	sourceCapacityReservationID string, instanceCount int32,
) (*InterruptibleCapacityReservationAllocation, error) {
	if sourceCapacityReservationID == "" {
		return nil, fmt.Errorf("%w: CapacityReservationId is required", ErrInvalidParameter)
	}

	if instanceCount <= 0 {
		return nil, fmt.Errorf("%w: InstanceCount must be positive", ErrInvalidParameter)
	}

	b.mu.Lock("CreateInterruptibleCapacityReservationAllocation")
	defer b.mu.Unlock()

	cr, ok := b.capacityReservations.Get(sourceCapacityReservationID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCapacityReservationNotFound, sourceCapacityReservationID)
	}

	if cr.AvailableInstanceCount < int(instanceCount) {
		return nil, fmt.Errorf(
			"%w: only %d instances available in %s", ErrCapacityReservationFull,
			cr.AvailableInstanceCount, sourceCapacityReservationID,
		)
	}

	cr.AvailableInstanceCount -= int(instanceCount)

	alloc := &InterruptibleCapacityReservationAllocation{
		SourceCapacityReservationID: sourceCapacityReservationID,
		Status:                      interruptibleAllocStatusActive,
		TargetInstanceCount:         instanceCount,
	}
	b.interruptibleCRAllocations.Put(alloc)

	cp := *alloc

	return &cp, nil
}

// UpdateInterruptibleCapacityReservationAllocation resizes a previously
// created interruptible allocation, adjusting the source reservation's
// AvailableInstanceCount by the delta.
func (b *InMemoryBackend) UpdateInterruptibleCapacityReservationAllocation(
	sourceCapacityReservationID string, targetInstanceCount int32,
) (*InterruptibleCapacityReservationAllocation, error) {
	if sourceCapacityReservationID == "" {
		return nil, fmt.Errorf("%w: CapacityReservationId is required", ErrInvalidParameter)
	}

	if targetInstanceCount < 0 {
		return nil, fmt.Errorf("%w: TargetInstanceCount must not be negative", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateInterruptibleCapacityReservationAllocation")
	defer b.mu.Unlock()

	cr, ok := b.capacityReservations.Get(sourceCapacityReservationID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCapacityReservationNotFound, sourceCapacityReservationID)
	}

	alloc, ok := b.interruptibleCRAllocations.Get(sourceCapacityReservationID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInterruptibleAllocationNotFound, sourceCapacityReservationID)
	}

	delta := targetInstanceCount - alloc.TargetInstanceCount
	if delta > 0 && cr.AvailableInstanceCount < int(delta) {
		return nil, fmt.Errorf(
			"%w: only %d instances available in %s", ErrCapacityReservationFull,
			cr.AvailableInstanceCount, sourceCapacityReservationID,
		)
	}

	cr.AvailableInstanceCount -= int(delta)
	alloc.TargetInstanceCount = targetInstanceCount
	alloc.Status = interruptibleAllocStatusActive

	cp := *alloc

	return &cp, nil
}

// GetCapacityReservationUsage derives real usage for a Capacity Reservation
// from the reservation itself, any instances currently targeting it, and any
// interruptible allocation carved out of it.
func (b *InMemoryBackend) GetCapacityReservationUsage(id string) (*CapacityReservationUsage, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: CapacityReservationId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetCapacityReservationUsage")
	defer b.mu.RUnlock()

	cr, ok := b.capacityReservations.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCapacityReservationNotFound, id)
	}

	var usedCount int32

	for _, inst := range b.instances.All() {
		if inst.CapacityReservationSpec.CapacityReservationID == id {
			usedCount++
		}
	}

	var usages []CapacityReservationInstanceUsage
	if usedCount > 0 {
		usages = []CapacityReservationInstanceUsage{{AccountID: b.AccountID, UsedInstanceCount: usedCount}}
	}

	usage := &CapacityReservationUsage{
		CapacityReservationID:  cr.CapacityReservationID,
		InstanceType:           cr.InstanceType,
		State:                  cr.State,
		AvailableInstanceCount: cr.AvailableInstanceCount,
		TotalInstanceCount:     cr.TotalInstanceCount,
		InstanceUsages:         usages,
	}

	if alloc, hasAlloc := b.interruptibleCRAllocations.Get(id); hasAlloc {
		usage.Interruptible = true
		allocCopy := *alloc
		usage.InterruptibleAllocation = &allocCopy
	}

	return usage, nil
}

// DescribeCapacityReservationTopology returns per-reservation topology rows
// derived from the existing Capacity Reservation state.
func (b *InMemoryBackend) DescribeCapacityReservationTopology(ids []string) []*CapacityReservationTopologyEntry {
	b.mu.RLock("DescribeCapacityReservationTopology")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	out := make([]*CapacityReservationTopologyEntry, 0, b.capacityReservations.Len())

	for _, cr := range b.capacityReservations.All() {
		if len(filter) > 0 && !filter[cr.CapacityReservationID] {
			continue
		}

		out = append(out, &CapacityReservationTopologyEntry{
			CapacityReservationID: cr.CapacityReservationID,
			InstanceType:          cr.InstanceType,
			AvailabilityZone:      cr.AvailabilityZone,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CapacityReservationID < out[j].CapacityReservationID
	})

	return out
}
