package athena

import (
	"fmt"
	"maps"
	"sort"
	"time"
)

const minCapacityDPUs int32 = 24

// canDeleteCapacityReservation reports whether a capacity reservation status allows deletion.
func canDeleteCapacityReservation(status string) bool {
	return status == stateCancelled || status == stateCancelling
}

// CreateCapacityReservation creates a new capacity reservation.
func (b *InMemoryBackend) CreateCapacityReservation(
	name string,
	targetDPUs int32,
	tags map[string]string,
) error {
	switch {
	case name == "":
		return fmt.Errorf("%w: Name is required", ErrValidation)
	case targetDPUs < minCapacityDPUs:
		return fmt.Errorf("%w: TargetDpus minimum is 24", ErrValidation)
	}

	b.mu.Lock("CreateCapacityReservation")
	defer b.mu.Unlock()

	if b.capacityReservations.Has(name) {
		return fmt.Errorf("%w: capacity reservation %q already exists", ErrAlreadyExists, name)
	}

	now := float64(time.Now().UnixMilli()) / millisToSeconds
	b.capacityReservations.Put(&CapacityReservation{
		Name:          name,
		Status:        "ACTIVE",
		TargetDpus:    targetDPUs,
		AllocatedDpus: targetDPUs,
		Tags:          maps.Clone(tags),
		CreationTime:  now,
		LastAllocation: &CapacityAllocation{
			RequestTime:           now,
			RequestCompletionTime: now,
			Status:                stateSucceeded,
		},
		LastSuccessfulAllocationTime: now,
	})

	return nil
}

// CancelCapacityReservation cancels an active capacity reservation.
func (b *InMemoryBackend) CancelCapacityReservation(name string) error {
	b.mu.Lock("CancelCapacityReservation")
	defer b.mu.Unlock()

	cr, ok := b.capacityReservations.Get(name)
	if !ok {
		return fmt.Errorf("%w: capacity reservation %q not found", ErrNotFound, name)
	}

	cr.Status = stateCancelling

	return nil
}

// DeleteCapacityReservation removes a capacity reservation.
// The reservation must be in CANCELLING or CANCELLED status.
func (b *InMemoryBackend) DeleteCapacityReservation(name string) error {
	b.mu.Lock("DeleteCapacityReservation")
	defer b.mu.Unlock()

	cr, ok := b.capacityReservations.Get(name)
	if !ok {
		return fmt.Errorf("%w: capacity reservation %q not found", ErrNotFound, name)
	}

	if !canDeleteCapacityReservation(cr.Status) {
		return fmt.Errorf(
			"%w: capacity reservation %q must be cancelled before deletion (current status: %s)",
			ErrValidation,
			name,
			cr.Status,
		)
	}

	b.capacityReservations.Delete(name)

	return nil
}

// GetCapacityReservation returns a capacity reservation.
func (b *InMemoryBackend) GetCapacityReservation(name string) (*CapacityReservation, error) {
	b.mu.RLock("GetCapacityReservation")
	defer b.mu.RUnlock()

	cr, ok := b.capacityReservations.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: capacity reservation %q not found", ErrNotFound, name)
	}

	cp := *cr
	cp.Tags = maps.Clone(cr.Tags)

	return &cp, nil
}

// ListCapacityReservations returns all capacity reservations.
func (b *InMemoryBackend) ListCapacityReservations() ([]CapacityReservation, error) {
	b.mu.RLock("ListCapacityReservations")
	defer b.mu.RUnlock()

	out := make([]CapacityReservation, 0, b.capacityReservations.Len())
	for _, cr := range b.capacityReservations.All() {
		cp := *cr
		cp.Tags = maps.Clone(cr.Tags)
		out = append(out, cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, nil
}

// UpdateCapacityReservation changes the target DPUs of a capacity reservation.
func (b *InMemoryBackend) UpdateCapacityReservation(name string, targetDPUs int32) error {
	if targetDPUs < minCapacityDPUs {
		return fmt.Errorf("%w: TargetDpus minimum is 24", ErrValidation)
	}

	b.mu.Lock("UpdateCapacityReservation")
	defer b.mu.Unlock()

	cr, ok := b.capacityReservations.Get(name)
	if !ok {
		return fmt.Errorf("%w: capacity reservation %q not found", ErrNotFound, name)
	}

	now := nowSeconds()
	cr.TargetDpus = targetDPUs
	cr.AllocatedDpus = targetDPUs
	cr.LastAllocation = &CapacityAllocation{
		RequestTime:           now,
		RequestCompletionTime: now,
		Status:                stateSucceeded,
	}
	cr.LastSuccessfulAllocationTime = now

	return nil
}

// PutCapacityAssignmentConfiguration sets the assignment configuration on a reservation.
func (b *InMemoryBackend) PutCapacityAssignmentConfiguration(
	name string, assignments []CapacityAssignment,
) error {
	if name == "" {
		return fmt.Errorf("%w: CapacityReservationName is required", ErrValidation)
	}

	b.mu.Lock("PutCapacityAssignmentConfiguration")
	defer b.mu.Unlock()

	if !b.capacityReservations.Has(name) {
		return fmt.Errorf("%w: capacity reservation %q not found", ErrNotFound, name)
	}

	cp := make([]CapacityAssignment, len(assignments))
	for i, a := range assignments {
		dst := make([]string, len(a.WorkGroupNames))
		copy(dst, a.WorkGroupNames)
		cp[i] = CapacityAssignment{WorkGroupNames: dst}
	}

	b.capacityAssignments.Put(&CapacityAssignmentConfiguration{
		CapacityReservationName: name,
		CapacityAssignments:     cp,
	})

	return nil
}

// GetCapacityAssignmentConfiguration returns the assignment configuration for a reservation.
func (b *InMemoryBackend) GetCapacityAssignmentConfiguration(name string) (*CapacityAssignmentConfiguration, error) {
	b.mu.RLock("GetCapacityAssignmentConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.capacityAssignments.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: capacity assignment %q not found", ErrNotFound, name)
	}

	cp := *cfg
	cp.CapacityAssignments = make([]CapacityAssignment, len(cfg.CapacityAssignments))

	for i, a := range cfg.CapacityAssignments {
		dst := make([]string, len(a.WorkGroupNames))
		copy(dst, a.WorkGroupNames)
		cp.CapacityAssignments[i] = CapacityAssignment{WorkGroupNames: dst}
	}

	return &cp, nil
}
