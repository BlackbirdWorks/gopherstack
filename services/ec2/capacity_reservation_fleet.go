package ec2

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ---- Capacity Reservation Fleet ----

// CreateCapacityReservationFleet creates a Capacity Reservation Fleet, allocating
// the total target capacity across the given instance type specifications using
// the "prioritized" allocation strategy: the lowest-Priority spec is fulfilled
// first (up to the full target capacity), remaining specs are fulfilled with any
// leftover capacity. A backing CapacityReservation is created for each spec that
// receives a non-zero instance count.
func (b *InMemoryBackend) CreateCapacityReservationFleet(
	specs []CapacityReservationFleetInstanceSpec,
	totalTargetCapacity int32,
	allocationStrategy, instanceMatchCriteria, tenancy string,
	endDate *time.Time,
	tags map[string]string,
) (*CapacityReservationFleet, error) {
	if len(specs) == 0 {
		return nil, fmt.Errorf("%w: InstanceTypeSpecifications is required", ErrInvalidParameter)
	}

	if totalTargetCapacity <= 0 {
		return nil, fmt.Errorf("%w: TotalTargetCapacity must be a positive integer", ErrInvalidParameter)
	}

	if allocationStrategy == "" {
		allocationStrategy = crFleetAllocDefault
	}

	if instanceMatchCriteria == "" {
		instanceMatchCriteria = crFleetMatchOpen
	}

	if tenancy == "" {
		tenancy = crFleetTenancyDefault
	}

	b.mu.Lock("CreateCapacityReservationFleet")
	defer b.mu.Unlock()

	resolved := make([]CapacityReservationFleetInstanceSpec, len(specs))
	copy(resolved, specs)
	sort.SliceStable(resolved, func(i, j int) bool { return resolved[i].Priority < resolved[j].Priority })

	now := time.Now().UTC()

	var fulfilled float64

	remaining := float64(totalTargetCapacity)
	for i := range resolved {
		if resolved[i].AvailabilityZone == "" {
			resolved[i].AvailabilityZone = b.Region + "a"
		}

		if remaining <= 0 {
			continue
		}

		weight := resolved[i].Weight
		if weight <= 0 {
			weight = 1
		}

		instanceCount := int32(math.Ceil(remaining / weight))
		crID := "cr-" + uuid.New().String()[:8]
		b.capacityReservations.Put(&CapacityReservation{
			CapacityReservationID:  crID,
			InstanceType:           resolved[i].InstanceType,
			AvailabilityZone:       resolved[i].AvailabilityZone,
			TotalInstanceCount:     int(instanceCount),
			AvailableInstanceCount: int(instanceCount),
			State:                  stateActive,
			CreateTime:             now,
			OwnedBy:                b.AccountID,
		})

		resolved[i].CapacityReservationID = crID
		resolved[i].TotalInstanceCount = instanceCount

		provided := weight * float64(instanceCount)
		if provided > remaining {
			fulfilled += remaining
			remaining = 0
		} else {
			fulfilled += provided
			remaining -= provided
		}
	}

	fleet := &CapacityReservationFleet{
		CapacityReservationFleetID: "crf-" + uuid.New().String()[:8],
		AllocationStrategy:         allocationStrategy,
		State:                      stateActive,
		InstanceMatchCriteria:      instanceMatchCriteria,
		Tenancy:                    tenancy,
		TotalTargetCapacity:        totalTargetCapacity,
		TotalFulfilledCapacity:     fulfilled,
		CreateTime:                 now,
		EndDate:                    endDate,
		InstanceTypeSpecifications: resolved,
	}
	b.capacityReservationFleets.Put(fleet)
	b.setTagsLocked(fleet.CapacityReservationFleetID, tags)

	cp := *fleet

	return &cp, nil
}

// DescribeCapacityReservationFleets returns Capacity Reservation Fleets matching
// the given IDs (all, if empty) and filters (state, tenancy, allocation-strategy,
// instance-match-criteria), sorted by ID.
func (b *InMemoryBackend) DescribeCapacityReservationFleets(
	ids []string,
	filters map[string][]string,
) []*CapacityReservationFleet {
	b.mu.RLock("DescribeCapacityReservationFleets")
	defer b.mu.RUnlock()

	idSet := toIDSet(ids)

	var result []*CapacityReservationFleet

	for _, fleet := range b.capacityReservationFleets.All() {
		matched := idAndFiltersMatch(idSet, fleet.CapacityReservationFleetID,
			matchesCapacityFilter(filters, "state", fleet.State),
			matchesCapacityFilter(filters, "tenancy", fleet.Tenancy),
			matchesCapacityFilter(filters, "allocation-strategy", fleet.AllocationStrategy),
			matchesCapacityFilter(filters, "instance-match-criteria", fleet.InstanceMatchCriteria),
		)
		if !matched {
			continue
		}

		cp := *fleet
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CapacityReservationFleetID < result[j].CapacityReservationFleetID
	})

	return result
}

// ModifyCapacityReservationFleet updates the total target capacity and/or end
// date of a Capacity Reservation Fleet. When totalTargetCapacity is non-nil, the
// fleet's highest-priority instance type specification (and its backing
// CapacityReservation) is resized to match the new target.
func (b *InMemoryBackend) ModifyCapacityReservationFleet(
	fleetID string,
	totalTargetCapacity *int32,
	endDate *time.Time,
	removeEndDate bool,
) error {
	if fleetID == "" {
		return fmt.Errorf("%w: CapacityReservationFleetId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyCapacityReservationFleet")
	defer b.mu.Unlock()

	fleet, ok := b.capacityReservationFleets.Get(fleetID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrCapacityReservationFleetNotFound, fleetID)
	}

	if removeEndDate {
		fleet.EndDate = nil
	} else if endDate != nil {
		fleet.EndDate = endDate
	}

	if totalTargetCapacity != nil {
		b.resizeFleetPrimarySpecLocked(fleet, *totalTargetCapacity)
	}

	return nil
}

// resizeFleetPrimarySpecLocked applies a new total target capacity to fleet,
// resizing its highest-priority instance type specification (and backing
// CapacityReservation) to match. Must be called with b.mu held.
func (b *InMemoryBackend) resizeFleetPrimarySpecLocked(fleet *CapacityReservationFleet, totalTargetCapacity int32) {
	fleet.TotalTargetCapacity = totalTargetCapacity
	fleet.TotalFulfilledCapacity = float64(totalTargetCapacity)

	if len(fleet.InstanceTypeSpecifications) == 0 {
		return
	}

	primary := &fleet.InstanceTypeSpecifications[0]

	weight := primary.Weight
	if weight <= 0 {
		weight = 1
	}

	primary.TotalInstanceCount = int32(math.Ceil(float64(totalTargetCapacity) / weight))

	cr, found := b.capacityReservations.Get(primary.CapacityReservationID)
	if !found {
		return
	}

	cr.TotalInstanceCount = int(primary.TotalInstanceCount)
	cr.AvailableInstanceCount = int(primary.TotalInstanceCount)
}

// CancelCapacityReservationFleets cancels the given Capacity Reservation Fleets
// and all of their backing Capacity Reservations. It returns the successfully
// cancelled fleets (with previous/current state) and any fleet IDs that could
// not be found.
func (b *InMemoryBackend) CancelCapacityReservationFleets(
	fleetIDs []string,
) ([]CapacityReservationFleetCancellation, []FailedCapacityReservationFleetCancellation) {
	b.mu.Lock("CancelCapacityReservationFleets")
	defer b.mu.Unlock()

	var (
		successful []CapacityReservationFleetCancellation
		failed     []FailedCapacityReservationFleetCancellation
	)

	for _, id := range fleetIDs {
		fleet, ok := b.capacityReservationFleets.Get(id)
		if !ok {
			failed = append(failed, FailedCapacityReservationFleetCancellation{
				CapacityReservationFleetID: id,
				ErrorCode:                  "InvalidCapacityReservationFleetId.NotFound",
				ErrorMessage:               fmt.Sprintf("The Capacity Reservation Fleet ID '%s' does not exist", id),
			})

			continue
		}

		previousState := fleet.State
		fleet.State = stateCancelled

		for _, spec := range fleet.InstanceTypeSpecifications {
			if cr, found := b.capacityReservations.Get(spec.CapacityReservationID); found {
				cr.State = stateCancelled
			}
		}

		successful = append(successful, CapacityReservationFleetCancellation{
			CapacityReservationFleetID: fleet.CapacityReservationFleetID,
			PreviousState:              previousState,
			CurrentState:               fleet.State,
		})
	}

	return successful, failed
}
