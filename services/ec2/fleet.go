package ec2

import (
	"fmt"
	"slices"
	"sort"

	"github.com/google/uuid"
)

func (b *InMemoryBackend) CreateFleet(fleetType string, totalTargetCapacity int) (*Fleet, error) {
	b.mu.Lock("CreateFleet")
	defer b.mu.Unlock()

	if fleetType == "" {
		fleetType = fleetTypeDefault
	}

	id := "fleet-" + uuid.New().String()[:8]
	f := &Fleet{
		FleetID:                         id,
		FleetState:                      SpotFleetStateActive,
		FleetType:                       fleetType,
		TotalTargetCapacity:             totalTargetCapacity,
		ExcessCapacityTerminationPolicy: "termination",
	}
	b.fleets.Put(f)

	cp := *f

	return &cp, nil
}

// FleetDeletionResult mirrors the real DeleteFleetSuccessItem: the state the
// fleet was in immediately before deletion, alongside its ID. AWS's real
// shape has no plain fleetState member for this op, only
// currentFleetState/previousFleetState (types.go, DeleteFleetSuccessItem).
type FleetDeletionResult struct {
	FleetID            string
	PreviousFleetState string
}

func (b *InMemoryBackend) DeleteFleets(ids []string) []FleetDeletionResult {
	b.mu.Lock("DeleteFleets")
	defer b.mu.Unlock()

	var deleted []FleetDeletionResult

	for _, id := range ids {
		if f, ok := b.fleets.Get(id); ok {
			prev := f.FleetState
			f.FleetState = tgwRouteStateDeleted
			b.fleets.Delete(id)
			delete(b.tags, id)
			deleted = append(deleted, FleetDeletionResult{FleetID: id, PreviousFleetState: prev})
		}
	}

	return deleted
}

func (b *InMemoryBackend) DescribeFleets(ids []string) []*Fleet {
	b.mu.RLock("DescribeFleets")
	defer b.mu.RUnlock()

	var result []*Fleet

	for _, f := range b.fleets.All() {
		if len(ids) > 0 && !slices.Contains(ids, f.FleetID) {
			continue
		}

		cp := *f
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].FleetID < result[j].FleetID
	})

	return result
}

func (b *InMemoryBackend) ModifyFleet(id string, totalTargetCapacity int, excessPolicy string) error {
	b.mu.Lock("ModifyFleet")
	defer b.mu.Unlock()

	f, ok := b.fleets.Get(id)
	if !ok {
		return fmt.Errorf("%w: %s", ErrFleetNotFound, id)
	}

	if totalTargetCapacity > 0 {
		f.TotalTargetCapacity = totalTargetCapacity
	}

	if excessPolicy != "" {
		f.ExcessCapacityTerminationPolicy = excessPolicy
	}

	return nil
}

// ---- Network Insights backend methods ----
