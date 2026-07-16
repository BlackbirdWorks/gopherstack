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

func (b *InMemoryBackend) DeleteFleets(ids []string) []string {
	b.mu.Lock("DeleteFleets")
	defer b.mu.Unlock()

	var deleted []string

	for _, id := range ids {
		if f, ok := b.fleets.Get(id); ok {
			f.FleetState = tgwRouteStateDeleted
			b.fleets.Delete(id)
			deleted = append(deleted, id)
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
