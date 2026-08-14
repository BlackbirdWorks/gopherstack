package ec2

import (
	"errors"
	"fmt"
)

// Errors for placement group operations.
var (
	ErrPlacementGroupNotFound      = errors.New("InvalidPlacementGroup.NotFound")
	ErrDuplicatePlacementGroupName = errors.New("InvalidPlacementGroup.Duplicate")
)

// PlacementGroup represents an EC2 placement group.
type PlacementGroup struct {
	Name     string `json:"name,omitempty"`
	Strategy string `json:"strategy,omitempty"`
	State    string `json:"state,omitempty"`
}

// CreatePlacementGroup creates a new placement group.
func (b *InMemoryBackend) CreatePlacementGroup(name, strategy string, tags map[string]string) (*PlacementGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: GroupName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreatePlacementGroup")
	defer b.mu.Unlock()

	if _, exists := b.placementGroups.Get(name); exists {
		return nil, fmt.Errorf("%w: %s", ErrDuplicatePlacementGroupName, name)
	}

	if strategy == "" {
		strategy = "cluster"
	}

	pg := &PlacementGroup{
		Name:     name,
		Strategy: strategy,
		State:    stateAvailable,
	}
	b.placementGroups.Put(pg)
	b.setTagsLocked(pg.Name, tags)

	return pg, nil
}

// DescribePlacementGroups returns placement groups, optionally filtered by names.
// When names are provided, lookups are O(len(names)) via the placement-group
// map rather than scanning every group in the backend.
func (b *InMemoryBackend) DescribePlacementGroups(names []string) []*PlacementGroup {
	b.mu.RLock("DescribePlacementGroups")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		out := make([]*PlacementGroup, 0, len(names))

		for _, n := range names {
			pg, ok := b.placementGroups.Get(n)
			if !ok {
				continue
			}

			cp := *pg
			out = append(out, &cp)
		}

		return out
	}

	out := make([]*PlacementGroup, 0, b.placementGroups.Len())

	for _, pg := range b.placementGroups.All() {
		cp := *pg
		out = append(out, &cp)
	}

	return out
}

// DeletePlacementGroup removes a placement group by name.
func (b *InMemoryBackend) DeletePlacementGroup(name string) error {
	b.mu.Lock("DeletePlacementGroup")
	defer b.mu.Unlock()

	if _, ok := b.placementGroups.Get(name); !ok {
		return fmt.Errorf("%w: %s", ErrPlacementGroupNotFound, name)
	}
	b.placementGroups.Delete(name)
	delete(b.tags, name)

	return nil
}
