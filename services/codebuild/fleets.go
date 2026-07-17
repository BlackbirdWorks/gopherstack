package codebuild

import (
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) buildFleetARN(name string) string {
	return arn.Build("codebuild", b.region, b.accountID, "fleet/"+name)
}

// CreateFleet creates a new compute fleet.
func (b *InMemoryBackend) CreateFleet(
	name string, baseCapacity int32, computeType, environmentType string, tags map[string]string,
) (*Fleet, error) {
	b.mu.Lock("CreateFleet")
	defer b.mu.Unlock()

	if b.fleets.Has(name) {
		return nil, ErrAlreadyExists
	}

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	now := float64(time.Now().Unix())
	f := &Fleet{
		Arn:             b.buildFleetARN(name),
		Name:            name,
		BaseCapacity:    baseCapacity,
		ComputeType:     computeType,
		EnvironmentType: environmentType,
		Status:          &FleetStatus{StatusCode: "ACTIVE"},
		Tags:            tagsCopy,
		Created:         now,
		LastModified:    now,
	}
	b.fleets.Put(f)

	out := *f

	return &out, nil
}

// BatchGetFleets returns fleets by name or ARN. Missing names are returned separately.
func (b *InMemoryBackend) BatchGetFleets(names []string) ([]*Fleet, []string) {
	b.mu.RLock("BatchGetFleets")
	defer b.mu.RUnlock()

	found := make([]*Fleet, 0, len(names))
	notFound := make([]string, 0, len(names))

	for _, nameOrARN := range names {
		f, ok := b.fleets.Get(nameOrARN)
		if !ok {
			if matches := b.fleetsByARN.Get(nameOrARN); len(matches) > 0 {
				f, ok = matches[0], true
			}
		}

		if ok {
			out := *f
			found = append(found, &out)
		} else {
			notFound = append(notFound, nameOrARN)
		}
	}

	return found, notFound
}

// ListFleets returns all fleet ARNs in sorted order.
func (b *InMemoryBackend) ListFleets() []string {
	b.mu.RLock("ListFleets")
	defer b.mu.RUnlock()

	items := b.fleets.All()
	arns := make([]string, 0, len(items))

	for _, f := range items {
		arns = append(arns, f.Arn)
	}

	sort.Strings(arns)

	return arns
}

// DeleteFleet removes a fleet by ARN.
func (b *InMemoryBackend) DeleteFleet(arnStr string) error {
	b.mu.Lock("DeleteFleet")
	defer b.mu.Unlock()

	if matches := b.fleetsByARN.Get(arnStr); len(matches) > 0 {
		b.fleets.Delete(matches[0].Name)

		return nil
	}

	// also try by name for convenience
	if b.fleets.Delete(arnStr) {
		return nil
	}

	return ErrNotFound
}

// UpdateFleet updates the base capacity of a fleet.
func (b *InMemoryBackend) UpdateFleet(arnStr string, baseCapacity int32) (*Fleet, error) {
	b.mu.Lock("UpdateFleet")
	defer b.mu.Unlock()

	matches := b.fleetsByARN.Get(arnStr)
	if len(matches) == 0 {
		return nil, ErrNotFound
	}

	f := matches[0]
	f.BaseCapacity = baseCapacity
	f.LastModified = float64(time.Now().Unix())
	out := *f

	return &out, nil
}
