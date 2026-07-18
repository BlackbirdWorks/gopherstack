package route53

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// CreateCidrCollection creates a new CIDR collection.
func (b *InMemoryBackend) CreateCidrCollection(
	name, _ /* callerRef */ string,
) (*CidrCollection, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}

	b.mu.Lock("CreateCidrCollection")
	defer b.mu.Unlock()

	for _, existing := range b.cidrCollections.All() {
		if existing.Name == name {
			return nil, fmt.Errorf(
				"%w: a CIDR collection named %s already exists",
				ErrCidrCollectionAlreadyExists,
				name,
			)
		}
	}

	id := "Z" + randomZoneID()
	col := &CidrCollection{
		ID:        id,
		Name:      name,
		Version:   1,
		ARN:       "arn:aws:route53:::cidrcollection/" + id,
		Locations: make(map[string][]string),
	}

	b.cidrCollections.Put(col)

	cp := *col
	cp.Locations = copyLocations(col.Locations)

	return &cp, nil
}

func copyLocations(m map[string][]string) map[string][]string {
	out := make(map[string][]string, len(m))
	for k, v := range m {
		cp := make([]string, len(v))
		copy(cp, v)
		out[k] = cp
	}

	return out
}

// ChangeCidrCollection applies PUT/DELETE_IF_EXISTS changes to a CIDR
// collection's locations. expectedVersion is the caller-supplied
// CollectionVersion, or nil when the request omitted it (the field is
// optional on the wire). When present, it must match the collection's
// current Version or the change is rejected with
// ErrCidrCollectionVersionMismatch — AWS's optimistic-concurrency guard
// against overwriting an intervening update.
//
//nolint:gocognit // validates and applies multiple change actions with per-action branching
func (b *InMemoryBackend) ChangeCidrCollection(
	collectionID string,
	changes []CidrCollectionChange,
	expectedVersion *int64,
) (*CidrCollection, error) {
	b.mu.Lock("ChangeCidrCollection")
	defer b.mu.Unlock()

	col, ok := b.cidrCollections.Get(collectionID)
	if !ok {
		return nil, fmt.Errorf(
			"%w: CIDR collection %s not found",
			ErrCidrCollectionNotFound,
			collectionID,
		)
	}

	if expectedVersion != nil && *expectedVersion != col.Version {
		return nil, fmt.Errorf(
			"%w: CIDR collection %s is at version %d, request specified %d",
			ErrCidrCollectionVersionMismatch,
			collectionID, col.Version, *expectedVersion,
		)
	}

	if col.Locations == nil {
		col.Locations = make(map[string][]string)
	}

	for _, ch := range changes {
		switch ch.Action {
		case "PUT":
			existing := col.Locations[ch.LocationName]
			cidrSet := make(map[string]bool, len(existing))
			for _, c := range existing {
				cidrSet[c] = true
			}

			for _, c := range ch.CidrList {
				if !cidrSet[c] {
					existing = append(existing, c)
					cidrSet[c] = true
				}
			}

			col.Locations[ch.LocationName] = existing

		case "DELETE_IF_EXISTS":
			existing := col.Locations[ch.LocationName]
			remove := make(map[string]bool, len(ch.CidrList))
			for _, c := range ch.CidrList {
				remove[c] = true
			}

			kept := existing[:0:0]
			for _, c := range existing {
				if !remove[c] {
					kept = append(kept, c)
				}
			}

			if len(kept) == 0 {
				delete(col.Locations, ch.LocationName)
			} else {
				col.Locations[ch.LocationName] = kept
			}
		}
	}

	col.Version++

	cp := *col
	cp.Locations = copyLocations(col.Locations)

	return &cp, nil
}

// ListCidrLocations returns all location names in a CIDR collection.
func (b *InMemoryBackend) ListCidrLocations(collectionID string) ([]string, error) {
	b.mu.RLock("ListCidrLocations")
	defer b.mu.RUnlock()

	col, ok := b.cidrCollections.Get(collectionID)
	if !ok {
		return nil, fmt.Errorf(
			"%w: CIDR collection %s not found",
			ErrCidrCollectionNotFound,
			collectionID,
		)
	}

	locations := collections.SortedKeys(col.Locations)

	return locations, nil
}

// ListCidrBlocks returns all CIDR blocks for a given location in a collection.
func (b *InMemoryBackend) ListCidrBlocks(collectionID, locationName string) ([]string, error) {
	b.mu.RLock("ListCidrBlocks")
	defer b.mu.RUnlock()

	col, ok := b.cidrCollections.Get(collectionID)
	if !ok {
		return nil, fmt.Errorf(
			"%w: CIDR collection %s not found",
			ErrCidrCollectionNotFound,
			collectionID,
		)
	}

	cidrs := col.Locations[locationName]
	result := make([]string, len(cidrs))
	copy(result, cidrs)

	return result, nil
}

// DeleteCidrCollection deletes a CIDR collection.
// DeleteCidrCollection deletes a CIDR collection. Real AWS requires the
// collection to be empty (no locations/CIDR blocks) before it can be
// deleted — see ErrCidrCollectionInUse.
func (b *InMemoryBackend) DeleteCidrCollection(id string) error {
	b.mu.Lock("DeleteCidrCollection")
	defer b.mu.Unlock()

	col, ok := b.cidrCollections.Get(id)
	if !ok {
		return fmt.Errorf("%w: CIDR collection %s not found", ErrCidrCollectionNotFound, id)
	}

	if len(col.Locations) > 0 {
		return fmt.Errorf(
			"%w: CIDR collection %s still has locations, empty it before deleting",
			ErrCidrCollectionInUse,
			id,
		)
	}

	b.cidrCollections.Delete(id)

	return nil
}

// ListCidrCollections returns all CIDR collections.
func (b *InMemoryBackend) ListCidrCollections() ([]*CidrCollection, error) {
	b.mu.RLock("ListCidrCollections")
	defer b.mu.RUnlock()

	all := b.cidrCollections.All()
	result := make([]*CidrCollection, 0, len(all))
	for _, col := range all {
		cp := *col
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return result, nil
}
