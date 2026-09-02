package route53

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
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

// CIDR collection change actions, per ChangeCidrCollection's wire contract.
const (
	cidrCollectionActionPut            = "PUT"
	cidrCollectionActionDeleteIfExists = "DELETE_IF_EXISTS"
)

// applyCidrCollectionPut adds any CIDR blocks in ch.CidrList not already
// present at ch.LocationName, deduplicating against the existing set.
func applyCidrCollectionPut(col *CidrCollection, ch CidrCollectionChange) {
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
}

// applyCidrCollectionDeleteIfExists removes any CIDR blocks in ch.CidrList
// from ch.LocationName, dropping the location entirely once it's emptied.
func applyCidrCollectionDeleteIfExists(col *CidrCollection, ch CidrCollectionChange) {
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

// applyCidrCollectionChange dispatches a single change to its action handler.
// Unrecognized actions are silently ignored, matching the pre-decomposition
// switch's default (the wire layer is responsible for rejecting unknown
// Action values before reaching the backend).
func applyCidrCollectionChange(col *CidrCollection, ch CidrCollectionChange) {
	switch ch.Action {
	case cidrCollectionActionPut:
		applyCidrCollectionPut(col, ch)
	case cidrCollectionActionDeleteIfExists:
		applyCidrCollectionDeleteIfExists(col, ch)
	}
}

// ChangeCidrCollection applies PUT/DELETE_IF_EXISTS changes to a CIDR
// collection's locations. expectedVersion is the caller-supplied
// CollectionVersion, or nil when the request omitted it (the field is
// optional on the wire). When present, it must match the collection's
// current Version or the change is rejected with
// ErrCidrCollectionVersionMismatch — AWS's optimistic-concurrency guard
// against overwriting an intervening update.
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
		applyCidrCollectionChange(col, ch)
	}

	col.Version++

	cp := *col
	cp.Locations = copyLocations(col.Locations)

	return &cp, nil
}

// ListCidrLocations returns a page of location names in a CIDR collection,
// paginated by NextToken (route53@v1.65.6 api_op_ListCidrLocations.go: no
// IsTruncated member, like its ListCidrCollections/ListCidrBlocks siblings).
// collections.SortedKeys is already deterministic across calls.
func (b *InMemoryBackend) ListCidrLocations(
	collectionID, nextToken string,
	maxResults int,
) (page.Page[string], error) {
	b.mu.RLock("ListCidrLocations")
	defer b.mu.RUnlock()

	col, ok := b.cidrCollections.Get(collectionID)
	if !ok {
		return page.Page[string]{}, fmt.Errorf(
			"%w: CIDR collection %s not found",
			ErrCidrCollectionNotFound,
			collectionID,
		)
	}

	locations := collections.SortedKeys(col.Locations)

	return page.New(locations, nextToken, maxResults, route53DefaultMaxItems), nil
}

// ListCidrBlocks returns a page of CIDR blocks for a given location in a
// collection, paginated by NextToken (route53@v1.65.6
// api_op_ListCidrBlocks.go). col.Locations[locationName] is an append-only
// slice (never a map), so it is already deterministic across calls.
func (b *InMemoryBackend) ListCidrBlocks(
	collectionID, locationName, nextToken string,
	maxResults int,
) (page.Page[string], error) {
	b.mu.RLock("ListCidrBlocks")
	defer b.mu.RUnlock()

	col, ok := b.cidrCollections.Get(collectionID)
	if !ok {
		return page.Page[string]{}, fmt.Errorf(
			"%w: CIDR collection %s not found",
			ErrCidrCollectionNotFound,
			collectionID,
		)
	}

	cidrs := col.Locations[locationName]
	result := make([]string, len(cidrs))
	copy(result, cidrs)

	return page.New(result, nextToken, maxResults, route53DefaultMaxItems), nil
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

// ListCidrCollections returns a page of CIDR collections, paginated by
// NextToken (route53@v1.65.6 api_op_ListCidrCollections.go: no IsTruncated
// member). Sorted by ID, which is unique, so the sort admits no ties
// despite b.cidrCollections.All() being an unordered map walk.
func (b *InMemoryBackend) ListCidrCollections(
	nextToken string,
	maxResults int,
) (page.Page[*CidrCollection], error) {
	b.mu.RLock("ListCidrCollections")
	defer b.mu.RUnlock()

	all := b.cidrCollections.All()
	result := make([]*CidrCollection, 0, len(all))
	for _, col := range all {
		cp := *col
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return page.New(result, nextToken, maxResults, route53DefaultMaxItems), nil
}
