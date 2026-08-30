package quicksight

import (
	"maps"
	"slices"
	"sort"
	"time"
)

// filterSpaceName is the SearchSpaces filter attribute name for matching on
// a space's display name (the real API's SPACE_NAME filter).
const filterSpaceName = "SPACE_NAME"

func spaceKey(accountID, spaceID string) string {
	return accountID + "/" + spaceID
}

// storedSpaceResource is the persisted representation of one resource
// attached to a space.
type storedSpaceResource struct {
	UpdatedAt    time.Time `json:"updatedAt"`
	ResourceArn  string    `json:"resourceArn"`
	ResourceType string    `json:"resourceType"`
	ResourceName string    `json:"resourceName,omitempty"`
}

// storedSpace is the persisted representation of a QuickSight space.
type storedSpace struct {
	CreatedAt    time.Time             `json:"createdAt"`
	UpdatedAt    time.Time             `json:"updatedAt"`
	SpaceID      string                `json:"spaceId"`
	Arn          string                `json:"arn"`
	Name         string                `json:"name"`
	Description  string                `json:"description,omitempty"`
	CreatedBy    string                `json:"createdBy,omitempty"`
	CreatedByArn string                `json:"createdByArn,omitempty"`
	Resources    []storedSpaceResource `json:"resources,omitempty"`
	Permissions  []ResourcePermission  `json:"permissions,omitempty"`
}

func (s *storedSpace) toSpace() *Space {
	resources := make([]SpaceResource, len(s.Resources))
	for i, r := range s.Resources {
		resources[i] = SpaceResource(r)
	}

	return &Space{
		CreatedAt:    s.CreatedAt,
		UpdatedAt:    s.UpdatedAt,
		SpaceID:      s.SpaceID,
		Arn:          s.Arn,
		Name:         s.Name,
		Description:  s.Description,
		CreatedBy:    s.CreatedBy,
		CreatedByArn: s.CreatedByArn,
		Resources:    resources,
		Permissions:  clonePermissions(s.Permissions),
	}
}

// ---- Spaces ----

func (b *InMemoryBackend) CreateSpace(accountID, spaceID, name, description string) (*Space, error) {
	if spaceID == "" || name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateSpace")
	defer b.mu.Unlock()

	key := spaceKey(accountID, spaceID)
	if b.spaces.Has(key) {
		return nil, ErrSpaceAlreadyExists
	}

	now := time.Now().UTC()
	s := &storedSpace{
		CreatedAt:   now,
		UpdatedAt:   now,
		SpaceID:     spaceID,
		Arn:         b.buildARN("space", spaceID),
		Name:        name,
		Description: description,
	}
	b.spaces.Put(s)

	return s.toSpace(), nil
}

func (b *InMemoryBackend) DescribeSpace(accountID, spaceID string) (*Space, error) {
	b.mu.RLock("DescribeSpace")
	defer b.mu.RUnlock()

	s, ok := b.spaces.Get(spaceKey(accountID, spaceID))
	if !ok {
		return nil, ErrSpaceNotFound
	}

	return s.toSpace(), nil
}

func (b *InMemoryBackend) UpdateSpace(accountID, spaceID, name, description string) (*Space, error) {
	b.mu.Lock("UpdateSpace")
	defer b.mu.Unlock()

	key := spaceKey(accountID, spaceID)
	s, ok := b.spaces.Get(key)
	if !ok {
		return nil, ErrSpaceNotFound
	}

	if name != "" {
		s.Name = name
	}
	if description != "" {
		s.Description = description
	}
	s.UpdatedAt = time.Now().UTC()

	return s.toSpace(), nil
}

func (b *InMemoryBackend) DeleteSpace(accountID, spaceID string) (*Space, error) {
	b.mu.Lock("DeleteSpace")
	defer b.mu.Unlock()

	key := spaceKey(accountID, spaceID)
	s, ok := b.spaces.Get(key)
	if !ok {
		return nil, ErrSpaceNotFound
	}

	delete(b.tags, s.Arn)
	b.spaces.Delete(key)

	return s.toSpace(), nil
}

func (b *InMemoryBackend) ListSpaces(_ string, maxResults int32, nextToken string) ([]*Space, string, error) {
	b.mu.RLock("ListSpaces")
	defer b.mu.RUnlock()

	all := b.spaces.All()
	sort.Slice(all, func(i, j int) bool { return all[i].SpaceID < all[j].SpaceID })

	result, next := paginateSpaces(all, maxResults, nextToken)

	return result, next, nil
}

func (b *InMemoryBackend) SearchSpaces(
	_ string,
	filters []SearchFilter,
	maxResults int32,
	nextToken string,
) ([]*Space, string, error) {
	b.mu.RLock("SearchSpaces")
	defer b.mu.RUnlock()

	var filtered []*storedSpace
	for _, s := range b.spaces.All() {
		if matchesAllNameFilters(s.Name, filters, filterSpaceName) {
			filtered = append(filtered, s)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].SpaceID < filtered[j].SpaceID })

	result, next := paginateSpaces(filtered, maxResults, nextToken)

	return result, next, nil
}

func paginateSpaces(all []*storedSpace, maxResults int32, nextToken string) ([]*Space, string) {
	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		start = len(all)
		for i, s := range all {
			if s.SpaceID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].SpaceID
	} else {
		end = len(all)
	}

	result := make([]*Space, 0, end-start)
	for _, s := range all[start:end] {
		result = append(result, s.toSpace())
	}

	return result, next
}

// ---- Space permissions ----

func (b *InMemoryBackend) DescribeSpacePermissions(accountID, spaceID string) (*Space, []ResourcePermission, error) {
	b.mu.RLock("DescribeSpacePermissions")
	defer b.mu.RUnlock()

	s, ok := b.spaces.Get(spaceKey(accountID, spaceID))
	if !ok {
		return nil, nil, ErrSpaceNotFound
	}

	return s.toSpace(), clonePermissions(s.Permissions), nil
}

func (b *InMemoryBackend) UpdateSpacePermissions(
	accountID, spaceID string,
	grant, revoke []ResourcePermission,
) (*Space, []ResourcePermission, error) {
	b.mu.Lock("UpdateSpacePermissions")
	defer b.mu.Unlock()

	key := spaceKey(accountID, spaceID)
	s, ok := b.spaces.Get(key)
	if !ok {
		return nil, nil, ErrSpaceNotFound
	}

	s.Permissions = applyGrantRevoke(s.Permissions, grant, revoke)
	s.UpdatedAt = time.Now().UTC()

	return s.toSpace(), clonePermissions(s.Permissions), nil
}

// ---- Space resources ----

func (b *InMemoryBackend) ListSpaceResources(accountID, spaceID string) ([]SpaceResource, error) {
	b.mu.RLock("ListSpaceResources")
	defer b.mu.RUnlock()

	s, ok := b.spaces.Get(spaceKey(accountID, spaceID))
	if !ok {
		return nil, ErrSpaceNotFound
	}

	return s.toSpace().Resources, nil
}

// UpdateSpaceResources adds/removes resources from a space, keyed by ARN.
// Each ARN is validated against arnExists (a real, derived check -- an ARN
// that doesn't identify a resource this backend holds fails rather than
// being silently accepted), mirroring UpdateAgent's association validation.
func (b *InMemoryBackend) UpdateSpaceResources(
	accountID, spaceID string,
	add, remove []SpaceResource,
) (*Space, []AssociationFailure, error) {
	b.mu.Lock("UpdateSpaceResources")
	defer b.mu.Unlock()

	key := spaceKey(accountID, spaceID)
	s, ok := b.spaces.Get(key)
	if !ok {
		return nil, nil, ErrSpaceNotFound
	}

	byArn := make(map[string]storedSpaceResource, len(s.Resources))
	for _, r := range s.Resources {
		byArn[r.ResourceArn] = r
	}

	var failed []AssociationFailure
	now := time.Now().UTC()

	for _, r := range add {
		if !b.arnExists(r.ResourceArn) {
			failed = append(failed, AssociationFailure{
				Arn: r.ResourceArn, ResourceType: r.ResourceType,
				ErrorCode: errResourceNotFound, ErrorMessage: "resource not found",
			})

			continue
		}
		byArn[r.ResourceArn] = storedSpaceResource{
			UpdatedAt: now, ResourceArn: r.ResourceArn, ResourceType: r.ResourceType, ResourceName: r.ResourceName,
		}
	}

	for _, r := range remove {
		if _, attached := byArn[r.ResourceArn]; !attached {
			failed = append(failed, AssociationFailure{
				Arn: r.ResourceArn, ResourceType: r.ResourceType,
				ErrorCode: errResourceNotFound, ErrorMessage: "resource not attached to space",
			})

			continue
		}
		delete(byArn, r.ResourceArn)
	}

	arns := slices.Collect(maps.Keys(byArn))
	sort.Strings(arns)
	resources := make([]storedSpaceResource, 0, len(arns))
	for _, arn := range arns {
		resources = append(resources, byArn[arn])
	}
	s.Resources = resources
	s.UpdatedAt = now

	return s.toSpace(), failed, nil
}
