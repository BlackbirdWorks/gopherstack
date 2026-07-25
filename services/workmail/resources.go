package workmail

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// --- Resources ---

func (b *InMemoryBackend) findResource(orgID, entityID string) *Resource {
	if r, ok := b.resources.Get(orgKey(orgID, entityID)); ok {
		return r
	}
	for _, r := range b.resourcesByOrg.Get(orgID) {
		if r.Name == entityID {
			return r
		}
	}

	return nil
}

// CreateResource creates a new WorkMail resource.
func (b *InMemoryBackend) CreateResource(
	orgID, name, resourceType, description string,
) (*Resource, error) {
	b.mu.Lock("CreateResource")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}
	validTypes := map[string]bool{"ROOM": true, "EQUIPMENT": true}
	if resourceType != "" && !validTypes[resourceType] {
		return nil, fmt.Errorf(
			"%w: invalid Type %q, must be ROOM or EQUIPMENT",
			ErrValidation,
			resourceType,
		)
	}

	for _, r := range b.resourcesByOrg.Get(orgID) {
		if r.Name == name {
			return nil, fmt.Errorf("%w: resource %q already exists", ErrConflict, name)
		}
	}

	resourceID := newID()
	now := time.Now().UTC()

	r := &Resource{
		CreatedAt:    now,
		ResourceID:   resourceID,
		Name:         name,
		ResourceType: resourceType,
		Description:  description,
		State:        stateDisabled,
		ARN:          b.entityARN(orgID, "resource", resourceID),
		orgID:        orgID,
	}

	b.resources.Put(r)
	b.delegates[orgID][resourceID] = make(map[string]bool)

	return r, nil
}

// DescribeResource returns resource details.
func (b *InMemoryBackend) DescribeResource(orgID, entityID string) (*Resource, error) {
	b.mu.RLock("DescribeResource")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	r := b.findResource(orgID, entityID)
	if r == nil {
		return nil, fmt.Errorf("%w: resource %q not found", ErrNotFound, entityID)
	}

	return r, nil
}

// UpdateResource updates resource fields.
func (b *InMemoryBackend) UpdateResource(orgID, entityID, name, description string) error {
	b.mu.Lock("UpdateResource")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	r := b.findResource(orgID, entityID)
	if r == nil {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, entityID)
	}

	if name != "" {
		r.Name = name
	}
	if description != "" {
		r.Description = description
	}

	return nil
}

// DeleteResource removes a resource. See the doc comment on DeleteGroup in
// groups.go for the shared-logic rationale.
//
//nolint:dupl // structurally-identical CRUD pair with DeleteGroup; see doc comment on DeleteGroup.
func (b *InMemoryBackend) DeleteResource(orgID, entityID string) error {
	b.mu.Lock("DeleteResource")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	r := b.findResource(orgID, entityID)
	if r == nil {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, entityID)
	}

	if r.State == stateEnabled {
		return fmt.Errorf(
			"%w: resource %q is in ENABLED state and cannot be deleted; call DeregisterFromWorkMail first",
			ErrEntityState,
			entityID,
		)
	}

	if r.Email != "" {
		delete(b.resourcesByEmail[orgID], r.Email)
		b.globalAliases.Delete(r.Email)
	}
	b.cascadeCleanEntity(orgID, r.ResourceID, r.ARN)
	b.resources.Delete(orgKey(orgID, r.ResourceID))
	delete(b.delegates[orgID], r.ResourceID)

	return nil
}

// ListResources returns a paginated list of resources, optionally narrowed
// by filter (see ResourceFilter -- mirrors ListResourcesInput.Filters).
func (b *InMemoryBackend) ListResources(
	orgID string,
	filter *ResourceFilter,
	maxResults int32,
	nextToken string,
) ([]*ResourceSummary, string, error) {
	b.mu.RLock("ListResources")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	rs := make([]*ResourceSummary, 0, len(b.resourcesByOrg.Get(orgID)))
	for _, r := range b.resourcesByOrg.Get(orgID) {
		if !resourceMatchesFilter(r, filter) {
			continue
		}
		rs = append(rs, &ResourceSummary{
			ResourceID:   r.ResourceID,
			Name:         r.Name,
			Email:        r.Email,
			ResourceType: r.ResourceType,
			State:        r.State,
			Description:  r.Description,
		})
	}
	sort.Slice(rs, func(i, j int) bool { return rs[i].Name < rs[j].Name })

	items, next := paginate(rs, maxResults, nextToken)

	return items, next, nil
}

// resourceMatchesFilter reports whether r satisfies every non-empty
// dimension of filter. A nil filter matches everything.
func resourceMatchesFilter(r *Resource, filter *ResourceFilter) bool {
	if filter == nil {
		return true
	}
	if filter.NamePrefix != "" && !strings.HasPrefix(r.Name, filter.NamePrefix) {
		return false
	}
	if filter.PrimaryEmailPrefix != "" && !strings.HasPrefix(r.Email, filter.PrimaryEmailPrefix) {
		return false
	}
	if filter.State != "" && r.State != filter.State {
		return false
	}

	return true
}
