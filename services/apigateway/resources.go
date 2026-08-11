package apigateway

import (
	"fmt"
	"sort"
	"strings"
)

// GetResources returns all resources for a REST API with pagination.
func (b *InMemoryBackend) GetResources(restAPIID, position string, limit int) ([]Resource, string, error) {
	b.mu.RLock("GetResources")
	defer b.mu.RUnlock()

	if !b.restApis.Has(restAPIID) {
		return nil, "", fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	group := b.resourcesByAPI.Get(restAPIID)
	all := make([]Resource, 0, len(group))
	for _, r := range group {
		all = append(all, *r)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	page, pos := paginatePageByKey(all, limit, position, func(r Resource) string { return r.ID })

	return page, pos, nil
}

// ResourcesForRouting returns every resource for the API plus a version counter that
// changes on any resource-set mutation. Unlike GetResources it is not paginated: the
// data-plane proxy needs the complete set to build a routing trie, and it uses the
// version to cache that trie across requests instead of rebuilding it every time.
func (b *InMemoryBackend) ResourcesForRouting(restAPIID string) ([]Resource, uint64, error) {
	b.mu.RLock("ResourcesForRouting")
	defer b.mu.RUnlock()

	if !b.restApis.Has(restAPIID) {
		return nil, 0, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	group := b.resourcesByAPI.Get(restAPIID)
	all := make([]Resource, 0, len(group))
	for _, r := range group {
		all = append(all, *r)
	}

	return all, b.resourceVersions[restAPIID], nil
}

// GetResource returns a single resource.
func (b *InMemoryBackend) GetResource(restAPIID, resourceID string) (*Resource, error) {
	b.mu.RLock("GetResource")
	defer b.mu.RUnlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	cp := *r

	return &cp, nil
}

// CreateResource creates a new resource under a parent.
func (b *InMemoryBackend) CreateResource(restAPIID, parentID, pathPart string) (*Resource, error) {
	if pathPart == "" {
		return nil, fmt.Errorf("%w: pathPart is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateResource")
	defer b.mu.Unlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	parent, ok := b.resources.Get(resourceKey(restAPIID, parentID))
	if !ok {
		return nil, fmt.Errorf("%w: parent resource %s not found", ErrResourceNotFound, parentID)
	}

	path := computePath(parent.Path, pathPart)

	id := randomID(resourceIDLength)
	res := &Resource{
		ID:              id,
		ParentID:        parentID,
		PathPart:        pathPart,
		Path:            path,
		RestAPIID:       restAPIID,
		ResourceMethods: make(map[string]*Method),
	}
	b.resources.Put(res)
	b.resourceVersions[restAPIID]++

	cp := *res

	return &cp, nil
}

// DeleteResource removes a resource.
func (b *InMemoryBackend) DeleteResource(restAPIID, resourceID string) error {
	b.mu.Lock("DeleteResource")
	defer b.mu.Unlock()

	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	if !b.resources.Delete(resourceKey(restAPIID, resourceID)) {
		return fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	b.resourceVersions[restAPIID]++

	return nil
}

func computePath(parentPath, pathPart string) string {
	if parentPath == "/" {
		return "/" + pathPart
	}

	return strings.TrimRight(parentPath, "/") + "/" + pathPart
}

// UpdateResource updates the pathPart and/or parent of a resource (recomputing
// Path for it and its whole subtree if either changed), or sets CORS.
func (b *InMemoryBackend) UpdateResource(restAPIID, resourceID string, input UpdateResourceInput) (*Resource, error) {
	b.mu.Lock("UpdateResource")
	defer b.mu.Unlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	res, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}

	if input.ParentID != "" && input.ParentID != res.ParentID {
		if err := b.reparentResource(restAPIID, res, input.ParentID); err != nil {
			return nil, err
		}
	}

	if input.PathPart != "" {
		res.PathPart = input.PathPart
	}

	if input.ParentID != "" || input.PathPart != "" {
		var parentPath string
		if res.ParentID != "" {
			if parent, exists := b.resources.Get(resourceKey(restAPIID, res.ParentID)); exists {
				parentPath = parent.Path
			}
		}

		res.Path = computePath(parentPath, res.PathPart)
		b.resourceVersions[restAPIID]++
		b.recomputeDescendantPaths(restAPIID, res)
	}

	if input.CorsConfiguration != nil {
		res.CorsConfiguration = input.CorsConfiguration
	}

	cp := *res

	return &cp, nil
}

// reparentResource validates and applies a PATCH "/parentId" move: the new
// parent must exist in the same REST API, and moving res under itself or one
// of its own descendants would create a cycle.
func (b *InMemoryBackend) reparentResource(restAPIID string, res *Resource, newParentID string) error {
	if !b.resources.Has(resourceKey(restAPIID, newParentID)) {
		return fmt.Errorf("%w: parent resource %s not found", ErrResourceNotFound, newParentID)
	}

	if b.isResourceOrDescendant(restAPIID, res.ID, newParentID) {
		return fmt.Errorf("%w: resource %s cannot become its own ancestor", ErrInvalidParameter, res.ID)
	}

	res.ParentID = newParentID

	return nil
}

// isResourceOrDescendant reports whether candidateID is resourceID itself or a
// descendant of it, used by reparentResource to reject a move that would
// create a cycle.
func (b *InMemoryBackend) isResourceOrDescendant(restAPIID, resourceID, candidateID string) bool {
	if resourceID == candidateID {
		return true
	}

	for _, r := range b.resourcesByAPI.Get(restAPIID) {
		if r.ParentID == resourceID && b.isResourceOrDescendant(restAPIID, r.ID, candidateID) {
			return true
		}
	}

	return false
}

// recomputeDescendantPaths refreshes Path for every descendant of res after
// its own Path changed (PATCH "/parentId" or "/pathPart"), since each
// resource's Path is stored precomputed rather than derived lazily from its
// ancestor chain.
func (b *InMemoryBackend) recomputeDescendantPaths(restAPIID string, res *Resource) {
	for _, child := range b.resourcesByAPI.Get(restAPIID) {
		if child.ParentID != res.ID {
			continue
		}

		child.Path = computePath(res.Path, child.PathPart)
		b.recomputeDescendantPaths(restAPIID, child)
	}
}
