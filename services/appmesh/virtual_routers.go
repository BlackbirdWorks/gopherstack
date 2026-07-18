package appmesh

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) virtualRouterARN(meshName, name string) string {
	return arn.Build("appmesh", b.region, b.accountID, fmt.Sprintf("mesh/%s/virtualRouter/%s", meshName, name))
}

func (b *InMemoryBackend) routeARN(meshName, vrName, routeName string) string {
	return arn.Build(
		"appmesh", b.region, b.accountID,
		fmt.Sprintf("mesh/%s/virtualRouter/%s/route/%s", meshName, vrName, routeName),
	)
}

// ─── VirtualRouter ───

func (b *InMemoryBackend) CreateVirtualRouter(
	meshName, name string, spec json.RawMessage, tags map[string]string,
) (*VirtualRouter, error) {
	b.mu.Lock("CreateVirtualRouter")
	defer b.mu.Unlock()
	if !b.meshes.Has(meshName) {
		return nil, ErrMeshNotFound
	}
	key := meshChildKey(meshName, name)
	if b.virtualRouters.Has(key) {
		return nil, ErrVirtualRouterAlreadyExists
	}
	arn := b.virtualRouterARN(meshName, name)
	vr := &VirtualRouter{
		Meta:              newMeta(arn, b.accountID),
		MeshName:          meshName,
		VirtualRouterName: name,
		Spec:              normalizeSpec(spec),
		Status:            statusActive,
	}
	b.virtualRouters.Put(vr)
	if len(tags) > 0 {
		b.tags[arn] = cloneTags(tags)
	}

	return vr, nil
}

func (b *InMemoryBackend) DescribeVirtualRouter(meshName, name string) (*VirtualRouter, error) {
	b.mu.RLock("DescribeVirtualRouter")
	defer b.mu.RUnlock()
	if !b.meshes.Has(meshName) {
		return nil, ErrMeshNotFound
	}
	vr, ok := b.virtualRouters.Get(meshChildKey(meshName, name))
	if !ok {
		return nil, ErrVirtualRouterNotFound
	}

	return vr, nil
}

func (b *InMemoryBackend) UpdateVirtualRouter(meshName, name string, spec json.RawMessage) (*VirtualRouter, error) {
	b.mu.Lock("UpdateVirtualRouter")
	defer b.mu.Unlock()
	if !b.meshes.Has(meshName) {
		return nil, ErrMeshNotFound
	}
	vr, ok := b.virtualRouters.Get(meshChildKey(meshName, name))
	if !ok {
		return nil, ErrVirtualRouterNotFound
	}
	vr.Spec = normalizeSpec(spec)
	vr.Meta.UpdatedAt = time.Now().UTC()
	vr.Meta.Version++

	return vr, nil
}

func (b *InMemoryBackend) DeleteVirtualRouter(meshName, name string) (*VirtualRouter, error) {
	b.mu.Lock("DeleteVirtualRouter")
	defer b.mu.Unlock()
	if !b.meshes.Has(meshName) {
		return nil, ErrMeshNotFound
	}
	key := meshChildKey(meshName, name)
	vr, ok := b.virtualRouters.Get(key)
	if !ok {
		return nil, ErrVirtualRouterNotFound
	}
	if len(b.routesByRouter.Get(meshChildKey(meshName, name))) > 0 {
		return nil, ErrVirtualRouterInUse
	}
	b.virtualRouters.Delete(key)
	delete(b.tags, vr.Meta.Arn)

	return vr, nil
}

//nolint:dupl // list/create pattern is structurally identical across resource types
func (b *InMemoryBackend) ListVirtualRouters(
	meshName string, maxResults int32, nextToken string,
) ([]*VirtualRouterSummary, string, error) {
	b.mu.RLock("ListVirtualRouters")
	defer b.mu.RUnlock()
	if !b.meshes.Has(meshName) {
		return nil, "", ErrMeshNotFound
	}
	routers := b.virtualRoutersByMesh.Get(meshName)
	names := make([]string, len(routers))
	for i, vr := range routers {
		names[i] = vr.VirtualRouterName
	}
	sort.Strings(names)
	items, next := paginateStrings(names, nextToken, maxResults)
	summaries := make([]*VirtualRouterSummary, 0, len(items))
	for _, n := range items {
		vr, _ := b.virtualRouters.Get(meshChildKey(meshName, n))
		summaries = append(summaries, &VirtualRouterSummary{
			CreatedAt:         vr.Meta.CreatedAt,
			UpdatedAt:         vr.Meta.UpdatedAt,
			Arn:               vr.Meta.Arn,
			MeshName:          meshName,
			VirtualRouterName: vr.VirtualRouterName,
			MeshOwner:         vr.Meta.MeshOwner,
			ResourceOwner:     vr.Meta.ResourceOwner,
			Version:           vr.Meta.Version,
		})
	}

	return summaries, next, nil
}

// ─── Route ───

func (b *InMemoryBackend) CreateRoute(
	meshName, virtualRouterName, routeName string, spec json.RawMessage, tags map[string]string,
) (*Route, error) {
	b.mu.Lock("CreateRoute")
	defer b.mu.Unlock()
	if !b.meshes.Has(meshName) {
		return nil, ErrMeshNotFound
	}
	if !b.virtualRouters.Has(meshChildKey(meshName, virtualRouterName)) {
		return nil, ErrVirtualRouterNotFound
	}
	key := routeCompositeKey(meshName, virtualRouterName, routeName)
	if b.routes.Has(key) {
		return nil, ErrRouteAlreadyExists
	}
	arn := b.routeARN(meshName, virtualRouterName, routeName)
	r := &Route{
		Meta:              newMeta(arn, b.accountID),
		MeshName:          meshName,
		VirtualRouterName: virtualRouterName,
		RouteName:         routeName,
		Spec:              normalizeSpec(spec),
		Status:            statusActive,
	}
	b.routes.Put(r)
	if len(tags) > 0 {
		b.tags[arn] = cloneTags(tags)
	}

	return r, nil
}

func (b *InMemoryBackend) DescribeRoute(meshName, virtualRouterName, routeName string) (*Route, error) {
	b.mu.RLock("DescribeRoute")
	defer b.mu.RUnlock()
	if !b.meshes.Has(meshName) {
		return nil, ErrMeshNotFound
	}
	if !b.virtualRouters.Has(meshChildKey(meshName, virtualRouterName)) {
		return nil, ErrVirtualRouterNotFound
	}
	r, ok := b.routes.Get(routeCompositeKey(meshName, virtualRouterName, routeName))
	if !ok {
		return nil, ErrRouteNotFound
	}

	return r, nil
}

func (b *InMemoryBackend) UpdateRoute(
	meshName, virtualRouterName, routeName string,
	spec json.RawMessage,
) (*Route, error) {
	b.mu.Lock("UpdateRoute")
	defer b.mu.Unlock()
	if !b.meshes.Has(meshName) {
		return nil, ErrMeshNotFound
	}
	if !b.virtualRouters.Has(meshChildKey(meshName, virtualRouterName)) {
		return nil, ErrVirtualRouterNotFound
	}
	r, ok := b.routes.Get(routeCompositeKey(meshName, virtualRouterName, routeName))
	if !ok {
		return nil, ErrRouteNotFound
	}
	r.Spec = normalizeSpec(spec)
	r.Meta.UpdatedAt = time.Now().UTC()
	r.Meta.Version++

	return r, nil
}

func (b *InMemoryBackend) DeleteRoute(meshName, virtualRouterName, routeName string) (*Route, error) {
	b.mu.Lock("DeleteRoute")
	defer b.mu.Unlock()
	if !b.meshes.Has(meshName) {
		return nil, ErrMeshNotFound
	}
	if !b.virtualRouters.Has(meshChildKey(meshName, virtualRouterName)) {
		return nil, ErrVirtualRouterNotFound
	}
	key := routeCompositeKey(meshName, virtualRouterName, routeName)
	r, ok := b.routes.Get(key)
	if !ok {
		return nil, ErrRouteNotFound
	}
	b.routes.Delete(key)
	delete(b.tags, r.Meta.Arn)

	return r, nil
}

func (b *InMemoryBackend) ListRoutes(
	meshName, virtualRouterName string, maxResults int32, nextToken string,
) ([]*RouteSummary, string, error) {
	b.mu.RLock("ListRoutes")
	defer b.mu.RUnlock()
	if !b.meshes.Has(meshName) {
		return nil, "", ErrMeshNotFound
	}
	if !b.virtualRouters.Has(meshChildKey(meshName, virtualRouterName)) {
		return nil, "", ErrVirtualRouterNotFound
	}
	routes := b.routesByRouter.Get(meshChildKey(meshName, virtualRouterName))
	names := make([]string, len(routes))
	for i, r := range routes {
		names[i] = r.RouteName
	}
	sort.Strings(names)
	items, next := paginateStrings(names, nextToken, maxResults)
	summaries := make([]*RouteSummary, 0, len(items))
	for _, n := range items {
		r, _ := b.routes.Get(routeCompositeKey(meshName, virtualRouterName, n))
		summaries = append(summaries, &RouteSummary{
			CreatedAt:         r.Meta.CreatedAt,
			UpdatedAt:         r.Meta.UpdatedAt,
			Arn:               r.Meta.Arn,
			MeshName:          meshName,
			VirtualRouterName: virtualRouterName,
			RouteName:         r.RouteName,
			MeshOwner:         r.Meta.MeshOwner,
			ResourceOwner:     r.Meta.ResourceOwner,
			Version:           r.Meta.Version,
		})
	}

	return summaries, next, nil
}
