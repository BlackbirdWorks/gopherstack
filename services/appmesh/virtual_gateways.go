package appmesh

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) virtualGatewayARN(meshName, name string) string {
	return arn.Build("appmesh", b.region, b.accountID, fmt.Sprintf("mesh/%s/virtualGateway/%s", meshName, name))
}

func (b *InMemoryBackend) gatewayRouteARN(meshName, vgName, routeName string) string {
	return arn.Build(
		"appmesh", b.region, b.accountID,
		fmt.Sprintf("mesh/%s/virtualGateway/%s/gatewayRoute/%s", meshName, vgName, routeName),
	)
}

// ─── VirtualGateway ───

func (b *InMemoryBackend) CreateVirtualGateway(
	meshName, name string, spec json.RawMessage, tags map[string]string,
) (*VirtualGateway, error) {
	b.mu.Lock("CreateVirtualGateway")
	defer b.mu.Unlock()
	if !b.meshes.Has(meshName) {
		return nil, ErrMeshNotFound
	}
	key := meshChildKey(meshName, name)
	if b.virtualGWs.Has(key) {
		return nil, ErrVirtualGatewayAlreadyExists
	}
	arn := b.virtualGatewayARN(meshName, name)
	vg := &VirtualGateway{
		Meta:               newMeta(arn, b.accountID),
		MeshName:           meshName,
		VirtualGatewayName: name,
		Spec:               normalizeSpec(spec),
		Status:             statusActive,
	}
	b.virtualGWs.Put(vg)
	if len(tags) > 0 {
		b.tags[arn] = cloneTags(tags)
	}

	return vg, nil
}

func (b *InMemoryBackend) DescribeVirtualGateway(meshName, name string) (*VirtualGateway, error) {
	b.mu.RLock("DescribeVirtualGateway")
	defer b.mu.RUnlock()
	if !b.meshes.Has(meshName) {
		return nil, ErrMeshNotFound
	}
	vg, ok := b.virtualGWs.Get(meshChildKey(meshName, name))
	if !ok {
		return nil, ErrVirtualGatewayNotFound
	}

	return vg, nil
}

func (b *InMemoryBackend) UpdateVirtualGateway(meshName, name string, spec json.RawMessage) (*VirtualGateway, error) {
	b.mu.Lock("UpdateVirtualGateway")
	defer b.mu.Unlock()
	if !b.meshes.Has(meshName) {
		return nil, ErrMeshNotFound
	}
	vg, ok := b.virtualGWs.Get(meshChildKey(meshName, name))
	if !ok {
		return nil, ErrVirtualGatewayNotFound
	}
	vg.Spec = normalizeSpec(spec)
	vg.Meta.UpdatedAt = time.Now().UTC()
	vg.Meta.Version++

	return vg, nil
}

func (b *InMemoryBackend) DeleteVirtualGateway(meshName, name string) (*VirtualGateway, error) {
	b.mu.Lock("DeleteVirtualGateway")
	defer b.mu.Unlock()
	if !b.meshes.Has(meshName) {
		return nil, ErrMeshNotFound
	}
	key := meshChildKey(meshName, name)
	vg, ok := b.virtualGWs.Get(key)
	if !ok {
		return nil, ErrVirtualGatewayNotFound
	}
	if len(b.gatewayRoutesByGateway.Get(meshChildKey(meshName, name))) > 0 {
		return nil, ErrVirtualGatewayInUse
	}
	b.virtualGWs.Delete(key)
	delete(b.tags, vg.Meta.Arn)

	return vg, nil
}

//nolint:dupl // list/create pattern is structurally identical across resource types
func (b *InMemoryBackend) ListVirtualGateways(
	meshName string, maxResults int32, nextToken string,
) ([]*VirtualGatewaySummary, string, error) {
	b.mu.RLock("ListVirtualGateways")
	defer b.mu.RUnlock()
	if !b.meshes.Has(meshName) {
		return nil, "", ErrMeshNotFound
	}
	gws := b.virtualGWsByMesh.Get(meshName)
	names := make([]string, len(gws))
	for i, vg := range gws {
		names[i] = vg.VirtualGatewayName
	}
	sort.Strings(names)
	items, next := paginateStrings(names, nextToken, maxResults)
	summaries := make([]*VirtualGatewaySummary, 0, len(items))
	for _, n := range items {
		vg, _ := b.virtualGWs.Get(meshChildKey(meshName, n))
		summaries = append(summaries, &VirtualGatewaySummary{
			CreatedAt:          vg.Meta.CreatedAt,
			UpdatedAt:          vg.Meta.UpdatedAt,
			Arn:                vg.Meta.Arn,
			MeshName:           meshName,
			VirtualGatewayName: vg.VirtualGatewayName,
			MeshOwner:          vg.Meta.MeshOwner,
			ResourceOwner:      vg.Meta.ResourceOwner,
			Version:            vg.Meta.Version,
		})
	}

	return summaries, next, nil
}

// ─── GatewayRoute ───

func (b *InMemoryBackend) CreateGatewayRoute(
	meshName, virtualGatewayName, routeName string, spec json.RawMessage, tags map[string]string,
) (*GatewayRoute, error) {
	b.mu.Lock("CreateGatewayRoute")
	defer b.mu.Unlock()
	if !b.meshes.Has(meshName) {
		return nil, ErrMeshNotFound
	}
	if !b.virtualGWs.Has(meshChildKey(meshName, virtualGatewayName)) {
		return nil, ErrVirtualGatewayNotFound
	}
	key := gatewayRouteCompositeKey(meshName, virtualGatewayName, routeName)
	if b.gatewayRoutes.Has(key) {
		return nil, ErrGatewayRouteAlreadyExists
	}
	arn := b.gatewayRouteARN(meshName, virtualGatewayName, routeName)
	gr := &GatewayRoute{
		Meta:               newMeta(arn, b.accountID),
		MeshName:           meshName,
		VirtualGatewayName: virtualGatewayName,
		GatewayRouteName:   routeName,
		Spec:               normalizeSpec(spec),
		Status:             statusActive,
	}
	b.gatewayRoutes.Put(gr)
	if len(tags) > 0 {
		b.tags[arn] = cloneTags(tags)
	}

	return gr, nil
}

func (b *InMemoryBackend) DescribeGatewayRoute(meshName, virtualGatewayName, routeName string) (*GatewayRoute, error) {
	b.mu.RLock("DescribeGatewayRoute")
	defer b.mu.RUnlock()
	if !b.meshes.Has(meshName) {
		return nil, ErrMeshNotFound
	}
	if !b.virtualGWs.Has(meshChildKey(meshName, virtualGatewayName)) {
		return nil, ErrVirtualGatewayNotFound
	}
	gr, ok := b.gatewayRoutes.Get(gatewayRouteCompositeKey(meshName, virtualGatewayName, routeName))
	if !ok {
		return nil, ErrGatewayRouteNotFound
	}

	return gr, nil
}

func (b *InMemoryBackend) UpdateGatewayRoute(
	meshName, virtualGatewayName, routeName string, spec json.RawMessage,
) (*GatewayRoute, error) {
	b.mu.Lock("UpdateGatewayRoute")
	defer b.mu.Unlock()
	if !b.meshes.Has(meshName) {
		return nil, ErrMeshNotFound
	}
	if !b.virtualGWs.Has(meshChildKey(meshName, virtualGatewayName)) {
		return nil, ErrVirtualGatewayNotFound
	}
	key := gatewayRouteCompositeKey(meshName, virtualGatewayName, routeName)
	gr, ok := b.gatewayRoutes.Get(key)
	if !ok {
		return nil, ErrGatewayRouteNotFound
	}
	gr.Spec = normalizeSpec(spec)
	gr.Meta.UpdatedAt = time.Now().UTC()
	gr.Meta.Version++

	return gr, nil
}

func (b *InMemoryBackend) DeleteGatewayRoute(meshName, virtualGatewayName, routeName string) (*GatewayRoute, error) {
	b.mu.Lock("DeleteGatewayRoute")
	defer b.mu.Unlock()
	if !b.meshes.Has(meshName) {
		return nil, ErrMeshNotFound
	}
	if !b.virtualGWs.Has(meshChildKey(meshName, virtualGatewayName)) {
		return nil, ErrVirtualGatewayNotFound
	}
	key := gatewayRouteCompositeKey(meshName, virtualGatewayName, routeName)
	gr, ok := b.gatewayRoutes.Get(key)
	if !ok {
		return nil, ErrGatewayRouteNotFound
	}
	b.gatewayRoutes.Delete(key)
	delete(b.tags, gr.Meta.Arn)

	return gr, nil
}

func (b *InMemoryBackend) ListGatewayRoutes(
	meshName, virtualGatewayName string, maxResults int32, nextToken string,
) ([]*GatewayRouteSummary, string, error) {
	b.mu.RLock("ListGatewayRoutes")
	defer b.mu.RUnlock()
	if !b.meshes.Has(meshName) {
		return nil, "", ErrMeshNotFound
	}
	if !b.virtualGWs.Has(meshChildKey(meshName, virtualGatewayName)) {
		return nil, "", ErrVirtualGatewayNotFound
	}
	routes := b.gatewayRoutesByGateway.Get(meshChildKey(meshName, virtualGatewayName))
	names := make([]string, len(routes))
	for i, gr := range routes {
		names[i] = gr.GatewayRouteName
	}
	sort.Strings(names)
	items, next := paginateStrings(names, nextToken, maxResults)
	summaries := make([]*GatewayRouteSummary, 0, len(items))
	for _, n := range items {
		gr, _ := b.gatewayRoutes.Get(gatewayRouteCompositeKey(meshName, virtualGatewayName, n))
		summaries = append(summaries, &GatewayRouteSummary{
			CreatedAt:          gr.Meta.CreatedAt,
			UpdatedAt:          gr.Meta.UpdatedAt,
			Arn:                gr.Meta.Arn,
			MeshName:           meshName,
			VirtualGatewayName: virtualGatewayName,
			GatewayRouteName:   gr.GatewayRouteName,
			MeshOwner:          gr.Meta.MeshOwner,
			ResourceOwner:      gr.Meta.ResourceOwner,
			Version:            gr.Meta.Version,
		})
	}

	return summaries, next, nil
}
