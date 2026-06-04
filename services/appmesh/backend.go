package appmesh

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrMeshNotFound is returned when a mesh does not exist.
	ErrMeshNotFound = awserr.New("mesh not found", awserr.ErrNotFound)
	// ErrMeshAlreadyExists is returned when a mesh already exists.
	ErrMeshAlreadyExists = awserr.New("mesh already exists", awserr.ErrAlreadyExists)
	// ErrMeshInUse is returned when a mesh has resources and cannot be deleted.
	ErrMeshInUse = awserr.New("mesh is in use", awserr.ErrConflict)
	// ErrVirtualNodeNotFound is returned when a virtual node does not exist.
	ErrVirtualNodeNotFound = awserr.New("virtual node not found", awserr.ErrNotFound)
	// ErrVirtualNodeAlreadyExists is returned when a virtual node already exists.
	ErrVirtualNodeAlreadyExists = awserr.New("virtual node already exists", awserr.ErrAlreadyExists)
	// ErrVirtualRouterNotFound is returned when a virtual router does not exist.
	ErrVirtualRouterNotFound = awserr.New("virtual router not found", awserr.ErrNotFound)
	// ErrVirtualRouterAlreadyExists is returned when a virtual router already exists.
	ErrVirtualRouterAlreadyExists = awserr.New("virtual router already exists", awserr.ErrAlreadyExists)
	// ErrVirtualRouterInUse is returned when a virtual router has routes.
	ErrVirtualRouterInUse = awserr.New("virtual router has routes", awserr.ErrConflict)
	// ErrRouteNotFound is returned when a route does not exist.
	ErrRouteNotFound = awserr.New("route not found", awserr.ErrNotFound)
	// ErrRouteAlreadyExists is returned when a route already exists.
	ErrRouteAlreadyExists = awserr.New("route already exists", awserr.ErrAlreadyExists)
	// ErrVirtualServiceNotFound is returned when a virtual service does not exist.
	ErrVirtualServiceNotFound = awserr.New("virtual service not found", awserr.ErrNotFound)
	// ErrVirtualServiceAlreadyExists is returned when a virtual service already exists.
	ErrVirtualServiceAlreadyExists = awserr.New("virtual service already exists", awserr.ErrAlreadyExists)
	// ErrVirtualGatewayNotFound is returned when a virtual gateway does not exist.
	ErrVirtualGatewayNotFound = awserr.New("virtual gateway not found", awserr.ErrNotFound)
	// ErrVirtualGatewayAlreadyExists is returned when a virtual gateway already exists.
	ErrVirtualGatewayAlreadyExists = awserr.New("virtual gateway already exists", awserr.ErrAlreadyExists)
	// ErrVirtualGatewayInUse is returned when a virtual gateway has gateway routes.
	ErrVirtualGatewayInUse = awserr.New("virtual gateway has gateway routes", awserr.ErrConflict)
	// ErrGatewayRouteNotFound is returned when a gateway route does not exist.
	ErrGatewayRouteNotFound = awserr.New("gateway route not found", awserr.ErrNotFound)
	// ErrGatewayRouteAlreadyExists is returned when a gateway route already exists.
	ErrGatewayRouteAlreadyExists = awserr.New("gateway route already exists", awserr.ErrAlreadyExists)
	// ErrResourceNotFound is returned when a tagged resource does not exist.
	ErrResourceNotFound = awserr.New("resource not found for tagging", awserr.ErrNotFound)
)

// routeKey is a composite key for routes (mesh + virtualRouter + route).
type routeKey struct {
	meshName          string
	virtualRouterName string
	routeName         string
}

// gatewayRouteKey is a composite key for gateway routes.
type gatewayRouteKey struct {
	meshName           string
	virtualGatewayName string
	gatewayRouteName   string
}

// InMemoryBackend is a thread-safe in-memory App Mesh backend.
type InMemoryBackend struct {
	mu             sync.RWMutex
	accountID      string
	region         string
	meshes         map[string]*Mesh
	virtualNodes   map[string]map[string]*VirtualNode  // meshName → vnName → vn
	virtualRouters map[string]map[string]*VirtualRouter
	routes         map[routeKey]*Route
	virtualSvcs    map[string]map[string]*VirtualService
	virtualGWs     map[string]map[string]*VirtualGateway
	gatewayRoutes  map[gatewayRouteKey]*GatewayRoute
	tags           map[string]map[string]string // arn → tags
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		accountID:      accountID,
		region:         region,
		meshes:         make(map[string]*Mesh),
		virtualNodes:   make(map[string]map[string]*VirtualNode),
		virtualRouters: make(map[string]map[string]*VirtualRouter),
		routes:         make(map[routeKey]*Route),
		virtualSvcs:    make(map[string]map[string]*VirtualService),
		virtualGWs:     make(map[string]map[string]*VirtualGateway),
		gatewayRoutes:  make(map[gatewayRouteKey]*GatewayRoute),
		tags:           make(map[string]map[string]string),
	}
}

func (b *InMemoryBackend) AccountID() string { return b.accountID }
func (b *InMemoryBackend) Region() string    { return b.region }

func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.meshes = make(map[string]*Mesh)
	b.virtualNodes = make(map[string]map[string]*VirtualNode)
	b.virtualRouters = make(map[string]map[string]*VirtualRouter)
	b.routes = make(map[routeKey]*Route)
	b.virtualSvcs = make(map[string]map[string]*VirtualService)
	b.virtualGWs = make(map[string]map[string]*VirtualGateway)
	b.gatewayRoutes = make(map[gatewayRouteKey]*GatewayRoute)
	b.tags = make(map[string]map[string]string)
}

func (b *InMemoryBackend) meshARN(meshName string) string {
	return fmt.Sprintf("arn:aws:appmesh:%s:%s:mesh/%s", b.region, b.accountID, meshName)
}

func (b *InMemoryBackend) virtualNodeARN(meshName, name string) string {
	return fmt.Sprintf("arn:aws:appmesh:%s:%s:mesh/%s/virtualNode/%s", b.region, b.accountID, meshName, name)
}

func (b *InMemoryBackend) virtualRouterARN(meshName, name string) string {
	return fmt.Sprintf("arn:aws:appmesh:%s:%s:mesh/%s/virtualRouter/%s", b.region, b.accountID, meshName, name)
}

func (b *InMemoryBackend) routeARN(meshName, vrName, routeName string) string {
	return fmt.Sprintf(
		"arn:aws:appmesh:%s:%s:mesh/%s/virtualRouter/%s/route/%s",
		b.region, b.accountID, meshName, vrName, routeName,
	)
}

func (b *InMemoryBackend) virtualServiceARN(meshName, name string) string {
	return fmt.Sprintf("arn:aws:appmesh:%s:%s:mesh/%s/virtualService/%s", b.region, b.accountID, meshName, name)
}

func (b *InMemoryBackend) virtualGatewayARN(meshName, name string) string {
	return fmt.Sprintf("arn:aws:appmesh:%s:%s:mesh/%s/virtualGateway/%s", b.region, b.accountID, meshName, name)
}

func (b *InMemoryBackend) gatewayRouteARN(meshName, vgName, routeName string) string {
	return fmt.Sprintf(
		"arn:aws:appmesh:%s:%s:mesh/%s/virtualGateway/%s/gatewayRoute/%s",
		b.region, b.accountID, meshName, vgName, routeName,
	)
}

func newUID() string {
	return fmt.Sprintf("%016x", time.Now().UnixNano())
}

func newMeta(arn, accountID string) ResourceMeta {
	now := time.Now().UTC()
	return ResourceMeta{
		CreatedAt:     now,
		UpdatedAt:     now,
		Arn:           arn,
		UID:           newUID(),
		MeshOwner:     accountID,
		ResourceOwner: accountID,
		Version:       1,
	}
}

// normalizeSpec returns a non-nil JSON object if spec is nil or empty.
func normalizeSpec(spec json.RawMessage) json.RawMessage {
	if len(spec) == 0 {
		return json.RawMessage(`{}`)
	}
	return spec
}

// ─── Mesh ───────────────────────────────────────────────────────────────────

func (b *InMemoryBackend) CreateMesh(name string, spec json.RawMessage, tags map[string]string) (*Mesh, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.meshes[name]; ok {
		return nil, ErrMeshAlreadyExists
	}
	arn := b.meshARN(name)
	m := &Mesh{
		Meta:   newMeta(arn, b.accountID),
		Name:   name,
		Spec:   normalizeSpec(spec),
		Status: "ACTIVE",
	}
	b.meshes[name] = m
	if len(tags) > 0 {
		b.tags[arn] = cloneTags(tags)
	}
	return m, nil
}

func (b *InMemoryBackend) DescribeMesh(name string) (*Mesh, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	m, ok := b.meshes[name]
	if !ok {
		return nil, ErrMeshNotFound
	}
	return m, nil
}

func (b *InMemoryBackend) UpdateMesh(name string, spec json.RawMessage) (*Mesh, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	m, ok := b.meshes[name]
	if !ok {
		return nil, ErrMeshNotFound
	}
	m.Spec = normalizeSpec(spec)
	m.Meta.UpdatedAt = time.Now().UTC()
	m.Meta.Version++
	return m, nil
}

func (b *InMemoryBackend) DeleteMesh(name string) (*Mesh, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	m, ok := b.meshes[name]
	if !ok {
		return nil, ErrMeshNotFound
	}
	if len(b.virtualNodes[name]) > 0 || len(b.virtualRouters[name]) > 0 ||
		len(b.virtualSvcs[name]) > 0 || len(b.virtualGWs[name]) > 0 {
		return nil, ErrMeshInUse
	}
	delete(b.meshes, name)
	delete(b.tags, m.Meta.Arn)
	return m, nil
}

func (b *InMemoryBackend) ListMeshes(maxResults int32, nextToken string) ([]*MeshSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	names := sortedKeys(b.meshes)
	items, next := paginateStrings(names, nextToken, maxResults)
	summaries := make([]*MeshSummary, 0, len(items))
	for _, n := range items {
		m := b.meshes[n]
		summaries = append(summaries, &MeshSummary{
			CreatedAt:     m.Meta.CreatedAt,
			UpdatedAt:     m.Meta.UpdatedAt,
			Arn:           m.Meta.Arn,
			Name:          m.Name,
			MeshOwner:     m.Meta.MeshOwner,
			ResourceOwner: m.Meta.ResourceOwner,
			Version:       m.Meta.Version,
		})
	}
	return summaries, next, nil
}

// ─── VirtualNode ─────────────────────────────────────────────────────────────

func (b *InMemoryBackend) CreateVirtualNode(
	meshName, name string, spec json.RawMessage, tags map[string]string,
) (*VirtualNode, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	if b.virtualNodes[meshName] == nil {
		b.virtualNodes[meshName] = make(map[string]*VirtualNode)
	}
	if _, ok := b.virtualNodes[meshName][name]; ok {
		return nil, ErrVirtualNodeAlreadyExists
	}
	arn := b.virtualNodeARN(meshName, name)
	vn := &VirtualNode{
		Meta:            newMeta(arn, b.accountID),
		MeshName:        meshName,
		VirtualNodeName: name,
		Spec:            normalizeSpec(spec),
		Status:          "ACTIVE",
	}
	b.virtualNodes[meshName][name] = vn
	if len(tags) > 0 {
		b.tags[arn] = cloneTags(tags)
	}
	return vn, nil
}

func (b *InMemoryBackend) DescribeVirtualNode(meshName, name string) (*VirtualNode, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	vn, ok := b.virtualNodes[meshName][name]
	if !ok {
		return nil, ErrVirtualNodeNotFound
	}
	return vn, nil
}

func (b *InMemoryBackend) UpdateVirtualNode(meshName, name string, spec json.RawMessage) (*VirtualNode, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	vn, ok := b.virtualNodes[meshName][name]
	if !ok {
		return nil, ErrVirtualNodeNotFound
	}
	vn.Spec = normalizeSpec(spec)
	vn.Meta.UpdatedAt = time.Now().UTC()
	vn.Meta.Version++
	return vn, nil
}

func (b *InMemoryBackend) DeleteVirtualNode(meshName, name string) (*VirtualNode, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	vn, ok := b.virtualNodes[meshName][name]
	if !ok {
		return nil, ErrVirtualNodeNotFound
	}
	delete(b.virtualNodes[meshName], name)
	delete(b.tags, vn.Meta.Arn)
	return vn, nil
}

func (b *InMemoryBackend) ListVirtualNodes(
	meshName string, maxResults int32, nextToken string,
) ([]*VirtualNodeSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, "", ErrMeshNotFound
	}
	nodes := b.virtualNodes[meshName]
	names := sortedKeys(nodes)
	items, next := paginateStrings(names, nextToken, maxResults)
	summaries := make([]*VirtualNodeSummary, 0, len(items))
	for _, n := range items {
		vn := nodes[n]
		summaries = append(summaries, &VirtualNodeSummary{
			CreatedAt:       vn.Meta.CreatedAt,
			UpdatedAt:       vn.Meta.UpdatedAt,
			Arn:             vn.Meta.Arn,
			MeshName:        meshName,
			VirtualNodeName: vn.VirtualNodeName,
			MeshOwner:       vn.Meta.MeshOwner,
			ResourceOwner:   vn.Meta.ResourceOwner,
			Version:         vn.Meta.Version,
		})
	}
	return summaries, next, nil
}

// ─── VirtualRouter ───────────────────────────────────────────────────────────

func (b *InMemoryBackend) CreateVirtualRouter(
	meshName, name string, spec json.RawMessage, tags map[string]string,
) (*VirtualRouter, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	if b.virtualRouters[meshName] == nil {
		b.virtualRouters[meshName] = make(map[string]*VirtualRouter)
	}
	if _, ok := b.virtualRouters[meshName][name]; ok {
		return nil, ErrVirtualRouterAlreadyExists
	}
	arn := b.virtualRouterARN(meshName, name)
	vr := &VirtualRouter{
		Meta:              newMeta(arn, b.accountID),
		MeshName:          meshName,
		VirtualRouterName: name,
		Spec:              normalizeSpec(spec),
		Status:            "ACTIVE",
	}
	b.virtualRouters[meshName][name] = vr
	if len(tags) > 0 {
		b.tags[arn] = cloneTags(tags)
	}
	return vr, nil
}

func (b *InMemoryBackend) DescribeVirtualRouter(meshName, name string) (*VirtualRouter, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	vr, ok := b.virtualRouters[meshName][name]
	if !ok {
		return nil, ErrVirtualRouterNotFound
	}
	return vr, nil
}

func (b *InMemoryBackend) UpdateVirtualRouter(meshName, name string, spec json.RawMessage) (*VirtualRouter, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	vr, ok := b.virtualRouters[meshName][name]
	if !ok {
		return nil, ErrVirtualRouterNotFound
	}
	vr.Spec = normalizeSpec(spec)
	vr.Meta.UpdatedAt = time.Now().UTC()
	vr.Meta.Version++
	return vr, nil
}

func (b *InMemoryBackend) DeleteVirtualRouter(meshName, name string) (*VirtualRouter, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	vr, ok := b.virtualRouters[meshName][name]
	if !ok {
		return nil, ErrVirtualRouterNotFound
	}
	for k := range b.routes {
		if k.meshName == meshName && k.virtualRouterName == name {
			return nil, ErrVirtualRouterInUse
		}
	}
	delete(b.virtualRouters[meshName], name)
	delete(b.tags, vr.Meta.Arn)
	return vr, nil
}

func (b *InMemoryBackend) ListVirtualRouters(
	meshName string, maxResults int32, nextToken string,
) ([]*VirtualRouterSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, "", ErrMeshNotFound
	}
	routers := b.virtualRouters[meshName]
	names := sortedKeys(routers)
	items, next := paginateStrings(names, nextToken, maxResults)
	summaries := make([]*VirtualRouterSummary, 0, len(items))
	for _, n := range items {
		vr := routers[n]
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

// ─── Route ───────────────────────────────────────────────────────────────────

func (b *InMemoryBackend) CreateRoute(
	meshName, virtualRouterName, routeName string, spec json.RawMessage, tags map[string]string,
) (*Route, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	if _, ok := b.virtualRouters[meshName][virtualRouterName]; !ok {
		return nil, ErrVirtualRouterNotFound
	}
	k := routeKey{meshName, virtualRouterName, routeName}
	if _, ok := b.routes[k]; ok {
		return nil, ErrRouteAlreadyExists
	}
	arn := b.routeARN(meshName, virtualRouterName, routeName)
	r := &Route{
		Meta:              newMeta(arn, b.accountID),
		MeshName:          meshName,
		VirtualRouterName: virtualRouterName,
		RouteName:         routeName,
		Spec:              normalizeSpec(spec),
		Status:            "ACTIVE",
	}
	b.routes[k] = r
	if len(tags) > 0 {
		b.tags[arn] = cloneTags(tags)
	}
	return r, nil
}

func (b *InMemoryBackend) DescribeRoute(meshName, virtualRouterName, routeName string) (*Route, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	if _, ok := b.virtualRouters[meshName][virtualRouterName]; !ok {
		return nil, ErrVirtualRouterNotFound
	}
	r, ok := b.routes[routeKey{meshName, virtualRouterName, routeName}]
	if !ok {
		return nil, ErrRouteNotFound
	}
	return r, nil
}

func (b *InMemoryBackend) UpdateRoute(meshName, virtualRouterName, routeName string, spec json.RawMessage) (*Route, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	if _, ok := b.virtualRouters[meshName][virtualRouterName]; !ok {
		return nil, ErrVirtualRouterNotFound
	}
	k := routeKey{meshName, virtualRouterName, routeName}
	r, ok := b.routes[k]
	if !ok {
		return nil, ErrRouteNotFound
	}
	r.Spec = normalizeSpec(spec)
	r.Meta.UpdatedAt = time.Now().UTC()
	r.Meta.Version++
	return r, nil
}

func (b *InMemoryBackend) DeleteRoute(meshName, virtualRouterName, routeName string) (*Route, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	if _, ok := b.virtualRouters[meshName][virtualRouterName]; !ok {
		return nil, ErrVirtualRouterNotFound
	}
	k := routeKey{meshName, virtualRouterName, routeName}
	r, ok := b.routes[k]
	if !ok {
		return nil, ErrRouteNotFound
	}
	delete(b.routes, k)
	delete(b.tags, r.Meta.Arn)
	return r, nil
}

func (b *InMemoryBackend) ListRoutes(
	meshName, virtualRouterName string, maxResults int32, nextToken string,
) ([]*RouteSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, "", ErrMeshNotFound
	}
	if _, ok := b.virtualRouters[meshName][virtualRouterName]; !ok {
		return nil, "", ErrVirtualRouterNotFound
	}
	var names []string
	for k := range b.routes {
		if k.meshName == meshName && k.virtualRouterName == virtualRouterName {
			names = append(names, k.routeName)
		}
	}
	sort.Strings(names)
	items, next := paginateStrings(names, nextToken, maxResults)
	summaries := make([]*RouteSummary, 0, len(items))
	for _, n := range items {
		r := b.routes[routeKey{meshName, virtualRouterName, n}]
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

// ─── VirtualService ──────────────────────────────────────────────────────────

func (b *InMemoryBackend) CreateVirtualService(
	meshName, name string, spec json.RawMessage, tags map[string]string,
) (*VirtualService, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	if b.virtualSvcs[meshName] == nil {
		b.virtualSvcs[meshName] = make(map[string]*VirtualService)
	}
	if _, ok := b.virtualSvcs[meshName][name]; ok {
		return nil, ErrVirtualServiceAlreadyExists
	}
	arn := b.virtualServiceARN(meshName, name)
	vs := &VirtualService{
		Meta:               newMeta(arn, b.accountID),
		MeshName:           meshName,
		VirtualServiceName: name,
		Spec:               normalizeSpec(spec),
		Status:             "ACTIVE",
	}
	b.virtualSvcs[meshName][name] = vs
	if len(tags) > 0 {
		b.tags[arn] = cloneTags(tags)
	}
	return vs, nil
}

func (b *InMemoryBackend) DescribeVirtualService(meshName, name string) (*VirtualService, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	vs, ok := b.virtualSvcs[meshName][name]
	if !ok {
		return nil, ErrVirtualServiceNotFound
	}
	return vs, nil
}

func (b *InMemoryBackend) UpdateVirtualService(meshName, name string, spec json.RawMessage) (*VirtualService, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	vs, ok := b.virtualSvcs[meshName][name]
	if !ok {
		return nil, ErrVirtualServiceNotFound
	}
	vs.Spec = normalizeSpec(spec)
	vs.Meta.UpdatedAt = time.Now().UTC()
	vs.Meta.Version++
	return vs, nil
}

func (b *InMemoryBackend) DeleteVirtualService(meshName, name string) (*VirtualService, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	vs, ok := b.virtualSvcs[meshName][name]
	if !ok {
		return nil, ErrVirtualServiceNotFound
	}
	delete(b.virtualSvcs[meshName], name)
	delete(b.tags, vs.Meta.Arn)
	return vs, nil
}

func (b *InMemoryBackend) ListVirtualServices(
	meshName string, maxResults int32, nextToken string,
) ([]*VirtualServiceSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, "", ErrMeshNotFound
	}
	svcs := b.virtualSvcs[meshName]
	names := sortedKeys(svcs)
	items, next := paginateStrings(names, nextToken, maxResults)
	summaries := make([]*VirtualServiceSummary, 0, len(items))
	for _, n := range items {
		vs := svcs[n]
		summaries = append(summaries, &VirtualServiceSummary{
			CreatedAt:          vs.Meta.CreatedAt,
			UpdatedAt:          vs.Meta.UpdatedAt,
			Arn:                vs.Meta.Arn,
			MeshName:           meshName,
			VirtualServiceName: vs.VirtualServiceName,
			MeshOwner:          vs.Meta.MeshOwner,
			ResourceOwner:      vs.Meta.ResourceOwner,
			Version:            vs.Meta.Version,
		})
	}
	return summaries, next, nil
}

// ─── VirtualGateway ──────────────────────────────────────────────────────────

func (b *InMemoryBackend) CreateVirtualGateway(
	meshName, name string, spec json.RawMessage, tags map[string]string,
) (*VirtualGateway, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	if b.virtualGWs[meshName] == nil {
		b.virtualGWs[meshName] = make(map[string]*VirtualGateway)
	}
	if _, ok := b.virtualGWs[meshName][name]; ok {
		return nil, ErrVirtualGatewayAlreadyExists
	}
	arn := b.virtualGatewayARN(meshName, name)
	vg := &VirtualGateway{
		Meta:               newMeta(arn, b.accountID),
		MeshName:           meshName,
		VirtualGatewayName: name,
		Spec:               normalizeSpec(spec),
		Status:             "ACTIVE",
	}
	b.virtualGWs[meshName][name] = vg
	if len(tags) > 0 {
		b.tags[arn] = cloneTags(tags)
	}
	return vg, nil
}

func (b *InMemoryBackend) DescribeVirtualGateway(meshName, name string) (*VirtualGateway, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	vg, ok := b.virtualGWs[meshName][name]
	if !ok {
		return nil, ErrVirtualGatewayNotFound
	}
	return vg, nil
}

func (b *InMemoryBackend) UpdateVirtualGateway(meshName, name string, spec json.RawMessage) (*VirtualGateway, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	vg, ok := b.virtualGWs[meshName][name]
	if !ok {
		return nil, ErrVirtualGatewayNotFound
	}
	vg.Spec = normalizeSpec(spec)
	vg.Meta.UpdatedAt = time.Now().UTC()
	vg.Meta.Version++
	return vg, nil
}

func (b *InMemoryBackend) DeleteVirtualGateway(meshName, name string) (*VirtualGateway, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	vg, ok := b.virtualGWs[meshName][name]
	if !ok {
		return nil, ErrVirtualGatewayNotFound
	}
	for k := range b.gatewayRoutes {
		if k.meshName == meshName && k.virtualGatewayName == name {
			return nil, ErrVirtualGatewayInUse
		}
	}
	delete(b.virtualGWs[meshName], name)
	delete(b.tags, vg.Meta.Arn)
	return vg, nil
}

func (b *InMemoryBackend) ListVirtualGateways(
	meshName string, maxResults int32, nextToken string,
) ([]*VirtualGatewaySummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, "", ErrMeshNotFound
	}
	gws := b.virtualGWs[meshName]
	names := sortedKeys(gws)
	items, next := paginateStrings(names, nextToken, maxResults)
	summaries := make([]*VirtualGatewaySummary, 0, len(items))
	for _, n := range items {
		vg := gws[n]
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

// ─── GatewayRoute ────────────────────────────────────────────────────────────

func (b *InMemoryBackend) CreateGatewayRoute(
	meshName, virtualGatewayName, routeName string, spec json.RawMessage, tags map[string]string,
) (*GatewayRoute, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	if _, ok := b.virtualGWs[meshName][virtualGatewayName]; !ok {
		return nil, ErrVirtualGatewayNotFound
	}
	k := gatewayRouteKey{meshName, virtualGatewayName, routeName}
	if _, ok := b.gatewayRoutes[k]; ok {
		return nil, ErrGatewayRouteAlreadyExists
	}
	arn := b.gatewayRouteARN(meshName, virtualGatewayName, routeName)
	gr := &GatewayRoute{
		Meta:               newMeta(arn, b.accountID),
		MeshName:           meshName,
		VirtualGatewayName: virtualGatewayName,
		GatewayRouteName:   routeName,
		Spec:               normalizeSpec(spec),
		Status:             "ACTIVE",
	}
	b.gatewayRoutes[k] = gr
	if len(tags) > 0 {
		b.tags[arn] = cloneTags(tags)
	}
	return gr, nil
}

func (b *InMemoryBackend) DescribeGatewayRoute(meshName, virtualGatewayName, routeName string) (*GatewayRoute, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	if _, ok := b.virtualGWs[meshName][virtualGatewayName]; !ok {
		return nil, ErrVirtualGatewayNotFound
	}
	gr, ok := b.gatewayRoutes[gatewayRouteKey{meshName, virtualGatewayName, routeName}]
	if !ok {
		return nil, ErrGatewayRouteNotFound
	}
	return gr, nil
}

func (b *InMemoryBackend) UpdateGatewayRoute(
	meshName, virtualGatewayName, routeName string, spec json.RawMessage,
) (*GatewayRoute, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	if _, ok := b.virtualGWs[meshName][virtualGatewayName]; !ok {
		return nil, ErrVirtualGatewayNotFound
	}
	k := gatewayRouteKey{meshName, virtualGatewayName, routeName}
	gr, ok := b.gatewayRoutes[k]
	if !ok {
		return nil, ErrGatewayRouteNotFound
	}
	gr.Spec = normalizeSpec(spec)
	gr.Meta.UpdatedAt = time.Now().UTC()
	gr.Meta.Version++
	return gr, nil
}

func (b *InMemoryBackend) DeleteGatewayRoute(meshName, virtualGatewayName, routeName string) (*GatewayRoute, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, ErrMeshNotFound
	}
	if _, ok := b.virtualGWs[meshName][virtualGatewayName]; !ok {
		return nil, ErrVirtualGatewayNotFound
	}
	k := gatewayRouteKey{meshName, virtualGatewayName, routeName}
	gr, ok := b.gatewayRoutes[k]
	if !ok {
		return nil, ErrGatewayRouteNotFound
	}
	delete(b.gatewayRoutes, k)
	delete(b.tags, gr.Meta.Arn)
	return gr, nil
}

func (b *InMemoryBackend) ListGatewayRoutes(
	meshName, virtualGatewayName string, maxResults int32, nextToken string,
) ([]*GatewayRouteSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if _, ok := b.meshes[meshName]; !ok {
		return nil, "", ErrMeshNotFound
	}
	if _, ok := b.virtualGWs[meshName][virtualGatewayName]; !ok {
		return nil, "", ErrVirtualGatewayNotFound
	}
	var names []string
	for k := range b.gatewayRoutes {
		if k.meshName == meshName && k.virtualGatewayName == virtualGatewayName {
			names = append(names, k.gatewayRouteName)
		}
	}
	sort.Strings(names)
	items, next := paginateStrings(names, nextToken, maxResults)
	summaries := make([]*GatewayRouteSummary, 0, len(items))
	for _, n := range items {
		gr := b.gatewayRoutes[gatewayRouteKey{meshName, virtualGatewayName, n}]
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

// ─── Tags ─────────────────────────────────────────────────────────────────────

func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.arnExists(arn) {
		return ErrResourceNotFound
	}
	if b.tags[arn] == nil {
		b.tags[arn] = make(map[string]string)
	}
	for k, v := range tags {
		b.tags[arn][k] = v
	}
	return nil
}

func (b *InMemoryBackend) UntagResource(arn string, keys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.arnExists(arn) {
		return ErrResourceNotFound
	}
	for _, k := range keys {
		delete(b.tags[arn], k)
	}
	return nil
}

func (b *InMemoryBackend) ListTagsForResource(arn string, maxResults int32, nextToken string) ([]TagRef, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if !b.arnExists(arn) {
		return nil, "", ErrResourceNotFound
	}
	tagMap := b.tags[arn]
	keys := make([]string, 0, len(tagMap))
	for k := range tagMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	items, next := paginateStrings(keys, nextToken, maxResults)
	refs := make([]TagRef, 0, len(items))
	for _, k := range items {
		refs = append(refs, TagRef{Key: k, Value: tagMap[k]})
	}
	return refs, next, nil
}

// arnExists checks whether the given ARN belongs to any known resource.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) arnExists(arn string) bool {
	for _, m := range b.meshes {
		if m.Meta.Arn == arn {
			return true
		}
	}
	for _, vnMap := range b.virtualNodes {
		for _, vn := range vnMap {
			if vn.Meta.Arn == arn {
				return true
			}
		}
	}
	for _, vrMap := range b.virtualRouters {
		for _, vr := range vrMap {
			if vr.Meta.Arn == arn {
				return true
			}
		}
	}
	for _, r := range b.routes {
		if r.Meta.Arn == arn {
			return true
		}
	}
	for _, vsMap := range b.virtualSvcs {
		for _, vs := range vsMap {
			if vs.Meta.Arn == arn {
				return true
			}
		}
	}
	for _, vgMap := range b.virtualGWs {
		for _, vg := range vgMap {
			if vg.Meta.Arn == arn {
				return true
			}
		}
	}
	for _, gr := range b.gatewayRoutes {
		if gr.Meta.Arn == arn {
			return true
		}
	}
	return false
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func cloneTags(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// sortedKeys returns a sorted slice of keys from any map[string]V.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// paginateStrings returns a page of items starting after nextToken and a new nextToken.
func paginateStrings(sorted []string, nextToken string, maxResults int32) ([]string, string) {
	start := 0
	if nextToken != "" {
		for i, s := range sorted {
			if strings.Compare(s, nextToken) > 0 {
				start = i
				break
			}
		}
		if start == 0 && (len(sorted) == 0 || strings.Compare(sorted[0], nextToken) <= 0) {
			return nil, ""
		}
	}
	items := sorted[start:]
	if maxResults <= 0 || int(maxResults) >= len(items) {
		return items, ""
	}
	page := items[:maxResults]
	return page, page[len(page)-1]
}

// ErrIs checks whether err wraps sentinel.
func ErrIs(err, sentinel error) bool {
	return errors.Is(err, sentinel)
}
