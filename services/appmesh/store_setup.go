package appmesh

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T / map[string]map[string]*T / map[compositeKey]*T resource
// field on InMemoryBackend is replaced with a *store.Table[T] (see
// pkgs/store's package doc for the underlying primitive). App Mesh follows
// the services/sesv2 / services/dax / services/vpclattice "clean direct +
// byParent Index" template throughout -- no DTO indirection is needed
// anywhere in this service, because every parent-identifying component a
// composite key needs (MeshName, VirtualRouterName, VirtualGatewayName) was
// already a real, wire-visible field on the value type before this
// conversion (App Mesh resources embed their own parent name so responses
// can echo it back), unlike services where the parent id had to be added as
// a new hidden field.
//
// meshes is flat with its own globally unique identity (Mesh.Name), so it
// registers directly with no composite key and no secondary index.
//
// virtualNodes, virtualRouters, virtualSvcs, and virtualGWs were previously
// nested by mesh (map[meshName]map[childName]*T). Each becomes a flat
// *store.Table[T] keyed by the composite "meshName|name" string (see
// meshChildKey below), with a companion "byMesh" *store.Index grouping
// entries by mesh for the per-mesh list scans (and, for
// DeleteMesh/DeleteVirtualRouter/DeleteVirtualGateway, the in-use checks)
// the nested maps used to answer directly.
//
// routes and gatewayRoutes were previously keyed by a composite struct one
// level deeper (mesh + virtual router/gateway + route). Each becomes a flat
// *store.Table[T] keyed by a three-part composite string (see
// routeCompositeKey / gatewayRouteCompositeKey below), with a companion
// "byRouter"/"byGateway" *store.Index grouping entries by their two-part
// parent key ("meshName|virtualRouterName" / "meshName|virtualGatewayName")
// for ListRoutes/ListGatewayRoutes and the DeleteVirtualRouter/
// DeleteVirtualGateway in-use checks.
//
// tags (map[string]map[string]string, keyed by ARN rather than by a *T with
// its own identity) does not fit store.Table's keyed shape and is left as a
// plain map -- see persistence.go for how it round-trips through
// Snapshot/Restore alongside the tables below.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// meshChildKey derives the composite primary key for a resource nested
// directly under a mesh (virtual node, virtual router, virtual service,
// virtual gateway); it doubles as the byMesh Index key when called with just
// the mesh name.
func meshChildKey(meshName, name string) string { return meshName + "|" + name }

// routeCompositeKey derives the composite primary key for a route, nested
// under a virtual router within a mesh.
func routeCompositeKey(meshName, virtualRouterName, routeName string) string {
	return meshChildKey(meshName, virtualRouterName) + "|" + routeName
}

// gatewayRouteCompositeKey derives the composite primary key for a gateway
// route, nested under a virtual gateway within a mesh.
func gatewayRouteCompositeKey(meshName, virtualGatewayName, routeName string) string {
	return meshChildKey(meshName, virtualGatewayName) + "|" + routeName
}

func meshKeyFn(v *Mesh) string { return v.Name }

func virtualNodeKeyFn(v *VirtualNode) string          { return meshChildKey(v.MeshName, v.VirtualNodeName) }
func virtualNodeMeshIndexKeyFn(v *VirtualNode) string { return v.MeshName }

func virtualRouterKeyFn(v *VirtualRouter) string {
	return meshChildKey(v.MeshName, v.VirtualRouterName)
}
func virtualRouterMeshIndexKeyFn(v *VirtualRouter) string { return v.MeshName }

func routeKeyFn(v *Route) string {
	return routeCompositeKey(v.MeshName, v.VirtualRouterName, v.RouteName)
}
func routeRouterIndexKeyFn(v *Route) string { return meshChildKey(v.MeshName, v.VirtualRouterName) }

func virtualServiceKeyFn(v *VirtualService) string {
	return meshChildKey(v.MeshName, v.VirtualServiceName)
}
func virtualServiceMeshIndexKeyFn(v *VirtualService) string { return v.MeshName }

func virtualGatewayKeyFn(v *VirtualGateway) string {
	return meshChildKey(v.MeshName, v.VirtualGatewayName)
}
func virtualGatewayMeshIndexKeyFn(v *VirtualGateway) string { return v.MeshName }

func gatewayRouteKeyFn(v *GatewayRoute) string {
	return gatewayRouteCompositeKey(v.MeshName, v.VirtualGatewayName, v.GatewayRouteName)
}
func gatewayRouteGatewayIndexKeyFn(v *GatewayRoute) string {
	return meshChildKey(v.MeshName, v.VirtualGatewayName)
}

// registerAllTables registers every converted resource collection (and its
// secondary indexes) on b.registry exactly once. It must be called during
// construction only (immediately after b.registry is created -- see
// NewInMemoryBackend), never on every Reset() -- store.Register panics on a
// duplicate name, so runtime resets go through b.registry.ResetAll() instead
// (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.meshes = store.Register(b.registry, "meshes", store.New(meshKeyFn))

	b.virtualNodes = store.Register(b.registry, "virtualNodes", store.New(virtualNodeKeyFn))
	b.virtualNodesByMesh = b.virtualNodes.AddIndex("byMesh", virtualNodeMeshIndexKeyFn)

	b.virtualRouters = store.Register(b.registry, "virtualRouters", store.New(virtualRouterKeyFn))
	b.virtualRoutersByMesh = b.virtualRouters.AddIndex("byMesh", virtualRouterMeshIndexKeyFn)

	b.routes = store.Register(b.registry, "routes", store.New(routeKeyFn))
	b.routesByRouter = b.routes.AddIndex("byRouter", routeRouterIndexKeyFn)

	b.virtualSvcs = store.Register(b.registry, "virtualServices", store.New(virtualServiceKeyFn))
	b.virtualSvcsByMesh = b.virtualSvcs.AddIndex("byMesh", virtualServiceMeshIndexKeyFn)

	b.virtualGWs = store.Register(b.registry, "virtualGateways", store.New(virtualGatewayKeyFn))
	b.virtualGWsByMesh = b.virtualGWs.AddIndex("byMesh", virtualGatewayMeshIndexKeyFn)

	b.gatewayRoutes = store.Register(b.registry, "gatewayRoutes", store.New(gatewayRouteKeyFn))
	b.gatewayRoutesByGateway = b.gatewayRoutes.AddIndex("byGateway", gatewayRouteGatewayIndexKeyFn)
}
