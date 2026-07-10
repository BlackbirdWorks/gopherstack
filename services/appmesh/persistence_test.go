package appmesh_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appmesh"
)

const (
	persistTestAccountID = "000000000000"
	persistTestRegion    = "us-east-1"
)

// persistenceFixtureIDs carries every identifier newPersistenceTestBackend
// creates, so the round-trip assertions in
// TestInMemoryBackend_SnapshotRestore_FullState can look each resource back
// up after Restore.
type persistenceFixtureIDs struct {
	meshName        string
	meshARN         string
	virtualNodeName string
	routerName      string
	routeName       string
	svcName         string
	gatewayName     string
	gatewayRouteID  string
}

// newPersistenceTestBackend creates a backend with one populated entry in
// every store.Table (and, transitively, every secondary store.Index --
// virtualNodesByMesh, virtualRoutersByMesh, routesByRouter,
// virtualSvcsByMesh, virtualGWsByMesh, gatewayRoutesByGateway; see
// store_setup.go) plus the one raw map left un-converted (tags), so a
// Snapshot from it exercises the entire persisted surface of the backend.
// App Mesh had no persistence at all before this conversion, so -- unlike
// most Phase 3.3 services -- nothing here is left deliberately
// unpersisted/ephemeral.
func newPersistenceTestBackend(t *testing.T) (*appmesh.InMemoryBackend, persistenceFixtureIDs) {
	t.Helper()

	b := appmesh.NewInMemoryBackend(persistTestAccountID, persistTestRegion)

	mesh, err := b.CreateMesh("mesh1", nil, map[string]string{"env": "test"})
	require.NoError(t, err)

	_, err = b.CreateVirtualNode("mesh1", "node1", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateVirtualRouter("mesh1", "router1", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateRoute("mesh1", "router1", "route1", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateVirtualService("mesh1", "svc1", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateVirtualGateway("mesh1", "gw1", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateGatewayRoute("mesh1", "gw1", "gwroute1", nil, nil)
	require.NoError(t, err)

	return b, persistenceFixtureIDs{
		meshName:        "mesh1",
		meshARN:         mesh.Meta.Arn,
		virtualNodeName: "node1",
		routerName:      "router1",
		routeName:       "route1",
		svcName:         "svc1",
		gatewayName:     "gw1",
		gatewayRouteID:  "gwroute1",
	}
}

// TestInMemoryBackend_SnapshotRestore_FullState round-trips every
// store.Table (meshes, virtualNodes, virtualRouters, routes,
// virtualServices, virtualGateways, gatewayRoutes), every secondary
// store.Index derived from them (byMesh on
// virtualNodes/virtualRouters/virtualServices/virtualGateways, byRouter on
// routes, byGateway on gatewayRoutes), and the one raw map left
// un-converted (tags) through Snapshot -> Restore into a fresh backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original, ids := newPersistenceTestBackend(t)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := appmesh.NewInMemoryBackend(persistTestAccountID, persistTestRegion)
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// meshes table.
	mesh, err := fresh.DescribeMesh(ids.meshName)
	require.NoError(t, err)
	assert.Equal(t, ids.meshName, mesh.Name)
	assert.Equal(t, ids.meshARN, mesh.Meta.Arn)

	_, err = fresh.CreateMesh(ids.meshName, nil, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, appmesh.ErrMeshAlreadyExists)

	// virtualNodes table + virtualNodesByMesh index.
	vn, err := fresh.DescribeVirtualNode(ids.meshName, ids.virtualNodeName)
	require.NoError(t, err)
	assert.Equal(t, ids.virtualNodeName, vn.VirtualNodeName)

	nodes, _, err := fresh.ListVirtualNodes(ids.meshName, 0, "")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	assert.Equal(t, ids.virtualNodeName, nodes[0].VirtualNodeName)

	// virtualRouters table + virtualRoutersByMesh index.
	vr, err := fresh.DescribeVirtualRouter(ids.meshName, ids.routerName)
	require.NoError(t, err)
	assert.Equal(t, ids.routerName, vr.VirtualRouterName)

	routers, _, err := fresh.ListVirtualRouters(ids.meshName, 0, "")
	require.NoError(t, err)
	require.Len(t, routers, 1)

	// routes table + routesByRouter index (also exercised by the
	// VirtualRouterInUse guard below).
	r, err := fresh.DescribeRoute(ids.meshName, ids.routerName, ids.routeName)
	require.NoError(t, err)
	assert.Equal(t, ids.routeName, r.RouteName)

	routes, _, err := fresh.ListRoutes(ids.meshName, ids.routerName, 0, "")
	require.NoError(t, err)
	require.Len(t, routes, 1)
	assert.Equal(t, ids.routeName, routes[0].RouteName)

	_, err = fresh.DeleteVirtualRouter(ids.meshName, ids.routerName)
	require.Error(t, err)
	require.ErrorIs(t, err, appmesh.ErrVirtualRouterInUse)

	// virtualServices table + virtualSvcsByMesh index.
	vs, err := fresh.DescribeVirtualService(ids.meshName, ids.svcName)
	require.NoError(t, err)
	assert.Equal(t, ids.svcName, vs.VirtualServiceName)

	svcs, _, err := fresh.ListVirtualServices(ids.meshName, 0, "")
	require.NoError(t, err)
	require.Len(t, svcs, 1)

	// virtualGateways table + virtualGWsByMesh index.
	vg, err := fresh.DescribeVirtualGateway(ids.meshName, ids.gatewayName)
	require.NoError(t, err)
	assert.Equal(t, ids.gatewayName, vg.VirtualGatewayName)

	gws, _, err := fresh.ListVirtualGateways(ids.meshName, 0, "")
	require.NoError(t, err)
	require.Len(t, gws, 1)

	// gatewayRoutes table + gatewayRoutesByGateway index (also exercised by
	// the VirtualGatewayInUse guard below).
	gr, err := fresh.DescribeGatewayRoute(ids.meshName, ids.gatewayName, ids.gatewayRouteID)
	require.NoError(t, err)
	assert.Equal(t, ids.gatewayRouteID, gr.GatewayRouteName)

	gatewayRoutes, _, err := fresh.ListGatewayRoutes(ids.meshName, ids.gatewayName, 0, "")
	require.NoError(t, err)
	require.Len(t, gatewayRoutes, 1)

	_, err = fresh.DeleteVirtualGateway(ids.meshName, ids.gatewayName)
	require.Error(t, err)
	require.ErrorIs(t, err, appmesh.ErrVirtualGatewayInUse)

	// mesh still in use (children survived restore too), proving
	// virtualNodesByMesh/virtualSvcsByMesh also round-tripped.
	_, err = fresh.DeleteMesh(ids.meshName)
	require.Error(t, err)
	require.ErrorIs(t, err, appmesh.ErrMeshInUse)

	// tags raw map.
	tagRefs, _, err := fresh.ListTagsForResource(ids.meshARN, 0, "")
	require.NoError(t, err)
	require.Len(t, tagRefs, 1)
	assert.Equal(t, "env", tagRefs[0].Key)
	assert.Equal(t, "test", tagRefs[0].Value)

	// Sanity: account/region carried through too.
	assert.Equal(t, persistTestRegion, fresh.Region())
	assert.Equal(t, persistTestAccountID, fresh.AccountID())
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error. App Mesh had no persistence at all before this conversion, so
// there is no legacy (Version == 0) snapshot format to special-case --
// absent-or-mismatched Version is handled identically.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b, ids := newPersistenceTestBackend(t)

	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	_, err = b.DescribeMesh(ids.meshName)
	require.ErrorIs(t, err, appmesh.ErrMeshNotFound)

	meshes, _, err := b.ListMeshes(0, "")
	require.NoError(t, err)
	assert.Empty(t, meshes)

	_, _, err = b.ListTagsForResource(ids.meshARN, 0, "")
	require.Error(t, err) // the mesh (and thus its ARN) no longer exists.
	assert.ErrorIs(t, err, appmesh.ErrResourceNotFound)
}

// TestInMemoryBackend_RestoreInvalidData verifies malformed JSON surfaces as
// an error rather than being silently discarded (that path is reserved for a
// syntactically valid but version-mismatched snapshot; see
// TestInMemoryBackend_RestoreVersionMismatch).
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := appmesh.NewInMemoryBackend(persistTestAccountID, persistTestRegion)
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend. Before Phase 3.3, App Mesh had neither: no
// persistence at all, so cli.go's generic setupPersistence (which discovers
// services via a persistence.Persistable type assertion on the registered
// Handler) never picked App Mesh up.
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	backend, ids := newPersistenceTestBackend(t)
	h := appmesh.NewHandler(backend)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := appmesh.NewHandler(appmesh.NewInMemoryBackend(persistTestAccountID, persistTestRegion))
	require.NoError(t, h2.Restore(t.Context(), snap))

	mesh, err := h2.Backend.DescribeMesh(ids.meshName)
	require.NoError(t, err)
	assert.Equal(t, ids.meshName, mesh.Name)
}
