package appmesh_test

import (
	"testing"

	appmeshsdk "github.com/aws/aws-sdk-go-v2/service/appmesh"
	"github.com/aws/aws-sdk-go-v2/service/appmesh/types"
	"github.com/stretchr/testify/require"
)

// TestSDKRoundTrip_ResourceWrapping proves that every family's
// Create/Describe/Update/Delete response can actually be decoded by the real
// aws-sdk-go-v2 appmesh client into a populated (non-nil, non-empty) resource
// struct. Every op's real invoked deserializer
// (awsRestjson1_deserializeOp<Op>.HandleDeserialize in the pinned SDK's
// deserializers.go, e.g. line 244 for CreateMesh) decodes the raw top-level
// response body directly into the resource's *Data type via
// awsRestjson1_deserializeDocument<Resource>Data(&output.<Field>, shape) --
// it does NOT go through a "mesh"/"virtualNode"/etc. wrapper key, unlike
// what the generated-but-dead-code `awsRestjson1_deserializeOpDocument<Op>
// Output` helper (also present in the same file) would suggest at a glance.
// A raw-body unit test can only confirm gopherstack's own expected keys are
// present; only round-tripping through the genuine SDK client proves the
// flat body it emits is what a real client can actually decode.
func TestSDKRoundTrip_ResourceWrapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *appmeshsdk.Client)
		name string
	}{
		{name: "mesh", run: roundTripMesh},
		{name: "virtualNode", run: roundTripVirtualNode},
		{name: "virtualRouter", run: roundTripVirtualRouter},
		{name: "route", run: roundTripRoute},
		{name: "virtualService", run: roundTripVirtualService},
		{name: "virtualGateway", run: roundTripVirtualGateway},
		{name: "gatewayRoute", run: roundTripGatewayRoute},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			tc.run(t, client)
		})
	}
}

func roundTripMesh(t *testing.T, client *appmeshsdk.Client) {
	t.Helper()
	ctx := t.Context()

	created, err := client.CreateMesh(ctx, &appmeshsdk.CreateMeshInput{
		MeshName: new("m1"),
		Spec:     &types.MeshSpec{},
	})
	require.NoError(t, err)
	require.NotNil(t, created.Mesh, "CreateMeshOutput.Mesh must not be nil")
	require.Equal(t, "m1", *created.Mesh.MeshName)

	described, err := client.DescribeMesh(ctx, &appmeshsdk.DescribeMeshInput{MeshName: new("m1")})
	require.NoError(t, err)
	require.NotNil(t, described.Mesh, "DescribeMeshOutput.Mesh must not be nil")
	require.Equal(t, "m1", *described.Mesh.MeshName)

	updated, err := client.UpdateMesh(ctx, &appmeshsdk.UpdateMeshInput{MeshName: new("m1"), Spec: &types.MeshSpec{}})
	require.NoError(t, err)
	require.NotNil(t, updated.Mesh, "UpdateMeshOutput.Mesh must not be nil")

	deleted, err := client.DeleteMesh(ctx, &appmeshsdk.DeleteMeshInput{MeshName: new("m1")})
	require.NoError(t, err)
	require.NotNil(t, deleted.Mesh, "DeleteMeshOutput.Mesh must not be nil")
}

func roundTripVirtualNode(t *testing.T, client *appmeshsdk.Client) {
	t.Helper()
	ctx := t.Context()

	_, err := client.CreateMesh(ctx, &appmeshsdk.CreateMeshInput{MeshName: new("m1"), Spec: &types.MeshSpec{}})
	require.NoError(t, err)

	created, err := client.CreateVirtualNode(ctx, &appmeshsdk.CreateVirtualNodeInput{
		MeshName:        new("m1"),
		VirtualNodeName: new("vn1"),
		Spec:            &types.VirtualNodeSpec{},
	})
	require.NoError(t, err)
	require.NotNil(t, created.VirtualNode, "CreateVirtualNodeOutput.VirtualNode must not be nil")
	require.Equal(t, "vn1", *created.VirtualNode.VirtualNodeName)

	described, err := client.DescribeVirtualNode(ctx, &appmeshsdk.DescribeVirtualNodeInput{
		MeshName: new("m1"), VirtualNodeName: new("vn1"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.VirtualNode, "DescribeVirtualNodeOutput.VirtualNode must not be nil")

	updated, err := client.UpdateVirtualNode(ctx, &appmeshsdk.UpdateVirtualNodeInput{
		MeshName: new("m1"), VirtualNodeName: new("vn1"), Spec: &types.VirtualNodeSpec{},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.VirtualNode, "UpdateVirtualNodeOutput.VirtualNode must not be nil")

	deleted, err := client.DeleteVirtualNode(ctx, &appmeshsdk.DeleteVirtualNodeInput{
		MeshName: new("m1"), VirtualNodeName: new("vn1"),
	})
	require.NoError(t, err)
	require.NotNil(t, deleted.VirtualNode, "DeleteVirtualNodeOutput.VirtualNode must not be nil")
}

func roundTripVirtualRouter(t *testing.T, client *appmeshsdk.Client) {
	t.Helper()
	ctx := t.Context()

	_, err := client.CreateMesh(ctx, &appmeshsdk.CreateMeshInput{MeshName: new("m1"), Spec: &types.MeshSpec{}})
	require.NoError(t, err)

	created, err := client.CreateVirtualRouter(ctx, &appmeshsdk.CreateVirtualRouterInput{
		MeshName:          new("m1"),
		VirtualRouterName: new("vr1"),
		Spec:              &types.VirtualRouterSpec{},
	})
	require.NoError(t, err)
	require.NotNil(t, created.VirtualRouter, "CreateVirtualRouterOutput.VirtualRouter must not be nil")
	require.Equal(t, "vr1", *created.VirtualRouter.VirtualRouterName)

	described, err := client.DescribeVirtualRouter(ctx, &appmeshsdk.DescribeVirtualRouterInput{
		MeshName: new("m1"), VirtualRouterName: new("vr1"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.VirtualRouter, "DescribeVirtualRouterOutput.VirtualRouter must not be nil")

	updated, err := client.UpdateVirtualRouter(ctx, &appmeshsdk.UpdateVirtualRouterInput{
		MeshName: new("m1"), VirtualRouterName: new("vr1"), Spec: &types.VirtualRouterSpec{},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.VirtualRouter, "UpdateVirtualRouterOutput.VirtualRouter must not be nil")

	deleted, err := client.DeleteVirtualRouter(ctx, &appmeshsdk.DeleteVirtualRouterInput{
		MeshName: new("m1"), VirtualRouterName: new("vr1"),
	})
	require.NoError(t, err)
	require.NotNil(t, deleted.VirtualRouter, "DeleteVirtualRouterOutput.VirtualRouter must not be nil")
}

func roundTripRoute(t *testing.T, client *appmeshsdk.Client) {
	t.Helper()
	ctx := t.Context()

	_, err := client.CreateMesh(ctx, &appmeshsdk.CreateMeshInput{MeshName: new("m1"), Spec: &types.MeshSpec{}})
	require.NoError(t, err)
	_, err = client.CreateVirtualRouter(ctx, &appmeshsdk.CreateVirtualRouterInput{
		MeshName: new("m1"), VirtualRouterName: new("vr1"), Spec: &types.VirtualRouterSpec{},
	})
	require.NoError(t, err)

	routeSpec := &types.RouteSpec{
		HttpRoute: &types.HttpRoute{
			Action: &types.HttpRouteAction{
				WeightedTargets: []types.WeightedTarget{
					{VirtualNode: new("vn1"), Weight: 1},
				},
			},
			Match: &types.HttpRouteMatch{Prefix: new("/")},
		},
	}

	created, err := client.CreateRoute(ctx, &appmeshsdk.CreateRouteInput{
		MeshName:          new("m1"),
		VirtualRouterName: new("vr1"),
		RouteName:         new("r1"),
		Spec:              routeSpec,
	})
	require.NoError(t, err)
	require.NotNil(t, created.Route, "CreateRouteOutput.Route must not be nil")
	require.Equal(t, "r1", *created.Route.RouteName)

	described, err := client.DescribeRoute(ctx, &appmeshsdk.DescribeRouteInput{
		MeshName: new("m1"), VirtualRouterName: new("vr1"), RouteName: new("r1"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.Route, "DescribeRouteOutput.Route must not be nil")

	updated, err := client.UpdateRoute(ctx, &appmeshsdk.UpdateRouteInput{
		MeshName: new("m1"), VirtualRouterName: new("vr1"), RouteName: new("r1"), Spec: routeSpec,
	})
	require.NoError(t, err)
	require.NotNil(t, updated.Route, "UpdateRouteOutput.Route must not be nil")

	deleted, err := client.DeleteRoute(ctx, &appmeshsdk.DeleteRouteInput{
		MeshName: new("m1"), VirtualRouterName: new("vr1"), RouteName: new("r1"),
	})
	require.NoError(t, err)
	require.NotNil(t, deleted.Route, "DeleteRouteOutput.Route must not be nil")
}

func roundTripVirtualService(t *testing.T, client *appmeshsdk.Client) {
	t.Helper()
	ctx := t.Context()

	_, err := client.CreateMesh(ctx, &appmeshsdk.CreateMeshInput{MeshName: new("m1"), Spec: &types.MeshSpec{}})
	require.NoError(t, err)

	created, err := client.CreateVirtualService(ctx, &appmeshsdk.CreateVirtualServiceInput{
		MeshName:           new("m1"),
		VirtualServiceName: new("svc.local"),
		Spec:               &types.VirtualServiceSpec{},
	})
	require.NoError(t, err)
	require.NotNil(t, created.VirtualService, "CreateVirtualServiceOutput.VirtualService must not be nil")
	require.Equal(t, "svc.local", *created.VirtualService.VirtualServiceName)

	described, err := client.DescribeVirtualService(ctx, &appmeshsdk.DescribeVirtualServiceInput{
		MeshName: new("m1"), VirtualServiceName: new("svc.local"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.VirtualService, "DescribeVirtualServiceOutput.VirtualService must not be nil")

	updated, err := client.UpdateVirtualService(ctx, &appmeshsdk.UpdateVirtualServiceInput{
		MeshName: new("m1"), VirtualServiceName: new("svc.local"), Spec: &types.VirtualServiceSpec{},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.VirtualService, "UpdateVirtualServiceOutput.VirtualService must not be nil")

	deleted, err := client.DeleteVirtualService(ctx, &appmeshsdk.DeleteVirtualServiceInput{
		MeshName: new("m1"), VirtualServiceName: new("svc.local"),
	})
	require.NoError(t, err)
	require.NotNil(t, deleted.VirtualService, "DeleteVirtualServiceOutput.VirtualService must not be nil")
}

func roundTripVirtualGateway(t *testing.T, client *appmeshsdk.Client) {
	t.Helper()
	ctx := t.Context()

	_, err := client.CreateMesh(ctx, &appmeshsdk.CreateMeshInput{MeshName: new("m1"), Spec: &types.MeshSpec{}})
	require.NoError(t, err)

	created, err := client.CreateVirtualGateway(ctx, &appmeshsdk.CreateVirtualGatewayInput{
		MeshName:           new("m1"),
		VirtualGatewayName: new("gw1"),
		Spec:               validVirtualGatewaySpec(),
	})
	require.NoError(t, err)
	require.NotNil(t, created.VirtualGateway, "CreateVirtualGatewayOutput.VirtualGateway must not be nil")
	require.Equal(t, "gw1", *created.VirtualGateway.VirtualGatewayName)

	described, err := client.DescribeVirtualGateway(ctx, &appmeshsdk.DescribeVirtualGatewayInput{
		MeshName: new("m1"), VirtualGatewayName: new("gw1"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.VirtualGateway, "DescribeVirtualGatewayOutput.VirtualGateway must not be nil")

	updated, err := client.UpdateVirtualGateway(ctx, &appmeshsdk.UpdateVirtualGatewayInput{
		MeshName: new("m1"), VirtualGatewayName: new("gw1"), Spec: validVirtualGatewaySpec(),
	})
	require.NoError(t, err)
	require.NotNil(t, updated.VirtualGateway, "UpdateVirtualGatewayOutput.VirtualGateway must not be nil")

	deleted, err := client.DeleteVirtualGateway(ctx, &appmeshsdk.DeleteVirtualGatewayInput{
		MeshName: new("m1"), VirtualGatewayName: new("gw1"),
	})
	require.NoError(t, err)
	require.NotNil(t, deleted.VirtualGateway, "DeleteVirtualGatewayOutput.VirtualGateway must not be nil")
}

func roundTripGatewayRoute(t *testing.T, client *appmeshsdk.Client) {
	t.Helper()
	ctx := t.Context()

	_, err := client.CreateMesh(ctx, &appmeshsdk.CreateMeshInput{MeshName: new("m1"), Spec: &types.MeshSpec{}})
	require.NoError(t, err)
	_, err = client.CreateVirtualGateway(ctx, &appmeshsdk.CreateVirtualGatewayInput{
		MeshName: new("m1"), VirtualGatewayName: new("gw1"), Spec: validVirtualGatewaySpec(),
	})
	require.NoError(t, err)

	grSpec := &types.GatewayRouteSpec{
		HttpRoute: &types.HttpGatewayRoute{
			Action: &types.HttpGatewayRouteAction{
				Target: &types.GatewayRouteTarget{
					VirtualService: &types.GatewayRouteVirtualService{VirtualServiceName: new("svc.local")},
				},
			},
			Match: &types.HttpGatewayRouteMatch{Prefix: new("/")},
		},
	}

	created, err := client.CreateGatewayRoute(ctx, &appmeshsdk.CreateGatewayRouteInput{
		MeshName:           new("m1"),
		VirtualGatewayName: new("gw1"),
		GatewayRouteName:   new("gr1"),
		Spec:               grSpec,
	})
	require.NoError(t, err)
	require.NotNil(t, created.GatewayRoute, "CreateGatewayRouteOutput.GatewayRoute must not be nil")
	require.Equal(t, "gr1", *created.GatewayRoute.GatewayRouteName)

	described, err := client.DescribeGatewayRoute(ctx, &appmeshsdk.DescribeGatewayRouteInput{
		MeshName: new("m1"), VirtualGatewayName: new("gw1"), GatewayRouteName: new("gr1"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.GatewayRoute, "DescribeGatewayRouteOutput.GatewayRoute must not be nil")

	updated, err := client.UpdateGatewayRoute(ctx, &appmeshsdk.UpdateGatewayRouteInput{
		MeshName: new("m1"), VirtualGatewayName: new("gw1"), GatewayRouteName: new("gr1"), Spec: grSpec,
	})
	require.NoError(t, err)
	require.NotNil(t, updated.GatewayRoute, "UpdateGatewayRouteOutput.GatewayRoute must not be nil")

	deleted, err := client.DeleteGatewayRoute(ctx, &appmeshsdk.DeleteGatewayRouteInput{
		MeshName: new("m1"), VirtualGatewayName: new("gw1"), GatewayRouteName: new("gr1"),
	})
	require.NoError(t, err)
	require.NotNil(t, deleted.GatewayRoute, "DeleteGatewayRouteOutput.GatewayRoute must not be nil")
}

func validVirtualGatewaySpec() *types.VirtualGatewaySpec {
	return &types.VirtualGatewaySpec{
		Listeners: []types.VirtualGatewayListener{
			{
				PortMapping: &types.VirtualGatewayPortMapping{
					Port:     new(int32(8080)),
					Protocol: types.VirtualGatewayPortProtocolHttp,
				},
			},
		},
	}
}
