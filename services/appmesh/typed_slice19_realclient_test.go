package appmesh_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appmeshsdk "github.com/aws/aws-sdk-go-v2/service/appmesh"
	"github.com/aws/aws-sdk-go-v2/service/appmesh/types"
	"github.com/stretchr/testify/require"
)

// TestTypedSlice19_AppMesh drives every appmesh op that a real
// aws-sdk-go-v2 client had never exercised before this slice: the six
// List<Family> ops for virtual nodes/routers/services/gateways plus routes
// and gateway routes, and the tag trio (TagResource/UntagResource/
// ListTagsForResource).
func TestTypedSlice19_AppMesh(t *testing.T) {
	t.Parallel()

	t.Run("list_families", func(t *testing.T) {
		t.Parallel()
		testListFamilies(t)
	})

	t.Run("tags", func(t *testing.T) {
		t.Parallel()
		testTagLifecycle(t)
	})
}

func testListFamilies(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	client := newTestHandlerAndClient(t)

	_, err := client.CreateMesh(ctx, &appmeshsdk.CreateMeshInput{MeshName: new("m1"), Spec: &types.MeshSpec{}})
	require.NoError(t, err)

	_, err = client.CreateVirtualNode(ctx, &appmeshsdk.CreateVirtualNodeInput{
		MeshName: new("m1"), VirtualNodeName: new("vn1"), Spec: &types.VirtualNodeSpec{},
	})
	require.NoError(t, err)
	nodes, err := client.ListVirtualNodes(ctx, &appmeshsdk.ListVirtualNodesInput{MeshName: new("m1")})
	require.NoError(t, err)
	require.Len(t, nodes.VirtualNodes, 1)
	require.Equal(t, "vn1", aws.ToString(nodes.VirtualNodes[0].VirtualNodeName))
	require.NotEmpty(t, aws.ToString(nodes.VirtualNodes[0].Arn), "VirtualNodeRef.Arn must decode")

	_, err = client.CreateVirtualRouter(ctx, &appmeshsdk.CreateVirtualRouterInput{
		MeshName: new("m1"), VirtualRouterName: new("vr1"), Spec: &types.VirtualRouterSpec{},
	})
	require.NoError(t, err)
	routers, err := client.ListVirtualRouters(ctx, &appmeshsdk.ListVirtualRoutersInput{MeshName: new("m1")})
	require.NoError(t, err)
	require.Len(t, routers.VirtualRouters, 1)
	require.Equal(t, "vr1", aws.ToString(routers.VirtualRouters[0].VirtualRouterName))

	routeSpec := &types.RouteSpec{
		HttpRoute: &types.HttpRoute{
			Action: &types.HttpRouteAction{
				WeightedTargets: []types.WeightedTarget{{VirtualNode: new("vn1"), Weight: 1}},
			},
			Match: &types.HttpRouteMatch{Prefix: new("/")},
		},
	}
	_, err = client.CreateRoute(ctx, &appmeshsdk.CreateRouteInput{
		MeshName: new("m1"), VirtualRouterName: new("vr1"), RouteName: new("r1"), Spec: routeSpec,
	})
	require.NoError(t, err)
	routes, err := client.ListRoutes(ctx, &appmeshsdk.ListRoutesInput{
		MeshName: new("m1"), VirtualRouterName: new("vr1"),
	})
	require.NoError(t, err)
	require.Len(t, routes.Routes, 1)
	require.Equal(t, "r1", aws.ToString(routes.Routes[0].RouteName))

	_, err = client.CreateVirtualService(ctx, &appmeshsdk.CreateVirtualServiceInput{
		MeshName: new("m1"), VirtualServiceName: new("svc.local"), Spec: &types.VirtualServiceSpec{},
	})
	require.NoError(t, err)
	svcs, err := client.ListVirtualServices(ctx, &appmeshsdk.ListVirtualServicesInput{MeshName: new("m1")})
	require.NoError(t, err)
	require.Len(t, svcs.VirtualServices, 1)
	require.Equal(t, "svc.local", aws.ToString(svcs.VirtualServices[0].VirtualServiceName))

	_, err = client.CreateVirtualGateway(ctx, &appmeshsdk.CreateVirtualGatewayInput{
		MeshName: new("m1"), VirtualGatewayName: new("gw1"), Spec: validVirtualGatewaySpec(),
	})
	require.NoError(t, err)
	gws, err := client.ListVirtualGateways(ctx, &appmeshsdk.ListVirtualGatewaysInput{MeshName: new("m1")})
	require.NoError(t, err)
	require.Len(t, gws.VirtualGateways, 1)
	require.Equal(t, "gw1", aws.ToString(gws.VirtualGateways[0].VirtualGatewayName))

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
	_, err = client.CreateGatewayRoute(ctx, &appmeshsdk.CreateGatewayRouteInput{
		MeshName: new("m1"), VirtualGatewayName: new("gw1"), GatewayRouteName: new("gr1"), Spec: grSpec,
	})
	require.NoError(t, err)
	grs, err := client.ListGatewayRoutes(ctx, &appmeshsdk.ListGatewayRoutesInput{
		MeshName: new("m1"), VirtualGatewayName: new("gw1"),
	})
	require.NoError(t, err)
	require.Len(t, grs.GatewayRoutes, 1)
	require.Equal(t, "gr1", aws.ToString(grs.GatewayRoutes[0].GatewayRouteName))
}

func testTagLifecycle(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	client := newTestHandlerAndClient(t)

	created, err := client.CreateMesh(
		ctx,
		&appmeshsdk.CreateMeshInput{MeshName: new("m-tags"), Spec: &types.MeshSpec{}},
	)
	require.NoError(t, err)
	require.NotNil(t, created.Mesh.Metadata)
	arn := aws.ToString(created.Mesh.Metadata.Arn)
	require.NotEmpty(t, arn)

	_, err = client.TagResource(ctx, &appmeshsdk.TagResourceInput{
		ResourceArn: aws.String(arn),
		Tags: []types.TagRef{
			{Key: new("env"), Value: new("prod")},
			{Key: new("team"), Value: new("net")},
		},
	})
	require.NoError(t, err, "TagResource must accept a resourceArn sent as a query param, not a body field")

	listed, err := client.ListTagsForResource(ctx, &appmeshsdk.ListTagsForResourceInput{ResourceArn: aws.String(arn)})
	require.NoError(t, err)
	require.Len(t, listed.Tags, 2)
	tagMap := make(map[string]string, len(listed.Tags))
	for _, tag := range listed.Tags {
		tagMap[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	require.Equal(t, "prod", tagMap["env"])
	require.Equal(t, "net", tagMap["team"])

	_, err = client.UntagResource(ctx, &appmeshsdk.UntagResourceInput{
		ResourceArn: aws.String(arn),
		TagKeys:     []string{"team"},
	})
	require.NoError(t, err, "UntagResource must accept a resourceArn sent as a query param, not a body field")

	after, err := client.ListTagsForResource(ctx, &appmeshsdk.ListTagsForResourceInput{ResourceArn: aws.String(arn)})
	require.NoError(t, err)
	require.Len(t, after.Tags, 1)
	require.Equal(t, "env", aws.ToString(after.Tags[0].Key))
}
