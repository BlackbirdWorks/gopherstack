package appmesh_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real App Mesh
// operation, extracted from appmesh@v1.38.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for a {meshName}/{virtualNodeName}/{virtualRouterName}/{routeName}/
// {virtualServiceName}/{virtualGatewayName}/{gatewayRouteName} URI label --
// splitPath (handler.go) never validates identifier shape, so the literal
// value doesn't matter here, only path depth and static segments. Notably,
// every Create op (CreateMesh, CreateVirtualNode, ...) uses PUT rather than
// POST -- confirmed from the serializer, not assumed from REST convention,
// since handler.go's own switch statements already key Create off PUT. 38
// real ops here, matching appmesh's real op count exactly (also matches
// GetSupportedOperations's own 38 entries one-for-one).
//
// A systematic check for a shared method+path across all 38 ops found zero
// collisions: e.g. DescribeMesh/UpdateMesh/DeleteMesh all share
// "/v20190125/meshes/{meshName}" but are disambiguated by method
// (GET/PUT/DELETE), and CreateVirtualNode (PUT on the collection path) vs.
// UpdateVirtualNode (PUT on the single-resource path) share a method but
// differ in path depth -- both distinctions parseMeshTopLevel/parseSubOp
// already switch on -- so no *required dynamic* (non-template) member --
// the s3/glacier vacuity-trap class -- was needed to disambiguate any route
// in this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CreateGatewayRoute", "PUT", "/v20190125/meshes/PLACEHOLDER/virtualGateway/PLACEHOLDER/gatewayRoutes"},
		{"CreateMesh", "PUT", "/v20190125/meshes"},
		{"CreateRoute", "PUT", "/v20190125/meshes/PLACEHOLDER/virtualRouter/PLACEHOLDER/routes"},
		{"CreateVirtualGateway", "PUT", "/v20190125/meshes/PLACEHOLDER/virtualGateways"},
		{"CreateVirtualNode", "PUT", "/v20190125/meshes/PLACEHOLDER/virtualNodes"},
		{"CreateVirtualRouter", "PUT", "/v20190125/meshes/PLACEHOLDER/virtualRouters"},
		{"CreateVirtualService", "PUT", "/v20190125/meshes/PLACEHOLDER/virtualServices"},
		{
			"DeleteGatewayRoute",
			"DELETE",
			"/v20190125/meshes/PLACEHOLDER/virtualGateway/PLACEHOLDER/gatewayRoutes/PLACEHOLDER",
		},
		{"DeleteMesh", "DELETE", "/v20190125/meshes/PLACEHOLDER"},
		{"DeleteRoute", "DELETE", "/v20190125/meshes/PLACEHOLDER/virtualRouter/PLACEHOLDER/routes/PLACEHOLDER"},
		{"DeleteVirtualGateway", "DELETE", "/v20190125/meshes/PLACEHOLDER/virtualGateways/PLACEHOLDER"},
		{"DeleteVirtualNode", "DELETE", "/v20190125/meshes/PLACEHOLDER/virtualNodes/PLACEHOLDER"},
		{"DeleteVirtualRouter", "DELETE", "/v20190125/meshes/PLACEHOLDER/virtualRouters/PLACEHOLDER"},
		{"DeleteVirtualService", "DELETE", "/v20190125/meshes/PLACEHOLDER/virtualServices/PLACEHOLDER"},
		{
			"DescribeGatewayRoute",
			"GET",
			"/v20190125/meshes/PLACEHOLDER/virtualGateway/PLACEHOLDER/gatewayRoutes/PLACEHOLDER",
		},
		{"DescribeMesh", "GET", "/v20190125/meshes/PLACEHOLDER"},
		{"DescribeRoute", "GET", "/v20190125/meshes/PLACEHOLDER/virtualRouter/PLACEHOLDER/routes/PLACEHOLDER"},
		{"DescribeVirtualGateway", "GET", "/v20190125/meshes/PLACEHOLDER/virtualGateways/PLACEHOLDER"},
		{"DescribeVirtualNode", "GET", "/v20190125/meshes/PLACEHOLDER/virtualNodes/PLACEHOLDER"},
		{"DescribeVirtualRouter", "GET", "/v20190125/meshes/PLACEHOLDER/virtualRouters/PLACEHOLDER"},
		{"DescribeVirtualService", "GET", "/v20190125/meshes/PLACEHOLDER/virtualServices/PLACEHOLDER"},
		{"ListGatewayRoutes", "GET", "/v20190125/meshes/PLACEHOLDER/virtualGateway/PLACEHOLDER/gatewayRoutes"},
		{"ListMeshes", "GET", "/v20190125/meshes"},
		{"ListRoutes", "GET", "/v20190125/meshes/PLACEHOLDER/virtualRouter/PLACEHOLDER/routes"},
		{"ListTagsForResource", "GET", "/v20190125/tags"},
		{"ListVirtualGateways", "GET", "/v20190125/meshes/PLACEHOLDER/virtualGateways"},
		{"ListVirtualNodes", "GET", "/v20190125/meshes/PLACEHOLDER/virtualNodes"},
		{"ListVirtualRouters", "GET", "/v20190125/meshes/PLACEHOLDER/virtualRouters"},
		{"ListVirtualServices", "GET", "/v20190125/meshes/PLACEHOLDER/virtualServices"},
		{"TagResource", "PUT", "/v20190125/tag"},
		{"UntagResource", "PUT", "/v20190125/untag"},
		{
			"UpdateGatewayRoute",
			"PUT",
			"/v20190125/meshes/PLACEHOLDER/virtualGateway/PLACEHOLDER/gatewayRoutes/PLACEHOLDER",
		},
		{"UpdateMesh", "PUT", "/v20190125/meshes/PLACEHOLDER"},
		{"UpdateRoute", "PUT", "/v20190125/meshes/PLACEHOLDER/virtualRouter/PLACEHOLDER/routes/PLACEHOLDER"},
		{"UpdateVirtualGateway", "PUT", "/v20190125/meshes/PLACEHOLDER/virtualGateways/PLACEHOLDER"},
		{"UpdateVirtualNode", "PUT", "/v20190125/meshes/PLACEHOLDER/virtualNodes/PLACEHOLDER"},
		{"UpdateVirtualRouter", "PUT", "/v20190125/meshes/PLACEHOLDER/virtualRouters/PLACEHOLDER"},
		{"UpdateVirtualService", "PUT", "/v20190125/meshes/PLACEHOLDER/virtualServices/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real App Mesh op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts parseOperation (handler.go) resolves it to the right op, all 38
// ops against appmesh's real op count. It then drives the same request
// through the real Handler() and asserts the response body is not the exact
// JSON literal {"code":"NotFoundException","message":"not found"} that
// every routing-miss default branch emits via errResp("NotFoundException",
// "not found") -- handler.go:162/180, handler_meshes.go:67,
// handler_virtual_routers.go:141/145, handler_virtual_gateways.go:141/145.
//
// A bare substring check on "not found" is NOT safe for this service: every
// legitimate backend not-found error is qualified ("mesh not found",
// "virtual node not found", "virtual router not found", "route not found",
// "virtual service not found", "virtual gateway not found", "gateway route
// not found", "resource not found for tagging" -- see errors.go), so those
// responses legitimately contain the "not found" substring too. Grepping
// every non-test .go file in this package for the bare literal "not found"
// confirms it appears only in the seven routing-miss sites above, never as
// a standalone backend error message, so an exact match on the miss body is
// safe where a substring check would not be.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	const missBody = `{"code":"NotFoundException","message":"not found"}` + "\n"

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotEqual(t, missBody, rec.Body.String(),
				"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
		})
	}
}
