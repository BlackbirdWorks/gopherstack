package appstream_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteOps is the authoritative operation list for AppStream, extracted
// from appstream@v1.64.5 serializers.go: every real op's
// smithyRpcv2cbor_serializeOp<Op>.HandleSerialize sets
// req.URL.Path = cborTestServicePath + "<Op>" and req.Method = POST -- the
// pinned SDK serves AppStream exclusively over rpc-v2-cbor (see
// cborServicePath's doc comment in rpcv2cbor.go), so unlike a REST-family
// service there is no per-op path template to drift, only the literal op
// name in the URL's final segment. Dispatch is a single h.ops map read by
// both RouteMatcher and h.dispatch, so there is no second table to drift
// out of step (gopherstack-jqh2 shape 3): this table instead guards against
// a typo'd map key in one of buildOps' five per-family helpers, which would
// make RouteMatcher reject a request a real client can legitimately send.
//
// Cross-checked 89/89 against h.ops by diffing every "req.URL.Path ="
// literal in serializers.go against every ": h.op" map-literal key across
// handler.go's stackFleetOps/appBlockOps/applicationOps/imageOps/miscOps --
// zero mismatches either direction.
//
// Regenerate by grepping serializers.go for
// `req.URL.Path = "/service/PhotonAdminProxyService/operation/` and pulling
// the trailing op name.
func sdkRouteOps() []string {
	return []string{
		"AssociateAppBlockBuilderAppBlock",
		"AssociateApplicationFleet",
		"AssociateApplicationToEntitlement",
		"AssociateFleet",
		"AssociateSoftwareToImageBuilder",
		"BatchAssociateUserStack",
		"BatchDisassociateUserStack",
		"CopyImage",
		"CreateAppBlock",
		"CreateAppBlockBuilder",
		"CreateAppBlockBuilderStreamingURL",
		"CreateApplication",
		"CreateDirectoryConfig",
		"CreateEntitlement",
		"CreateExportImageTask",
		"CreateFleet",
		"CreateImageBuilder",
		"CreateImageBuilderStreamingURL",
		"CreateImportedImage",
		"CreateStack",
		"CreateStreamingURL",
		"CreateThemeForStack",
		"CreateUpdatedImage",
		"CreateUsageReportSubscription",
		"CreateUser",
		"DeleteAppBlock",
		"DeleteAppBlockBuilder",
		"DeleteApplication",
		"DeleteDirectoryConfig",
		"DeleteEntitlement",
		"DeleteFleet",
		"DeleteImage",
		"DeleteImageBuilder",
		"DeleteImagePermissions",
		"DeleteStack",
		"DeleteThemeForStack",
		"DeleteUsageReportSubscription",
		"DeleteUser",
		"DescribeAppBlockBuilderAppBlockAssociations",
		"DescribeAppBlockBuilders",
		"DescribeAppBlocks",
		"DescribeApplicationFleetAssociations",
		"DescribeApplications",
		"DescribeAppLicenseUsage",
		"DescribeDirectoryConfigs",
		"DescribeEntitlements",
		"DescribeFleets",
		"DescribeImageBuilders",
		"DescribeImagePermissions",
		"DescribeImages",
		"DescribeSessions",
		"DescribeSoftwareAssociations",
		"DescribeStacks",
		"DescribeThemeForStack",
		"DescribeUsageReportSubscriptions",
		"DescribeUsers",
		"DescribeUserStackAssociations",
		"DisableUser",
		"DisassociateAppBlockBuilderAppBlock",
		"DisassociateApplicationFleet",
		"DisassociateApplicationFromEntitlement",
		"DisassociateFleet",
		"DisassociateSoftwareFromImageBuilder",
		"DrainSessionInstance",
		"EnableUser",
		"ExpireSession",
		"GetExportImageTask",
		"ListAssociatedFleets",
		"ListAssociatedStacks",
		"ListEntitledApplications",
		"ListExportImageTasks",
		"ListTagsForResource",
		"StartAppBlockBuilder",
		"StartFleet",
		"StartImageBuilder",
		"StartSoftwareDeploymentToImageBuilder",
		"StopAppBlockBuilder",
		"StopFleet",
		"StopImageBuilder",
		"TagResource",
		"UntagResource",
		"UpdateAppBlockBuilder",
		"UpdateApplication",
		"UpdateDirectoryConfig",
		"UpdateEntitlement",
		"UpdateFleet",
		"UpdateImagePermissions",
		"UpdateStack",
		"UpdateThemeForStack",
	}
}

// TestExtractOperation_SDKRouteTable drives every real AppStream op's
// authoritative rpc-v2-cbor path (see sdkRouteOps) through ExtractOperation
// and RouteMatcher, asserting both the op name and dispatch-table
// membership are correct. gopherstack-jqh2 pass 4: re-extracted all 89
// AppStream ops from the pinned SDK and confirmed the existing h.ops table
// already matches exactly.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, op := range sdkRouteOps() {
		t.Run(strings.ToLower(op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, cborTestServicePath+op, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.True(t, h.RouteMatcher()(c), "op=%s: RouteMatcher rejected a real SDK request", op)
			require.Equal(t, op, h.ExtractOperation(c))
		})
	}
}
