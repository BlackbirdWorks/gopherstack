package lakeformation_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real Lake
// Formation operation, extracted from lakeformation@v1.50.4 serializers.go:
// each entry's "request.Method" and the string passed to
// httpbinding.SplitURI in that op's
// awsRestjson1_serializeOp<Op>.HandleSerialize. Lake Formation's real API
// uses a static literal "/<OperationName>" path per op (verified directly
// in serializers.go, not an artifact of this extraction) -- every entry
// below is POST to its own op name, no PLACEHOLDER segments exist in this
// service.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AddLFTagsToResource", "POST", "/AddLFTagsToResource"},
		{"AssumeDecoratedRoleWithSAML", "POST", "/AssumeDecoratedRoleWithSAML"},
		{"BatchGrantPermissions", "POST", "/BatchGrantPermissions"},
		{"BatchRevokePermissions", "POST", "/BatchRevokePermissions"},
		{"CancelTransaction", "POST", "/CancelTransaction"},
		{"CommitTransaction", "POST", "/CommitTransaction"},
		{"CreateDataCellsFilter", "POST", "/CreateDataCellsFilter"},
		{"CreateLFTag", "POST", "/CreateLFTag"},
		{"CreateLFTagExpression", "POST", "/CreateLFTagExpression"},
		{"CreateLakeFormationIdentityCenterConfiguration", "POST", "/CreateLakeFormationIdentityCenterConfiguration"},
		{"CreateLakeFormationOptIn", "POST", "/CreateLakeFormationOptIn"},
		{"DeleteDataCellsFilter", "POST", "/DeleteDataCellsFilter"},
		{"DeleteLFTag", "POST", "/DeleteLFTag"},
		{"DeleteLFTagExpression", "POST", "/DeleteLFTagExpression"},
		{"DeleteLakeFormationIdentityCenterConfiguration", "POST", "/DeleteLakeFormationIdentityCenterConfiguration"},
		{"DeleteLakeFormationOptIn", "POST", "/DeleteLakeFormationOptIn"},
		{"DeleteObjectsOnCancel", "POST", "/DeleteObjectsOnCancel"},
		{"DeregisterResource", "POST", "/DeregisterResource"},
		{
			"DescribeLakeFormationIdentityCenterConfiguration", "POST",
			"/DescribeLakeFormationIdentityCenterConfiguration",
		},
		{"DescribeResource", "POST", "/DescribeResource"},
		{"DescribeTransaction", "POST", "/DescribeTransaction"},
		{"ExtendTransaction", "POST", "/ExtendTransaction"},
		{"GetDataCellsFilter", "POST", "/GetDataCellsFilter"},
		{"GetDataLakePrincipal", "POST", "/GetDataLakePrincipal"},
		{"GetDataLakeSettings", "POST", "/GetDataLakeSettings"},
		{"GetEffectivePermissionsForPath", "POST", "/GetEffectivePermissionsForPath"},
		{"GetLFTag", "POST", "/GetLFTag"},
		{"GetLFTagExpression", "POST", "/GetLFTagExpression"},
		{"GetQueryState", "POST", "/GetQueryState"},
		{"GetQueryStatistics", "POST", "/GetQueryStatistics"},
		{"GetResourceLFTags", "POST", "/GetResourceLFTags"},
		{"GetTableObjects", "POST", "/GetTableObjects"},
		{"GetTemporaryDataLocationCredentials", "POST", "/GetTemporaryDataLocationCredentials"},
		{"GetTemporaryGluePartitionCredentials", "POST", "/GetTemporaryGluePartitionCredentials"},
		{"GetTemporaryGlueTableCredentials", "POST", "/GetTemporaryGlueTableCredentials"},
		{"GetWorkUnitResults", "POST", "/GetWorkUnitResults"},
		{"GetWorkUnits", "POST", "/GetWorkUnits"},
		{"GrantPermissions", "POST", "/GrantPermissions"},
		{"ListDataCellsFilter", "POST", "/ListDataCellsFilter"},
		{"ListLFTagExpressions", "POST", "/ListLFTagExpressions"},
		{"ListLFTags", "POST", "/ListLFTags"},
		{"ListLakeFormationOptIns", "POST", "/ListLakeFormationOptIns"},
		{"ListPermissions", "POST", "/ListPermissions"},
		{"ListResources", "POST", "/ListResources"},
		{"ListTableStorageOptimizers", "POST", "/ListTableStorageOptimizers"},
		{"ListTransactions", "POST", "/ListTransactions"},
		{"PutDataLakeSettings", "POST", "/PutDataLakeSettings"},
		{"RegisterResource", "POST", "/RegisterResource"},
		{"RemoveLFTagsFromResource", "POST", "/RemoveLFTagsFromResource"},
		{"RevokePermissions", "POST", "/RevokePermissions"},
		{"SearchDatabasesByLFTags", "POST", "/SearchDatabasesByLFTags"},
		{"SearchTablesByLFTags", "POST", "/SearchTablesByLFTags"},
		{"StartQueryPlanning", "POST", "/StartQueryPlanning"},
		{"StartTransaction", "POST", "/StartTransaction"},
		{"UpdateDataCellsFilter", "POST", "/UpdateDataCellsFilter"},
		{"UpdateLFTag", "POST", "/UpdateLFTag"},
		{"UpdateLFTagExpression", "POST", "/UpdateLFTagExpression"},
		{"UpdateLakeFormationIdentityCenterConfiguration", "POST", "/UpdateLakeFormationIdentityCenterConfiguration"},
		{"UpdateResource", "POST", "/UpdateResource"},
		{"UpdateTableObjects", "POST", "/UpdateTableObjects"},
		{"UpdateTableStorageOptimizer", "POST", "/UpdateTableStorageOptimizer"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Lake Formation op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts it resolves to the right op. gopherstack-jqh2 pass 3: re-extracted
// all 61 lakeformation ops from the pinned SDK and confirmed all three of
// this service's op-name tables (RouteMatcher's isLakeFormationPath switch,
// buildOps' dispatch map, GetSupportedOperations' advertised list) already
// match the real op set exactly -- no drift between them.
//
// It then drives the same request through the real Handler() and asserts it
// did not fall through to the "unknown operation: " InvalidInputException
// that dispatch's map-lookup miss emits (handler.go:321-328) -- guarding
// against an operation name that resolves correctly but has no entry in
// h.ops (gopherstack-ey26).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown operation: ",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
