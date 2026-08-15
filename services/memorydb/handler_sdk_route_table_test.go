package memorydb_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real MemoryDB
// operation, extracted from memorydb@v1.36.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("AmazonMemoryDB.<Op>")
// and always POSTs to "/" -- MemoryDB is JSON-RPC 1.1 (services/_PROTOCOLS.md),
// so unlike a REST-family service there is no path template to get wrong:
// dispatch is entirely by this one header. ExtractOperation and Handler()
// both derive the action the same way (TrimPrefix on "AmazonMemoryDB."), so
// the class of bug this table can catch is a dispatch-table key that doesn't
// exactly match the real op name (typo, wrong case -- MemoryDB is
// case-sensitive JSON-RPC), not a route-template mismatch.
//
// This table covers all 45 real MemoryDB ops, which is also
// gopherstack's full implemented set (h.GetSupportedOperations(), 45/45)
// as of memorydb@v1.36.4 -- confirmed by diffing GetSupportedOperations()
// against this exact list, zero mismatches either direction.
//
// One dispatch-table key was found and deliberately excluded from this
// table: "ExportSnapshot" is wired in memorydbCoreOps'
// dispatchSnapshotAndEngineOps switch (handler.go) and is dispatchable, but
// it is not a real MemoryDB SDK operation -- confirmed against botocore's
// memorydb service-2.json, whose snapshot family is only
// CopySnapshot/CreateSnapshot/DeleteSnapshot/DescribeSnapshots; MemoryDB has
// no export-to-S3 API at all. This is already documented in handler.go's
// GetSupportedOperations() comment as deliberate internal test scaffolding,
// unreachable by any real client since MemoryDB dispatches purely by
// X-Amz-Target header value. Recorded here rather than "fixed".
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AmazonMemoryDB.` and pulling the
// suffix after the dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"BatchUpdateCluster", "AmazonMemoryDB.BatchUpdateCluster"},
		{"CopySnapshot", "AmazonMemoryDB.CopySnapshot"},
		{"CreateACL", "AmazonMemoryDB.CreateACL"},
		{"CreateCluster", "AmazonMemoryDB.CreateCluster"},
		{"CreateMultiRegionCluster", "AmazonMemoryDB.CreateMultiRegionCluster"},
		{"CreateParameterGroup", "AmazonMemoryDB.CreateParameterGroup"},
		{"CreateSnapshot", "AmazonMemoryDB.CreateSnapshot"},
		{"CreateSubnetGroup", "AmazonMemoryDB.CreateSubnetGroup"},
		{"CreateUser", "AmazonMemoryDB.CreateUser"},
		{"DeleteACL", "AmazonMemoryDB.DeleteACL"},
		{"DeleteCluster", "AmazonMemoryDB.DeleteCluster"},
		{"DeleteMultiRegionCluster", "AmazonMemoryDB.DeleteMultiRegionCluster"},
		{"DeleteParameterGroup", "AmazonMemoryDB.DeleteParameterGroup"},
		{"DeleteSnapshot", "AmazonMemoryDB.DeleteSnapshot"},
		{"DeleteSubnetGroup", "AmazonMemoryDB.DeleteSubnetGroup"},
		{"DeleteUser", "AmazonMemoryDB.DeleteUser"},
		{"DescribeACLs", "AmazonMemoryDB.DescribeACLs"},
		{"DescribeClusters", "AmazonMemoryDB.DescribeClusters"},
		{"DescribeEngineVersions", "AmazonMemoryDB.DescribeEngineVersions"},
		{"DescribeEvents", "AmazonMemoryDB.DescribeEvents"},
		{"DescribeMultiRegionClusters", "AmazonMemoryDB.DescribeMultiRegionClusters"},
		{"DescribeMultiRegionParameterGroups", "AmazonMemoryDB.DescribeMultiRegionParameterGroups"},
		{"DescribeMultiRegionParameters", "AmazonMemoryDB.DescribeMultiRegionParameters"},
		{"DescribeParameterGroups", "AmazonMemoryDB.DescribeParameterGroups"},
		{"DescribeParameters", "AmazonMemoryDB.DescribeParameters"},
		{"DescribeReservedNodes", "AmazonMemoryDB.DescribeReservedNodes"},
		{"DescribeReservedNodesOfferings", "AmazonMemoryDB.DescribeReservedNodesOfferings"},
		{"DescribeServiceUpdates", "AmazonMemoryDB.DescribeServiceUpdates"},
		{"DescribeSnapshots", "AmazonMemoryDB.DescribeSnapshots"},
		{"DescribeSubnetGroups", "AmazonMemoryDB.DescribeSubnetGroups"},
		{"DescribeUsers", "AmazonMemoryDB.DescribeUsers"},
		{"FailoverShard", "AmazonMemoryDB.FailoverShard"},
		{"ListAllowedMultiRegionClusterUpdates", "AmazonMemoryDB.ListAllowedMultiRegionClusterUpdates"},
		{"ListAllowedNodeTypeUpdates", "AmazonMemoryDB.ListAllowedNodeTypeUpdates"},
		{"ListTags", "AmazonMemoryDB.ListTags"},
		{"PurchaseReservedNodesOffering", "AmazonMemoryDB.PurchaseReservedNodesOffering"},
		{"ResetParameterGroup", "AmazonMemoryDB.ResetParameterGroup"},
		{"TagResource", "AmazonMemoryDB.TagResource"},
		{"UntagResource", "AmazonMemoryDB.UntagResource"},
		{"UpdateACL", "AmazonMemoryDB.UpdateACL"},
		{"UpdateCluster", "AmazonMemoryDB.UpdateCluster"},
		{"UpdateMultiRegionCluster", "AmazonMemoryDB.UpdateMultiRegionCluster"},
		{"UpdateParameterGroup", "AmazonMemoryDB.UpdateParameterGroup"},
		{"UpdateSubnetGroup", "AmazonMemoryDB.UpdateSubnetGroup"},
		{"UpdateUser", "AmazonMemoryDB.UpdateUser"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real MemoryDB operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler() does
// not fall through to the "UnknownOperationException" sentinel (handler.go's
// dispatch() miss, its sole production call site) that a dispatch-table key
// mismatch would produce.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnknownOperationException",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
