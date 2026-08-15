package dax_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Amazon DAX
// operation, extracted from dax@v1.32.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("AmazonDAXV3.<Op>")
// and always POSTs to "/" -- DAX is JSON-RPC 1.1 (services/_PROTOCOLS.md),
// so unlike a REST-family service there is no path template to get wrong:
// dispatch is entirely by this one header. ExtractOperation and Handler()
// (via h.dispatch's flat package-level daxOperations map) both derive the
// action the same way (TrimPrefix on "AmazonDAXV3."), so the class of bug
// this table catches is a dispatch-table key that doesn't exactly match the
// real op name (typo, wrong case -- DAX is case-sensitive JSON-RPC), not a
// route-template mismatch.
//
// dax's Handler struct also embeds a live *DataPlane (the binary-protocol
// data-plane listener in dataplane_server.go) -- confirmed by reading
// handler.go: that field is a persistence/lifecycle concern (Snapshot/
// Restore, StartWorker-style listener management), entirely orthogonal to
// the X-Amz-Target dispatch this table exercises. NewHandler leaves
// DataPlane nil unless a caller wires one up separately, and neither
// ExtractOperation nor dispatch() ever reads it -- so it does not matter
// for routing, the same conclusion the task names for
// apigatewaymanagementapi's analogous flag.
//
// This table covers all 21 real DAX ops (dax@v1.32.4) -- confirmed by
// diffing this SDK-extracted list against both GetSupportedOperations() (a
// hand-written literal) and the actual daxOperations dispatch map (also a
// hand-written literal, not built by ranging over anything): zero
// mismatches in either direction.
//
// EXCLUDED: daxOperations also wires "ResetParameterGroup" to
// handleResetParameterGroup, but GetSupportedOperations()'s own comment
// documents it is NOT a real DAX SDK operation (verified against
// botocore's dax service-2.json -- no such action exists) and deliberately
// leaves it off the advertised list. Confirmed here too: no
// "ResetParameterGroup" X-Amz-Target exists anywhere in the pinned SDK's
// serializers.go. It is excluded from this table rather than tabled as if
// it were a real route, the same treatment shield gives its
// "__SimulateAttack" test hook.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AmazonDAXV3.` and pulling the suffix
// after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CreateCluster", "AmazonDAXV3.CreateCluster"},
		{"CreateParameterGroup", "AmazonDAXV3.CreateParameterGroup"},
		{"CreateSubnetGroup", "AmazonDAXV3.CreateSubnetGroup"},
		{"DecreaseReplicationFactor", "AmazonDAXV3.DecreaseReplicationFactor"},
		{"DeleteCluster", "AmazonDAXV3.DeleteCluster"},
		{"DeleteParameterGroup", "AmazonDAXV3.DeleteParameterGroup"},
		{"DeleteSubnetGroup", "AmazonDAXV3.DeleteSubnetGroup"},
		{"DescribeClusters", "AmazonDAXV3.DescribeClusters"},
		{"DescribeDefaultParameters", "AmazonDAXV3.DescribeDefaultParameters"},
		{"DescribeEvents", "AmazonDAXV3.DescribeEvents"},
		{"DescribeParameterGroups", "AmazonDAXV3.DescribeParameterGroups"},
		{"DescribeParameters", "AmazonDAXV3.DescribeParameters"},
		{"DescribeSubnetGroups", "AmazonDAXV3.DescribeSubnetGroups"},
		{"IncreaseReplicationFactor", "AmazonDAXV3.IncreaseReplicationFactor"},
		{"ListTags", "AmazonDAXV3.ListTags"},
		{"RebootNode", "AmazonDAXV3.RebootNode"},
		{"TagResource", "AmazonDAXV3.TagResource"},
		{"UntagResource", "AmazonDAXV3.UntagResource"},
		{"UpdateCluster", "AmazonDAXV3.UpdateCluster"},
		{"UpdateParameterGroup", "AmazonDAXV3.UpdateParameterGroup"},
		{"UpdateSubnetGroup", "AmazonDAXV3.UpdateSubnetGroup"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real DAX operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler()
// does not fall through to h.dispatch's single unmatched-route return
// (fmt.Errorf("%w: %s", errUnknownAction, operation), handler.go's
// dispatch() single production call site).
//
// Unlike most of this campaign's tables, DAX's dispatch-miss sentinel maps
// to a wire type ("InvalidAction", via daxErrCodeMappings) that is NOT
// shared with any other mapped error in this package -- grepped: the
// literal `"InvalidAction"` appears in exactly one daxErrCodeMappings entry
// (errUnknownAction) plus one other production site (the missing-header
// branch in Handler(), a different miss path that never fires here since
// every case below sets a real target). So asserting on the wire __type is
// safe here, unlike ce/support/timestreamwrite/directconnect in this same
// pass, where the miss sentinel's type is shared with ordinary validation
// errors.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := dax.NewHandler(dax.NewInMemoryBackend("123456789012", "us-east-1"))

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "InvalidAction",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
