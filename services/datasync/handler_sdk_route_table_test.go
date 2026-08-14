package datasync_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/datasync"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real AWS
// DataSync operation, extracted from datasync@v1.61.4 serializers.go: each
// op's awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("FmrsService.<Op>")
// and always request.Request.Method = "POST" against path "/" -- DataSync
// is JSON-RPC 1.1 (services/_PROTOCOLS.md), so unlike a REST-family service
// there is no path template to get wrong: dispatch is entirely by this one
// header. ExtractOperation and the shared pkgs/service.HandleTarget both
// derive the action the same way (split on "."), so the class of bug this
// table can catch is a dispatch-table key that doesn't exactly match the
// real op name (typo, wrong case -- DataSync is case-sensitive JSON-RPC),
// not a route-template mismatch.
//
// This table covers all 53 real DataSync ops -- confirmed by diffing both
// GetSupportedOperations() and the actual buildOps() dispatch map against
// this exact list: zero mismatches in either direction, no dead or excluded
// keys.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("FmrsService.` and pulling the suffix
// after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CancelTaskExecution", "FmrsService.CancelTaskExecution"},
		{"CreateAgent", "FmrsService.CreateAgent"},
		{"CreateLocationAzureBlob", "FmrsService.CreateLocationAzureBlob"},
		{"CreateLocationEfs", "FmrsService.CreateLocationEfs"},
		{"CreateLocationFsxLustre", "FmrsService.CreateLocationFsxLustre"},
		{"CreateLocationFsxOntap", "FmrsService.CreateLocationFsxOntap"},
		{"CreateLocationFsxOpenZfs", "FmrsService.CreateLocationFsxOpenZfs"},
		{"CreateLocationFsxWindows", "FmrsService.CreateLocationFsxWindows"},
		{"CreateLocationHdfs", "FmrsService.CreateLocationHdfs"},
		{"CreateLocationNfs", "FmrsService.CreateLocationNfs"},
		{"CreateLocationObjectStorage", "FmrsService.CreateLocationObjectStorage"},
		{"CreateLocationS3", "FmrsService.CreateLocationS3"},
		{"CreateLocationSmb", "FmrsService.CreateLocationSmb"},
		{"CreateTask", "FmrsService.CreateTask"},
		{"DeleteAgent", "FmrsService.DeleteAgent"},
		{"DeleteLocation", "FmrsService.DeleteLocation"},
		{"DeleteTask", "FmrsService.DeleteTask"},
		{"DescribeAgent", "FmrsService.DescribeAgent"},
		{"DescribeLocationAzureBlob", "FmrsService.DescribeLocationAzureBlob"},
		{"DescribeLocationEfs", "FmrsService.DescribeLocationEfs"},
		{"DescribeLocationFsxLustre", "FmrsService.DescribeLocationFsxLustre"},
		{"DescribeLocationFsxOntap", "FmrsService.DescribeLocationFsxOntap"},
		{"DescribeLocationFsxOpenZfs", "FmrsService.DescribeLocationFsxOpenZfs"},
		{"DescribeLocationFsxWindows", "FmrsService.DescribeLocationFsxWindows"},
		{"DescribeLocationHdfs", "FmrsService.DescribeLocationHdfs"},
		{"DescribeLocationNfs", "FmrsService.DescribeLocationNfs"},
		{"DescribeLocationObjectStorage", "FmrsService.DescribeLocationObjectStorage"},
		{"DescribeLocationS3", "FmrsService.DescribeLocationS3"},
		{"DescribeLocationSmb", "FmrsService.DescribeLocationSmb"},
		{"DescribeTask", "FmrsService.DescribeTask"},
		{"DescribeTaskExecution", "FmrsService.DescribeTaskExecution"},
		{"ListAgents", "FmrsService.ListAgents"},
		{"ListLocations", "FmrsService.ListLocations"},
		{"ListTagsForResource", "FmrsService.ListTagsForResource"},
		{"ListTaskExecutions", "FmrsService.ListTaskExecutions"},
		{"ListTasks", "FmrsService.ListTasks"},
		{"StartTaskExecution", "FmrsService.StartTaskExecution"},
		{"TagResource", "FmrsService.TagResource"},
		{"UntagResource", "FmrsService.UntagResource"},
		{"UpdateAgent", "FmrsService.UpdateAgent"},
		{"UpdateLocationAzureBlob", "FmrsService.UpdateLocationAzureBlob"},
		{"UpdateLocationEfs", "FmrsService.UpdateLocationEfs"},
		{"UpdateLocationFsxLustre", "FmrsService.UpdateLocationFsxLustre"},
		{"UpdateLocationFsxOntap", "FmrsService.UpdateLocationFsxOntap"},
		{"UpdateLocationFsxOpenZfs", "FmrsService.UpdateLocationFsxOpenZfs"},
		{"UpdateLocationFsxWindows", "FmrsService.UpdateLocationFsxWindows"},
		{"UpdateLocationHdfs", "FmrsService.UpdateLocationHdfs"},
		{"UpdateLocationNfs", "FmrsService.UpdateLocationNfs"},
		{"UpdateLocationObjectStorage", "FmrsService.UpdateLocationObjectStorage"},
		{"UpdateLocationS3", "FmrsService.UpdateLocationS3"},
		{"UpdateLocationSmb", "FmrsService.UpdateLocationSmb"},
		{"UpdateTask", "FmrsService.UpdateTask"},
		{"UpdateTaskExecution", "FmrsService.UpdateTaskExecution"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real DataSync operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler()
// does not fall through to the dispatch-miss sentinel a dispatch-table key
// mismatch would produce.
//
// DataSync's dispatch-miss sentinel (errUnknownAction, handler.go:28) is
// wire-mapped to "InvalidRequestException" -- the SAME wire type ordinary
// validation errors, JSON syntax/type errors, and awserr.ErrInvalidParameter
// all share in handleError's switch (handler.go:220-249). Asserting on that
// shared wire type would be the workmail/transfer trap exactly: this table's
// all-empty-body ({}) requests already fail validation for most ops (missing
// required fields), which also produces "InvalidRequestException". So this
// test asserts on errUnknownAction's own message text ("unknown action: ")
// instead, which is unique to the dispatch() miss (grepped, single
// production call site at handler.go:209).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := datasync.NewInMemoryBackend("111122223333", "us-east-1")
			h := datasync.NewHandler(backend)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown action:",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
