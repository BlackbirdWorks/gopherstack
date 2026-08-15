package codebuild_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real CodeBuild
// operation, extracted from codebuild@v1.72.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("CodeBuild_20161006.<Op>")
// and always POSTs to "/" -- CodeBuild is JSON-RPC 1.1
// (services/_PROTOCOLS.md), so unlike a REST-family service there is no
// path template to get wrong: dispatch is entirely by this one header.
// ExtractOperation and Handler() both derive the action the same way
// (TrimPrefix on "CodeBuild_20161006."), so the class of bug this table can
// catch is a dispatch-table key that doesn't exactly match the real op name
// (typo, wrong case -- CodeBuild is case-sensitive JSON-RPC), not a
// route-template mismatch.
//
// This table covers all 59 real CodeBuild ops, which is also gopherstack's
// full implemented set (h.GetSupportedOperations(), 59/59) as of
// codebuild@v1.72.4 -- confirmed by diffing both GetSupportedOperations()
// and the actual dispatchTable() dispatch table against this exact list,
// zero mismatches either direction: no dead key, no gap.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("CodeBuild_20161006.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"BatchDeleteBuilds", "CodeBuild_20161006.BatchDeleteBuilds"},
		{"BatchGetBuildBatches", "CodeBuild_20161006.BatchGetBuildBatches"},
		{"BatchGetBuilds", "CodeBuild_20161006.BatchGetBuilds"},
		{"BatchGetCommandExecutions", "CodeBuild_20161006.BatchGetCommandExecutions"},
		{"BatchGetFleets", "CodeBuild_20161006.BatchGetFleets"},
		{"BatchGetProjects", "CodeBuild_20161006.BatchGetProjects"},
		{"BatchGetReportGroups", "CodeBuild_20161006.BatchGetReportGroups"},
		{"BatchGetReports", "CodeBuild_20161006.BatchGetReports"},
		{"BatchGetSandboxes", "CodeBuild_20161006.BatchGetSandboxes"},
		{"CreateFleet", "CodeBuild_20161006.CreateFleet"},
		{"CreateProject", "CodeBuild_20161006.CreateProject"},
		{"CreateReportGroup", "CodeBuild_20161006.CreateReportGroup"},
		{"CreateWebhook", "CodeBuild_20161006.CreateWebhook"},
		{"DeleteBuildBatch", "CodeBuild_20161006.DeleteBuildBatch"},
		{"DeleteFleet", "CodeBuild_20161006.DeleteFleet"},
		{"DeleteProject", "CodeBuild_20161006.DeleteProject"},
		{"DeleteReport", "CodeBuild_20161006.DeleteReport"},
		{"DeleteReportGroup", "CodeBuild_20161006.DeleteReportGroup"},
		{"DeleteResourcePolicy", "CodeBuild_20161006.DeleteResourcePolicy"},
		{"DeleteSourceCredentials", "CodeBuild_20161006.DeleteSourceCredentials"},
		{"DeleteWebhook", "CodeBuild_20161006.DeleteWebhook"},
		{"DescribeCodeCoverages", "CodeBuild_20161006.DescribeCodeCoverages"},
		{"DescribeTestCases", "CodeBuild_20161006.DescribeTestCases"},
		{"GetReportGroupTrend", "CodeBuild_20161006.GetReportGroupTrend"},
		{"GetResourcePolicy", "CodeBuild_20161006.GetResourcePolicy"},
		{"ImportSourceCredentials", "CodeBuild_20161006.ImportSourceCredentials"},
		{"InvalidateProjectCache", "CodeBuild_20161006.InvalidateProjectCache"},
		{"ListBuildBatches", "CodeBuild_20161006.ListBuildBatches"},
		{"ListBuildBatchesForProject", "CodeBuild_20161006.ListBuildBatchesForProject"},
		{"ListBuilds", "CodeBuild_20161006.ListBuilds"},
		{"ListBuildsForProject", "CodeBuild_20161006.ListBuildsForProject"},
		{"ListCommandExecutionsForSandbox", "CodeBuild_20161006.ListCommandExecutionsForSandbox"},
		{"ListCuratedEnvironmentImages", "CodeBuild_20161006.ListCuratedEnvironmentImages"},
		{"ListFleets", "CodeBuild_20161006.ListFleets"},
		{"ListProjects", "CodeBuild_20161006.ListProjects"},
		{"ListReportGroups", "CodeBuild_20161006.ListReportGroups"},
		{"ListReports", "CodeBuild_20161006.ListReports"},
		{"ListReportsForReportGroup", "CodeBuild_20161006.ListReportsForReportGroup"},
		{"ListSandboxes", "CodeBuild_20161006.ListSandboxes"},
		{"ListSandboxesForProject", "CodeBuild_20161006.ListSandboxesForProject"},
		{"ListSharedProjects", "CodeBuild_20161006.ListSharedProjects"},
		{"ListSharedReportGroups", "CodeBuild_20161006.ListSharedReportGroups"},
		{"ListSourceCredentials", "CodeBuild_20161006.ListSourceCredentials"},
		{"PutResourcePolicy", "CodeBuild_20161006.PutResourcePolicy"},
		{"RetryBuildBatch", "CodeBuild_20161006.RetryBuildBatch"},
		{"RetryBuild", "CodeBuild_20161006.RetryBuild"},
		{"StartBuildBatch", "CodeBuild_20161006.StartBuildBatch"},
		{"StartBuild", "CodeBuild_20161006.StartBuild"},
		{"StartCommandExecution", "CodeBuild_20161006.StartCommandExecution"},
		{"StartSandbox", "CodeBuild_20161006.StartSandbox"},
		{"StartSandboxConnection", "CodeBuild_20161006.StartSandboxConnection"},
		{"StopBuildBatch", "CodeBuild_20161006.StopBuildBatch"},
		{"StopBuild", "CodeBuild_20161006.StopBuild"},
		{"StopSandbox", "CodeBuild_20161006.StopSandbox"},
		{"UpdateFleet", "CodeBuild_20161006.UpdateFleet"},
		{"UpdateProject", "CodeBuild_20161006.UpdateProject"},
		{"UpdateProjectVisibility", "CodeBuild_20161006.UpdateProjectVisibility"},
		{"UpdateReportGroup", "CodeBuild_20161006.UpdateReportGroup"},
		{"UpdateWebhook", "CodeBuild_20161006.UpdateWebhook"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real CodeBuild operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler()
// does not fall through to the dispatch-miss sentinel a dispatch-table key
// mismatch would produce.
//
// CodeBuild's dispatch-miss sentinel (errUnknownAction, "unknown action")
// is wire-mapped to "InvalidInputException" alongside errInvalidRequest
// (which backs every required-field check in this package) and ErrValidation
// -- see handleError's switch in handler.go. Asserting on that wire type here
// would be the workmail/transfer trap: a false positive on ordinary working
// validation. This test instead asserts on errUnknownAction's own message
// text ("unknown action"), which is unique in the package (grepped) and only
// ever produced by the dispatch miss at dispatch()'s single
// fmt.Errorf("%w: %s", errUnknownAction, action) call site.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
			h := codebuild.NewHandler(backend)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown action",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
