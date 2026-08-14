package codeconnections_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeconnections"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real AWS
// CodeConnections operation, extracted from codeconnections@v1.13.4
// serializers.go: each op's awsAwsjson10_serializeOp<Op>.HandleSerialize
// sets httpBindingEncoder.SetHeader("X-Amz-Target").String(
// "CodeConnections_20231201.<Op>") and always POSTs to "/" --
// CodeConnections is JSON-RPC 1.0 (services/_PROTOCOLS.md), so dispatch is
// entirely by this one header, not a path template.
//
// CodeConnections and CodeStarConnections are the same underlying AWS API
// after a 2023 rename, but they do NOT share a target prefix: this
// service's own pinned SDK gives "CodeConnections_20231201", read directly
// from serializers.go here -- distinct from codestarconnections's own
// "CodeStar_connections_20191201" (see that service's own route table),
// confirmed independently rather than assumed either way. Both APIs
// expose the identical 27 operation names, just under different target
// strings and different release dates.
//
// This table covers all 27 real CodeConnections ops
// (codeconnections@v1.13.4) -- confirmed by diffing both
// GetSupportedOperations() and the actual buildOps() map's key set
// against this exact list: zero mismatches in either direction. Both are
// separate hand-maintained literals here (neither is built by ranging
// over the other), so the two diffs are genuinely independent checks.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("CodeConnections_20231201.` and
// pulling the suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CreateConnection", "CodeConnections_20231201.CreateConnection"},
		{"CreateHost", "CodeConnections_20231201.CreateHost"},
		{"CreateRepositoryLink", "CodeConnections_20231201.CreateRepositoryLink"},
		{"CreateSyncConfiguration", "CodeConnections_20231201.CreateSyncConfiguration"},
		{"DeleteConnection", "CodeConnections_20231201.DeleteConnection"},
		{"DeleteHost", "CodeConnections_20231201.DeleteHost"},
		{"DeleteRepositoryLink", "CodeConnections_20231201.DeleteRepositoryLink"},
		{"DeleteSyncConfiguration", "CodeConnections_20231201.DeleteSyncConfiguration"},
		{"GetConnection", "CodeConnections_20231201.GetConnection"},
		{"GetHost", "CodeConnections_20231201.GetHost"},
		{"GetRepositoryLink", "CodeConnections_20231201.GetRepositoryLink"},
		{"GetRepositorySyncStatus", "CodeConnections_20231201.GetRepositorySyncStatus"},
		{"GetResourceSyncStatus", "CodeConnections_20231201.GetResourceSyncStatus"},
		{"GetSyncBlockerSummary", "CodeConnections_20231201.GetSyncBlockerSummary"},
		{"GetSyncConfiguration", "CodeConnections_20231201.GetSyncConfiguration"},
		{"ListConnections", "CodeConnections_20231201.ListConnections"},
		{"ListHosts", "CodeConnections_20231201.ListHosts"},
		{"ListRepositoryLinks", "CodeConnections_20231201.ListRepositoryLinks"},
		{"ListRepositorySyncDefinitions", "CodeConnections_20231201.ListRepositorySyncDefinitions"},
		{"ListSyncConfigurations", "CodeConnections_20231201.ListSyncConfigurations"},
		{"ListTagsForResource", "CodeConnections_20231201.ListTagsForResource"},
		{"TagResource", "CodeConnections_20231201.TagResource"},
		{"UntagResource", "CodeConnections_20231201.UntagResource"},
		{"UpdateHost", "CodeConnections_20231201.UpdateHost"},
		{"UpdateRepositoryLink", "CodeConnections_20231201.UpdateRepositoryLink"},
		{"UpdateSyncBlocker", "CodeConnections_20231201.UpdateSyncBlocker"},
		{"UpdateSyncConfiguration", "CodeConnections_20231201.UpdateSyncConfiguration"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real CodeConnections
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to h.dispatch's unmatched-route branch
// (fmt.Errorf("%w: %s", errUnknownAction, action), handler.go's single
// production call site).
//
// This asserts on WIRE TYPE ("UnknownOperationException"), unlike most
// route tables in this campaign: resolveErrorType gives errUnknownAction
// its own dedicated case, distinct from ErrValidation's
// "InvalidInputException" -- "UnknownOperationException" appears nowhere
// else in this package (grepped), so it uniquely identifies a dispatch
// miss. (codestarconnections, the sibling service sharing this exact
// sentinel constructor, does NOT keep this distinction -- its handleError
// folds errUnknownAction into the same case as ErrValidation, so its own
// route table must assert on message text instead; see that service's
// comment for the contrast.)
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := codeconnections.NewHandler(codeconnections.NewInMemoryBackend("000000000000", "us-east-1"))

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
