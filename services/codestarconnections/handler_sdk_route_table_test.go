package codestarconnections_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codestarconnections"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real AWS
// CodeStar Connections operation, extracted from
// codestarconnections@v1.38.4 serializers.go: each op's
// awsAwsjson10_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String(
// "CodeStar_connections_20191201.<Op>") and always POSTs to "/" --
// CodeStar Connections is JSON-RPC 1.0 (services/_PROTOCOLS.md), so
// dispatch is entirely by this one header, not a path template.
//
// CodeStarConnections and CodeConnections are the same underlying AWS API
// after a 2023 rename, but they do NOT share a target prefix: this
// service's own pinned SDK gives "CodeStar_connections_20191201", read
// directly from serializers.go here -- distinct from codeconnections's
// own "CodeConnections_20231201" (see that service's own route table),
// confirmed independently rather than assumed either way. Both APIs
// expose the identical 27 operation names, just under different target
// strings and different release dates.
//
// This table covers all 27 real CodeStar Connections ops
// (codestarconnections@v1.38.4) -- confirmed by diffing both
// GetSupportedOperations() and the actual buildOps() map's key set
// against this exact list: zero mismatches in either direction. Both are
// separate hand-maintained literals here (neither is built by ranging
// over the other), so the two diffs are genuinely independent checks.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("CodeStar_connections_20191201.` and
// pulling the suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CreateConnection", "CodeStar_connections_20191201.CreateConnection"},
		{"CreateHost", "CodeStar_connections_20191201.CreateHost"},
		{"CreateRepositoryLink", "CodeStar_connections_20191201.CreateRepositoryLink"},
		{"CreateSyncConfiguration", "CodeStar_connections_20191201.CreateSyncConfiguration"},
		{"DeleteConnection", "CodeStar_connections_20191201.DeleteConnection"},
		{"DeleteHost", "CodeStar_connections_20191201.DeleteHost"},
		{"DeleteRepositoryLink", "CodeStar_connections_20191201.DeleteRepositoryLink"},
		{"DeleteSyncConfiguration", "CodeStar_connections_20191201.DeleteSyncConfiguration"},
		{"GetConnection", "CodeStar_connections_20191201.GetConnection"},
		{"GetHost", "CodeStar_connections_20191201.GetHost"},
		{"GetRepositoryLink", "CodeStar_connections_20191201.GetRepositoryLink"},
		{"GetRepositorySyncStatus", "CodeStar_connections_20191201.GetRepositorySyncStatus"},
		{"GetResourceSyncStatus", "CodeStar_connections_20191201.GetResourceSyncStatus"},
		{"GetSyncBlockerSummary", "CodeStar_connections_20191201.GetSyncBlockerSummary"},
		{"GetSyncConfiguration", "CodeStar_connections_20191201.GetSyncConfiguration"},
		{"ListConnections", "CodeStar_connections_20191201.ListConnections"},
		{"ListHosts", "CodeStar_connections_20191201.ListHosts"},
		{"ListRepositoryLinks", "CodeStar_connections_20191201.ListRepositoryLinks"},
		{"ListRepositorySyncDefinitions", "CodeStar_connections_20191201.ListRepositorySyncDefinitions"},
		{"ListSyncConfigurations", "CodeStar_connections_20191201.ListSyncConfigurations"},
		{"ListTagsForResource", "CodeStar_connections_20191201.ListTagsForResource"},
		{"TagResource", "CodeStar_connections_20191201.TagResource"},
		{"UntagResource", "CodeStar_connections_20191201.UntagResource"},
		{"UpdateHost", "CodeStar_connections_20191201.UpdateHost"},
		{"UpdateRepositoryLink", "CodeStar_connections_20191201.UpdateRepositoryLink"},
		{"UpdateSyncBlocker", "CodeStar_connections_20191201.UpdateSyncBlocker"},
		{"UpdateSyncConfiguration", "CodeStar_connections_20191201.UpdateSyncConfiguration"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real CodeStar
// Connections operation's authoritative X-Amz-Target through
// ExtractOperation and Handler(), asserting the header resolves to the
// right op name and that Handler() does not fall through to h.dispatch's
// unmatched-route branch (fmt.Errorf("%w: %s", errUnknownAction, action),
// handler.go's single production call site).
//
// This asserts on MESSAGE TEXT ("UnknownOperationException: <op>"), not
// wire type: unlike its sibling codeconnections (which shares this exact
// sentinel constructor -- errUnknownAction = awserr.New(
// "UnknownOperationException", awserr.ErrNotFound) -- but gives it a
// dedicated case in resolveErrorType), THIS service's handleError folds
// errUnknownAction into the same case as ErrAlreadyExists, ErrValidation,
// errInvalidRequest and the JSON syntax/type-error branches, all mapping
// to the shared "InvalidInputException". So "UnknownOperationException"
// never appears in the __type field here -- only in the message text,
// which still carries the sentinel's own Error() string
// ("UnknownOperationException", from awserr.New) followed by ": <action>".
// Grepped: no other error path in this package produces that substring.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := codestarconnections.NewHandler(codestarconnections.NewInMemoryBackend("000000000000", "us-east-1"))

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnknownOperationException: "+tc.op,
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
