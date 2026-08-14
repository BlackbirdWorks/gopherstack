package account_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real AWS Account
// Management operation, extracted from account@v1.35.4 serializers.go: each
// entry's "request.Method" and the string passed to httpbinding.SplitURI in
// that op's awsRestjson1_serializeOp<Op>.HandleSerialize. Every operation is
// POST to a fixed, parameter-free path -- AccountId and every other input
// member travel in the JSON body, never as a URI label or query string (see
// handler.go's package doc comment) -- so unlike most REST-JSON services in
// this campaign, no PLACEHOLDER is needed anywhere in this table. 16 real
// ops here, matching Account's real op count exactly (also matches
// GetSupportedOperations's own 16 entries one-for-one).
//
// A systematic check for a shared method+path across all 16 ops found zero
// collisions -- every op has its own unique static path, so no *required
// dynamic* (non-template) member -- the s3/glacier vacuity-trap class -- was
// needed to disambiguate any route in this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AcceptPrimaryEmailUpdate", "POST", "/acceptPrimaryEmailUpdate"},
		{"DeleteAlternateContact", "POST", "/deleteAlternateContact"},
		{"DisableRegion", "POST", "/disableRegion"},
		{"EnableRegion", "POST", "/enableRegion"},
		{"GetAccountInformation", "POST", "/getAccountInformation"},
		{"GetAlternateContact", "POST", "/getAlternateContact"},
		{"GetContactInformation", "POST", "/getContactInformation"},
		{"GetGovCloudAccountInformation", "POST", "/getGovCloudAccountInformation"},
		{"GetPrimaryEmail", "POST", "/getPrimaryEmail"},
		{"GetPrimaryEmailUpdateStatus", "POST", "/getPrimaryEmailUpdateStatus"},
		{"GetRegionOptStatus", "POST", "/getRegionOptStatus"},
		{"ListRegions", "POST", "/listRegions"},
		{"PutAccountName", "POST", "/putAccountName"},
		{"PutAlternateContact", "POST", "/putAlternateContact"},
		{"PutContactInformation", "POST", "/putContactInformation"},
		{"StartPrimaryEmailUpdate", "POST", "/startPrimaryEmailUpdate"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Account op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the operationNames lookup (handler.go) resolves it to the right
// op, all 16 ops against Account's real op count. It then drives the same
// request through the real Handler() and asserts the response does not
// contain the exact literal "unsupported operation" that route's
// dispatch-miss branch (handler.go:194) emits under InvalidAction with HTTP
// 404 when operationHandlers has no entry for the path.
//
// "unsupported operation" was grepped across every non-test .go file in
// this package and found nowhere else: every domain error instead routes
// through writeBackendError, whose messages are err.Error() on this
// package's own errors.go sentinels (e.g. "ResourceNotFoundException: no
// alternate contact found"), none of which contain that two-word literal.
// Note route's *other* miss branch -- an unsupported HTTP method, "unsupported
// method" (handler.go:189) -- is a different literal and isn't reachable by
// any of these cases, since every real op here is POST-only.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unsupported operation",
				"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
		})
	}
}
