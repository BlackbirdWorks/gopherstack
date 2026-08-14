package fis_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real FIS
// operation, extracted from fis@v1.40.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {id}/{experimentId}/{experimentTemplateId}/{accountId}/
// {resourceArn}/{resourceType} URI label -- parseFISPath and its per-family
// helpers (handler.go) never validate identifier shape, so the literal
// value doesn't matter here, only path depth and static segments. 26 real
// ops here, matching FIS's real op count exactly (also matches
// GetSupportedOperations's own 26 entries one-for-one). The handler also
// accepts a second, non-canonical "POST /experiments/{id}/stop" route to
// StopExperiment (parseFISExperimentSubPath) alongside the SDK's real
// "DELETE /experiments/{id}" -- only the real SDK route is tabled here,
// since the table's job is to prove the SDK's own routes dispatch
// correctly, not to enumerate every route the handler happens to accept.
//
// A systematic check for a shared method+path across all 26 ops found zero
// collisions -- every op has its own unique (method, path) pair, so no
// *required dynamic* (non-template) member -- the s3/glacier vacuity-trap
// class -- was needed to disambiguate any route in this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CreateExperimentTemplate", "POST", "/experimentTemplates"},
		{
			"CreateTargetAccountConfiguration",
			"POST",
			"/experimentTemplates/PLACEHOLDER/targetAccountConfigurations/PLACEHOLDER",
		},
		{"DeleteExperimentTemplate", "DELETE", "/experimentTemplates/PLACEHOLDER"},
		{
			"DeleteTargetAccountConfiguration",
			"DELETE",
			"/experimentTemplates/PLACEHOLDER/targetAccountConfigurations/PLACEHOLDER",
		},
		{"GetAction", "GET", "/actions/PLACEHOLDER"},
		{"GetExperiment", "GET", "/experiments/PLACEHOLDER"},
		{
			"GetExperimentTargetAccountConfiguration",
			"GET",
			"/experiments/PLACEHOLDER/targetAccountConfigurations/PLACEHOLDER",
		},
		{"GetExperimentTemplate", "GET", "/experimentTemplates/PLACEHOLDER"},
		{"GetSafetyLever", "GET", "/safetyLevers/PLACEHOLDER"},
		{
			"GetTargetAccountConfiguration",
			"GET",
			"/experimentTemplates/PLACEHOLDER/targetAccountConfigurations/PLACEHOLDER",
		},
		{"GetTargetResourceType", "GET", "/targetResourceTypes/PLACEHOLDER"},
		{"ListActions", "GET", "/actions"},
		{"ListExperimentResolvedTargets", "GET", "/experiments/PLACEHOLDER/resolvedTargets"},
		{"ListExperiments", "GET", "/experiments"},
		{"ListExperimentTargetAccountConfigurations", "GET", "/experiments/PLACEHOLDER/targetAccountConfigurations"},
		{"ListExperimentTemplates", "GET", "/experimentTemplates"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"ListTargetAccountConfigurations", "GET", "/experimentTemplates/PLACEHOLDER/targetAccountConfigurations"},
		{"ListTargetResourceTypes", "GET", "/targetResourceTypes"},
		{"StartExperiment", "POST", "/experiments"},
		{"StopExperiment", "DELETE", "/experiments/PLACEHOLDER"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateExperimentTemplate", "PATCH", "/experimentTemplates/PLACEHOLDER"},
		{"UpdateSafetyLeverState", "PATCH", "/safetyLevers/PLACEHOLDER/state"},
		{
			"UpdateTargetAccountConfiguration",
			"PATCH",
			"/experimentTemplates/PLACEHOLDER/targetAccountConfigurations/PLACEHOLDER",
		},
	}
}

// TestExtractOperation_SDKRouteTable drives every real FIS op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts parseFISPath (handler.go) resolves it to the right op, all 26 ops
// against FIS's real op count. It then drives the same request through the
// real Handler() and asserts the response does not contain the exact
// literal "not found" that Handler() emits via writeError(c,
// http.StatusNotFound, "not found", "") when parseFISPath returns an empty
// op.
//
// "not found" was grepped across every non-test .go file in this package
// and found nowhere else: every domain not-found sentinel in errors.go
// (ErrTemplateNotFound, ErrExperimentNotFound, ErrActionNotFound, etc.) has
// an err.Error() built from a single CamelCase token like
// "ExperimentTemplateNotFound", none of which contain the space-separated
// literal "not found". dispatch()'s own unknown-op branch ("unknown
// operation: "+op) is a second miss text, but it is unreachable from any
// HTTP request -- parseFISPath only ever returns a known op constant or "",
// and the "" case is caught by Handler() before dispatch() is called.
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
			assert.NotContains(t, rec.Body.String(), "not found",
				"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
		})
	}
}
