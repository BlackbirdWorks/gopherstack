package eventbridge_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// schemasSDKRouteCases is the authoritative method+path for every real
// Schemas operation this package routes, extracted from schemas@v1.37.4
// serializers.go: each op's awsRestjson1_serializeOp<Op>.HandleSerialize
// sets request.Method and calls httpbinding.SplitURI with the literal
// string below. PLACEHOLDER stands in for a {RegistryName}/{SchemaName}/
// {Language}/{SchemaVersion} URI label -- parseSchemasPath
// (handler_schemas_rest.go) never validates identifier shape, only path
// depth and static segments, so the literal value doesn't matter here.
//
// All 17 of Schemas' REST-JSON1 ops that this package's dispatch table also
// exposes under the fabricated JSON-RPC convention (see
// handler_sdk_route_table_test.go) are covered. schemas@v1.37.4 has 12
// further real ops (CreateDiscoverer, DeleteDiscoverer, DescribeDiscoverer,
// ListDiscoverers, StartDiscoverer, StopDiscoverer, UpdateDiscoverer,
// ExportSchema, GetResourcePolicy, PutResourcePolicy, DeleteResourcePolicy,
// ListTagsForResource/TagResource/UntagResource) that this package's
// dispatch table has no entry for at all under either transport -- those
// are out of scope for gopherstack-92ft (which is about making already-wired
// but unreachable ops reachable, not adding new ones) and are not claimed
// here.
//
// A systematic check of the 6 distinct path templates below against every
// other RouteMatcher in the repo found exactly one textual overlap: Batch's
// blanket "/v1/" prefix (services/batch/handler.go). See
// handler_dispatch.go's RouteMatcher doc comment for why that overlap does
// not need SigV4 scoping to resolve -- this handler's existing
// PriorityHeaderExact already outranks Batch's PriorityPathVersioned, so
// Batch's matcher is never reached for these paths.
//
// Regenerate by grepping serializers.go for every
// "type awsRestjson1_serializeOp<Op> struct" and pulling "request.Method"
// and the httpbinding.SplitURI(...) argument from the body of its
// HandleSerialize method (see git history for the exact extraction script).
func schemasSDKRouteCases() []struct{ op, method, path string } {
	const r = "/v1/registries/name/PLACEHOLDER"
	const s = r + "/schemas/name/PLACEHOLDER"

	return []struct{ op, method, path string }{
		{"ListRegistries", "GET", "/v1/registries"},
		{"CreateRegistry", "POST", r},
		{"DeleteRegistry", "DELETE", r},
		{"DescribeRegistry", "GET", r},
		{"UpdateRegistry", "PUT", r},
		{"ListSchemas", "GET", r + "/schemas"},
		{"SearchSchemas", "GET", r + "/schemas/search"},
		{"CreateSchema", "POST", s},
		{"DeleteSchema", "DELETE", s},
		{"DescribeSchema", "GET", s},
		{"UpdateSchema", "PUT", s},
		{"ListSchemaVersions", "GET", s + "/versions"},
		{"DeleteSchemaVersion", "DELETE", s + "/version/PLACEHOLDER"},
		{"GetDiscoveredSchema", "POST", "/v1/discover"},
		{"PutCodeBinding", "POST", s + "/language/PLACEHOLDER"},
		{"DescribeCodeBinding", "GET", s + "/language/PLACEHOLDER"},
		{"GetCodeBindingSource", "GET", s + "/language/PLACEHOLDER/source"},
	}
}

// TestExtractOperation_SchemasRESTRouteTable drives every real Schemas op's
// authoritative method+path (see schemasSDKRouteCases) through
// RouteMatcher, ExtractOperation, and Handler(), asserting: RouteMatcher
// accepts the request with no X-Amz-Target header at all (the real Schemas
// client never sends one -- unlike a hand-built request driving the
// fabricated "AWSSchemas." header, which does not exercise RouteMatcher's
// actual real-client gate); ExtractOperation resolves the right op name;
// and Handler() does not fall through to the JSON-RPC dispatch table's
// UnknownOperationException sentinel.
func TestExtractOperation_SchemasRESTRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range schemasSDKRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := eventbridge.NewInMemoryBackend()
			h := eventbridge.NewHandler(backend)
			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader("{}"))
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			require.True(t, h.RouteMatcher()(c), "method=%s path=%s: RouteMatcher rejected a real-shaped request",
				tc.method, tc.path)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnknownOperationException",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
