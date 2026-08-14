package eventbridge_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real EventBridge
// operation, extracted from eventbridge@v1.48.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("AWSEvents.<Op>") and
// always POSTs to "/" -- EventBridge is JSON-RPC 1.1 (services/_PROTOCOLS.md),
// so unlike a REST-family service there is no path template to get wrong:
// dispatch is entirely by this one header. ExtractOperation and Handler()
// both derive the action the same way (split on "."), so the class of bug
// this table can catch is a dispatch-table key that doesn't exactly match
// the real op name (typo, wrong case -- EventBridge is case-sensitive
// JSON-RPC), not a route-template mismatch. Note the real SDK sends
// "AWSEvents." as the target prefix (confirmed directly in serializers.go);
// existing tests in this package use "AmazonEventBridge." as an internal
// convention that happens to also work only because ExtractOperation just
// splits on "." and does not check the prefix value -- this table drives the
// real prefix instead, per gopherstack-n1mb's instruction to use the SDK's
// own wire value rather than gopherstack's convention.
//
// This table covers all 57 real EventBridge ops (eventbridge@v1.48.4).
//
// h.GetSupportedOperations() reports 74 entries, not 57 -- 17 of them
// (CreateRegistry, DeleteRegistry, DescribeRegistry, ListRegistries,
// UpdateRegistry, CreateSchema, DeleteSchema, DescribeSchema, ListSchemas,
// SearchSchemas, UpdateSchema, ListSchemaVersions, DeleteSchemaVersion,
// GetDiscoveredSchema, PutCodeBinding, DescribeCodeBinding,
// GetCodeBindingSource) are real operation names, but they belong to the
// separate Schemas AWS service, not EventBridge -- confirmed against the
// pinned schemas@v1.37.4 serializers.go, which is REST-JSON 1
// (awsRestjson1_ prefix, dispatched by HTTP method+path, never by
// X-Amz-Target at all). This table's target-header cases still exercise
// them under the fabricated "AWSSchemas." X-Amz-Target prefix, an
// internal-only convention no real Schemas client sends -- but that path is
// NOT the only way to reach them any more (gopherstack-92ft): handler_schemas_rest.go
// now ALSO routes all 17 by their real REST-JSON1 method+path, alongside
// this JSON-RPC dispatch (both are kept; see
// handler_schemas_rest_route_table_test.go's TestExtractOperation_SchemasRESTRouteTable,
// which drives the real transport, and handler_schemas_real_client_test.go,
// which drives the real pinned schemas SDK client end to end). Kept out of
// THIS table (rather than duplicated into it) because this table's whole
// point is validating the JSON-RPC target-header dispatch specifically, the
// same reason rds excluded GetPerformanceInsightsMetrics and cognitoidp
// excluded AdminSetUserMFASetting: names that don't belong to this
// protocol's route table.
//
// Unlike Schemas, this package used to ALSO host a fabricated copy of the
// Pipes API (CreatePipe/DeletePipe/DescribePipe/ListPipes/UpdatePipe) under
// the same unreachable convention. That copy was deleted (gopherstack-92ft):
// a correctly-routed services/pipes directory already exists, covering all
// 10 real Pipes ops (including these 5) by the real REST-JSON method+path
// per pipes@v1.26.4 -- see services/pipes/handler_sdk_route_table_test.go.
//
// A further 4 dispatch-table keys (GetEventBusPolicy, PutEventBusPolicy,
// DescribeSchemaVersion, ListCodeBindings) are wired in h.ops but appear in
// neither GetSupportedOperations() nor this table -- already documented
// in-source (handler_dispatch.go) as not real SDK operation names, dead to
// any client, invisible to a diff of the reported list because that list
// already omits them. Confirmed here rather than "fixed".
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AWSEvents.` and pulling the suffix
// after the dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"ActivateEventSource", "AWSEvents.ActivateEventSource"},
		{"CancelReplay", "AWSEvents.CancelReplay"},
		{"CreateApiDestination", "AWSEvents.CreateApiDestination"},
		{"CreateArchive", "AWSEvents.CreateArchive"},
		{"CreateConnection", "AWSEvents.CreateConnection"},
		{"CreateEndpoint", "AWSEvents.CreateEndpoint"},
		{"CreateEventBus", "AWSEvents.CreateEventBus"},
		{"CreatePartnerEventSource", "AWSEvents.CreatePartnerEventSource"},
		{"DeactivateEventSource", "AWSEvents.DeactivateEventSource"},
		{"DeauthorizeConnection", "AWSEvents.DeauthorizeConnection"},
		{"DeleteApiDestination", "AWSEvents.DeleteApiDestination"},
		{"DeleteArchive", "AWSEvents.DeleteArchive"},
		{"DeleteConnection", "AWSEvents.DeleteConnection"},
		{"DeleteEndpoint", "AWSEvents.DeleteEndpoint"},
		{"DeleteEventBus", "AWSEvents.DeleteEventBus"},
		{"DeletePartnerEventSource", "AWSEvents.DeletePartnerEventSource"},
		{"DeleteRule", "AWSEvents.DeleteRule"},
		{"DescribeApiDestination", "AWSEvents.DescribeApiDestination"},
		{"DescribeArchive", "AWSEvents.DescribeArchive"},
		{"DescribeConnection", "AWSEvents.DescribeConnection"},
		{"DescribeEndpoint", "AWSEvents.DescribeEndpoint"},
		{"DescribeEventBus", "AWSEvents.DescribeEventBus"},
		{"DescribeEventSource", "AWSEvents.DescribeEventSource"},
		{"DescribePartnerEventSource", "AWSEvents.DescribePartnerEventSource"},
		{"DescribeReplay", "AWSEvents.DescribeReplay"},
		{"DescribeRule", "AWSEvents.DescribeRule"},
		{"DisableRule", "AWSEvents.DisableRule"},
		{"EnableRule", "AWSEvents.EnableRule"},
		{"ListApiDestinations", "AWSEvents.ListApiDestinations"},
		{"ListArchives", "AWSEvents.ListArchives"},
		{"ListConnections", "AWSEvents.ListConnections"},
		{"ListEndpoints", "AWSEvents.ListEndpoints"},
		{"ListEventBuses", "AWSEvents.ListEventBuses"},
		{"ListEventSources", "AWSEvents.ListEventSources"},
		{"ListPartnerEventSourceAccounts", "AWSEvents.ListPartnerEventSourceAccounts"},
		{"ListPartnerEventSources", "AWSEvents.ListPartnerEventSources"},
		{"ListReplays", "AWSEvents.ListReplays"},
		{"ListRuleNamesByTarget", "AWSEvents.ListRuleNamesByTarget"},
		{"ListRules", "AWSEvents.ListRules"},
		{"ListTagsForResource", "AWSEvents.ListTagsForResource"},
		{"ListTargetsByRule", "AWSEvents.ListTargetsByRule"},
		{"PutEvents", "AWSEvents.PutEvents"},
		{"PutPartnerEvents", "AWSEvents.PutPartnerEvents"},
		{"PutPermission", "AWSEvents.PutPermission"},
		{"PutRule", "AWSEvents.PutRule"},
		{"PutTargets", "AWSEvents.PutTargets"},
		{"RemovePermission", "AWSEvents.RemovePermission"},
		{"RemoveTargets", "AWSEvents.RemoveTargets"},
		{"StartReplay", "AWSEvents.StartReplay"},
		{"TagResource", "AWSEvents.TagResource"},
		{"TestEventPattern", "AWSEvents.TestEventPattern"},
		{"UntagResource", "AWSEvents.UntagResource"},
		{"UpdateApiDestination", "AWSEvents.UpdateApiDestination"},
		{"UpdateArchive", "AWSEvents.UpdateArchive"},
		{"UpdateConnection", "AWSEvents.UpdateConnection"},
		{"UpdateEndpoint", "AWSEvents.UpdateEndpoint"},
		{"UpdateEventBus", "AWSEvents.UpdateEventBus"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real EventBridge
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to the "UnknownOperationException"
// sentinel (errUnknownOperation, handler_dispatch.go, its sole production
// call site) that a dispatch-table key mismatch would produce.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := eventbridge.NewInMemoryBackend()
			h := eventbridge.NewHandler(backend)
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
