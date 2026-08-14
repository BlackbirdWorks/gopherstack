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
// h.GetSupportedOperations() reports 79 entries, not 57 --
// 22 of them (CreatePipe/DeletePipe/DescribePipe/ListPipes/UpdatePipe, and
// 17 Schema-Registry-shaped ops: CreateRegistry, DeleteRegistry,
// DescribeRegistry, ListRegistries, UpdateRegistry, CreateSchema,
// DeleteSchema, DescribeSchema, ListSchemas, SearchSchemas, UpdateSchema,
// ListSchemaVersions, DeleteSchemaVersion, GetDiscoveredSchema,
// PutCodeBinding, DescribeCodeBinding, GetCodeBindingSource) are real
// operation names, but they belong to the separate Pipes and Schemas AWS
// services, not EventBridge -- confirmed against the pinned pipes@v1.26.4
// and schemas@v1.37.4 serializers.go, both of which are REST-JSON 1
// (awsRestjson1_ prefix, dispatched by HTTP method+path, never by
// X-Amz-Target at all). No real Pipes or Schemas SDK client can ever reach
// this handler's dispatch table under these names; they are wired here only
// as an internal-only convention (a dedicated services/pipes directory
// separately hosts the real, correctly-protocolled Pipes API). Excluded from
// this table for the same reason rds excluded GetPerformanceInsightsMetrics
// and cognitoidp excluded AdminSetUserMFASetting: real-looking names that
// cannot be driven by any real pinned SDK client under this service's
// protocol.
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
