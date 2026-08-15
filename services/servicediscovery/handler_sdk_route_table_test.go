package servicediscovery_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real AWS Cloud
// Map (ServiceDiscovery) operation, extracted from
// servicediscovery@v1.43.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String(
// "Route53AutoNaming_v20170314.<Op>") and always POSTs to "/" -- Cloud Map
// is JSON-RPC 1.1 (services/_PROTOCOLS.md), so dispatch is entirely by
// this one header, not a path template. "Route53AutoNaming" is this
// service's real internal AWS codename, distinct from both
// "servicediscovery" and "Cloud Map" -- confirmed directly from
// serializers.go, not assumed.
//
// This table covers all 30 real Cloud Map ops (servicediscovery@v1.43.4)
// -- confirmed by diffing both GetSupportedOperations() (a hand-maintained
// literal) and the actual dispatch surface against this exact list: zero
// mismatches in either direction. Dispatch here is NOT a flat map -- it's
// a chain of four ok-flag helpers (dispatchNamespace, dispatchService,
// dispatchInstance, dispatchMeta), each a plain switch on op name that
// returns (result, matched bool, err) and falls through to the next
// helper on no match; the case labels across all four switches were
// extracted and diffed, not just one function's. GetSupportedOperations()
// is a separate hand-maintained literal, not built by ranging over any of
// the four switches, so the two diffs are genuinely independent checks.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("Route53AutoNaming_v20170314.` and
// pulling the suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CreateHttpNamespace", "Route53AutoNaming_v20170314.CreateHttpNamespace"},
		{"CreatePrivateDnsNamespace", "Route53AutoNaming_v20170314.CreatePrivateDnsNamespace"},
		{"CreatePublicDnsNamespace", "Route53AutoNaming_v20170314.CreatePublicDnsNamespace"},
		{"CreateService", "Route53AutoNaming_v20170314.CreateService"},
		{"DeleteNamespace", "Route53AutoNaming_v20170314.DeleteNamespace"},
		{"DeleteService", "Route53AutoNaming_v20170314.DeleteService"},
		{"DeleteServiceAttributes", "Route53AutoNaming_v20170314.DeleteServiceAttributes"},
		{"DeregisterInstance", "Route53AutoNaming_v20170314.DeregisterInstance"},
		{"DiscoverInstances", "Route53AutoNaming_v20170314.DiscoverInstances"},
		{"DiscoverInstancesRevision", "Route53AutoNaming_v20170314.DiscoverInstancesRevision"},
		{"GetInstance", "Route53AutoNaming_v20170314.GetInstance"},
		{"GetInstancesHealthStatus", "Route53AutoNaming_v20170314.GetInstancesHealthStatus"},
		{"GetNamespace", "Route53AutoNaming_v20170314.GetNamespace"},
		{"GetOperation", "Route53AutoNaming_v20170314.GetOperation"},
		{"GetService", "Route53AutoNaming_v20170314.GetService"},
		{"GetServiceAttributes", "Route53AutoNaming_v20170314.GetServiceAttributes"},
		{"ListInstances", "Route53AutoNaming_v20170314.ListInstances"},
		{"ListNamespaces", "Route53AutoNaming_v20170314.ListNamespaces"},
		{"ListOperations", "Route53AutoNaming_v20170314.ListOperations"},
		{"ListServices", "Route53AutoNaming_v20170314.ListServices"},
		{"ListTagsForResource", "Route53AutoNaming_v20170314.ListTagsForResource"},
		{"RegisterInstance", "Route53AutoNaming_v20170314.RegisterInstance"},
		{"TagResource", "Route53AutoNaming_v20170314.TagResource"},
		{"UntagResource", "Route53AutoNaming_v20170314.UntagResource"},
		{"UpdateHttpNamespace", "Route53AutoNaming_v20170314.UpdateHttpNamespace"},
		{"UpdateInstanceCustomHealthStatus", "Route53AutoNaming_v20170314.UpdateInstanceCustomHealthStatus"},
		{"UpdatePrivateDnsNamespace", "Route53AutoNaming_v20170314.UpdatePrivateDnsNamespace"},
		{"UpdatePublicDnsNamespace", "Route53AutoNaming_v20170314.UpdatePublicDnsNamespace"},
		{"UpdateService", "Route53AutoNaming_v20170314.UpdateService"},
		{"UpdateServiceAttributes", "Route53AutoNaming_v20170314.UpdateServiceAttributes"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Cloud Map
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to dispatchMeta's default branch
// (fmt.Errorf("%w: %s", errUnknownAction, op), handler.go's single
// production call site for this exact message).
//
// This asserts on MESSAGE TEXT ("unknown action: <op>"), not wire type:
// errUnknownAction resolves through sentinelErrorCodes to the shared
// "InvalidInput" type, the same code ErrInvalidInput and malformed-JSON
// requests produce -- a type assertion here would not distinguish a
// dispatch miss from a routine bad-input error. errUnknownAction's message
// has exactly one production call site (grepped) and is not produced by
// any other error path.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := servicediscovery.NewHandler(servicediscovery.NewInMemoryBackend("000000000000", "us-east-1"))

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown action: "+tc.op,
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
