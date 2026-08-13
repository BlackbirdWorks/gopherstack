package outposts_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/outposts"
)

// sdkRouteCases is the authoritative method+path for every real Outposts
// operation, extracted from outposts@v1.66.1 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Param} URI label -- the router does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op.
//
// Two real AWS API quirks, both verified directly in serializers.go, not
// extraction artifacts: GetOutpostBillingInformation and GetRenewalPricing
// alone use the singular "/outpost/{id}/..." while every other outpost op
// uses plural "/outposts/{id}/..."; ListOrders alone uses a standalone
// verb-prefixed "/list-orders" path rather than GET on the "/orders"
// collection CreateOrder POSTs to.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CancelCapacityTask", "POST", "/outposts/PLACEHOLDER/capacity/PLACEHOLDER"},
		{"CancelOrder", "POST", "/orders/PLACEHOLDER/cancel"},
		{"CreateOrder", "POST", "/orders"},
		{"CreateOutpost", "POST", "/outposts"},
		{"CreateQuote", "POST", "/quotes"},
		{"CreateRenewal", "POST", "/renewals"},
		{"CreateSite", "POST", "/sites"},
		{"DeleteOutpost", "DELETE", "/outposts/PLACEHOLDER"},
		{"DeleteQuote", "DELETE", "/quotes/PLACEHOLDER"},
		{"DeleteSite", "DELETE", "/sites/PLACEHOLDER"},
		{"GetCapacityTask", "GET", "/outposts/PLACEHOLDER/capacity/PLACEHOLDER"},
		{"GetCatalogItem", "GET", "/catalog/item/PLACEHOLDER"},
		{"GetConnection", "GET", "/connections/PLACEHOLDER"},
		{"GetOrder", "GET", "/orders/PLACEHOLDER"},
		{"GetOutpost", "GET", "/outposts/PLACEHOLDER"},
		{"GetOutpostBillingInformation", "GET", "/outpost/PLACEHOLDER/billing-information"},
		{"GetOutpostInstanceTypes", "GET", "/outposts/PLACEHOLDER/instanceTypes"},
		{"GetOutpostSupportedInstanceTypes", "GET", "/outposts/PLACEHOLDER/supportedInstanceTypes"},
		{"GetQuote", "GET", "/quotes/PLACEHOLDER"},
		{"GetRenewalPricing", "GET", "/outpost/PLACEHOLDER/renewal-pricing"},
		{"GetSite", "GET", "/sites/PLACEHOLDER"},
		{"GetSiteAddress", "GET", "/sites/PLACEHOLDER/address"},
		{"ListAssetInstances", "GET", "/outposts/PLACEHOLDER/assetInstances"},
		{"ListAssets", "GET", "/outposts/PLACEHOLDER/assets"},
		{
			"ListBlockingInstancesForCapacityTask", "GET",
			"/outposts/PLACEHOLDER/capacity/PLACEHOLDER/blockingInstances",
		},
		{"ListCapacityTasks", "GET", "/capacity/tasks"},
		{"ListCatalogItems", "GET", "/catalog/items"},
		{"ListOrderableInstanceTypes", "GET", "/instanceTypes"},
		{"ListOrders", "GET", "/list-orders"},
		{"ListOutposts", "GET", "/outposts"},
		{"ListQuotes", "GET", "/quotes"},
		{"ListSites", "GET", "/sites"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"StartCapacityTask", "POST", "/outposts/PLACEHOLDER/capacity"},
		{"StartConnection", "POST", "/connections"},
		{"StartOutpostDecommission", "POST", "/outposts/PLACEHOLDER/decommission"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateOutpost", "PATCH", "/outposts/PLACEHOLDER"},
		{"UpdateQuote", "PATCH", "/quotes/PLACEHOLDER"},
		{"UpdateSite", "PATCH", "/sites/PLACEHOLDER"},
		{"UpdateSiteAddress", "PUT", "/sites/PLACEHOLDER/address"},
		{"UpdateSiteRackPhysicalProperties", "PATCH", "/sites/PLACEHOLDER/rackPhysicalProperties"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Outposts op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op. gopherstack-jqh2 pass
// 3: re-extracted all 43 outposts ops from the pinned SDK and confirmed the
// existing route table already correct, including both real AWS singular
// "/outpost/{id}/..." vs plural "/outposts/{id}/..." and standalone
// "/list-orders" quirks documented above.
//
// It then drives the same request through the real Handler() and asserts it
// did not fall through to the "unknown path" error handler.go:174-177 emits
// when h.routeRequest (the same lookup ExtractOperation calls) returns a nil
// dispatch func -- guarding against a request whose route lookup diverges
// between the two calls (gopherstack-ey26).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	backend := outposts.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	t.Cleanup(backend.Close)
	h := outposts.NewHandler(backend)

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown path",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
