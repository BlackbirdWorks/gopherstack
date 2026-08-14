package cloudtrail_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real CloudTrail
// operation, extracted from cloudtrail@v1.58.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("CloudTrail_20131101.<Op>")
// and always POSTs to "/" -- CloudTrail is JSON-RPC 1.1 (services/_PROTOCOLS.md),
// so unlike a REST-family service there is no path template to get wrong:
// dispatch is entirely by this one header. ExtractOperation (TrimPrefix on
// "CloudTrail_20131101.") and Handler() (via h.dispatch's h.ops flat map
// lookup) both derive the action the same way, so the class of bug this
// table catches is a dispatch-table key that doesn't exactly match the real
// op name (typo, wrong case), not a route-template mismatch.
//
// This table covers all 60 real CloudTrail ops (cloudtrail@v1.58.4) --
// confirmed by diffing both GetSupportedOperations() (a hand-written
// literal, not built by ranging over h.ops) and buildOps()'s flat map keys
// against this exact list: zero mismatches in either direction, no dead or
// excluded keys. The two diffs are genuinely independent.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("CloudTrail_20131101.` and pulling the
// suffix after the dot.
func sdkRouteCases() []string {
	return []string{
		"AddTags",
		"CancelQuery",
		"CreateChannel",
		"CreateDashboard",
		"CreateEventDataStore",
		"CreateTrail",
		"DeleteChannel",
		"DeleteDashboard",
		"DeleteEventDataStore",
		"DeleteResourcePolicy",
		"DeleteTrail",
		"DeregisterOrganizationDelegatedAdmin",
		"DescribeQuery",
		"DescribeTrails",
		"DisableFederation",
		"EnableFederation",
		"GenerateQuery",
		"GetChannel",
		"GetDashboard",
		"GetEventConfiguration",
		"GetEventDataStore",
		"GetEventSelectors",
		"GetImport",
		"GetInsightSelectors",
		"GetQueryResults",
		"GetResourcePolicy",
		"GetTrail",
		"GetTrailStatus",
		"ListChannels",
		"ListDashboards",
		"ListEventDataStores",
		"ListImportFailures",
		"ListImports",
		"ListInsightsData",
		"ListInsightsMetricData",
		"ListPublicKeys",
		"ListQueries",
		"ListTags",
		"ListTrails",
		"LookupEvents",
		"PutEventConfiguration",
		"PutEventSelectors",
		"PutInsightSelectors",
		"PutResourcePolicy",
		"RegisterOrganizationDelegatedAdmin",
		"RemoveTags",
		"RestoreEventDataStore",
		"SearchSampleQueries",
		"StartDashboardRefresh",
		"StartEventDataStoreIngestion",
		"StartImport",
		"StartLogging",
		"StartQuery",
		"StopEventDataStoreIngestion",
		"StopImport",
		"StopLogging",
		"UpdateChannel",
		"UpdateDashboard",
		"UpdateEventDataStore",
		"UpdateTrail",
	}
}

// TestExtractOperation_SDKRouteTable drives every real CloudTrail
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to the dispatch-miss branch (handler.go's
// dispatch(), the sole production call site that emits
// "unknown operation: "+operation).
//
// The dispatch-miss branch's wire type, InvalidParameterCombinationException,
// is NOT safe to assert alone: handler_event_selectors.go's errInvalidRequest
// ("EventDataStore or TrailName is required") maps to the identical wire
// type via handleError's errors.Is(err, errInvalidRequest) case, so a
// mistyped dispatch key could 400 with the same __type as a legitimate
// validation error and a naive "status is 400" or "__type is X" check would
// not catch it. The dispatch-miss message text ("unknown operation: ") is
// unique to that one call site (grepped handler.go and
// handler_event_selectors.go) and is what this test asserts against
// instead.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, op := range sdkRouteCases() {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			backend := cloudtrail.NewInMemoryBackend("123456789012", config.DefaultRegion)
			h := cloudtrail.NewHandler(backend)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", "CloudTrail_20131101."+op)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown operation:",
				"action=%s: dispatched to the unmatched-route handler", op)
		})
	}
}
