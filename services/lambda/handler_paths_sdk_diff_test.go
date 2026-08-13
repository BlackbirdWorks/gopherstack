package lambda_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real lambda
// operation, extracted from lambda@v1.101.2 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Param} URI label -- the router does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op. The
// GetLayerVersionByArn/GetProvisionedConcurrencyConfig entries carry their
// real query-string discriminators (?find=LayerVersion, ?Qualifier=...)
// since those are load-bearing for route resolution, unlike the other
// PLACEHOLDER path segments.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AddLayerVersionPermission", "POST", "/2018-10-31/layers/PLACEHOLDER/versions/PLACEHOLDER/policy"},
		{"AddPermission", "POST", "/2015-03-31/functions/PLACEHOLDER/policy"},
		{"CheckpointDurableExecution", "POST", "/2025-12-01/durable-executions/PLACEHOLDER/checkpoint"},
		{"CreateAlias", "POST", "/2015-03-31/functions/PLACEHOLDER/aliases"},
		{"CreateCapacityProvider", "POST", "/2025-11-30/capacity-providers"},
		{"CreateCodeSigningConfig", "POST", "/2020-04-22/code-signing-configs"},
		{"CreateEventSourceMapping", "POST", "/2015-03-31/event-source-mappings"},
		{"CreateFunction", "POST", "/2015-03-31/functions"},
		{"CreateFunctionUrlConfig", "POST", "/2021-10-31/functions/PLACEHOLDER/url"},
		{"DeleteAlias", "DELETE", "/2015-03-31/functions/PLACEHOLDER/aliases/PLACEHOLDER"},
		{"DeleteCapacityProvider", "DELETE", "/2025-11-30/capacity-providers/PLACEHOLDER"},
		{"DeleteCodeSigningConfig", "DELETE", "/2020-04-22/code-signing-configs/PLACEHOLDER"},
		{"DeleteEventSourceMapping", "DELETE", "/2015-03-31/event-source-mappings/PLACEHOLDER"},
		{"DeleteFunction", "DELETE", "/2015-03-31/functions/PLACEHOLDER"},
		{"DeleteFunctionCodeSigningConfig", "DELETE", "/2020-06-30/functions/PLACEHOLDER/code-signing-config"},
		{"DeleteFunctionConcurrency", "DELETE", "/2017-10-31/functions/PLACEHOLDER/concurrency"},
		{"DeleteFunctionEventInvokeConfig", "DELETE", "/2019-09-25/functions/PLACEHOLDER/event-invoke-config"},
		{"DeleteFunctionUrlConfig", "DELETE", "/2021-10-31/functions/PLACEHOLDER/url"},
		{"DeleteLayerVersion", "DELETE", "/2018-10-31/layers/PLACEHOLDER/versions/PLACEHOLDER"},
		{"DeleteProvisionedConcurrencyConfig", "DELETE", "/2019-09-30/functions/PLACEHOLDER/provisioned-concurrency"},
		{"GetAccountSettings", "GET", "/2016-08-19/account-settings"},
		{"GetAlias", "GET", "/2015-03-31/functions/PLACEHOLDER/aliases/PLACEHOLDER"},
		{"GetCapacityProvider", "GET", "/2025-11-30/capacity-providers/PLACEHOLDER"},
		{"GetCodeSigningConfig", "GET", "/2020-04-22/code-signing-configs/PLACEHOLDER"},
		{"GetDurableExecution", "GET", "/2025-12-01/durable-executions/PLACEHOLDER"},
		{"GetDurableExecutionHistory", "GET", "/2025-12-01/durable-executions/PLACEHOLDER/history"},
		{"GetDurableExecutionState", "GET", "/2025-12-01/durable-executions/PLACEHOLDER/state"},
		{"GetEventSourceMapping", "GET", "/2015-03-31/event-source-mappings/PLACEHOLDER"},
		{"GetFunction", "GET", "/2015-03-31/functions/PLACEHOLDER"},
		{"GetFunctionCodeSigningConfig", "GET", "/2020-06-30/functions/PLACEHOLDER/code-signing-config"},
		{"GetFunctionConcurrency", "GET", "/2019-09-30/functions/PLACEHOLDER/concurrency"},
		{"GetFunctionConfiguration", "GET", "/2015-03-31/functions/PLACEHOLDER/configuration"},
		{"GetFunctionEventInvokeConfig", "GET", "/2019-09-25/functions/PLACEHOLDER/event-invoke-config"},
		{"GetFunctionRecursionConfig", "GET", "/2024-08-31/functions/PLACEHOLDER/recursion-config"},
		{"GetFunctionScalingConfig", "GET", "/2025-11-30/functions/PLACEHOLDER/function-scaling-config"},
		{"GetFunctionUrlConfig", "GET", "/2021-10-31/functions/PLACEHOLDER/url"},
		{"GetLayerVersion", "GET", "/2018-10-31/layers/PLACEHOLDER/versions/PLACEHOLDER"},
		{"GetLayerVersionByArn", "GET", "/2018-10-31/layers?find=LayerVersion&Arn=PLACEHOLDER"},
		{"GetLayerVersionPolicy", "GET", "/2018-10-31/layers/PLACEHOLDER/versions/PLACEHOLDER/policy"},
		{"GetPolicy", "GET", "/2015-03-31/functions/PLACEHOLDER/policy"},
		{
			"GetProvisionedConcurrencyConfig", "GET",
			"/2019-09-30/functions/PLACEHOLDER/provisioned-concurrency?Qualifier=PLACEHOLDER",
		},
		{"GetRuntimeManagementConfig", "GET", "/2021-07-20/functions/PLACEHOLDER/runtime-management-config"},
		{"Invoke", "POST", "/2015-03-31/functions/PLACEHOLDER/invocations"},
		{"InvokeAsync", "POST", "/2014-11-13/functions/PLACEHOLDER/invoke-async"},
		{"InvokeWithResponseStream", "POST", "/2021-11-15/functions/PLACEHOLDER/response-streaming-invocations"},
		{"ListAliases", "GET", "/2015-03-31/functions/PLACEHOLDER/aliases"},
		{"ListCapacityProviders", "GET", "/2025-11-30/capacity-providers"},
		{"ListCodeSigningConfigs", "GET", "/2020-04-22/code-signing-configs"},
		{"ListDurableExecutionsByFunction", "GET", "/2025-12-01/functions/PLACEHOLDER/durable-executions"},
		{"ListEventSourceMappings", "GET", "/2015-03-31/event-source-mappings"},
		{"ListFunctionEventInvokeConfigs", "GET", "/2019-09-25/functions/PLACEHOLDER/event-invoke-config/list"},
		{"ListFunctionUrlConfigs", "GET", "/2021-10-31/functions/PLACEHOLDER/urls"},
		{
			"ListFunctionVersionsByCapacityProvider", "GET",
			"/2025-11-30/capacity-providers/PLACEHOLDER/function-versions",
		},
		{"ListFunctions", "GET", "/2015-03-31/functions"},
		{"ListFunctionsByCodeSigningConfig", "GET", "/2020-04-22/code-signing-configs/PLACEHOLDER/functions"},
		{"ListLayerVersions", "GET", "/2018-10-31/layers/PLACEHOLDER/versions"},
		{"ListLayers", "GET", "/2018-10-31/layers"},
		{
			"ListProvisionedConcurrencyConfigs", "GET",
			"/2019-09-30/functions/PLACEHOLDER/provisioned-concurrency?List=ALL",
		},
		{"ListTags", "GET", "/2017-03-31/tags/PLACEHOLDER"},
		{"ListVersionsByFunction", "GET", "/2015-03-31/functions/PLACEHOLDER/versions"},
		{"PublishLayerVersion", "POST", "/2018-10-31/layers/PLACEHOLDER/versions"},
		{"PublishVersion", "POST", "/2015-03-31/functions/PLACEHOLDER/versions"},
		{"PutFunctionCodeSigningConfig", "PUT", "/2020-06-30/functions/PLACEHOLDER/code-signing-config"},
		{"PutFunctionConcurrency", "PUT", "/2017-10-31/functions/PLACEHOLDER/concurrency"},
		{"PutFunctionEventInvokeConfig", "PUT", "/2019-09-25/functions/PLACEHOLDER/event-invoke-config"},
		{"PutFunctionRecursionConfig", "PUT", "/2024-08-31/functions/PLACEHOLDER/recursion-config"},
		{"PutFunctionScalingConfig", "PUT", "/2025-11-30/functions/PLACEHOLDER/function-scaling-config"},
		{"PutProvisionedConcurrencyConfig", "PUT", "/2019-09-30/functions/PLACEHOLDER/provisioned-concurrency"},
		{"PutRuntimeManagementConfig", "PUT", "/2021-07-20/functions/PLACEHOLDER/runtime-management-config"},
		{
			"RemoveLayerVersionPermission", "DELETE",
			"/2018-10-31/layers/PLACEHOLDER/versions/PLACEHOLDER/policy/PLACEHOLDER",
		},
		{"RemovePermission", "DELETE", "/2015-03-31/functions/PLACEHOLDER/policy/PLACEHOLDER"},
		{"SendDurableExecutionCallbackFailure", "POST", "/2025-12-01/durable-execution-callbacks/PLACEHOLDER/fail"},
		{
			"SendDurableExecutionCallbackHeartbeat", "POST",
			"/2025-12-01/durable-execution-callbacks/PLACEHOLDER/heartbeat",
		},
		{"SendDurableExecutionCallbackSuccess", "POST", "/2025-12-01/durable-execution-callbacks/PLACEHOLDER/succeed"},
		{"StopDurableExecution", "POST", "/2025-12-01/durable-executions/PLACEHOLDER/stop"},
		{"TagResource", "POST", "/2017-03-31/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/2017-03-31/tags/PLACEHOLDER"},
		{"UpdateAlias", "PUT", "/2015-03-31/functions/PLACEHOLDER/aliases/PLACEHOLDER"},
		{"UpdateCapacityProvider", "PUT", "/2025-11-30/capacity-providers/PLACEHOLDER"},
		{"UpdateCodeSigningConfig", "PUT", "/2020-04-22/code-signing-configs/PLACEHOLDER"},
		{"UpdateEventSourceMapping", "PUT", "/2015-03-31/event-source-mappings/PLACEHOLDER"},
		{"UpdateFunctionCode", "PUT", "/2015-03-31/functions/PLACEHOLDER/code"},
		{"UpdateFunctionConfiguration", "PUT", "/2015-03-31/functions/PLACEHOLDER/configuration"},
		{"UpdateFunctionEventInvokeConfig", "POST", "/2019-09-25/functions/PLACEHOLDER/event-invoke-config"},
		{"UpdateFunctionUrlConfig", "PUT", "/2021-10-31/functions/PLACEHOLDER/url"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real lambda op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op, then drives the same
// request through the real Handler() and asserts it did not fall through to
// the "route not found" ResourceNotFoundException that handler.go's dispatch
// default case emits -- lambda keeps a hand-duplicated extraction tree
// separate from real dispatch (see handler.go:164-169), so an op name that
// resolves correctly could still have no matching dispatch case
// (gopherstack-ey26). gopherstack-l5ir: this
// audit found and fixed 9 unreachable/misrouted ops: GetLayerVersionByArn
// (fictional /layers-by-arn path instead of the real ?find=LayerVersion query
// flag shared with ListLayers), ListFunctionEventInvokeConfigs (fictional
// plural "/event-invoke-configs" suffix instead of the real
// "/event-invoke-config/list"), GetFunctionRecursionConfig/
// PutFunctionRecursionConfig (wrong date, 2024-08-28 vs real 2024-08-31),
// GetFunctionScalingConfig/PutFunctionScalingConfig (wrong date AND wrong
// path segment: 2023-10-26/"scaling-config" vs real
// 2025-11-30/"function-scaling-config"), and ListTags/TagResource/
// UntagResource (wrong date, 2015-03-31 vs real 2017-03-31 -- all three
// tagging operations were unreachable). See PARITY.md for the full account.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			if got != tc.op {
				t.Errorf("method=%s path=%s: got op %q, want %q", tc.method, tc.path, got, tc.op)
			}

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "route not found",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
