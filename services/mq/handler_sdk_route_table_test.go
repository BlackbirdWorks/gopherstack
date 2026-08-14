package mq_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real Amazon MQ
// operation, extracted from mq@v1.39.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for a {BrokerId}/{ConfigurationId}/{ConfigurationRevision}/{Username}/
// {ResourceArn} URI label -- parseRoute (handler.go) never validates
// identifier shape, so the literal value doesn't matter here, only path
// depth and static segments. 25 real ops here, matching mq's real op count
// exactly (also matches GetSupportedOperations's own 25 entries
// one-for-one).
//
// A systematic check for a shared method+path across all 25 ops found zero
// collisions: DescribeBroker/UpdateBroker/DeleteBroker share
// "/v1/brokers/{BrokerId}" and CreateUser/DescribeUser/UpdateUser/DeleteUser
// share "/v1/brokers/{BrokerId}/users/{Username}", but each group is
// disambiguated by method (GET/PUT/DELETE/POST), which parseBrokerRoute and
// parseUserRoute already switch on -- so no *required dynamic*
// (non-template) member -- the s3/glacier vacuity-trap class -- was needed
// to disambiguate any route in this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CreateBroker", "POST", "/v1/brokers"},
		{"CreateConfiguration", "POST", "/v1/configurations"},
		{"CreateTags", "POST", "/v1/tags/PLACEHOLDER"},
		{"CreateUser", "POST", "/v1/brokers/PLACEHOLDER/users/PLACEHOLDER"},
		{"DeleteBroker", "DELETE", "/v1/brokers/PLACEHOLDER"},
		{"DeleteConfiguration", "DELETE", "/v1/configurations/PLACEHOLDER"},
		{"DeleteTags", "DELETE", "/v1/tags/PLACEHOLDER"},
		{"DeleteUser", "DELETE", "/v1/brokers/PLACEHOLDER/users/PLACEHOLDER"},
		{"DescribeBroker", "GET", "/v1/brokers/PLACEHOLDER"},
		{"DescribeBrokerEngineTypes", "GET", "/v1/broker-engine-types"},
		{"DescribeBrokerInstanceOptions", "GET", "/v1/broker-instance-options"},
		{"DescribeConfiguration", "GET", "/v1/configurations/PLACEHOLDER"},
		{"DescribeConfigurationRevision", "GET", "/v1/configurations/PLACEHOLDER/revisions/PLACEHOLDER"},
		{"DescribeSharedResources", "GET", "/v1/brokers/PLACEHOLDER/shared-resources"},
		{"DescribeUser", "GET", "/v1/brokers/PLACEHOLDER/users/PLACEHOLDER"},
		{"ListBrokers", "GET", "/v1/brokers"},
		{"ListConfigurationRevisions", "GET", "/v1/configurations/PLACEHOLDER/revisions"},
		{"ListConfigurations", "GET", "/v1/configurations"},
		{"ListTags", "GET", "/v1/tags/PLACEHOLDER"},
		{"ListUsers", "GET", "/v1/brokers/PLACEHOLDER/users"},
		{"Promote", "POST", "/v1/brokers/PLACEHOLDER/promote"},
		{"RebootBroker", "POST", "/v1/brokers/PLACEHOLDER/reboot"},
		{"UpdateBroker", "PUT", "/v1/brokers/PLACEHOLDER"},
		{"UpdateConfiguration", "PUT", "/v1/configurations/PLACEHOLDER"},
		{"UpdateUser", "PUT", "/v1/brokers/PLACEHOLDER/users/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Amazon MQ op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts parseRoute (handler.go) resolves it to the right op, all 25 ops
// against mq's real op count. It then drives the same request through the
// real Handler() and asserts the response does not contain the exact
// literal "unknown operation: " that dispatchMutating's terminal default
// case (handler.go) emits wrapping the request path when parseRoute fails
// to match -- this handler's only dispatch-miss mode: opUnknown routes fall
// through dispatchReadOps into dispatchMutating regardless of method, and
// grepping "unknown operation" across every non-test .go file in this
// package finds only that one emission site, so no second miss path exists
// to confuse with the sentinel.
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
			assert.NotContains(t, rec.Body.String(), "unknown operation: ",
				"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
		})
	}
}
