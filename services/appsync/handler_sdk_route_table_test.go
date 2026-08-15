package appsync_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real AppSync
// operation, extracted from appsync@v1.56.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Param} URI label -- the router does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AssociateApi", "POST", "/v1/domainnames/PLACEHOLDER/apiassociation"},
		{"AssociateMergedGraphqlApi", "POST", "/v1/sourceApis/PLACEHOLDER/mergedApiAssociations"},
		{"AssociateSourceGraphqlApi", "POST", "/v1/mergedApis/PLACEHOLDER/sourceApiAssociations"},
		{"CreateApi", "POST", "/v2/apis"},
		{"CreateApiCache", "POST", "/v1/apis/PLACEHOLDER/ApiCaches"},
		{"CreateApiKey", "POST", "/v1/apis/PLACEHOLDER/apikeys"},
		{"CreateChannelNamespace", "POST", "/v2/apis/PLACEHOLDER/channelNamespaces"},
		{"CreateDataSource", "POST", "/v1/apis/PLACEHOLDER/datasources"},
		{"CreateDomainName", "POST", "/v1/domainnames"},
		{"CreateFunction", "POST", "/v1/apis/PLACEHOLDER/functions"},
		{"CreateGraphqlApi", "POST", "/v1/apis"},
		{"CreateResolver", "POST", "/v1/apis/PLACEHOLDER/types/PLACEHOLDER/resolvers"},
		{"CreateType", "POST", "/v1/apis/PLACEHOLDER/types"},
		{"DeleteApi", "DELETE", "/v2/apis/PLACEHOLDER"},
		{"DeleteApiCache", "DELETE", "/v1/apis/PLACEHOLDER/ApiCaches"},
		{"DeleteApiKey", "DELETE", "/v1/apis/PLACEHOLDER/apikeys/PLACEHOLDER"},
		{"DeleteChannelNamespace", "DELETE", "/v2/apis/PLACEHOLDER/channelNamespaces/PLACEHOLDER"},
		{"DeleteDataSource", "DELETE", "/v1/apis/PLACEHOLDER/datasources/PLACEHOLDER"},
		{"DeleteDomainName", "DELETE", "/v1/domainnames/PLACEHOLDER"},
		{"DeleteFunction", "DELETE", "/v1/apis/PLACEHOLDER/functions/PLACEHOLDER"},
		{"DeleteGraphqlApi", "DELETE", "/v1/apis/PLACEHOLDER"},
		{"DeleteResolver", "DELETE", "/v1/apis/PLACEHOLDER/types/PLACEHOLDER/resolvers/PLACEHOLDER"},
		{"DeleteType", "DELETE", "/v1/apis/PLACEHOLDER/types/PLACEHOLDER"},
		{"DisassociateApi", "DELETE", "/v1/domainnames/PLACEHOLDER/apiassociation"},
		{"DisassociateMergedGraphqlApi", "DELETE", "/v1/sourceApis/PLACEHOLDER/mergedApiAssociations/PLACEHOLDER"},
		{"DisassociateSourceGraphqlApi", "DELETE", "/v1/mergedApis/PLACEHOLDER/sourceApiAssociations/PLACEHOLDER"},
		{"EvaluateCode", "POST", "/v1/dataplane-evaluatecode"},
		{"EvaluateMappingTemplate", "POST", "/v1/dataplane-evaluatetemplate"},
		{"FlushApiCache", "DELETE", "/v1/apis/PLACEHOLDER/FlushCache"},
		{"GetApi", "GET", "/v2/apis/PLACEHOLDER"},
		{"GetApiAssociation", "GET", "/v1/domainnames/PLACEHOLDER/apiassociation"},
		{"GetApiCache", "GET", "/v1/apis/PLACEHOLDER/ApiCaches"},
		{"GetChannelNamespace", "GET", "/v2/apis/PLACEHOLDER/channelNamespaces/PLACEHOLDER"},
		{"GetDataSource", "GET", "/v1/apis/PLACEHOLDER/datasources/PLACEHOLDER"},
		{"GetDataSourceIntrospection", "GET", "/v1/datasources/introspections/PLACEHOLDER"},
		{"GetDomainName", "GET", "/v1/domainnames/PLACEHOLDER"},
		{"GetFunction", "GET", "/v1/apis/PLACEHOLDER/functions/PLACEHOLDER"},
		{"GetGraphqlApi", "GET", "/v1/apis/PLACEHOLDER"},
		{"GetGraphqlApiEnvironmentVariables", "GET", "/v1/apis/PLACEHOLDER/environmentVariables"},
		{"GetIntrospectionSchema", "GET", "/v1/apis/PLACEHOLDER/schema"},
		{"GetResolver", "GET", "/v1/apis/PLACEHOLDER/types/PLACEHOLDER/resolvers/PLACEHOLDER"},
		{"GetSchemaCreationStatus", "GET", "/v1/apis/PLACEHOLDER/schemacreation"},
		{"GetSourceApiAssociation", "GET", "/v1/mergedApis/PLACEHOLDER/sourceApiAssociations/PLACEHOLDER"},
		{"GetType", "GET", "/v1/apis/PLACEHOLDER/types/PLACEHOLDER"},
		{"ListApiKeys", "GET", "/v1/apis/PLACEHOLDER/apikeys"},
		{"ListApis", "GET", "/v2/apis"},
		{"ListChannelNamespaces", "GET", "/v2/apis/PLACEHOLDER/channelNamespaces"},
		{"ListDataSources", "GET", "/v1/apis/PLACEHOLDER/datasources"},
		{"ListDomainNames", "GET", "/v1/domainnames"},
		{"ListFunctions", "GET", "/v1/apis/PLACEHOLDER/functions"},
		{"ListGraphqlApis", "GET", "/v1/apis"},
		{"ListResolvers", "GET", "/v1/apis/PLACEHOLDER/types/PLACEHOLDER/resolvers"},
		{"ListResolversByFunction", "GET", "/v1/apis/PLACEHOLDER/functions/PLACEHOLDER/resolvers"},
		{"ListSourceApiAssociations", "GET", "/v1/apis/PLACEHOLDER/sourceApiAssociations"},
		{"ListTagsForResource", "GET", "/v1/tags/PLACEHOLDER"},
		{"ListTypes", "GET", "/v1/apis/PLACEHOLDER/types"},
		{"ListTypesByAssociation", "GET", "/v1/mergedApis/PLACEHOLDER/sourceApiAssociations/PLACEHOLDER/types"},
		{"PutGraphqlApiEnvironmentVariables", "PUT", "/v1/apis/PLACEHOLDER/environmentVariables"},
		{"StartDataSourceIntrospection", "POST", "/v1/datasources/introspections"},
		{"StartSchemaCreation", "POST", "/v1/apis/PLACEHOLDER/schemacreation"},
		{"StartSchemaMerge", "POST", "/v1/mergedApis/PLACEHOLDER/sourceApiAssociations/PLACEHOLDER/merge"},
		{"TagResource", "POST", "/v1/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/v1/tags/PLACEHOLDER"},
		{"UpdateApi", "POST", "/v2/apis/PLACEHOLDER"},
		{"UpdateApiCache", "POST", "/v1/apis/PLACEHOLDER/ApiCaches/update"},
		{"UpdateApiKey", "POST", "/v1/apis/PLACEHOLDER/apikeys/PLACEHOLDER"},
		{"UpdateChannelNamespace", "POST", "/v2/apis/PLACEHOLDER/channelNamespaces/PLACEHOLDER"},
		{"UpdateDataSource", "POST", "/v1/apis/PLACEHOLDER/datasources/PLACEHOLDER"},
		{"UpdateDomainName", "POST", "/v1/domainnames/PLACEHOLDER"},
		{"UpdateFunction", "POST", "/v1/apis/PLACEHOLDER/functions/PLACEHOLDER"},
		{"UpdateGraphqlApi", "POST", "/v1/apis/PLACEHOLDER"},
		{"UpdateResolver", "POST", "/v1/apis/PLACEHOLDER/types/PLACEHOLDER/resolvers/PLACEHOLDER"},
		{"UpdateSourceApiAssociation", "POST", "/v1/mergedApis/PLACEHOLDER/sourceApiAssociations/PLACEHOLDER"},
		{"UpdateType", "POST", "/v1/apis/PLACEHOLDER/types/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real AppSync op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op. gopherstack-jqh2 pass
// 3: re-extracted all 74 appsync ops from the pinned SDK and confirmed the
// existing parseOperation table already correct, including its several
// same-path/different-method collisions (/v1/apis/{apiId}/ApiCaches,
// /v1/tags/{arn}, /v2/apis/{apiId}).
//
// It then drives the same request through the real Handler() and asserts it
// did not fall through to the literal "Not found" body that the various
// route/path-shape dispatch defaults emit throughout handler.go and its
// per-family handlers -- distinct from handleError's backend-not-found
// responses (handler_errors.go), which always carry the specific
// err.Error() text instead of that literal string. This guards against an
// operation name that resolves correctly but has no matching case in the
// dispatch chain (gopherstack-ey26).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

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
			assert.NotContains(t, rec.Body.String(), "Not found",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
