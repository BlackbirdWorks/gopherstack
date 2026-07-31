package appsync_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

// chromeUserAgent is a realistic browser User-Agent value, used to prove a
// browser-shaped request (which cannot set User-Agent to an SDK marker --
// see RouteMatcher's doc comment) still matches via X-Amz-User-Agent.
const chromeUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"

func TestParseOperation_DataSourceIntrospections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "start_introspection",
			method: http.MethodPost,
			path:   "/v1/dataSource-introspections",
			wantOp: "StartDataSourceIntrospection",
		},
		{
			name:   "get_introspection",
			method: http.MethodGet,
			path:   "/v1/dataSource-introspections/abc123",
			wantOp: "GetDataSourceIntrospection",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doRequest(t, h, tt.method, tt.path, nil)
			// The operation header is used by metrics; we only validate the handler was reached.
			_ = rec
		})
	}
}

// TestHandler_ExtractOperation_RealDataSourceIntrospections locks the real AWS SDK
// endpoint "/v1/datasources/introspections[/{id}]" (distinct from the legacy
// "/v1/dataSource-introspections" alias exercised by
// TestParseOperation_DataSourceIntrospections above).
func TestHandler_ExtractOperation_RealDataSourceIntrospections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "start_introspection",
			method: http.MethodPost,
			path:   "/v1/datasources/introspections",
			wantOp: "StartDataSourceIntrospection",
		},
		{
			name:   "get_introspection",
			method: http.MethodGet,
			path:   "/v1/datasources/introspections/abc123",
			wantOp: "GetDataSourceIntrospection",
		},
		{
			name:   "wrong_subpath",
			method: http.MethodPost,
			path:   "/v1/datasources/somethingelse",
			wantOp: "Unknown",
		},
		{
			name:   "get_on_collection_wrong_method",
			method: http.MethodGet,
			path:   "/v1/datasources/introspections",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(""))
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

// TestHandler_ExtractOperation_StartSchemaMerge locks the real AWS SDK endpoint
// "POST /v1/mergedApis/{mergedApiIdentifier}/sourceApiAssociations/{associationId}/merge".
func TestHandler_ExtractOperation_StartSchemaMerge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "post_merge",
			method: http.MethodPost,
			path:   "/v1/mergedApis/merged1/sourceApiAssociations/assoc1/merge",
			wantOp: "StartSchemaMerge",
		},
		{
			name:   "get_merge_wrong_method",
			method: http.MethodGet,
			path:   "/v1/mergedApis/merged1/sourceApiAssociations/assoc1/merge",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(""))
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestParseOperation_DataplaneEvaluations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{
			name:   "evaluate_template",
			method: http.MethodPost,
			path:   "/v1/dataplane-evaluations/template",
		},
		{
			name:   "evaluate_code",
			method: http.MethodPost,
			path:   "/v1/dataplane-evaluations/code",
		},
		{
			name:   "unknown_subpath",
			method: http.MethodPost,
			path:   "/v1/dataplane-evaluations/unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			// Just verify the handler is reached (no panic).
			_ = doRequest(t, h, tt.method, tt.path, map[string]any{"template": "x", "code": "x"})
		})
	}
}

func TestParseOperation_V2APIsItem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		method     string
		wantStatus int
	}{
		{
			name:       "get_api",
			method:     http.MethodGet,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_api",
			method:     http.MethodDelete,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "update_api_put",
			method:     http.MethodPut,
			wantStatus: http.StatusNotFound,
			body:       map[string]any{"name": "new-name"},
		},
		{
			name:       "update_api_patch",
			method:     http.MethodPatch,
			wantStatus: http.StatusNotFound,
			body:       map[string]any{"name": "new-name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			r := doV2Request(t, h, tt.method, "/v2/apis/nonexistent-api-id", tt.body)
			assert.Equal(t, tt.wantStatus, r.Code)
		})
	}
}

func TestParseOperation_V2APIsNamedResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "get_channel_namespace",
			method:     http.MethodGet,
			path:       "/v2/apis/some-api/channelNamespaces/some-ns",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_channel_namespace",
			method:     http.MethodDelete,
			path:       "/v2/apis/some-api/channelNamespaces/some-ns",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "update_channel_namespace_put",
			method:     http.MethodPut,
			path:       "/v2/apis/some-api/channelNamespaces/some-ns",
			body:       map[string]any{"codeHandlers": ""},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "update_channel_namespace_patch",
			method:     http.MethodPatch,
			path:       "/v2/apis/some-api/channelNamespaces/some-ns",
			body:       map[string]any{"codeHandlers": ""},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "unknown_named_resource",
			method:     http.MethodGet,
			path:       "/v2/apis/some-api/unknown/some-name",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doV2Request(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestParseOperation_SubTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		wantStatus int
	}{
		{
			name:       "tag_resource",
			method:     http.MethodPost,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "list_tags",
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
		},
		{
			name:       "untag_resource",
			method:     http.MethodDelete,
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			path := fmt.Sprintf("/v1/apis/%s/tags", api.APIID)

			var body any
			switch tt.method {
			case http.MethodPost:
				body = map[string]any{"tags": map[string]string{"key": "value"}}
			case http.MethodDelete:
				path += "?tagKeys=key"
			}

			rec := doRequest(t, h, tt.method, path, body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	assert.Equal(t, "AppSync", h.Name())
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	e := echo.New()

	tests := []struct {
		name      string
		path      string
		userAgent string
		xAmzUA    string
		match     bool
	}{
		{name: "matches_v1_apis", path: "/v1/apis", match: true},
		{name: "matches_v1_apis_with_id", path: "/v1/apis/abc123", match: true},
		{name: "no_match_other_path", path: "/restapis/foo", match: false},
		{name: "no_match_root", path: "/", match: false},
		// /v2/apis is shared with API Gateway V2; only match when User-Agent contains "api/appsync".
		{
			name:      "v2_apis_with_appsync_ua",
			path:      "/v2/apis",
			userAgent: "aws-sdk-go-v2/1.0 api/appsync/1.53.5",
			match:     true,
		},
		{
			name:      "v2_apis_with_apigwv2_ua",
			path:      "/v2/apis",
			userAgent: "aws-sdk-go-v2/1.0 api/apigatewayv2/1.33.7",
			match:     false,
		},
		{name: "v2_apis_no_ua", path: "/v2/apis", userAgent: "", match: false},
		{
			name:      "v2_apis_with_id_appsync_ua",
			path:      "/v2/apis/abc123",
			userAgent: "aws-sdk-go-v2/1.0 api/appsync/1.53.5",
			match:     true,
		},
		{name: "v2_apis_with_id_no_ua", path: "/v2/apis/abc123", userAgent: "", match: false},
		{
			// A real browser cannot set User-Agent itself (forbidden by the Fetch
			// spec) -- the browser sends its own literal UA there, and the AWS SDK
			// for JavaScript puts its SDK identification in X-Amz-User-Agent
			// instead, using the API model's PascalCase serviceId ("AppSync", not
			// aws-sdk-go-v2's lowercase "appsync").
			name:      "v2_apis_browser_appsync_x_amz_user_agent",
			path:      "/v2/apis",
			userAgent: chromeUserAgent,
			xAmzUA:    "aws-sdk-js/3.1094.0 ua/2.1 os/browser lang/js md/react-native api/AppSync/3.1094.0",
			match:     true,
		},
		{
			// Same browser shape, but for the sibling service (API Gateway V2) that
			// shares this path prefix -- must NOT be claimed by AppSync's matcher.
			name:      "v2_apis_browser_apigwv2_x_amz_user_agent",
			path:      "/v2/apis",
			userAgent: chromeUserAgent,
			xAmzUA:    "aws-sdk-js/3.1094.0 ua/2.1 os/browser lang/js api/ApiGatewayV2/3.1094.0",
			match:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if tt.userAgent != "" {
				req.Header.Set("User-Agent", tt.userAgent)
			}
			if tt.xAmzUA != "" {
				req.Header.Set("X-Amz-User-Agent", tt.xAmzUA)
			}
			c := e.NewContext(req, httptest.NewRecorder())
			matcher := h.RouteMatcher()
			assert.Equal(t, tt.match, matcher(c))
		})
	}
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateGraphqlApi")
	assert.Contains(t, ops, "CreateResolver")
	// "ExecuteGraphQL" is deliberately NOT advertised: it is an internal route
	// label for the GraphQL data-plane endpoint, not a real AppSync SDK
	// operation (aws-sdk-go-v2/service/appsync.Client has no such method) — see
	// opExecuteGraphQL's doc comment in handler.go.
	assert.NotContains(t, ops, "ExecuteGraphQL")
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{name: "POST /v1/apis", method: http.MethodPost, path: "/v1/apis", wantOp: "CreateGraphqlApi"},
		{name: "GET /v1/apis", method: http.MethodGet, path: "/v1/apis", wantOp: "ListGraphqlApis"},
		{name: "GET /v1/apis/id", method: http.MethodGet, path: "/v1/apis/abc", wantOp: "GetGraphqlApi"},
		{name: "DELETE /v1/apis/id", method: http.MethodDelete, path: "/v1/apis/abc", wantOp: "DeleteGraphqlApi"},
		{
			name:   "POST schemacreations",
			method: http.MethodPost,
			path:   "/v1/apis/abc/schemacreation",
			wantOp: "StartSchemaCreation",
		},
		{
			name:   "GET schemacreations",
			method: http.MethodGet,
			path:   "/v1/apis/abc/schemacreation",
			wantOp: "GetSchemaCreationStatus",
		},
		{name: "GET schema", method: http.MethodGet, path: "/v1/apis/abc/schema", wantOp: "GetIntrospectionSchema"},
		{
			name:   "POST datasources",
			method: http.MethodPost,
			path:   "/v1/apis/abc/datasources",
			wantOp: "CreateDataSource",
		},
		{name: "GET datasources", method: http.MethodGet, path: "/v1/apis/abc/datasources", wantOp: "ListDataSources"},
		{
			name:   "GET datasource",
			method: http.MethodGet,
			path:   "/v1/apis/abc/datasources/myds",
			wantOp: "GetDataSource",
		},
		{
			name:   "POST resolvers",
			method: http.MethodPost,
			path:   "/v1/apis/abc/types/Query/resolvers",
			wantOp: "CreateResolver",
		},
		{
			name:   "GET resolvers",
			method: http.MethodGet,
			path:   "/v1/apis/abc/types/Query/resolvers",
			wantOp: "ListResolvers",
		},
		{
			name:   "GET resolver",
			method: http.MethodGet,
			path:   "/v1/apis/abc/types/Query/resolvers/getItem",
			wantOp: "GetResolver",
		},
		{
			name:   "DELETE resolver",
			method: http.MethodDelete,
			path:   "/v1/apis/abc/types/Query/resolvers/getItem",
			wantOp: "DeleteResolver",
		},
		{name: "POST graphql", method: http.MethodPost, path: "/v1/apis/abc/graphql", wantOp: "ExecuteGraphQL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(""))
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	assert.Equal(t, 85, h.MatchPriority())
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

	tests := []struct {
		name    string
		path    string
		wantRes string
	}{
		{
			name:    "extracts_api_id",
			path:    "/v1/apis/" + api.APIID,
			wantRes: api.APIID,
		},
		{
			name:    "returns_empty_for_list_path",
			path:    "/v1/apis",
			wantRes: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantRes, h.ExtractResource(c))
		})
	}
}

func TestHandler_ExtractOperation_CreateRoutes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "POST /v1/apis/{id}/apikeys",
			method: http.MethodPost,
			path:   "/v1/apis/abc/apikeys",
			wantOp: "CreateApiKey",
		},
		{
			name:   "POST /v1/apis/{id}/ApiCaches",
			method: http.MethodPost,
			path:   "/v1/apis/abc/ApiCaches",
			wantOp: "CreateApiCache",
		},
		{
			name:   "POST /v1/apis/{id}/functions",
			method: http.MethodPost,
			path:   "/v1/apis/abc/functions",
			wantOp: "CreateFunction",
		},
		{
			name:   "POST /v1/apis/{id}/types",
			method: http.MethodPost,
			path:   "/v1/apis/abc/types",
			wantOp: "CreateType",
		},
		{
			name:   "POST /v1/domainnames",
			method: http.MethodPost,
			path:   "/v1/domainnames",
			wantOp: "CreateDomainName",
		},
		{
			name:   "POST /v1/domainnames/{dn}/apiassociation",
			method: http.MethodPost,
			path:   "/v1/domainnames/api.example.com/apiassociation",
			wantOp: "AssociateApi",
		},
		{
			name:   "POST /v1/sourceApis/{id}/mergedApiAssociations",
			method: http.MethodPost,
			path:   "/v1/sourceApis/source-id/mergedApiAssociations",
			wantOp: "AssociateMergedGraphqlApi",
		},
		{
			name:   "POST /v1/mergedApis/{id}/sourceApiAssociations",
			method: http.MethodPost,
			path:   "/v1/mergedApis/merged-id/sourceApiAssociations",
			wantOp: "AssociateSourceGraphqlApi",
		},
		{
			name:   "POST /v2/apis",
			method: http.MethodPost,
			path:   "/v2/apis",
			wantOp: "CreateApi",
		},
		{
			name:   "POST /v2/apis/{id}/channelNamespaces",
			method: http.MethodPost,
			path:   "/v2/apis/abc/channelNamespaces",
			wantOp: "CreateChannelNamespace",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(""))
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_MethodNotAllowed_CollectionRoutes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "apikeys_method_not_allowed",
			method:     http.MethodDelete, // DELETE at collection level (no keyId) → 405
			path:       "/v1/apis/abc/apikeys",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "apicaches_method_not_allowed",
			method:     http.MethodPatch, // PATCH → 405
			path:       "/v1/apis/abc/ApiCaches",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "functions_method_not_allowed",
			method:     http.MethodDelete, // DELETE at collection level → 405
			path:       "/v1/apis/abc/functions",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "domainnames_method_not_allowed",
			method:     http.MethodDelete, // DELETE at /v1/domainnames (no name) → 405
			path:       "/v1/domainnames",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "apiassociation_method_not_allowed",
			method:     http.MethodPut, // PUT /apiassociation → 405 (not a valid method)
			path:       "/v1/domainnames/api.example.com/apiassociation",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "mergedApiAssociations_method_not_allowed",
			method:     http.MethodGet,
			path:       "/v1/sourceApis/source-id/mergedApiAssociations",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "sourceApiAssociations_method_not_allowed",
			method:     http.MethodPut, // PUT on sourceApiAssociations collection → 405
			path:       "/v1/mergedApis/merged-id/sourceApiAssociations",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "v2_apis_method_not_allowed",
			method:     http.MethodPatch, // PATCH /v2/apis → 405
			path:       "/v2/apis",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "channel_namespaces_method_not_allowed",
			method:     http.MethodPut, // PUT on channelNamespaces collection → 405 (GET/POST are now valid)
			path:       "/v2/apis/abc/channelNamespaces",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "domainnames_unknown_path",
			method:     http.MethodGet,
			path:       "/v1/domainnames/api.example.com/unknown",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "sourceApis_unknown_path",
			method:     http.MethodGet,
			path:       "/v1/sourceApis/source-id/unknown",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "mergedApis_unknown_path",
			method:     http.MethodGet,
			path:       "/v1/mergedApis/merged-id/unknown",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "v2_apis_unknown_path",
			method:     http.MethodGet,
			path:       "/v2/apis/abc/unknown",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ChaosInterface(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	h.DefaultRegion = "us-east-1"

	assert.Equal(t, "appsync", h.ChaosServiceName())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.Contains(t, h.ChaosOperations(), "CreateGraphqlApi")
	assert.Contains(t, h.ChaosRegions(), "us-east-1")
}

func TestHandler_ExtractResource_DomainPath(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/v1/domainnames/example.com/apiAssociation", nil)
	c := e.NewContext(req, httptest.NewRecorder())
	resource := h.ExtractResource(c)
	assert.Equal(t, "example.com", resource)
}

func TestHandler_ExtractResource_EmptyForUnknown(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/unknown/path", nil)
	c := e.NewContext(req, httptest.NewRecorder())
	resource := h.ExtractResource(c)
	assert.Empty(t, resource)
}

// Test_UpdateOps_UsePOSTMethod verifies that every AppSync Update* operation is
// reachable via POST — the method the real AWS SDK actually sends — not just the
// PUT/PATCH this handler previously required exclusively.
func Test_UpdateOps_UsePOSTMethod(t *testing.T) {
	t.Parallel()

	t.Run("UpdateGraphqlApi", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID, map[string]any{"name": "Renamed"})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("UpdateApi_v2", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		api, err := b.CreateAPI("TestEventAPI", "", nil, nil)
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/v2/apis/"+api.APIID, map[string]any{"name": "Renamed"})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("UpdateDataSource", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
		require.NoError(t, err)
		_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "myds", Type: "NONE"})
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/datasources/myds",
			map[string]any{"name": "myds", "type": "NONE", "description": "updated"})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("UpdateFunction", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
		require.NoError(t, err)
		_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "ds", Type: "NONE"})
		require.NoError(t, err)
		fn, err := b.CreateFunction(api.APIID, &appsync.Function{Name: "fn", DataSourceName: "ds"})
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/functions/"+fn.FunctionID,
			map[string]any{"name": "fn", "dataSourceName": "ds", "description": "updated"})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("UpdateType", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
		require.NoError(t, err)
		_, err = b.CreateType(api.APIID, "type MyType { id: ID! }", appsync.TypeFormatSDL)
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/types/MyType",
			map[string]any{"definition": "type MyType { id: ID! name: String! }", "format": "SDL"})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("UpdateResolver", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
		require.NoError(t, err)
		_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "ds", Type: "NONE"})
		require.NoError(t, err)
		_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
			FieldName: "hello", DataSourceName: "ds", Kind: "UNIT",
		})
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/types/Query/resolvers/hello",
			map[string]any{"dataSourceName": "ds", "kind": "UNIT"})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("UpdateApiKey", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
		require.NoError(t, err)
		key, err := b.CreateAPIKey(api.APIID, "orig", 0)
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/apikeys/"+key.ID,
			map[string]any{"description": "updated"})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("UpdateDomainName", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		_, err := b.CreateDomainName("api.example.com", "arn:aws:acm:us-east-1:000000000000:cert/abc", "", nil)
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/v1/domainnames/api.example.com",
			map[string]any{"description": "updated"})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("UpdateChannelNamespace", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		api, err := b.CreateAPI("TestEventAPI", "", nil, nil)
		require.NoError(t, err)
		_, err = b.CreateChannelNamespace(api.APIID, "ns1", nil, nil)
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost, "/v2/apis/"+api.APIID+"/channelNamespaces/ns1",
			map[string]any{"codeHandlers": "export const handler = () => {}"})
		require.Equal(t, http.StatusOK, rec.Code)
	})
}

// Test_TagResource_RealPath verifies tag operations are reachable at the real AWS SDK
// path "/v1/tags/{resourceArn}" — not the previously (and still, for back-compat)
// supported "/v1/apis/{apiId}/tags" alias. The ARN itself contains "/" (between the
// "apis" resource-type segment and the api ID), matching the ARN-with-slash routing
// bug class flagged for this sweep.
func Test_TagResource_RealPath(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	tagPath := "/v1/tags/" + api.ARN

	// TagResource (POST).
	rec := doRequest(t, h, http.MethodPost, tagPath, map[string]any{"tags": map[string]any{"env": "prod"}})
	require.Equal(t, http.StatusNoContent, rec.Code)

	// ListTagsForResource (GET).
	rec2 := doRequest(t, h, http.MethodGet, tagPath, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var listResp map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&listResp))
	tagMap, ok := listResp["tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", tagMap["env"])

	// UntagResource (DELETE).
	rec3 := doRequest(t, h, http.MethodDelete, tagPath+"?tagKeys=env", nil)
	require.Equal(t, http.StatusNoContent, rec3.Code)

	rec4 := doRequest(t, h, http.MethodGet, tagPath, nil)
	require.NoError(t, json.NewDecoder(rec4.Body).Decode(&listResp))
	assert.Empty(t, listResp["tags"])
}

// Test_TagResource_RealPath_EventAPI verifies tagging works for v2 Api (Event API)
// resources too — both v1 GraphqlApi and v2 Api share the "apis/{id}" ARN shape.
func Test_TagResource_RealPath_EventAPI(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateAPI("TestEventAPI", "", nil, nil)
	require.NoError(t, err)

	tagPath := "/v1/tags/" + api.ARN

	rec := doRequest(t, h, http.MethodPost, tagPath, map[string]any{"tags": map[string]any{"team": "core"}})
	require.Equal(t, http.StatusNoContent, rec.Code)

	rec2 := doRequest(t, h, http.MethodGet, tagPath, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&resp))
	tagMap, ok := resp["tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "core", tagMap["team"])
}

// Test_TagResource_RealPath_NotFound verifies an ARN for a nonexistent api resolves to
// NotFoundException, not a silent success or an InternalFailure.
func Test_TagResource_RealPath_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodGet,
		"/v1/tags/arn:aws:appsync:us-east-1:000000000000:apis/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// Test_ApiCache_RealPaths verifies UpdateApiCache and FlushApiCache at their real AWS
// SDK paths ("/v1/apis/{apiId}/ApiCaches/update" and "/v1/apis/{apiId}/FlushCache")
// rather than only the previously-implemented (and still supported) aliases.
func Test_ApiCache_RealPaths(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateAPICache(
		api.APIID,
		&appsync.APICache{TTL: 60, Type: "SMALL", APICachingBehavior: "FULL_REQUEST_CACHING"},
	)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/ApiCaches/update",
		map[string]any{"ttl": 120, "type": "LARGE"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	cache := resp["apiCache"].(map[string]any)
	assert.Equal(t, "LARGE", cache["type"])

	rec2 := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/FlushCache", nil)
	assert.Equal(t, http.StatusNoContent, rec2.Code)
}

// Test_Evaluate_RealPaths verifies EvaluateCode/EvaluateMappingTemplate at their real
// AWS SDK paths ("/v1/dataplane-evaluatecode" and "/v1/dataplane-evaluatetemplate") —
// two standalone top-level paths, not "/v1/dataplane-evaluations/{code,template}".
func Test_Evaluate_RealPaths(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodPost, "/v1/dataplane-evaluatecode", map[string]any{
		"code":    `export function request(ctx) { return {}; }`,
		"context": "",
		"runtime": map[string]any{"name": "APPSYNC_JS"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, http.MethodPost, "/v1/dataplane-evaluatetemplate", map[string]any{
		"template": `{"version": "2017-02-28", "payload": {}}`,
		"context":  "",
	})
	require.Equal(t, http.StatusOK, rec2.Code)
}

// Test_ListSourceApiAssociations_ByAPIID verifies the real AWS SDK path
// "/v1/apis/{apiId}/sourceApiAssociations" (keyed by the merged API's own apiId) works
// alongside the existing "/v1/mergedApis/{mergedApiIdentifier}/sourceApiAssociations"
// path used by Associate/Get/Update/DisassociateSourceGraphqlApi.
func Test_ListSourceApiAssociations_ByAPIID(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	merged, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)
	source, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.AssociateSourceGraphqlAPI(merged.APIID, source.APIID, "", "")
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+merged.APIID+"/sourceApiAssociations", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	// The real AWS SDK's ListSourceApiAssociationsOutput wraps the list under
	// "sourceApiAssociationSummaries", not "sourceApiAssociations" (the latter is only
	// the URL path segment name).
	assocs, ok := resp["sourceApiAssociationSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, assocs, 1)
}

// Test_ListApis_ResponseKey verifies ListApis wraps its list under "apis" — the real
// AWS SDK's ListApisOutput field name — not "items" (a disguised no-op: a real client
// would always see an empty list back, since it deserializes strictly by field name).
func Test_ListApis_ResponseKey(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	_, err := b.CreateAPI("TestEventAPI", "", nil, nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/v2/apis", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	apis, ok := resp["apis"].([]any)
	require.True(t, ok, "response must wrap the list under \"apis\", got keys %v", resp)
	assert.Len(t, apis, 1)
	assert.NotContains(t, resp, "items")
}

// TestRouteMatcher_RealPaths verifies h.RouteMatcher() — the entry point real SDK
// requests are dispatched through — accepts every path fixed in this sweep. A path that
// only Handler() recognizes but RouteMatcher() rejects never reaches this service.
func TestRouteMatcher_RealPaths(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	matcher := h.RouteMatcher()

	tests := []struct {
		method    string
		path      string
		userAgent string
	}{
		{method: http.MethodPost, path: "/v1/tags/arn:aws:appsync:us-east-1:000000000000:apis/abc123"},
		{method: http.MethodGet, path: "/v1/tags/arn:aws:appsync:us-east-1:000000000000:apis/abc123"},
		{method: http.MethodDelete, path: "/v1/tags/arn:aws:appsync:us-east-1:000000000000:apis/abc123"},
		{method: http.MethodPost, path: "/v1/dataplane-evaluatecode"},
		{method: http.MethodPost, path: "/v1/dataplane-evaluatetemplate"},
		{method: http.MethodPost, path: "/v1/apis/abc123"}, // UpdateGraphqlApi
		{
			// UpdateApi; /v2/apis is shared with API Gateway V2, so the real SDK's
			// distinguishing User-Agent is required for a match (see RouteMatcher doc).
			method: http.MethodPost, path: "/v2/apis/abc123",
			userAgent: "aws-sdk-go-v2/1.0 api/appsync/1.55.0",
		},
		{method: http.MethodPost, path: "/v1/apis/abc123/ApiCaches/update"},
		{method: http.MethodDelete, path: "/v1/apis/abc123/FlushCache"},
		{method: http.MethodGet, path: "/v1/apis/abc123/sourceApiAssociations"},
		{method: http.MethodPost, path: "/v1/datasources/introspections"},
		{method: http.MethodGet, path: "/v1/datasources/introspections/abc123"},
		{
			method: http.MethodPost,
			path:   "/v1/mergedApis/merged1/sourceApiAssociations/assoc1/merge",
		},
	}

	e := echo.New()

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			if tt.userAgent != "" {
				req.Header.Set("User-Agent", tt.userAgent)
			}

			c := e.NewContext(req, httptest.NewRecorder())
			assert.True(t, matcher(c), "RouteMatcher rejected %s %s", tt.method, tt.path)
		})
	}
}
