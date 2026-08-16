package apigatewayv2_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// newTestHandler creates a fresh Handler backed by an InMemoryBackend for tests.
func newTestHandler() *apigatewayv2.Handler {
	return apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend())
}

// doRequest performs an HTTP request against the handler and returns the recorder.
func doRequest(t *testing.T, h *apigatewayv2.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyReader *bytes.Reader

	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		bodyReader = bytes.NewReader(b)
	} else {
		bodyReader = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, bodyReader)
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rr)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rr
}

// doRequestRaw sends a POST request with a raw string body and returns the recorder.
func doRequestRaw(t *testing.T, h *apigatewayv2.Handler, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rr)
	require.NoError(t, h.Handler()(c))

	return rr
}

// createAPI is a test helper that creates an API and returns its ID.
func createAPI(t *testing.T, h *apigatewayv2.Handler, name string) string {
	t.Helper()

	rr := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{
		"name":         name,
		"protocolType": "HTTP",
	})

	require.Equal(t, http.StatusCreated, rr.Code)

	var api apigatewayv2.API
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &api))

	return api.APIID
}

func TestHandler_RouteMatching(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tests := []struct {
		name    string
		path    string
		matches bool
	}{
		{
			name:    "v2_apis_path",
			path:    "/v2/apis",
			matches: true,
		},
		{
			name:    "v2_sub_path",
			path:    "/v2/apis/abc/stages",
			matches: true,
		},
		{
			name:    "non_v2_path",
			path:    "/restapis",
			matches: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rr := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rr)

			matcher := h.RouteMatcher()
			assert.Equal(t, tt.matches, matcher(c))
		})
	}
}

func TestHandler_HandlerMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	assert.Equal(t, "APIGatewayV2", h.Name())
	assert.Equal(t, "apigatewayv2", h.ChaosServiceName())
	assert.NotEmpty(t, h.GetSupportedOperations())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "with_api_id",
			path: "/v2/apis/abc123/stages",
			want: "abc123",
		},
		{
			name: "empty_path",
			path: "/v2/apis",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rr := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rr)

			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestHandler_NotFoundPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rr := doRequest(t, h, http.MethodGet, "/not-a-v2-path", nil)
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "create_api",
			method: http.MethodPost,
			path:   "/v2/apis",
			wantOp: "CreateApi",
		},
		{
			name:   "get_apis",
			method: http.MethodGet,
			path:   "/v2/apis",
			wantOp: "GetApis",
		},
		{
			name:   "get_api",
			method: http.MethodGet,
			path:   "/v2/apis/abc123",
			wantOp: "GetApi",
		},
		{
			name:   "delete_api",
			method: http.MethodDelete,
			path:   "/v2/apis/abc123",
			wantOp: "DeleteApi",
		},
		{
			name:   "create_stage",
			method: http.MethodPost,
			path:   "/v2/apis/abc123/stages",
			wantOp: "CreateStage",
		},
		{
			name:   "get_stage",
			method: http.MethodGet,
			path:   "/v2/apis/abc123/stages/prod",
			wantOp: "GetStage",
		},
		{
			name:   "create_route",
			method: http.MethodPost,
			path:   "/v2/apis/abc123/routes",
			wantOp: "CreateRoute",
		},
		{
			name:   "delete_route",
			method: http.MethodDelete,
			path:   "/v2/apis/abc123/routes/r1",
			wantOp: "DeleteRoute",
		},
		{
			name:   "create_deployment",
			method: http.MethodPost,
			path:   "/v2/apis/abc123/deployments",
			wantOp: "CreateDeployment",
		},
		{
			name:   "get_deployment",
			method: http.MethodGet,
			path:   "/v2/apis/abc123/deployments/d1",
			wantOp: "GetDeployment",
		},
		{
			name:   "create_authorizer",
			method: http.MethodPost,
			path:   "/v2/apis/abc123/authorizers",
			wantOp: "CreateAuthorizer",
		},
		{
			name:   "create_integration_response",
			method: http.MethodPost,
			path:   "/v2/apis/abc123/integrations/int1/integrationresponses",
			wantOp: "CreateIntegrationResponse",
		},
		{
			name:   "create_route_response",
			method: http.MethodPost,
			path:   "/v2/apis/abc123/routes/r1/routeresponses",
			wantOp: "CreateRouteResponse",
		},
		{
			name:   "create_domain_name",
			method: http.MethodPost,
			path:   "/v2/domainnames",
			wantOp: "CreateDomainName",
		},
		{
			name:   "create_api_mapping",
			method: http.MethodPost,
			path:   "/v2/domainnames/example.com/apimappings",
			wantOp: "CreateApiMapping",
		},
		{
			name:   "create_portal",
			method: http.MethodPost,
			path:   "/v2/portals",
			wantOp: "CreatePortal",
		},
		{
			name:   "create_portal_product",
			method: http.MethodPost,
			path:   "/v2/portalproducts",
			wantOp: "CreatePortalProduct",
		},
		{
			name:   "create_product_page",
			method: http.MethodPost,
			path:   "/v2/portalproducts/pp1/productpages",
			wantOp: "CreateProductPage",
		},
		{
			name:   "create_product_rest_endpoint_page",
			method: http.MethodPost,
			path:   "/v2/portalproducts/pp1/productrestendpointpages",
			wantOp: "CreateProductRestEndpointPage",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rr := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rr)

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "delete_on_apis_list",
			method:     http.MethodDelete,
			path:       "/v2/apis",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "post_on_api_by_id",
			method:     http.MethodPost,
			path:       "/v2/apis/abc123",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "unknown_collection",
			method:     http.MethodGet,
			path:       "/v2/apis/abc123/unknown",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "unknown_sub_resource",
			method:     http.MethodGet,
			path:       "/v2/apis/abc123/unknown/res123",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rr := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := apigatewayv2.NewInMemoryBackend()
	h := apigatewayv2.NewHandler(backend)

	// Create an API via backend
	api, err := backend.CreateAPI(
		context.Background(),
		apigatewayv2.CreateAPIInput{Name: "snap-api", ProtocolType: "HTTP"},
	)
	require.NoError(t, err)

	// Test Snapshot
	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Restore into a new backend/handler
	b2 := apigatewayv2.NewInMemoryBackend()
	h2 := apigatewayv2.NewHandler(b2)
	require.NoError(t, h2.Restore(t.Context(), snap))

	got, err := b2.GetAPI(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, "snap-api", got.Name)
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	assert.Equal(t, 85, h.MatchPriority())
}

func TestHandler_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "create_domain_name_missing_field",
			method:     http.MethodPost,
			path:       "/v2/domainnames",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create_portal_product_missing_display_name",
			method:     http.MethodPost,
			path:       "/v2/portalproducts",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rr := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

func TestHandler_ExtractOperation_NewOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "get_domain_names",
			method: http.MethodGet,
			path:   "/v2/domainnames",
			wantOp: "GetDomainNames",
		},
		{
			name:   "get_domain_name",
			method: http.MethodGet,
			path:   "/v2/domainnames/example.com",
			wantOp: "GetDomainName",
		},
		{
			name:   "delete_domain_name",
			method: http.MethodDelete,
			path:   "/v2/domainnames/example.com",
			wantOp: "DeleteDomainName",
		},
		{
			name:   "get_api_mappings",
			method: http.MethodGet,
			path:   "/v2/domainnames/example.com/apimappings",
			wantOp: "GetApiMappings",
		},
		{
			name:   "get_api_mapping",
			method: http.MethodGet,
			path:   "/v2/domainnames/example.com/apimappings/abc123",
			wantOp: "GetApiMapping",
		},
		{
			name:   "delete_api_mapping",
			method: http.MethodDelete,
			path:   "/v2/domainnames/example.com/apimappings/abc123",
			wantOp: "DeleteApiMapping",
		},
		{
			name:   "list_portals",
			method: http.MethodGet,
			path:   "/v2/portals",
			wantOp: "ListPortals",
		},
		{
			name:   "get_portal",
			method: http.MethodGet,
			path:   "/v2/portals/abc123",
			wantOp: "GetPortal",
		},
		{
			name:   "list_portal_products",
			method: http.MethodGet,
			path:   "/v2/portalproducts",
			wantOp: "ListPortalProducts",
		},
		{
			name:   "get_portal_product",
			method: http.MethodGet,
			path:   "/v2/portalproducts/abc123",
			wantOp: "GetPortalProduct",
		},
		{
			name:   "list_product_pages",
			method: http.MethodGet,
			path:   "/v2/portalproducts/abc123/productpages",
			wantOp: "ListProductPages",
		},
		{
			name:   "list_product_re_pages",
			method: http.MethodGet,
			path:   "/v2/portalproducts/abc123/productrestendpointpages",
			wantOp: "ListProductRestEndpointPages",
		},
		{
			name:   "get_models",
			method: http.MethodGet,
			path:   "/v2/apis/abc123/models",
			wantOp: "GetModels",
		},
		{
			name:   "get_model",
			method: http.MethodGet,
			path:   "/v2/apis/abc123/models/modelId",
			wantOp: "GetModel",
		},
		{
			name:   "delete_model",
			method: http.MethodDelete,
			path:   "/v2/apis/abc123/models/modelId",
			wantOp: "DeleteModel",
		},
		{
			name:   "get_integration_responses",
			method: http.MethodGet,
			path:   "/v2/apis/abc123/integrations/int1/integrationresponses",
			wantOp: "GetIntegrationResponses",
		},
		{
			name:   "get_integration_response",
			method: http.MethodGet,
			path:   "/v2/apis/abc123/integrations/int1/integrationresponses/resp1",
			wantOp: "GetIntegrationResponse",
		},
		{
			name:   "delete_integration_response",
			method: http.MethodDelete,
			path:   "/v2/apis/abc123/integrations/int1/integrationresponses/resp1",
			wantOp: "DeleteIntegrationResponse",
		},
		{
			name:   "get_route_responses",
			method: http.MethodGet,
			path:   "/v2/apis/abc123/routes/r1/routeresponses",
			wantOp: "GetRouteResponses",
		},
		{
			name:   "get_route_response",
			method: http.MethodGet,
			path:   "/v2/apis/abc123/routes/r1/routeresponses/resp1",
			wantOp: "GetRouteResponse",
		},
		{
			name:   "delete_route_response",
			method: http.MethodDelete,
			path:   "/v2/apis/abc123/routes/r1/routeresponses/resp1",
			wantOp: "DeleteRouteResponse",
		},
		{
			name:   "get_tags",
			method: http.MethodGet,
			path:   "/v2/tags/arn:aws:apigateway:us-east-1::/apis/abc123",
			wantOp: "GetTags",
		},
		{
			name:   "tag_resource",
			method: http.MethodPost,
			path:   "/v2/tags/arn:aws:apigateway:us-east-1::/apis/abc123",
			wantOp: "TagResource",
		},
		{
			name:   "untag_resource",
			method: http.MethodDelete,
			path:   "/v2/tags/arn:aws:apigateway:us-east-1::/apis/abc123",
			wantOp: "UntagResource",
		},
		{
			name:   "create_vpc_link",
			method: http.MethodPost,
			path:   "/v2/vpclinks",
			wantOp: "CreateVpcLink",
		},
		{
			name:   "get_vpc_link",
			method: http.MethodGet,
			path:   "/v2/vpclinks/vpc1",
			wantOp: "GetVpcLink",
		},
		{
			name:   "list_routing_rules",
			method: http.MethodGet,
			path:   "/v2/domainnames/example.com/routingrules",
			wantOp: "ListRoutingRules",
		},
		{
			name:   "put_routing_rule",
			method: http.MethodPut,
			path:   "/v2/domainnames/example.com/routingrules/rule1",
			wantOp: "PutRoutingRule",
		},
		{
			name:   "delete_access_log_settings",
			method: http.MethodDelete,
			path:   "/v2/apis/abc123/stages/prod/accesslogsettings",
			wantOp: "DeleteAccessLogSettings",
		},
		{
			name:   "delete_cors_configuration",
			method: http.MethodDelete,
			path:   "/v2/apis/abc123/cors",
			wantOp: "DeleteCorsConfiguration",
		},
		{
			name:   "sharing_policy_get",
			method: http.MethodGet,
			path:   "/v2/portalproducts/pp1/sharingpolicy",
			wantOp: "GetPortalProductSharingPolicy",
		},
		{
			name:   "delete_route_request_parameter",
			method: http.MethodDelete,
			path:   "/v2/apis/abc123/routes/r1/requestparameters/param",
			wantOp: "DeleteRouteRequestParameter",
		},
		{
			name:   "delete_route_settings",
			method: http.MethodDelete,
			path:   "/v2/apis/abc123/stages/prod/routesettings/$default",
			wantOp: "DeleteRouteSettings",
		},
		{
			name:   "disable_portal",
			method: http.MethodDelete,
			path:   "/v2/portals/p1/publish",
			wantOp: "DisablePortal",
		},
		{
			name:   "preview_portal",
			method: http.MethodPost,
			path:   "/v2/portals/p1/preview",
			wantOp: "PreviewPortal",
		},
		{
			name:   "publish_portal",
			method: http.MethodPost,
			path:   "/v2/portals/p1/publish",
			wantOp: "PublishPortal",
		},
		{
			name:   "export_api",
			method: http.MethodGet,
			path:   "/v2/apis/abc123/exports/oas30",
			wantOp: "ExportApi",
		},
		{
			name:   "import_api",
			method: http.MethodPut,
			path:   "/v2/apis",
			wantOp: "ImportApi",
		},
		{
			name:   "reimport_api",
			method: http.MethodPut,
			path:   "/v2/apis/abc123",
			wantOp: "ReimportApi",
		},
		{
			name:   "get_model_template",
			method: http.MethodGet,
			path:   "/v2/apis/abc123/models/m1/template",
			wantOp: "GetModelTemplate",
		},
		{
			name:   "update_product_page",
			method: http.MethodPatch,
			path:   "/v2/portalproducts/p1/productpages/page1",
			wantOp: "UpdateProductPage",
		},
		{
			name:   "update_product_rest_endpoint_page",
			method: http.MethodPatch,
			path:   "/v2/portalproducts/p1/productrestendpointpages/page1",
			wantOp: "UpdateProductRestEndpointPage",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rr := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rr)

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func createStage(t *testing.T, h *apigatewayv2.Handler, apiID, stageName string) {
	t.Helper()
	rr := doRequest(t, h, http.MethodPost, fmt.Sprintf("/v2/apis/%s/stages", apiID), map[string]any{
		"stageName": stageName,
	})
	require.Equal(t, http.StatusCreated, rr.Code)
}

func createDomainName(t *testing.T, h *apigatewayv2.Handler, domainName string) {
	t.Helper()
	rr := doRequest(t, h, http.MethodPost, "/v2/domainnames", map[string]any{
		"domainName": domainName,
	})
	require.Equal(t, http.StatusCreated, rr.Code)
}

func TestErrorTypeHeader_AcrossHandlers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *apigatewayv2.Handler) (string, string, any)
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name: "get_missing_api_404",
			setup: func(_ *testing.T, _ *apigatewayv2.Handler) (string, string, any) {
				return http.MethodGet, "/v2/apis/does-not-exist", nil
			},
			wantStatus: http.StatusNotFound,
			wantType:   "NotFoundException",
		},
		{
			name: "create_route_empty_key_400",
			setup: func(t *testing.T, h *apigatewayv2.Handler) (string, string, any) {
				t.Helper()

				apiID := createAPI(t, h, "err-api-1")

				return http.MethodPost, "/v2/apis/" + apiID + "/routes", map[string]any{"routeKey": ""}
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "BadRequestException",
		},
		{
			name: "duplicate_route_409",
			setup: func(t *testing.T, h *apigatewayv2.Handler) (string, string, any) {
				t.Helper()

				apiID := createAPI(t, h, "err-api-2")
				rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/routes",
					map[string]any{"routeKey": "GET /dup"})
				require.Equal(t, http.StatusCreated, rr.Code)

				return http.MethodPost, "/v2/apis/" + apiID + "/routes", map[string]any{"routeKey": "GET /dup"}
			},
			wantStatus: http.StatusConflict,
			wantType:   "ConflictException",
		},
		{
			name: "get_missing_route_404",
			setup: func(t *testing.T, h *apigatewayv2.Handler) (string, string, any) {
				t.Helper()

				apiID := createAPI(t, h, "err-api-3")

				return http.MethodGet, "/v2/apis/" + apiID + "/routes/nope", nil
			},
			wantStatus: http.StatusNotFound,
			wantType:   "NotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			method, path, body := tt.setup(t, h)

			rr := doRequest(t, h, method, path, body)

			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Equal(t, tt.wantType, rr.Header().Get(errTypeHeaderKey))
		})
	}
}
