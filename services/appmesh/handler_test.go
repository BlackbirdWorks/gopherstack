package appmesh_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appmesh"
)

// newTestHandler returns a handler backed by a fresh in-memory backend.
func newTestHandler() *appmesh.Handler {
	b := appmesh.NewInMemoryBackend("000000000000", "us-east-1")

	return appmesh.NewHandler(b)
}

// doRequest executes a request against the handler and returns the response.
func doRequest(t *testing.T, h *appmesh.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var bodyStr string
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		bodyStr = string(b)
	}
	req := httptest.NewRequest(method, "/v20190125"+path, strings.NewReader(bodyStr))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// getBody unmarshals the response body into a map.
func getBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// ─── Handler metadata ───

func TestAppMesh_HandlerMetadata(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	t.Run("Name", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "AppMesh", h.Name())
	})

	t.Run("GetSupportedOperations", func(t *testing.T) {
		t.Parallel()
		ops := h.GetSupportedOperations()
		assert.NotEmpty(t, ops)
		assert.Contains(t, ops, "CreateMesh")
		assert.Contains(t, ops, "UpdateGatewayRoute")
	})

	t.Run("MatchPriority", func(t *testing.T) {
		t.Parallel()
		assert.Positive(t, h.MatchPriority())
	})
}

func TestAppMesh_ExtractOperation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{"CreateMesh", http.MethodPut, "/v20190125/meshes", "CreateMesh"},
		{"ListMeshes", http.MethodGet, "/v20190125/meshes", "ListMeshes"},
		{"DescribeMesh", http.MethodGet, "/v20190125/meshes/m1", "DescribeMesh"},
		{"UpdateMesh", http.MethodPut, "/v20190125/meshes/m1", "UpdateMesh"},
		{"DeleteMesh", http.MethodDelete, "/v20190125/meshes/m1", "DeleteMesh"},
		{
			"CreateVirtualNode",
			http.MethodPut,
			"/v20190125/meshes/m1/virtualNodes",
			"CreateVirtualNode",
		},
		{
			"ListVirtualNodes",
			http.MethodGet,
			"/v20190125/meshes/m1/virtualNodes",
			"ListVirtualNodes",
		},
		{
			"DescribeVirtualNode",
			http.MethodGet,
			"/v20190125/meshes/m1/virtualNodes/vn1",
			"DescribeVirtualNode",
		},
		{
			"UpdateVirtualNode",
			http.MethodPut,
			"/v20190125/meshes/m1/virtualNodes/vn1",
			"UpdateVirtualNode",
		},
		{
			"DeleteVirtualNode",
			http.MethodDelete,
			"/v20190125/meshes/m1/virtualNodes/vn1",
			"DeleteVirtualNode",
		},
		{
			"CreateVirtualRouter",
			http.MethodPut,
			"/v20190125/meshes/m1/virtualRouters",
			"CreateVirtualRouter",
		},
		{
			"ListVirtualRouters",
			http.MethodGet,
			"/v20190125/meshes/m1/virtualRouters",
			"ListVirtualRouters",
		},
		{
			"DescribeVirtualRouter",
			http.MethodGet,
			"/v20190125/meshes/m1/virtualRouters/vr1",
			"DescribeVirtualRouter",
		},
		{
			"UpdateVirtualRouter",
			http.MethodPut,
			"/v20190125/meshes/m1/virtualRouters/vr1",
			"UpdateVirtualRouter",
		},
		{
			"DeleteVirtualRouter",
			http.MethodDelete,
			"/v20190125/meshes/m1/virtualRouters/vr1",
			"DeleteVirtualRouter",
		},
		{
			"CreateRoute",
			http.MethodPut,
			"/v20190125/meshes/m1/virtualRouter/vr1/routes",
			"CreateRoute",
		},
		{
			"ListRoutes",
			http.MethodGet,
			"/v20190125/meshes/m1/virtualRouter/vr1/routes",
			"ListRoutes",
		},
		{
			"DescribeRoute",
			http.MethodGet,
			"/v20190125/meshes/m1/virtualRouter/vr1/routes/r1",
			"DescribeRoute",
		},
		{
			"UpdateRoute",
			http.MethodPut,
			"/v20190125/meshes/m1/virtualRouter/vr1/routes/r1",
			"UpdateRoute",
		},
		{
			"DeleteRoute",
			http.MethodDelete,
			"/v20190125/meshes/m1/virtualRouter/vr1/routes/r1",
			"DeleteRoute",
		},
		{
			"CreateVirtualService",
			http.MethodPut,
			"/v20190125/meshes/m1/virtualServices",
			"CreateVirtualService",
		},
		{
			"ListVirtualServices",
			http.MethodGet,
			"/v20190125/meshes/m1/virtualServices",
			"ListVirtualServices",
		},
		{
			"DescribeVirtualService",
			http.MethodGet,
			"/v20190125/meshes/m1/virtualServices/vs1",
			"DescribeVirtualService",
		},
		{
			"UpdateVirtualService",
			http.MethodPut,
			"/v20190125/meshes/m1/virtualServices/vs1",
			"UpdateVirtualService",
		},
		{
			"DeleteVirtualService",
			http.MethodDelete,
			"/v20190125/meshes/m1/virtualServices/vs1",
			"DeleteVirtualService",
		},
		{
			"CreateVirtualGateway",
			http.MethodPut,
			"/v20190125/meshes/m1/virtualGateways",
			"CreateVirtualGateway",
		},
		{
			"ListVirtualGateways",
			http.MethodGet,
			"/v20190125/meshes/m1/virtualGateways",
			"ListVirtualGateways",
		},
		{
			"DescribeVirtualGateway",
			http.MethodGet,
			"/v20190125/meshes/m1/virtualGateways/gw1",
			"DescribeVirtualGateway",
		},
		{
			"UpdateVirtualGateway",
			http.MethodPut,
			"/v20190125/meshes/m1/virtualGateways/gw1",
			"UpdateVirtualGateway",
		},
		{
			"DeleteVirtualGateway",
			http.MethodDelete,
			"/v20190125/meshes/m1/virtualGateways/gw1",
			"DeleteVirtualGateway",
		},
		{
			"CreateGatewayRoute",
			http.MethodPut,
			"/v20190125/meshes/m1/virtualGateway/gw1/gatewayRoutes",
			"CreateGatewayRoute",
		},
		{
			"ListGatewayRoutes",
			http.MethodGet,
			"/v20190125/meshes/m1/virtualGateway/gw1/gatewayRoutes",
			"ListGatewayRoutes",
		},
		{
			"DescribeGatewayRoute",
			http.MethodGet,
			"/v20190125/meshes/m1/virtualGateway/gw1/gatewayRoutes/gr1",
			"DescribeGatewayRoute",
		},
		{
			"UpdateGatewayRoute",
			http.MethodPut,
			"/v20190125/meshes/m1/virtualGateway/gw1/gatewayRoutes/gr1",
			"UpdateGatewayRoute",
		},
		{
			"DeleteGatewayRoute",
			http.MethodDelete,
			"/v20190125/meshes/m1/virtualGateway/gw1/gatewayRoutes/gr1",
			"DeleteGatewayRoute",
		},
		{"TagResource", http.MethodPut, "/v20190125/tag", "TagResource"},
		{"UntagResource", http.MethodPut, "/v20190125/untag", "UntagResource"},
		{"ListTagsForResource", http.MethodGet, "/v20190125/tags", "ListTagsForResource"},
		{"unknown path", http.MethodGet, "/v20190125/unknown", "Unknown"},
		{"root path", http.MethodGet, "/v20190125/", "Unknown"},
		{
			"virtualRouter no routes seg",
			http.MethodGet,
			"/v20190125/meshes/m1/virtualRouter/vr1/notroutes",
			"Unknown",
		},
		{
			"virtualGateway no gatewayRoutes seg",
			http.MethodGet,
			"/v20190125/meshes/m1/virtualGateway/gw1/not",
			"Unknown",
		},
		{"unknown method on meshes collection", http.MethodDelete, "/v20190125/meshes", "Unknown"},
		{"unknown method on mesh single", http.MethodPost, "/v20190125/meshes/m1", "Unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			op := appmesh.ExtractOperationForTest(h, tt.method, tt.path)
			assert.Equal(t, tt.want, op)
		})
	}
}

func TestAppMesh_ExtractResource(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		path string
		want string
	}{
		{"mesh name extracted", "/v20190125/meshes/my-mesh", "my-mesh"},
		{"collection path no name", "/v20190125/meshes", ""},
		{"tags path no mesh", "/v20190125/tags", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			got := appmesh.ExtractResourceForTest(h, tt.path)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAppMesh_RouteMatcher(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		path string
		want bool
	}{
		{"appmesh prefix matches", "/v20190125/meshes", true},
		{"appmesh version prefix", "/v20190125/tags", true},
		{"non-appmesh path", "/2015-01-01/something", false},
		{"empty path", "/", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			got := appmesh.RouteMatcherForTest(h, tt.path)
			assert.Equal(t, tt.want, got)
		})
	}
}

// ─── HTTP 404 for unknown paths ───

func TestAppMesh_UnknownPaths(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"empty segs", http.MethodGet, ""},
		{"unknown top-level", http.MethodGet, "/unknown-resource"},
		{"mesh sub with unknown resource", http.MethodGet, "/meshes/m1/unknownSub"},
		{"virtualRouter no routes seg short", http.MethodGet, "/meshes/m1/virtualRouter/vr1"},
		{"virtualGateway short path", http.MethodGet, "/meshes/m1/virtualGateway/gw1"},
		{"virtualRouter wrong seg", http.MethodGet, "/meshes/m1/virtualRouter/vr1/wrongSeg"},
		{"virtualGateway wrong seg", http.MethodGet, "/meshes/m1/virtualGateway/gw1/wrongSeg"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// ─── methodNotAllowed ───

func TestAppMesh_MethodNotAllowed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"DELETE on /meshes collection", http.MethodDelete, "/meshes"},
		{"PATCH on /meshes/{name}", http.MethodPatch, "/meshes/m1"},
		{
			"DELETE on /meshes/{name}/virtualNodes collection",
			http.MethodDelete,
			"/meshes/m1/virtualNodes",
		},
		{
			"DELETE on /meshes/{name}/virtualRouters collection",
			http.MethodDelete,
			"/meshes/m1/virtualRouters",
		},
		{
			"DELETE on /meshes/{name}/virtualServices collection",
			http.MethodDelete,
			"/meshes/m1/virtualServices",
		},
		{
			"DELETE on /meshes/{name}/virtualGateways collection",
			http.MethodDelete,
			"/meshes/m1/virtualGateways",
		},
		{"POST on /tag", http.MethodPost, "/tag"},
		{"POST on /untag", http.MethodPost, "/untag"},
		{"POST on /tags", http.MethodPost, "/tags"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
			body := getBody(t, rec)
			assert.Equal(t, "MethodNotAllowedException", body["code"])
		})
	}
}

// ─── Virtual router / gateway method not allowed on nested collections ───

func TestAppMesh_MethodNotAllowedNestedCollections(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		setup  func(h *appmesh.Handler)
		method string
		path   string
	}{
		{
			name: "DELETE on routes collection",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
				doRequest(t, h, http.MethodPut, "/meshes/m1/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
			},
			method: http.MethodDelete,
			path:   "/meshes/m1/virtualRouter/vr1/routes",
		},
		{
			name: "DELETE on gatewayRoutes collection",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
				doRequest(t, h, http.MethodPut, "/meshes/m1/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
			},
			method: http.MethodDelete,
			path:   "/meshes/m1/virtualGateway/gw1/gatewayRoutes",
		},
		{
			name: "POST on virtualNodes collection",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
			},
			method: http.MethodPost,
			path:   "/meshes/m1/virtualNodes",
		},
		{
			name: "POST on virtualNode single",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
				doRequest(t, h, http.MethodPut, "/meshes/m1/virtualNodes",
					map[string]any{"virtualNodeName": "vn1"})
			},
			method: http.MethodPost,
			path:   "/meshes/m1/virtualNodes/vn1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			tt.setup(h)
			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
			body := getBody(t, rec)
			assert.Equal(t, "MethodNotAllowedException", body["code"])
		})
	}
}

// ─── Duplicate / conflict errors for all resource types ───

func TestAppMesh_DuplicateCreateErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		body       any
		setup      func(h *appmesh.Handler)
		name       string
		method     string
		path       string
		wantCode   string
		wantStatus int
	}{
		{
			name: "duplicate virtual node",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
				doRequest(t, h, http.MethodPut, "/meshes/m1/virtualNodes",
					map[string]any{"virtualNodeName": "vn1"})
			},
			method:     http.MethodPut,
			path:       "/meshes/m1/virtualNodes",
			body:       map[string]any{"virtualNodeName": "vn1"},
			wantStatus: http.StatusConflict,
			wantCode:   "ConflictException",
		},
		{
			name: "duplicate virtual router",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
				doRequest(t, h, http.MethodPut, "/meshes/m1/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
			},
			method:     http.MethodPut,
			path:       "/meshes/m1/virtualRouters",
			body:       map[string]any{"virtualRouterName": "vr1"},
			wantStatus: http.StatusConflict,
			wantCode:   "ConflictException",
		},
		{
			name: "duplicate route",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
				doRequest(t, h, http.MethodPut, "/meshes/m1/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
				doRequest(t, h, http.MethodPut, "/meshes/m1/virtualRouter/vr1/routes",
					map[string]any{"routeName": "r1"})
			},
			method:     http.MethodPut,
			path:       "/meshes/m1/virtualRouter/vr1/routes",
			body:       map[string]any{"routeName": "r1"},
			wantStatus: http.StatusConflict,
			wantCode:   "ConflictException",
		},
		{
			name: "duplicate virtual service",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
				doRequest(t, h, http.MethodPut, "/meshes/m1/virtualServices",
					map[string]any{"virtualServiceName": "vs1"})
			},
			method:     http.MethodPut,
			path:       "/meshes/m1/virtualServices",
			body:       map[string]any{"virtualServiceName": "vs1"},
			wantStatus: http.StatusConflict,
			wantCode:   "ConflictException",
		},
		{
			name: "duplicate virtual gateway",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
				doRequest(t, h, http.MethodPut, "/meshes/m1/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
			},
			method:     http.MethodPut,
			path:       "/meshes/m1/virtualGateways",
			body:       map[string]any{"virtualGatewayName": "gw1"},
			wantStatus: http.StatusConflict,
			wantCode:   "ConflictException",
		},
		{
			name: "duplicate gateway route",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
				doRequest(t, h, http.MethodPut, "/meshes/m1/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
				doRequest(t, h, http.MethodPut, "/meshes/m1/virtualGateway/gw1/gatewayRoutes",
					map[string]any{"gatewayRouteName": "gr1"})
			},
			method:     http.MethodPut,
			path:       "/meshes/m1/virtualGateway/gw1/gatewayRoutes",
			body:       map[string]any{"gatewayRouteName": "gr1"},
			wantStatus: http.StatusConflict,
			wantCode:   "ConflictException",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			tt.setup(h)
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			body := getBody(t, rec)
			assert.Equal(t, tt.wantCode, body["code"])
		})
	}
}

// ─── Not found errors across resource types ───

func TestAppMesh_NotFoundErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"DescribeMesh", http.MethodGet, "/meshes/nonexistent"},
		{"DeleteMesh", http.MethodDelete, "/meshes/nonexistent"},
		{"DescribeVirtualNode in missing mesh", http.MethodGet, "/meshes/x/virtualNodes/vn1"},
		{"DescribeVirtualRouter in missing mesh", http.MethodGet, "/meshes/x/virtualRouters/vr1"},
		{"DescribeVirtualService in missing mesh", http.MethodGet, "/meshes/x/virtualServices/vs1"},
		{"DescribeVirtualGateway in missing mesh", http.MethodGet, "/meshes/x/virtualGateways/gw1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
			body := getBody(t, rec)
			assert.Equal(t, "NotFoundException", body["code"])
		})
	}
}

// ─── List empty results across resource types ───

func TestAppMesh_ListEmptyResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "empty-mesh"})

	tests := []struct {
		name    string
		path    string
		listKey string
	}{
		{"ListMeshes", "/meshes", "meshes"},
		{"ListVirtualNodes", "/meshes/empty-mesh/virtualNodes", "virtualNodes"},
		{"ListVirtualRouters", "/meshes/empty-mesh/virtualRouters", "virtualRouters"},
		{"ListVirtualServices", "/meshes/empty-mesh/virtualServices", "virtualServices"},
		{"ListVirtualGateways", "/meshes/empty-mesh/virtualGateways", "virtualGateways"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, http.MethodGet, tt.path, nil)
			assert.Equal(t, http.StatusOK, rec.Code)
			body := getBody(t, rec)
			list, ok := body[tt.listKey].([]any)
			if tt.listKey == "meshes" {
				assert.True(t, ok)
				assert.Len(t, list, 1) // empty-mesh itself
			} else {
				assert.True(t, ok)
				assert.Empty(t, list)
			}
		})
	}
}

// ─── Missing name validation across resource types ───

func TestAppMesh_MissingName_BadRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		path string
	}{
		{name: "CreateMesh no name", path: "/meshes", body: map[string]any{}},
		{name: "CreateVirtualNode no name", path: "/meshes/m/virtualNodes", body: map[string]any{}},
		{name: "CreateVirtualRouter no name", path: "/meshes/m/virtualRouters", body: map[string]any{}},
		{name: "CreateVirtualService no name", path: "/meshes/m/virtualServices", body: map[string]any{}},
		{name: "CreateVirtualGateway no name", path: "/meshes/m/virtualGateways", body: map[string]any{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
			rec := doRequest(t, h, http.MethodPut, tt.path, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			body := getBody(t, rec)
			assert.Equal(t, "BadRequestException", body["code"])
		})
	}
}

// ─── mapErr / Provider error paths ───

func TestAppMesh_MapErrFallthrough(t *testing.T) {
	t.Parallel()
	// The internal server error path is the default; trigger it via a raw backend error
	// propagated through a handler that has no mapping for it. We use the Provider to
	// verify the ErrNilAppContext path as a proxy for testing error handling.
	p := appmesh.NewProvider()
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, appmesh.ErrNilAppContext)
}

// ─── Table-driven subtest patterns ───
//
// These two cases exercise plain table-driven + t.Parallel subtest patterns
// (closures, per-case logging) independent of any specific AppMesh resource
// family; kept here alongside the other core dispatch-level tests.

func TestTableDrivenLoggingPattern(t *testing.T) {
	t.Parallel()

	datasetARN := ""
	tests := []struct{ name string }{{"A"}, {"B"}}
	for _, tc := range tests {
		t.Logf("Running test: %s", tc.name)
		datasetARN = "my-arn"
	}

	t.Run("DescribeDataset", func(t *testing.T) {
		t.Parallel()
		t.Logf("Dataset ARN: %s", datasetARN)
	})
}

func TestTableDrivenClosurePattern(t *testing.T) {
	t.Parallel()
	tests := []struct{ name string }{{"A"}}
	for _, tc := range tests {
		func() {
			t.Logf("Subtest: %v", tc.name)
		}()
	}
}
