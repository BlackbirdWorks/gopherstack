package appmesh_test

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

// ─── Mesh CRUD ────────────────────────────────────────────────────────────────

func TestAppMesh_MeshCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// CreateMesh
	rec := doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "my-mesh"})
	assert.Equal(t, http.StatusOK, rec.Code)
	body := getBody(t, rec)
	mesh := body["mesh"].(map[string]any)
	assert.Equal(t, "my-mesh", mesh["meshName"])
	assert.Equal(t, "ACTIVE", mesh["status"].(map[string]any)["status"])
	meta := mesh["metadata"].(map[string]any)
	assert.NotEmpty(t, meta["arn"])
	assert.NotEmpty(t, meta["uid"])
	assert.Equal(t, float64(1), meta["version"])
	assert.Contains(t, meta["arn"].(string), "arn:aws:appmesh:us-east-1:000000000000:mesh/my-mesh")

	// DescribeMesh
	rec = doRequest(t, h, http.MethodGet, "/meshes/my-mesh", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	body = getBody(t, rec)
	assert.Equal(t, "my-mesh", body["mesh"].(map[string]any)["meshName"])

	// ListMeshes
	rec = doRequest(t, h, http.MethodGet, "/meshes", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	body = getBody(t, rec)
	meshes := body["meshes"].([]any)
	assert.Len(t, meshes, 1)

	// UpdateMesh
	rec = doRequest(t, h, http.MethodPut, "/meshes/my-mesh",
		map[string]any{"spec": map[string]any{"egressFilter": map[string]any{"type": "ALLOW_ALL"}}})
	assert.Equal(t, http.StatusOK, rec.Code)
	body = getBody(t, rec)
	mesh = body["mesh"].(map[string]any)
	assert.Equal(t, float64(2), mesh["metadata"].(map[string]any)["version"])

	// DeleteMesh
	rec = doRequest(t, h, http.MethodDelete, "/meshes/my-mesh", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Confirm deleted
	rec = doRequest(t, h, http.MethodGet, "/meshes/my-mesh", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAppMesh_MeshCreateDuplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "dup"})
	rec := doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "dup"})
	assert.Equal(t, http.StatusConflict, rec.Code)
	body := getBody(t, rec)
	assert.Equal(t, "ConflictException", body["code"])
}

func TestAppMesh_MeshDeleteWithResources(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "mesh1"})
	doRequest(t, h, http.MethodPut, "/meshes/mesh1/virtualNodes",
		map[string]any{"virtualNodeName": "vn1"})

	rec := doRequest(t, h, http.MethodDelete, "/meshes/mesh1", nil)
	assert.Equal(t, http.StatusConflict, rec.Code)
	body := getBody(t, rec)
	assert.Equal(t, "ResourceInUseException", body["code"])
}

// ─── VirtualNode CRUD ─────────────────────────────────────────────────────────

func TestAppMesh_VirtualNodeCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})

	// Create
	rec := doRequest(t, h, http.MethodPut, "/meshes/m1/virtualNodes",
		map[string]any{"virtualNodeName": "vn1"})
	assert.Equal(t, http.StatusOK, rec.Code)
	body := getBody(t, rec)
	vn := body["virtualNode"].(map[string]any)
	assert.Equal(t, "vn1", vn["virtualNodeName"])
	assert.Contains(t, vn["metadata"].(map[string]any)["arn"].(string), "virtualNode/vn1")

	// Describe
	rec = doRequest(t, h, http.MethodGet, "/meshes/m1/virtualNodes/vn1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = doRequest(t, h, http.MethodGet, "/meshes/m1/virtualNodes", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	body = getBody(t, rec)
	assert.Len(t, body["virtualNodes"].([]any), 1)

	// Update
	rec = doRequest(t, h, http.MethodPut, "/meshes/m1/virtualNodes/vn1",
		map[string]any{"spec": map[string]any{}})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/meshes/m1/virtualNodes/vn1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Confirm deleted
	rec = doRequest(t, h, http.MethodGet, "/meshes/m1/virtualNodes/vn1", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ─── VirtualRouter + Route CRUD ───────────────────────────────────────────────

func TestAppMesh_VirtualRouterAndRouteCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})

	// Create virtual router
	rec := doRequest(t, h, http.MethodPut, "/meshes/m1/virtualRouters",
		map[string]any{"virtualRouterName": "vr1"})
	assert.Equal(t, http.StatusOK, rec.Code)
	body := getBody(t, rec)
	vr := body["virtualRouter"].(map[string]any)
	assert.Equal(t, "vr1", vr["virtualRouterName"])

	// Create route (note singular /virtualRouter/ in path)
	rec = doRequest(t, h, http.MethodPut, "/meshes/m1/virtualRouter/vr1/routes",
		map[string]any{"routeName": "r1"})
	assert.Equal(t, http.StatusOK, rec.Code)
	body = getBody(t, rec)
	route := body["route"].(map[string]any)
	assert.Equal(t, "r1", route["routeName"])
	assert.Equal(t, "vr1", route["virtualRouterName"])
	assert.Contains(t, route["metadata"].(map[string]any)["arn"].(string), "route/r1")

	// List routes
	rec = doRequest(t, h, http.MethodGet, "/meshes/m1/virtualRouter/vr1/routes", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	body = getBody(t, rec)
	assert.Len(t, body["routes"].([]any), 1)

	// DeleteRouter with routes → conflict
	rec = doRequest(t, h, http.MethodDelete, "/meshes/m1/virtualRouters/vr1", nil)
	assert.Equal(t, http.StatusConflict, rec.Code)

	// Delete route first
	rec = doRequest(t, h, http.MethodDelete, "/meshes/m1/virtualRouter/vr1/routes/r1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Now delete router
	rec = doRequest(t, h, http.MethodDelete, "/meshes/m1/virtualRouters/vr1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ─── VirtualService CRUD ─────────────────────────────────────────────────────

func TestAppMesh_VirtualServiceCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})

	rec := doRequest(t, h, http.MethodPut, "/meshes/m1/virtualServices",
		map[string]any{"virtualServiceName": "svc.local"})
	assert.Equal(t, http.StatusOK, rec.Code)
	body := getBody(t, rec)
	vs := body["virtualService"].(map[string]any)
	assert.Equal(t, "svc.local", vs["virtualServiceName"])

	rec = doRequest(t, h, http.MethodGet, "/meshes/m1/virtualServices", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	body = getBody(t, rec)
	assert.Len(t, body["virtualServices"].([]any), 1)

	rec = doRequest(t, h, http.MethodDelete, "/meshes/m1/virtualServices/svc.local", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ─── VirtualGateway + GatewayRoute CRUD ──────────────────────────────────────

func TestAppMesh_VirtualGatewayAndGatewayRouteCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})

	// Create virtual gateway
	rec := doRequest(t, h, http.MethodPut, "/meshes/m1/virtualGateways",
		map[string]any{"virtualGatewayName": "gw1"})
	assert.Equal(t, http.StatusOK, rec.Code)
	body := getBody(t, rec)
	vg := body["virtualGateway"].(map[string]any)
	assert.Equal(t, "gw1", vg["virtualGatewayName"])

	// Create gateway route (singular /virtualGateway/ in path)
	rec = doRequest(t, h, http.MethodPut, "/meshes/m1/virtualGateway/gw1/gatewayRoutes",
		map[string]any{"gatewayRouteName": "gr1"})
	assert.Equal(t, http.StatusOK, rec.Code)
	body = getBody(t, rec)
	gr := body["gatewayRoute"].(map[string]any)
	assert.Equal(t, "gr1", gr["gatewayRouteName"])
	assert.Equal(t, "gw1", gr["virtualGatewayName"])

	// List gateway routes
	rec = doRequest(t, h, http.MethodGet, "/meshes/m1/virtualGateway/gw1/gatewayRoutes", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	body = getBody(t, rec)
	assert.Len(t, body["gatewayRoutes"].([]any), 1)

	// Delete gateway with routes → conflict
	rec = doRequest(t, h, http.MethodDelete, "/meshes/m1/virtualGateways/gw1", nil)
	assert.Equal(t, http.StatusConflict, rec.Code)

	// Delete route first, then gateway
	rec = doRequest(t, h, http.MethodDelete, "/meshes/m1/virtualGateway/gw1/gatewayRoutes/gr1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodDelete, "/meshes/m1/virtualGateways/gw1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func TestAppMesh_TagOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{
		"meshName": "tagged-mesh",
		"tags":     []map[string]string{{"key": "env", "value": "test"}},
	})

	// Get mesh ARN
	rec := doRequest(t, h, http.MethodGet, "/meshes/tagged-mesh", nil)
	body := getBody(t, rec)
	arn := body["mesh"].(map[string]any)["metadata"].(map[string]any)["arn"].(string)

	// ListTags
	rec = doRequest(t, h, http.MethodGet, fmt.Sprintf("/tags?resourceArn=%s", arn), nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	body = getBody(t, rec)
	tags := body["tags"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0].(map[string]any)["key"])

	// TagResource
	rec = doRequest(t, h, http.MethodPut, "/tag",
		map[string]any{"resourceArn": arn, "tags": []map[string]string{{"key": "team", "value": "platform"}}})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify tag added
	rec = doRequest(t, h, http.MethodGet, fmt.Sprintf("/tags?resourceArn=%s", arn), nil)
	body = getBody(t, rec)
	assert.Len(t, body["tags"].([]any), 2)

	// UntagResource
	rec = doRequest(t, h, http.MethodPut, "/untag",
		map[string]any{"resourceArn": arn, "tagKeys": []string{"env"}})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify tag removed
	rec = doRequest(t, h, http.MethodGet, fmt.Sprintf("/tags?resourceArn=%s", arn), nil)
	body = getBody(t, rec)
	assert.Len(t, body["tags"].([]any), 1)
	assert.Equal(t, "team", body["tags"].([]any)[0].(map[string]any)["key"])
}

// ─── Not found errors ─────────────────────────────────────────────────────────

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

// ─── List empty results ───────────────────────────────────────────────────────

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

// ─── Missing name validation ──────────────────────────────────────────────────

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
