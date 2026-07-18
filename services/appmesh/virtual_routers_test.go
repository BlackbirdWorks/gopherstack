package appmesh_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appmesh"
)

// ─── VirtualRouter + Route CRUD (handler) ───

func TestAppMesh_VirtualRouterAndRouteCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})

	// Create virtual router
	rec := doRequest(t, h, http.MethodPut, "/meshes/m1/virtualRouters",
		map[string]any{"virtualRouterName": "vr1"})
	assert.Equal(t, http.StatusOK, rec.Code)
	vr := getBody(t, rec)
	assert.Equal(t, "vr1", vr["virtualRouterName"])

	// Create route (note singular /virtualRouter/ in path)
	rec = doRequest(t, h, http.MethodPut, "/meshes/m1/virtualRouter/vr1/routes",
		map[string]any{"routeName": "r1"})
	assert.Equal(t, http.StatusOK, rec.Code)
	route := getBody(t, rec)
	assert.Equal(t, "r1", route["routeName"])
	assert.Equal(t, "vr1", route["virtualRouterName"])
	assert.Contains(t, route["metadata"].(map[string]any)["arn"].(string), "route/r1")

	// List routes
	rec = doRequest(t, h, http.MethodGet, "/meshes/m1/virtualRouter/vr1/routes", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	body := getBody(t, rec)
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

func TestAppMesh_UpdateVirtualRouter(t *testing.T) {
	t.Parallel()
	tests := []struct {
		body       any
		setup      func(h *appmesh.Handler)
		name       string
		meshName   string
		vrName     string
		wantCode   string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
				doRequest(t, h, http.MethodPut, "/meshes/m1/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
			},
			meshName:   "m1",
			vrName:     "vr1",
			body:       map[string]any{"spec": map[string]any{}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "mesh not found",
			setup:      func(_ *appmesh.Handler) {},
			meshName:   "no-mesh",
			vrName:     "vr1",
			body:       map[string]any{},
			wantStatus: http.StatusNotFound,
			wantCode:   "NotFoundException",
		},
		{
			name: "virtual router not found",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
			},
			meshName:   "m1",
			vrName:     "no-vr",
			body:       map[string]any{},
			wantStatus: http.StatusNotFound,
			wantCode:   "NotFoundException",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			tt.setup(h)
			path := fmt.Sprintf("/meshes/%s/virtualRouters/%s", tt.meshName, tt.vrName)
			rec := doRequest(t, h, http.MethodPut, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantCode != "" {
				body := getBody(t, rec)
				assert.Equal(t, tt.wantCode, body["code"])
			} else if tt.wantStatus == http.StatusOK {
				vr := getBody(t, rec)
				assert.Equal(t, tt.vrName, vr["virtualRouterName"])
			}
		})
	}
}

func TestAppMesh_UpdateRoute(t *testing.T) {
	t.Parallel()
	tests := []struct {
		body       any
		setup      func(h *appmesh.Handler)
		name       string
		meshName   string
		vrName     string
		routeName  string
		wantCode   string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
				doRequest(t, h, http.MethodPut, "/meshes/m1/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
				doRequest(t, h, http.MethodPut, "/meshes/m1/virtualRouter/vr1/routes",
					map[string]any{"routeName": "r1"})
			},
			meshName:   "m1",
			vrName:     "vr1",
			routeName:  "r1",
			body:       map[string]any{"spec": map[string]any{}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "mesh not found",
			setup:      func(_ *appmesh.Handler) {},
			meshName:   "no-mesh",
			vrName:     "vr1",
			routeName:  "r1",
			body:       map[string]any{},
			wantStatus: http.StatusNotFound,
			wantCode:   "NotFoundException",
		},
		{
			name: "route not found",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
				doRequest(t, h, http.MethodPut, "/meshes/m1/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
			},
			meshName:   "m1",
			vrName:     "vr1",
			routeName:  "no-route",
			body:       map[string]any{},
			wantStatus: http.StatusNotFound,
			wantCode:   "NotFoundException",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			tt.setup(h)
			path := fmt.Sprintf(
				"/meshes/%s/virtualRouter/%s/routes/%s",
				tt.meshName,
				tt.vrName,
				tt.routeName,
			)
			rec := doRequest(t, h, http.MethodPut, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantCode != "" {
				body := getBody(t, rec)
				assert.Equal(t, tt.wantCode, body["code"])
			}
		})
	}
}

// ─── Describe/Delete route on missing virtual router ───

func TestAppMesh_RouteOpsOnMissingVirtualRouter(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		method string
	}{
		{"describe route", http.MethodGet},
		{"delete route", http.MethodDelete},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
			// No virtual router created — routes should 404
			rec := doRequest(t, h, tt.method, "/meshes/m1/virtualRouter/no-vr/routes/r1", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
			body := getBody(t, rec)
			assert.Equal(t, "NotFoundException", body["code"])
		})
	}
}

// ─── CreateRoute missing routeName ───

func TestAppMesh_CreateRouteMissingName(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
	doRequest(t, h, http.MethodPut, "/meshes/m1/virtualRouters",
		map[string]any{"virtualRouterName": "vr1"})

	rec := doRequest(t, h, http.MethodPut, "/meshes/m1/virtualRouter/vr1/routes",
		map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := getBody(t, rec)
	assert.Equal(t, "BadRequestException", body["code"])
}

// ─── Describe route on missing resource ───

func TestAppMesh_DescribeRouteNotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
	doRequest(t, h, http.MethodPut, "/meshes/m1/virtualRouters",
		map[string]any{"virtualRouterName": "vr1"})

	rec := doRequest(t, h, http.MethodGet, "/meshes/m1/virtualRouter/vr1/routes/no-route", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	body := getBody(t, rec)
	assert.Equal(t, "NotFoundException", body["code"])
}

// ─── VirtualRouter / Route backend tests ───

func TestBackend_UpdateVirtualRouter(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErr  error
		setup    func(b *appmesh.InMemoryBackend)
		name     string
		meshName string
		vrName   string
		wantNil  bool
	}{
		{
			name:     "mesh not found",
			meshName: "no-mesh",
			vrName:   "vr1",
			setup:    func(_ *appmesh.InMemoryBackend) {},
			wantErr:  appmesh.ErrMeshNotFound,
		},
		{
			name:     "virtual router not found",
			meshName: "m1",
			vrName:   "no-vr",
			setup: func(b *appmesh.InMemoryBackend) {
				_, _ = b.CreateMesh("m1", nil, nil)
			},
			wantErr: appmesh.ErrVirtualRouterNotFound,
		},
		{
			name:     "success",
			meshName: "m1",
			vrName:   "vr1",
			setup: func(b *appmesh.InMemoryBackend) {
				_, _ = b.CreateMesh("m1", nil, nil)
				_, _ = b.CreateVirtualRouter("m1", "vr1", nil, nil)
			},
			wantNil: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := appmesh.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)
			vr, err := b.UpdateVirtualRouter(tt.meshName, tt.vrName, json.RawMessage(`{}`))
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.True(t, appmesh.ErrIs(err, tt.wantErr))

				return
			}
			require.NoError(t, err)
			assert.NotNil(t, vr)
		})
	}
}

func TestBackend_UpdateRoute(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErr   error
		setup     func(b *appmesh.InMemoryBackend)
		name      string
		meshName  string
		vrName    string
		routeName string
	}{
		{
			name:      "mesh not found",
			meshName:  "no-mesh",
			vrName:    "vr1",
			routeName: "r1",
			setup:     func(_ *appmesh.InMemoryBackend) {},
			wantErr:   appmesh.ErrMeshNotFound,
		},
		{
			name:      "virtual router not found",
			meshName:  "m1",
			vrName:    "no-vr",
			routeName: "r1",
			setup: func(b *appmesh.InMemoryBackend) {
				_, _ = b.CreateMesh("m1", nil, nil)
			},
			wantErr: appmesh.ErrVirtualRouterNotFound,
		},
		{
			name:      "route not found",
			meshName:  "m1",
			vrName:    "vr1",
			routeName: "no-route",
			setup: func(b *appmesh.InMemoryBackend) {
				_, _ = b.CreateMesh("m1", nil, nil)
				_, _ = b.CreateVirtualRouter("m1", "vr1", nil, nil)
			},
			wantErr: appmesh.ErrRouteNotFound,
		},
		{
			name:      "success",
			meshName:  "m1",
			vrName:    "vr1",
			routeName: "r1",
			setup: func(b *appmesh.InMemoryBackend) {
				_, _ = b.CreateMesh("m1", nil, nil)
				_, _ = b.CreateVirtualRouter("m1", "vr1", nil, nil)
				_, _ = b.CreateRoute("m1", "vr1", "r1", nil, nil)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := appmesh.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)
			r, err := b.UpdateRoute(tt.meshName, tt.vrName, tt.routeName, json.RawMessage(`{}`))
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.True(t, appmesh.ErrIs(err, tt.wantErr))

				return
			}
			require.NoError(t, err)
			assert.NotNil(t, r)
			assert.Equal(t, int64(2), r.Meta.Version)
		})
	}
}
