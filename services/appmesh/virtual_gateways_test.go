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

// ─── VirtualGateway + GatewayRoute CRUD (handler) ───

func TestAppMesh_VirtualGatewayAndGatewayRouteCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})

	// Create virtual gateway
	rec := doRequest(t, h, http.MethodPut, "/meshes/m1/virtualGateways",
		map[string]any{"virtualGatewayName": "gw1"})
	assert.Equal(t, http.StatusOK, rec.Code)
	vg := getBody(t, rec)
	assert.Equal(t, "gw1", vg["virtualGatewayName"])

	// Create gateway route (singular /virtualGateway/ in path)
	rec = doRequest(t, h, http.MethodPut, "/meshes/m1/virtualGateway/gw1/gatewayRoutes",
		map[string]any{"gatewayRouteName": "gr1"})
	assert.Equal(t, http.StatusOK, rec.Code)
	gr := getBody(t, rec)
	assert.Equal(t, "gr1", gr["gatewayRouteName"])
	assert.Equal(t, "gw1", gr["virtualGatewayName"])

	// List gateway routes
	rec = doRequest(t, h, http.MethodGet, "/meshes/m1/virtualGateway/gw1/gatewayRoutes", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	body := getBody(t, rec)
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

func TestAppMesh_UpdateVirtualGateway(t *testing.T) {
	t.Parallel()
	tests := []struct {
		body       any
		setup      func(h *appmesh.Handler)
		name       string
		meshName   string
		vgName     string
		wantCode   string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
				doRequest(t, h, http.MethodPut, "/meshes/m1/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
			},
			meshName:   "m1",
			vgName:     "gw1",
			body:       map[string]any{"spec": map[string]any{}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			setup:      func(_ *appmesh.Handler) {},
			meshName:   "no-mesh",
			vgName:     "gw1",
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
			path := fmt.Sprintf("/meshes/%s/virtualGateways/%s", tt.meshName, tt.vgName)
			rec := doRequest(t, h, http.MethodPut, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantCode != "" {
				body := getBody(t, rec)
				assert.Equal(t, tt.wantCode, body["code"])
			}
		})
	}
}

func TestAppMesh_UpdateGatewayRoute(t *testing.T) {
	t.Parallel()
	tests := []struct {
		body       any
		setup      func(h *appmesh.Handler)
		name       string
		meshName   string
		vgName     string
		routeName  string
		wantCode   string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
				doRequest(t, h, http.MethodPut, "/meshes/m1/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
				doRequest(t, h, http.MethodPut, "/meshes/m1/virtualGateway/gw1/gatewayRoutes",
					map[string]any{"gatewayRouteName": "gr1"})
			},
			meshName:   "m1",
			vgName:     "gw1",
			routeName:  "gr1",
			body:       map[string]any{"spec": map[string]any{}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			setup:      func(_ *appmesh.Handler) {},
			meshName:   "no-mesh",
			vgName:     "gw1",
			routeName:  "gr1",
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
			path := fmt.Sprintf("/meshes/%s/virtualGateway/%s/gatewayRoutes/%s",
				tt.meshName, tt.vgName, tt.routeName)
			rec := doRequest(t, h, http.MethodPut, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantCode != "" {
				body := getBody(t, rec)
				assert.Equal(t, tt.wantCode, body["code"])
			}
		})
	}
}

// ─── CreateGatewayRoute missing gatewayRouteName ───

func TestAppMesh_CreateGatewayRouteMissingName(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
	doRequest(t, h, http.MethodPut, "/meshes/m1/virtualGateways",
		map[string]any{"virtualGatewayName": "gw1"})

	rec := doRequest(t, h, http.MethodPut, "/meshes/m1/virtualGateway/gw1/gatewayRoutes",
		map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := getBody(t, rec)
	assert.Equal(t, "BadRequestException", body["code"])
}

// ─── Describe gateway route on missing resource ───

func TestAppMesh_DescribeGatewayRouteNotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
	doRequest(t, h, http.MethodPut, "/meshes/m1/virtualGateways",
		map[string]any{"virtualGatewayName": "gw1"})

	rec := doRequest(t, h, http.MethodGet, "/meshes/m1/virtualGateway/gw1/gatewayRoutes/no-gr", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	body := getBody(t, rec)
	assert.Equal(t, "NotFoundException", body["code"])
}

// ─── VirtualGateway / GatewayRoute backend tests ───

func TestBackend_UpdateVirtualGateway(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErr  error
		setup    func(b *appmesh.InMemoryBackend)
		name     string
		meshName string
		vgName   string
	}{
		{
			name:     "mesh not found",
			meshName: "no-mesh",
			vgName:   "gw1",
			setup:    func(_ *appmesh.InMemoryBackend) {},
			wantErr:  appmesh.ErrMeshNotFound,
		},
		{
			name:     "virtual gateway not found",
			meshName: "m1",
			vgName:   "no-gw",
			setup: func(b *appmesh.InMemoryBackend) {
				_, _ = b.CreateMesh("m1", nil, nil)
			},
			wantErr: appmesh.ErrVirtualGatewayNotFound,
		},
		{
			name:     "success",
			meshName: "m1",
			vgName:   "gw1",
			setup: func(b *appmesh.InMemoryBackend) {
				_, _ = b.CreateMesh("m1", nil, nil)
				_, _ = b.CreateVirtualGateway("m1", "gw1", nil, nil)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := appmesh.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)
			vg, err := b.UpdateVirtualGateway(tt.meshName, tt.vgName, json.RawMessage(`{}`))
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.True(t, appmesh.ErrIs(err, tt.wantErr))

				return
			}
			require.NoError(t, err)
			assert.NotNil(t, vg)
		})
	}
}

func TestBackend_UpdateGatewayRoute(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErr   error
		setup     func(b *appmesh.InMemoryBackend)
		name      string
		meshName  string
		vgName    string
		routeName string
	}{
		{
			name:      "mesh not found",
			meshName:  "no-mesh",
			vgName:    "gw1",
			routeName: "gr1",
			setup:     func(_ *appmesh.InMemoryBackend) {},
			wantErr:   appmesh.ErrMeshNotFound,
		},
		{
			name:      "virtual gateway not found",
			meshName:  "m1",
			vgName:    "no-gw",
			routeName: "gr1",
			setup: func(b *appmesh.InMemoryBackend) {
				_, _ = b.CreateMesh("m1", nil, nil)
			},
			wantErr: appmesh.ErrVirtualGatewayNotFound,
		},
		{
			name:      "gateway route not found",
			meshName:  "m1",
			vgName:    "gw1",
			routeName: "no-gr",
			setup: func(b *appmesh.InMemoryBackend) {
				_, _ = b.CreateMesh("m1", nil, nil)
				_, _ = b.CreateVirtualGateway("m1", "gw1", nil, nil)
			},
			wantErr: appmesh.ErrGatewayRouteNotFound,
		},
		{
			name:      "success",
			meshName:  "m1",
			vgName:    "gw1",
			routeName: "gr1",
			setup: func(b *appmesh.InMemoryBackend) {
				_, _ = b.CreateMesh("m1", nil, nil)
				_, _ = b.CreateVirtualGateway("m1", "gw1", nil, nil)
				_, _ = b.CreateGatewayRoute("m1", "gw1", "gr1", nil, nil)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := appmesh.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)
			gr, err := b.UpdateGatewayRoute(
				tt.meshName,
				tt.vgName,
				tt.routeName,
				json.RawMessage(`{}`),
			)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.True(t, appmesh.ErrIs(err, tt.wantErr))

				return
			}
			require.NoError(t, err)
			assert.NotNil(t, gr)
			assert.Equal(t, int64(2), gr.Meta.Version)
		})
	}
}
