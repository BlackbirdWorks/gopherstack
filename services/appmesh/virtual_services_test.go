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

// ─── VirtualService CRUD (handler) ───

func TestAppMesh_VirtualServiceCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})

	rec := doRequest(t, h, http.MethodPut, "/meshes/m1/virtualServices",
		map[string]any{"virtualServiceName": "svc.local"})
	assert.Equal(t, http.StatusOK, rec.Code)
	vs := getBody(t, rec)
	assert.Equal(t, "svc.local", vs["virtualServiceName"])

	rec = doRequest(t, h, http.MethodGet, "/meshes/m1/virtualServices", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	body := getBody(t, rec)
	assert.Len(t, body["virtualServices"].([]any), 1)

	rec = doRequest(t, h, http.MethodDelete, "/meshes/m1/virtualServices/svc.local", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAppMesh_UpdateVirtualService(t *testing.T) {
	t.Parallel()
	tests := []struct {
		body       any
		setup      func(h *appmesh.Handler)
		name       string
		meshName   string
		vsName     string
		wantCode   string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m1"})
				doRequest(t, h, http.MethodPut, "/meshes/m1/virtualServices",
					map[string]any{"virtualServiceName": "vs1"})
			},
			meshName:   "m1",
			vsName:     "vs1",
			body:       map[string]any{"spec": map[string]any{}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			setup:      func(_ *appmesh.Handler) {},
			meshName:   "no-mesh",
			vsName:     "vs1",
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
			path := fmt.Sprintf("/meshes/%s/virtualServices/%s", tt.meshName, tt.vsName)
			rec := doRequest(t, h, http.MethodPut, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantCode != "" {
				body := getBody(t, rec)
				assert.Equal(t, tt.wantCode, body["code"])
			}
		})
	}
}

// ─── VirtualService backend tests ───

func TestBackend_UpdateVirtualService(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErr  error
		setup    func(b *appmesh.InMemoryBackend)
		name     string
		meshName string
		vsName   string
	}{
		{
			name:     "mesh not found",
			meshName: "no-mesh",
			vsName:   "vs1",
			setup:    func(_ *appmesh.InMemoryBackend) {},
			wantErr:  appmesh.ErrMeshNotFound,
		},
		{
			name:     "virtual service not found",
			meshName: "m1",
			vsName:   "no-vs",
			setup: func(b *appmesh.InMemoryBackend) {
				_, _ = b.CreateMesh("m1", nil, nil)
			},
			wantErr: appmesh.ErrVirtualServiceNotFound,
		},
		{
			name:     "success",
			meshName: "m1",
			vsName:   "vs1",
			setup: func(b *appmesh.InMemoryBackend) {
				_, _ = b.CreateMesh("m1", nil, nil)
				_, _ = b.CreateVirtualService("m1", "vs1", nil, nil)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := appmesh.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)
			vs, err := b.UpdateVirtualService(tt.meshName, tt.vsName, json.RawMessage(`{}`))
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.True(t, appmesh.ErrIs(err, tt.wantErr))

				return
			}
			require.NoError(t, err)
			assert.NotNil(t, vs)
		})
	}
}
