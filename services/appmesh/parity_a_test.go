package appmesh_test

// parity_a_test.go — §A parity fix: single-resource responses wrapped under the resource type key.
//
// Real AWS App Mesh wraps every Create/Describe/Update/Delete response under a
// resource-type key:
//   CreateMesh → {"mesh": {...}}
//   CreateVirtualNode → {"virtualNode": {...}}
//   etc.
//
// This test verifies all 7 resource types produce the correct wrapper key.

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appmesh"
)

// TestParity_ResponseWrapper verifies that every single-resource response
// wraps its payload under the canonical AWS resource-type key.
func TestParity_ResponseWrapper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(h *appmesh.Handler)
		method     string
		path       string
		body       any
		wrapperKey string
	}{
		// ── Mesh ──────────────────────────────────────────────────────────────────
		{
			name:       "CreateMesh",
			setup:      func(_ *appmesh.Handler) {},
			method:     http.MethodPut,
			path:       "/meshes",
			body:       map[string]any{"meshName": "wrap-test"},
			wrapperKey: "mesh",
		},
		{
			name: "DescribeMesh",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "wrap-test"})
			},
			method:     http.MethodGet,
			path:       "/meshes/wrap-test",
			wrapperKey: "mesh",
		},
		{
			name: "UpdateMesh",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "wrap-test"})
			},
			method:     http.MethodPut,
			path:       "/meshes/wrap-test",
			body:       map[string]any{},
			wrapperKey: "mesh",
		},
		{
			name: "DeleteMesh",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "wrap-test"})
			},
			method:     http.MethodDelete,
			path:       "/meshes/wrap-test",
			wrapperKey: "mesh",
		},
		// ── VirtualNode ───────────────────────────────────────────────────────────
		{
			name: "CreateVirtualNode",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
			},
			method:     http.MethodPut,
			path:       "/meshes/m/virtualNodes",
			body:       map[string]any{"virtualNodeName": "vn1"},
			wrapperKey: "virtualNode",
		},
		{
			name: "DescribeVirtualNode",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualNodes",
					map[string]any{"virtualNodeName": "vn1"})
			},
			method:     http.MethodGet,
			path:       "/meshes/m/virtualNodes/vn1",
			wrapperKey: "virtualNode",
		},
		{
			name: "UpdateVirtualNode",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualNodes",
					map[string]any{"virtualNodeName": "vn1"})
			},
			method:     http.MethodPut,
			path:       "/meshes/m/virtualNodes/vn1",
			body:       map[string]any{"spec": map[string]any{}},
			wrapperKey: "virtualNode",
		},
		{
			name: "DeleteVirtualNode",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualNodes",
					map[string]any{"virtualNodeName": "vn1"})
			},
			method:     http.MethodDelete,
			path:       "/meshes/m/virtualNodes/vn1",
			wrapperKey: "virtualNode",
		},
		// ── VirtualRouter ─────────────────────────────────────────────────────────
		{
			name: "CreateVirtualRouter",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
			},
			method:     http.MethodPut,
			path:       "/meshes/m/virtualRouters",
			body:       map[string]any{"virtualRouterName": "vr1"},
			wrapperKey: "virtualRouter",
		},
		{
			name: "DescribeVirtualRouter",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
			},
			method:     http.MethodGet,
			path:       "/meshes/m/virtualRouters/vr1",
			wrapperKey: "virtualRouter",
		},
		{
			name: "UpdateVirtualRouter",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
			},
			method:     http.MethodPut,
			path:       "/meshes/m/virtualRouters/vr1",
			body:       map[string]any{"spec": map[string]any{}},
			wrapperKey: "virtualRouter",
		},
		{
			name: "DeleteVirtualRouter",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
			},
			method:     http.MethodDelete,
			path:       "/meshes/m/virtualRouters/vr1",
			wrapperKey: "virtualRouter",
		},
		// ── Route ─────────────────────────────────────────────────────────────────
		{
			name: "CreateRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
			},
			method:     http.MethodPut,
			path:       "/meshes/m/virtualRouter/vr1/routes",
			body:       map[string]any{"routeName": "rt1"},
			wrapperKey: "route",
		},
		{
			name: "DescribeRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouter/vr1/routes",
					map[string]any{"routeName": "rt1"})
			},
			method:     http.MethodGet,
			path:       "/meshes/m/virtualRouter/vr1/routes/rt1",
			wrapperKey: "route",
		},
		{
			name: "UpdateRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouter/vr1/routes",
					map[string]any{"routeName": "rt1"})
			},
			method:     http.MethodPut,
			path:       "/meshes/m/virtualRouter/vr1/routes/rt1",
			body:       map[string]any{"spec": map[string]any{}},
			wrapperKey: "route",
		},
		{
			name: "DeleteRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouter/vr1/routes",
					map[string]any{"routeName": "rt1"})
			},
			method:     http.MethodDelete,
			path:       "/meshes/m/virtualRouter/vr1/routes/rt1",
			wrapperKey: "route",
		},
		// ── VirtualService ────────────────────────────────────────────────────────
		{
			name: "CreateVirtualService",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
			},
			method:     http.MethodPut,
			path:       "/meshes/m/virtualServices",
			body:       map[string]any{"virtualServiceName": "svc.local"},
			wrapperKey: "virtualService",
		},
		{
			name: "DescribeVirtualService",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualServices",
					map[string]any{"virtualServiceName": "svc.local"})
			},
			method:     http.MethodGet,
			path:       "/meshes/m/virtualServices/svc.local",
			wrapperKey: "virtualService",
		},
		{
			name: "UpdateVirtualService",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualServices",
					map[string]any{"virtualServiceName": "svc.local"})
			},
			method:     http.MethodPut,
			path:       "/meshes/m/virtualServices/svc.local",
			body:       map[string]any{"spec": map[string]any{}},
			wrapperKey: "virtualService",
		},
		{
			name: "DeleteVirtualService",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualServices",
					map[string]any{"virtualServiceName": "svc.local"})
			},
			method:     http.MethodDelete,
			path:       "/meshes/m/virtualServices/svc.local",
			wrapperKey: "virtualService",
		},
		// ── VirtualGateway ────────────────────────────────────────────────────────
		{
			name: "CreateVirtualGateway",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
			},
			method:     http.MethodPut,
			path:       "/meshes/m/virtualGateways",
			body:       map[string]any{"virtualGatewayName": "gw1"},
			wrapperKey: "virtualGateway",
		},
		{
			name: "DescribeVirtualGateway",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
			},
			method:     http.MethodGet,
			path:       "/meshes/m/virtualGateways/gw1",
			wrapperKey: "virtualGateway",
		},
		{
			name: "UpdateVirtualGateway",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
			},
			method:     http.MethodPut,
			path:       "/meshes/m/virtualGateways/gw1",
			body:       map[string]any{"spec": map[string]any{}},
			wrapperKey: "virtualGateway",
		},
		{
			name: "DeleteVirtualGateway",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
			},
			method:     http.MethodDelete,
			path:       "/meshes/m/virtualGateways/gw1",
			wrapperKey: "virtualGateway",
		},
		// ── GatewayRoute ──────────────────────────────────────────────────────────
		{
			name: "CreateGatewayRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
			},
			method:     http.MethodPut,
			path:       "/meshes/m/virtualGateway/gw1/gatewayRoutes",
			body:       map[string]any{"gatewayRouteName": "gr1"},
			wrapperKey: "gatewayRoute",
		},
		{
			name: "DescribeGatewayRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateway/gw1/gatewayRoutes",
					map[string]any{"gatewayRouteName": "gr1"})
			},
			method:     http.MethodGet,
			path:       "/meshes/m/virtualGateway/gw1/gatewayRoutes/gr1",
			wrapperKey: "gatewayRoute",
		},
		{
			name: "UpdateGatewayRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateway/gw1/gatewayRoutes",
					map[string]any{"gatewayRouteName": "gr1"})
			},
			method:     http.MethodPut,
			path:       "/meshes/m/virtualGateway/gw1/gatewayRoutes/gr1",
			body:       map[string]any{"spec": map[string]any{}},
			wrapperKey: "gatewayRoute",
		},
		{
			name: "DeleteGatewayRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateway/gw1/gatewayRoutes",
					map[string]any{"gatewayRouteName": "gr1"})
			},
			method:     http.MethodDelete,
			path:       "/meshes/m/virtualGateway/gw1/gatewayRoutes/gr1",
			wrapperKey: "gatewayRoute",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			tt.setup(h)
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			require.Equal(t, http.StatusOK, rec.Code, "%s: unexpected status", tt.name)

			body := getBody(t, rec)
			resource, ok := body[tt.wrapperKey].(map[string]any)
			require.True(t, ok,
				"%s: response must be wrapped under %q key; got keys: %v",
				tt.name, tt.wrapperKey, mapKeys(body))
			assert.NotEmpty(t, resource, "%s: wrapped resource must not be empty", tt.name)
		})
	}
}

// mapKeys returns the top-level keys of a map for use in error messages.
func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	return keys
}
