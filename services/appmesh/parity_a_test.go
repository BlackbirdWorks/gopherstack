package appmesh_test

// parity_a_test.go — §A parity fix: single-resource responses match AWS wire format.
//
// Real AWS App Mesh returns every Create/Describe/Update/Delete response with
// the resource data at the top level of the JSON body (no wrapper key):
//   CreateMesh → {"meshName": "...", "metadata": {...}, ...}
//   CreateVirtualNode → {"meshName": "...", "virtualNodeName": "...", ...}
//   etc.
//
// This test verifies all 7 resource types return the expected top-level field.

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appmesh"
)

// TestParity_ResponseTopLevel verifies that every single-resource response
// returns data at the top level (no resource-type wrapper key).
func TestParity_ResponseTopLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(h *appmesh.Handler)
		method    string
		path      string
		body      any
		topField  string // a field expected at the top level of the response
	}{
		// ── Mesh ──────────────────────────────────────────────────────────────────
		{
			name:     "CreateMesh",
			setup:    func(_ *appmesh.Handler) {},
			method:   http.MethodPut,
			path:     "/meshes",
			body:     map[string]any{"meshName": "wrap-test"},
			topField: "meshName",
		},
		{
			name: "DescribeMesh",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "wrap-test"})
			},
			method:   http.MethodGet,
			path:     "/meshes/wrap-test",
			topField: "meshName",
		},
		{
			name: "UpdateMesh",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "wrap-test"})
			},
			method:   http.MethodPut,
			path:     "/meshes/wrap-test",
			body:     map[string]any{},
			topField: "meshName",
		},
		{
			name: "DeleteMesh",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "wrap-test"})
			},
			method:   http.MethodDelete,
			path:     "/meshes/wrap-test",
			topField: "meshName",
		},
		// ── VirtualNode ───────────────────────────────────────────────────────────
		{
			name: "CreateVirtualNode",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualNodes",
			body:     map[string]any{"virtualNodeName": "vn1"},
			topField: "virtualNodeName",
		},
		{
			name: "DescribeVirtualNode",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualNodes",
					map[string]any{"virtualNodeName": "vn1"})
			},
			method:   http.MethodGet,
			path:     "/meshes/m/virtualNodes/vn1",
			topField: "virtualNodeName",
		},
		{
			name: "UpdateVirtualNode",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualNodes",
					map[string]any{"virtualNodeName": "vn1"})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualNodes/vn1",
			body:     map[string]any{"spec": map[string]any{}},
			topField: "virtualNodeName",
		},
		{
			name: "DeleteVirtualNode",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualNodes",
					map[string]any{"virtualNodeName": "vn1"})
			},
			method:   http.MethodDelete,
			path:     "/meshes/m/virtualNodes/vn1",
			topField: "virtualNodeName",
		},
		// ── VirtualRouter ─────────────────────────────────────────────────────────
		{
			name: "CreateVirtualRouter",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualRouters",
			body:     map[string]any{"virtualRouterName": "vr1"},
			topField: "virtualRouterName",
		},
		{
			name: "DescribeVirtualRouter",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
			},
			method:   http.MethodGet,
			path:     "/meshes/m/virtualRouters/vr1",
			topField: "virtualRouterName",
		},
		{
			name: "UpdateVirtualRouter",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualRouters/vr1",
			body:     map[string]any{"spec": map[string]any{}},
			topField: "virtualRouterName",
		},
		{
			name: "DeleteVirtualRouter",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
			},
			method:   http.MethodDelete,
			path:     "/meshes/m/virtualRouters/vr1",
			topField: "virtualRouterName",
		},
		// ── Route ─────────────────────────────────────────────────────────────────
		{
			name: "CreateRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualRouter/vr1/routes",
			body:     map[string]any{"routeName": "rt1", "spec": map[string]any{}},
			topField: "routeName",
		},
		{
			name: "DescribeRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouter/vr1/routes",
					map[string]any{"routeName": "rt1", "spec": map[string]any{}})
			},
			method:   http.MethodGet,
			path:     "/meshes/m/virtualRouter/vr1/routes/rt1",
			topField: "routeName",
		},
		{
			name: "UpdateRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouters",
					map[string]any{"virtualRouterName": "vr1"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualRouter/vr1/routes",
					map[string]any{"routeName": "rt1", "spec": map[string]any{}})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualRouter/vr1/routes/rt1",
			body:     map[string]any{"spec": map[string]any{}},
			topField: "routeName",
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
			method:   http.MethodDelete,
			path:     "/meshes/m/virtualRouter/vr1/routes/rt1",
			topField: "routeName",
		},
		// ── VirtualService ────────────────────────────────────────────────────────
		{
			name: "CreateVirtualService",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualServices",
			body:     map[string]any{"virtualServiceName": "svc.local"},
			topField: "virtualServiceName",
		},
		{
			name: "DescribeVirtualService",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualServices",
					map[string]any{"virtualServiceName": "svc.local"})
			},
			method:   http.MethodGet,
			path:     "/meshes/m/virtualServices/svc.local",
			topField: "virtualServiceName",
		},
		{
			name: "UpdateVirtualService",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualServices",
					map[string]any{"virtualServiceName": "svc.local"})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualServices/svc.local",
			body:     map[string]any{"spec": map[string]any{}},
			topField: "virtualServiceName",
		},
		{
			name: "DeleteVirtualService",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualServices",
					map[string]any{"virtualServiceName": "svc.local"})
			},
			method:   http.MethodDelete,
			path:     "/meshes/m/virtualServices/svc.local",
			topField: "virtualServiceName",
		},
		// ── VirtualGateway ────────────────────────────────────────────────────────
		{
			name: "CreateVirtualGateway",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualGateways",
			body:     map[string]any{"virtualGatewayName": "gw1"},
			topField: "virtualGatewayName",
		},
		{
			name: "DescribeVirtualGateway",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
			},
			method:   http.MethodGet,
			path:     "/meshes/m/virtualGateways/gw1",
			topField: "virtualGatewayName",
		},
		{
			name: "UpdateVirtualGateway",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualGateways/gw1",
			body:     map[string]any{"spec": map[string]any{}},
			topField: "virtualGatewayName",
		},
		{
			name: "DeleteVirtualGateway",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
			},
			method:   http.MethodDelete,
			path:     "/meshes/m/virtualGateways/gw1",
			topField: "virtualGatewayName",
		},
		// ── GatewayRoute ──────────────────────────────────────────────────────────
		{
			name: "CreateGatewayRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualGateway/gw1/gatewayRoutes",
			body:     map[string]any{"gatewayRouteName": "gr1", "spec": map[string]any{}},
			topField: "gatewayRouteName",
		},
		{
			name: "DescribeGatewayRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateway/gw1/gatewayRoutes",
					map[string]any{"gatewayRouteName": "gr1", "spec": map[string]any{}})
			},
			method:   http.MethodGet,
			path:     "/meshes/m/virtualGateway/gw1/gatewayRoutes/gr1",
			topField: "gatewayRouteName",
		},
		{
			name: "UpdateGatewayRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateway/gw1/gatewayRoutes",
					map[string]any{"gatewayRouteName": "gr1", "spec": map[string]any{}})
			},
			method:   http.MethodPut,
			path:     "/meshes/m/virtualGateway/gw1/gatewayRoutes/gr1",
			body:     map[string]any{"spec": map[string]any{}},
			topField: "gatewayRouteName",
		},
		{
			name: "DeleteGatewayRoute",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "m"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateways",
					map[string]any{"virtualGatewayName": "gw1"})
				doRequest(t, h, http.MethodPut, "/meshes/m/virtualGateway/gw1/gatewayRoutes",
					map[string]any{"gatewayRouteName": "gr1", "spec": map[string]any{}})
			},
			method:   http.MethodDelete,
			path:     "/meshes/m/virtualGateway/gw1/gatewayRoutes/gr1",
			topField: "gatewayRouteName",
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
			_, ok := body[tt.topField]
			assert.True(t, ok,
				"%s: response must have %q at top level; got keys: %v",
				tt.name, tt.topField, mapKeys(body))
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
