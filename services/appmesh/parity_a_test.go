package appmesh_test

// parity_a_test.go — §A parity fix: single-resource responses match AWS wire format.
//
// Real AWS App Mesh wraps every Create/Describe/Update/Delete response body in
// a resource-type key, confirmed directly against the
// aws-sdk-go-v2/service/appmesh deserializers (e.g.
// awsRestjson1_deserializeOpDocumentCreateMeshOutput reads the "mesh" key;
// awsRestjson1_deserializeOpDocumentCreateVirtualNodeOutput reads
// "virtualNode"; etc.) — NOT flat at the top level:
//   CreateMesh → {"mesh": {"meshName": "...", "metadata": {...}, ...}}
//   CreateVirtualNode → {"virtualNode": {"meshName": "...", "virtualNodeName": "...", ...}}
//   etc.
//
// This test verifies all 7 resource types wrap their response under the
// expected key, with the identifying field nested inside it.

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
		name     string
		setup    func(h *appmesh.Handler)
		method   string
		path     string
		body     any
		wrapKey  string // the resource-type key the response must be wrapped under
		topField string // a field expected inside the wrapped resource object
	}{
		// ── Mesh ──────────────────────────────────────────────────────────────────
		{
			name:     "CreateMesh",
			setup:    func(_ *appmesh.Handler) {},
			method:   http.MethodPut,
			path:     "/meshes",
			body:     map[string]any{"meshName": "wrap-test"},
			wrapKey:  "mesh",
			topField: "meshName",
		},
		{
			name: "DescribeMesh",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "wrap-test"})
			},
			method:   http.MethodGet,
			path:     "/meshes/wrap-test",
			wrapKey:  "mesh",
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
			wrapKey:  "mesh",
			topField: "meshName",
		},
		{
			name: "DeleteMesh",
			setup: func(h *appmesh.Handler) {
				doRequest(t, h, http.MethodPut, "/meshes", map[string]any{"meshName": "wrap-test"})
			},
			method:   http.MethodDelete,
			path:     "/meshes/wrap-test",
			wrapKey:  "mesh",
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
			wrapKey:  "virtualNode",
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
			wrapKey:  "virtualNode",
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
			wrapKey:  "virtualNode",
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
			wrapKey:  "virtualNode",
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
			wrapKey:  "virtualRouter",
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
			wrapKey:  "virtualRouter",
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
			wrapKey:  "virtualRouter",
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
			wrapKey:  "virtualRouter",
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
			wrapKey:  "route",
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
			wrapKey:  "route",
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
			wrapKey:  "route",
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
			wrapKey:  "route",
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
			wrapKey:  "virtualService",
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
			wrapKey:  "virtualService",
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
			wrapKey:  "virtualService",
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
			wrapKey:  "virtualService",
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
			wrapKey:  "virtualGateway",
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
			wrapKey:  "virtualGateway",
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
			wrapKey:  "virtualGateway",
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
			wrapKey:  "virtualGateway",
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
			wrapKey:  "gatewayRoute",
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
			wrapKey:  "gatewayRoute",
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
			wrapKey:  "gatewayRoute",
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
			wrapKey:  "gatewayRoute",
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
			wrapped, ok := body[tt.wrapKey].(map[string]any)
			require.True(t, ok,
				"%s: response must be wrapped under %q; got keys: %v",
				tt.name, tt.wrapKey, mapKeys(body))
			_, ok = wrapped[tt.topField]
			assert.True(t, ok,
				"%s: wrapped %q object must have %q; got keys: %v",
				tt.name, tt.wrapKey, tt.topField, mapKeys(wrapped))
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
