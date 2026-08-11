package appmesh_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appmesh"
)

// TestBackend_DeleteReturnsTerminalStatus verifies every Delete* backend
// method returns the resource with status DELETED rather than the ACTIVE
// status it held before deletion. Confirmed against real App Mesh's
// documented DeleteMesh response (docs.aws.amazon.com/app-mesh/latest/APIReference/API_DeleteMesh.html
// example response: `"status": {"status": "DELETED"}`) and the DELETED enum
// member present on every *StatusCode type in
// aws-sdk-go-v2/service/appmesh@v1.38.4/types/enums.go (MeshStatusCode,
// VirtualNodeStatusCode, VirtualRouterStatusCode, RouteStatusCode,
// VirtualServiceStatusCode, VirtualGatewayStatusCode, GatewayRouteStatusCode).
func TestBackend_DeleteReturnsTerminalStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		delete func(t *testing.T, b *appmesh.InMemoryBackend) string
		name   string
	}{
		{
			name: "mesh",
			delete: func(t *testing.T, b *appmesh.InMemoryBackend) string {
				t.Helper()
				_, err := b.CreateMesh("m1", nil, nil)
				require.NoError(t, err)
				m, err := b.DeleteMesh("m1")
				require.NoError(t, err)

				return m.Status
			},
		},
		{
			name: "virtual node",
			delete: func(t *testing.T, b *appmesh.InMemoryBackend) string {
				t.Helper()
				_, err := b.CreateMesh("m1", nil, nil)
				require.NoError(t, err)
				_, err = b.CreateVirtualNode("m1", "vn1", nil, nil)
				require.NoError(t, err)
				vn, err := b.DeleteVirtualNode("m1", "vn1")
				require.NoError(t, err)

				return vn.Status
			},
		},
		{
			name: "virtual router",
			delete: func(t *testing.T, b *appmesh.InMemoryBackend) string {
				t.Helper()
				_, err := b.CreateMesh("m1", nil, nil)
				require.NoError(t, err)
				_, err = b.CreateVirtualRouter("m1", "vr1", nil, nil)
				require.NoError(t, err)
				vr, err := b.DeleteVirtualRouter("m1", "vr1")
				require.NoError(t, err)

				return vr.Status
			},
		},
		{
			name: "route",
			delete: func(t *testing.T, b *appmesh.InMemoryBackend) string {
				t.Helper()
				_, err := b.CreateMesh("m1", nil, nil)
				require.NoError(t, err)
				_, err = b.CreateVirtualRouter("m1", "vr1", nil, nil)
				require.NoError(t, err)
				_, err = b.CreateRoute("m1", "vr1", "r1", nil, nil)
				require.NoError(t, err)
				r, err := b.DeleteRoute("m1", "vr1", "r1")
				require.NoError(t, err)

				return r.Status
			},
		},
		{
			name: "virtual service",
			delete: func(t *testing.T, b *appmesh.InMemoryBackend) string {
				t.Helper()
				_, err := b.CreateMesh("m1", nil, nil)
				require.NoError(t, err)
				_, err = b.CreateVirtualService("m1", "vs1", nil, nil)
				require.NoError(t, err)
				vs, err := b.DeleteVirtualService("m1", "vs1")
				require.NoError(t, err)

				return vs.Status
			},
		},
		{
			name: "virtual gateway",
			delete: func(t *testing.T, b *appmesh.InMemoryBackend) string {
				t.Helper()
				_, err := b.CreateMesh("m1", nil, nil)
				require.NoError(t, err)
				_, err = b.CreateVirtualGateway("m1", "vg1", nil, nil)
				require.NoError(t, err)
				vg, err := b.DeleteVirtualGateway("m1", "vg1")
				require.NoError(t, err)

				return vg.Status
			},
		},
		{
			name: "gateway route",
			delete: func(t *testing.T, b *appmesh.InMemoryBackend) string {
				t.Helper()
				_, err := b.CreateMesh("m1", nil, nil)
				require.NoError(t, err)
				_, err = b.CreateVirtualGateway("m1", "vg1", nil, nil)
				require.NoError(t, err)
				_, err = b.CreateGatewayRoute("m1", "vg1", "gr1", nil, nil)
				require.NoError(t, err)
				gr, err := b.DeleteGatewayRoute("m1", "vg1", "gr1")
				require.NoError(t, err)

				return gr.Status
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := appmesh.NewInMemoryBackend("000000000000", "us-east-1")
			got := tt.delete(t, b)
			assert.Equal(t, "DELETED", got)
		})
	}
}

// TestAppMesh_DeleteMeshWireStatus verifies the HTTP response body for
// DeleteMesh carries status.status "DELETED", matching the real service's
// documented response shape (see TestBackend_DeleteReturnsTerminalStatus).
func TestAppMesh_DeleteMeshWireStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, "PUT", "/meshes", map[string]any{"meshName": "m1"})

	rec := doRequest(t, h, "DELETE", "/meshes/m1", nil)
	require.Equal(t, 200, rec.Code)
	body := getBody(t, rec)
	status, ok := body["status"].(map[string]any)
	require.True(t, ok, "status must be a nested object")
	assert.Equal(t, "DELETED", status["status"])
}
