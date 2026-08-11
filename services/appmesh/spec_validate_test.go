package appmesh_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/appmesh"
)

// TestBackend_MeshSpecValidation verifies CreateMesh rejects a structurally
// invalid MeshSpec (types.MeshSpec: egressFilter.type must be one of
// ALLOW_ALL|DROP_ALL; serviceDiscovery.ipPreference, if present, must be one
// of the four IpPreference enum members — aws-sdk-go-v2/service/appmesh@v1.38.4/types/enums.go).
func TestBackend_MeshSpecValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		spec    string
		wantErr bool
	}{
		{name: "empty spec ok", spec: ``, wantErr: false},
		{name: "unrelated fields passthrough", spec: `{"unknownField":"x"}`, wantErr: false},
		{name: "valid egress filter", spec: `{"egressFilter":{"type":"ALLOW_ALL"}}`, wantErr: false},
		{name: "invalid egress filter type", spec: `{"egressFilter":{"type":"BOGUS"}}`, wantErr: true},
		{name: "valid ip preference", spec: `{"serviceDiscovery":{"ipPreference":"IPv4_ONLY"}}`, wantErr: false},
		{name: "invalid ip preference", spec: `{"serviceDiscovery":{"ipPreference":"BOGUS"}}`, wantErr: true},
		{name: "wrong type for egressFilter", spec: `{"egressFilter":"not-an-object"}`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := appmesh.NewInMemoryBackend("000000000000", "us-east-1")
			var raw json.RawMessage
			if tt.spec != "" {
				raw = json.RawMessage(tt.spec)
			}
			_, err := b.CreateMesh("m1", raw, nil)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrInvalidParameter)

				return
			}
			require.NoError(t, err)
		})
	}
}

// TestBackend_VirtualRouterSpecValidation verifies CreateVirtualRouter
// rejects a structurally invalid VirtualRouterSpec (types.VirtualRouterSpec:
// each listener's portMapping.port must be in [1, 65535] — confirmed via
// docs.aws.amazon.com/app-mesh/latest/APIReference/API_PortMapping.html —
// and portMapping.protocol must be one of http|tcp|http2|grpc).
func TestBackend_VirtualRouterSpecValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		spec    string
		wantErr bool
	}{
		{name: "empty spec ok", spec: ``, wantErr: false},
		{
			name:    "valid listener",
			spec:    `{"listeners":[{"portMapping":{"port":8080,"protocol":"http"}}]}`,
			wantErr: false,
		},
		{
			name:    "invalid protocol",
			spec:    `{"listeners":[{"portMapping":{"port":8080,"protocol":"bogus"}}]}`,
			wantErr: true,
		},
		{
			name:    "port out of range",
			spec:    `{"listeners":[{"portMapping":{"port":70000,"protocol":"http"}}]}`,
			wantErr: true,
		},
		{
			name:    "port zero",
			spec:    `{"listeners":[{"portMapping":{"port":0,"protocol":"http"}}]}`,
			wantErr: true,
		},
		{
			name:    "missing port mapping",
			spec:    `{"listeners":[{}]}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := appmesh.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateMesh("m1", nil, nil)
			require.NoError(t, err)

			var raw json.RawMessage
			if tt.spec != "" {
				raw = json.RawMessage(tt.spec)
			}
			_, err = b.CreateVirtualRouter("m1", "vr1", raw, nil)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrInvalidParameter)

				return
			}
			require.NoError(t, err)
		})
	}
}

// TestBackend_VirtualServiceSpecValidation verifies CreateVirtualService
// rejects a structurally invalid VirtualServiceSpec (types.VirtualServiceSpec:
// provider is a smithy union — exactly one of virtualNode/virtualRouter may
// be set, each requiring its name field — aws-sdk-go-v2/service/appmesh@v1.38.4/types/types.go
// VirtualServiceProviderMemberVirtualNode/MemberVirtualRouter).
func TestBackend_VirtualServiceSpecValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		spec    string
		wantErr bool
	}{
		{name: "empty spec ok", spec: ``, wantErr: false},
		{
			name:    "valid virtual node provider",
			spec:    `{"provider":{"virtualNode":{"virtualNodeName":"vn1"}}}`,
			wantErr: false,
		},
		{
			name:    "valid virtual router provider",
			spec:    `{"provider":{"virtualRouter":{"virtualRouterName":"vr1"}}}`,
			wantErr: false,
		},
		{
			name: "both provider members set",
			spec: `{"provider":{"virtualNode":{"virtualNodeName":"vn1"},` +
				`"virtualRouter":{"virtualRouterName":"vr1"}}}`,
			wantErr: true,
		},
		{
			name:    "no provider member set",
			spec:    `{"provider":{}}`,
			wantErr: true,
		},
		{
			name:    "provider member missing name",
			spec:    `{"provider":{"virtualNode":{}}}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := appmesh.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateMesh("m1", nil, nil)
			require.NoError(t, err)

			var raw json.RawMessage
			if tt.spec != "" {
				raw = json.RawMessage(tt.spec)
			}
			_, err = b.CreateVirtualService("m1", "vs1", raw, nil)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrInvalidParameter)

				return
			}
			require.NoError(t, err)
		})
	}
}
