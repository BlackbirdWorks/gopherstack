package eks_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

func TestDockerCluster_PublishedPortBindsLoopbackByDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		host   string
		wantIP string
	}{
		{name: "default", host: "", wantIP: "127.0.0.1"},
		{name: "loopback_host", host: "127.0.0.1", wantIP: "127.0.0.1"},
		{name: "external_host", host: "eks.test", wantIP: "0.0.0.0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeClusterRuntime{}
			cfg := eks.ClusterEngineConfig{Probe: newGatedClusterProbe().probe, Host: tt.host}
			b := dockerClusterBackend(t, rt, cfg)

			_, err := b.CreateCluster("c1", "", "", nil, nil, nil)
			require.NoError(t, err)
			waitSpecs(t, rt)

			ip, _, _, err := container.ParsePortSpec(rt.createdSpecs()[0].Ports[0])
			require.NoError(t, err)
			assert.Equal(t, tt.wantIP, ip.String())
		})
	}
}

func TestEnableClusters_DefaultTokenOnlyOnLoopback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		host    string
		token   string
		wantErr bool
	}{
		{name: "default_host_default_token", host: "", token: ""},
		{name: "loopback_default_token", host: "127.0.0.1", token: ""},
		{name: "external_default_token", host: "10.1.2.3", token: "", wantErr: true},
		{name: "external_hostname_default_token", host: "eks.test", token: "", wantErr: true},
		{name: "external_explicit_token", host: "10.1.2.3", token: "abcdefghijklmnop"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := eks.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
			t.Cleanup(b.Close)

			cfg := eks.ClusterEngineConfig{Runtime: &fakeClusterRuntime{}, Host: tt.host, Token: tt.token}
			err := b.EnableClusters(cfg)
			if tt.wantErr {
				require.ErrorIs(t, err, eks.ErrDefaultTokenExposed)

				return
			}

			require.NoError(t, err)
		})
	}
}
