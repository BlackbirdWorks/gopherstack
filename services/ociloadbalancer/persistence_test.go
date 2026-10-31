package ociloadbalancer_test

import (
	"crypto/tls"
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/loadbalancer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ociloadbalancer "github.com/blackbirdworks/gopherstack/services/ociloadbalancer"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	certPEM, keyPEM := genCert(t)
	src := newEnv(t, envOpts{})
	a := startBackend(t, "a")
	d := baseDetails("persist")
	d.FreeformTags = map[string]string{"k": "v"}
	d.BackendSets = map[string]loadbalancer.BackendSetDetails{"bs": backendSet("ROUND_ROBIN", a)}
	d.Certificates = map[string]loadbalancer.CertificateDetails{
		"cert": {CertificateName: sp("cert"), PublicCertificate: sp(string(certPEM)), PrivateKey: sp(string(keyPEM))},
	}
	d.Listeners = map[string]loadbalancer.ListenerDetails{
		"plain": listener("bs"),
		"tls": {
			DefaultBackendSetName: sp("bs"), Port: sp(443), Protocol: sp("HTTP"),
			SslConfiguration: &loadbalancer.SslConfigurationDetails{CertificateName: sp("cert")},
		},
	}
	id := src.mkLB(t, d)
	snap := src.backend.Snapshot(t.Context())

	dst := newEnv(t, envOpts{})
	require.NoError(t, dst.backend.Restore(t.Context(), snap))

	got := dst.get(t, id)
	assert.Equal(t, "persist", *got.DisplayName)
	assert.Equal(t, "v", got.FreeformTags["k"])
	assert.Equal(t, loadbalancer.LoadBalancerLifecycleStateActive, got.LifecycleState)
	assert.Contains(t, got.Certificates, "cert")

	t.Run("listeners reopen and route", func(t *testing.T) {
		r := dst.call(t, id, httpPort, "", "/again", nil)
		assert.Equal(t, http.StatusOK, r.status)
		assert.Equal(t, "a /again", r.body)
	})

	t.Run("private keys are not restored", func(t *testing.T) {
		conn, err := tls.Dial("tcp", dst.addr(t, id, 443), tlsClient(t, certPEM, "localhost").Transport.(*http.Transport).TLSClientConfig)
		if err == nil {
			_ = conn.Close()
		}

		require.Error(t, err)
	})

	t.Run("work requests survive", func(t *testing.T) {
		wrs, err := dst.lb.ListWorkRequests(t.Context(), loadbalancer.ListWorkRequestsRequest{LoadBalancerId: &id})
		require.NoError(t, err)
		assert.NotEmpty(t, wrs.Items)
	})

	t.Run("restore of garbage fails", func(t *testing.T) {
		require.Error(t, dst.backend.Restore(t.Context(), []byte("{")))
	})

	t.Run("reset closes listeners", func(t *testing.T) {
		dst.backend.Reset()
		assert.Empty(t, dst.backend.Endpoints())
		_, err := dst.lb.GetLoadBalancer(t.Context(), loadbalancer.GetLoadBalancerRequest{LoadBalancerId: &id})
		assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
	})
}

type cfgStub struct{ s ociloadbalancer.Settings }

func (c cfgStub) GetOCILoadBalancerSettings() ociloadbalancer.Settings { return c.s }

func TestProviderInit(t *testing.T) {
	t.Parallel()

	p := &ociloadbalancer.Provider{}

	_, err := p.Init(nil)
	require.ErrorIs(t, err, ociloadbalancer.ErrNilAppContext)

	tests := []struct {
		name string
		cfg  any
		port int
	}{
		{"defaults", nil, ociloadbalancer.DefaultPort},
		{"from config", cfgStub{ociloadbalancer.Settings{Port: 9999, EnforceAuth: true}}, 9999},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reg, err := p.Init(&service.AppContext{Config: tt.cfg})
			require.NoError(t, err)

			h, ok := reg.(*ociloadbalancer.Handler)
			require.True(t, ok)
			assert.Equal(t, tt.port, h.Port)
			assert.Equal(t, "OCILoadBalancer", h.Name())
			assert.NotEmpty(t, h.GetSupportedOperations())
			assert.Equal(t, 8165, ociloadbalancer.DefaultPort)
		})
	}
}
