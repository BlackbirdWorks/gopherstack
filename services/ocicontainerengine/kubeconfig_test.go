package ocicontainerengine_test

import (
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"io"
	"net/http"
	"testing"

	ke "github.com/oracle/oci-go-sdk/v65/containerengine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type kubeconfig struct {
	Kind           string `yaml:"kind"`
	CurrentContext string `yaml:"current-context"`
	Clusters       []struct {
		Name    string `yaml:"name"`
		Cluster struct {
			Server string `yaml:"server"`
			CA     string `yaml:"certificate-authority-data"`
		} `yaml:"cluster"`
	} `yaml:"clusters"`
	Users []struct {
		Name string `yaml:"name"`
		User struct {
			Token string `yaml:"token"`
		} `yaml:"user"`
	} `yaml:"users"`
	Contexts []struct {
		Name    string `yaml:"name"`
		Context struct {
			Cluster string `yaml:"cluster"`
			User    string `yaml:"user"`
		} `yaml:"context"`
	} `yaml:"contexts"`
}

func readKubeconfig(t *testing.T, resp ke.CreateKubeconfigResponse) kubeconfig {
	t.Helper()

	defer resp.Content.Close()

	raw, err := io.ReadAll(resp.Content)
	require.NoError(t, err)

	var kc kubeconfig
	require.NoError(t, yaml.Unmarshal(raw, &kc))

	return kc
}

func TestCreateKubeconfig(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	native := e.mkCluster(t, "kc-native", nil)
	private := e.mkCluster(
		t,
		"kc-private",
		func(d *ke.CreateClusterDetails) { d.EndpointConfig.IsPublicIpEnabled = new(false) },
	)
	legacy := e.mkCluster(t, "kc-legacy", func(d *ke.CreateClusterDetails) { d.EndpointConfig = nil })

	tests := []struct {
		name       string
		cluster    ke.Cluster
		endpoint   ke.CreateClusterKubeconfigContentDetailsEndpointEnum
		wantServer string
		wantErr    int
	}{
		{"default prefers public", native, "", "https://" + *native.Endpoints.PublicEndpoint, 0},
		{
			"public",
			native,
			ke.CreateClusterKubeconfigContentDetailsEndpointPublicEndpoint,
			"https://" + *native.Endpoints.PublicEndpoint,
			0,
		},
		{
			"private",
			native,
			ke.CreateClusterKubeconfigContentDetailsEndpointPrivateEndpoint,
			"https://" + *native.Endpoints.PrivateEndpoint,
			0,
		},
		{"default private only", private, "", "https://" + *private.Endpoints.PrivateEndpoint, 0},
		{
			"no public endpoint",
			private,
			ke.CreateClusterKubeconfigContentDetailsEndpointPublicEndpoint,
			"",
			http.StatusBadRequest,
		},
		{
			"legacy",
			legacy,
			ke.CreateClusterKubeconfigContentDetailsEndpointLegacyKubernetes,
			"https://" + *legacy.Endpoints.Kubernetes,
			0,
		},
		{"legacy default", legacy, "", "https://" + *legacy.Endpoints.Kubernetes, 0},
		{
			"native has no legacy",
			native,
			ke.CreateClusterKubeconfigContentDetailsEndpointLegacyKubernetes,
			"",
			http.StatusBadRequest,
		},
		{
			"no vcn hostname",
			native,
			ke.CreateClusterKubeconfigContentDetailsEndpointVcnHostname,
			"",
			http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := e.ce.CreateKubeconfig(t.Context(), ke.CreateKubeconfigRequest{
				ClusterId: tt.cluster.Id,
				Endpoint:  tt.endpoint,
			})
			if tt.wantErr != 0 {
				assert.Equal(t, tt.wantErr, svcErr(t, err).GetHTTPStatusCode())

				return
			}

			require.NoError(t, err)

			kc := readKubeconfig(t, resp)
			assert.Equal(t, "Config", kc.Kind)
			require.Len(t, kc.Clusters, 1)
			require.Len(t, kc.Users, 1)
			require.Len(t, kc.Contexts, 1)
			assert.Equal(t, tt.wantServer, kc.Clusters[0].Cluster.Server)
			assert.Equal(t, kc.Contexts[0].Name, kc.CurrentContext)
			assert.Equal(t, kc.Clusters[0].Name, kc.Contexts[0].Context.Cluster)
			assert.Equal(t, kc.Users[0].Name, kc.Contexts[0].Context.User)
			assert.Regexp(t, `^gopherstack-oke-[0-9a-f]{48}$`, kc.Users[0].User.Token)

			caPEM, err := base64.StdEncoding.DecodeString(kc.Clusters[0].Cluster.CA)
			require.NoError(t, err)

			block, _ := pem.Decode(caPEM)
			require.NotNil(t, block)

			cert, err := x509.ParseCertificate(block.Bytes)
			require.NoError(t, err)
			assert.True(t, cert.IsCA)
		})
	}
}

func TestKubeconfigStableAndValidated(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	c := e.mkCluster(t, "kc-stable", nil)

	first, err := e.ce.CreateKubeconfig(t.Context(), ke.CreateKubeconfigRequest{ClusterId: c.Id})
	require.NoError(t, err)

	second, err := e.ce.CreateKubeconfig(t.Context(), ke.CreateKubeconfigRequest{
		ClusterId:    c.Id,
		TokenVersion: new("2.0.0"), Expiration: new(600),
	})
	require.NoError(t, err)
	assert.Equal(t, readKubeconfig(t, first), readKubeconfig(t, second), "CA and token are stable per cluster")

	tests := []struct {
		name string
		in   ke.CreateClusterKubeconfigContentDetails
	}{
		{"bad token version", ke.CreateClusterKubeconfigContentDetails{TokenVersion: new("1.0.0")}},
		{"bad expiration", ke.CreateClusterKubeconfigContentDetails{Expiration: new(0)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, rerr := e.ce.CreateKubeconfig(t.Context(), ke.CreateKubeconfigRequest{
				ClusterId: c.Id, CreateClusterKubeconfigContentDetails: tt.in,
			})
			assert.Equal(t, http.StatusBadRequest, svcErr(t, rerr).GetHTTPStatusCode())
		})
	}

	_, err = e.ce.CreateKubeconfig(
		t.Context(),
		ke.CreateKubeconfigRequest{ClusterId: new("ocid1.cluster.oc1.iad.nope")},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}
