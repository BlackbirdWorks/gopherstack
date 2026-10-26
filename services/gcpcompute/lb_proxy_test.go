package gcpcompute_test

import (
	"net/http"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelfManagedCertificate(t *testing.T) {
	t.Parallel()

	good, key := selfSignedPEM(t, "a.example.com", "b.example.com")
	_, otherKey := selfSignedPEM(t, "x.example.com")

	tests := []struct {
		cert   *computepb.SslCertificate
		name   string
		status int
	}{
		{name: "valid", cert: &computepb.SslCertificate{Certificate: s(good), PrivateKey: s(key)}},
		{
			name: "via selfManaged block",
			cert: &computepb.SslCertificate{
				SelfManaged: &computepb.SslCertificateSelfManagedSslCertificate{
					Certificate: s(good),
					PrivateKey:  s(key),
				},
			},
		},
		{
			name:   "garbage",
			cert:   &computepb.SslCertificate{Certificate: s("x"), PrivateKey: s("y")},
			status: http.StatusBadRequest,
		},
		{
			name:   "key mismatch",
			cert:   &computepb.SslCertificate{Certificate: s(good), PrivateKey: s(otherKey)},
			status: http.StatusBadRequest,
		},
		{name: "no key", cert: &computepb.SslCertificate{Certificate: s(good)}, status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			certs := client(t, e, compute.NewSslCertificatesRESTClient)
			tt.cert.Name = s("cert")

			op, err := certs.Insert(
				t.Context(),
				&computepb.InsertSslCertificateRequest{Project: project, SslCertificateResource: tt.cert},
			)
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "")

				return
			}

			e.done(t, op, err)

			got, gerr := certs.Get(
				t.Context(),
				&computepb.GetSslCertificateRequest{Project: project, SslCertificate: "cert"},
			)
			require.NoError(t, gerr)
			assert.Equal(t, "SELF_MANAGED", got.GetType())
			assert.Equal(t, []string{"a.example.com", "b.example.com"}, got.GetSubjectAlternativeNames())
			assert.Contains(t, got.GetExpireTime(), "2036-01-02T03:04:05")
			assert.Empty(t, got.GetPrivateKey())
			assert.NotEmpty(t, got.GetCertificate())
		})
	}
}

func TestManagedCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		domains []string
		status  int
	}{
		{name: "valid", domains: []string{"www.example.com", "example.com"}},
		{name: "wildcard", domains: []string{"*.example.com"}, status: http.StatusBadRequest},
		{name: "no domains", status: http.StatusBadRequest},
		{name: "not a domain", domains: []string{"localhost"}, status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			certs := client(t, e, compute.NewSslCertificatesRESTClient)
			op, err := certs.Insert(t.Context(), &computepb.InsertSslCertificateRequest{
				Project: project, SslCertificateResource: &computepb.SslCertificate{
					Name: s(
						"m",
					),
					Type:    s("MANAGED"),
					Managed: &computepb.SslCertificateManagedSslCertificate{Domains: tt.domains},
				},
			})

			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "")

				return
			}

			e.done(t, op, err)

			e.clock.Advance(24 * 3600e9)

			got, gerr := certs.Get(
				t.Context(),
				&computepb.GetSslCertificateRequest{Project: project, SslCertificate: "m"},
			)
			require.NoError(t, gerr)
			assert.Equal(t, statusProv, got.GetManaged().GetStatus())
			assert.Equal(t, statusProv, got.GetManaged().GetDomainStatus()["example.com"])
		})
	}
}

func TestSslPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pol      *computepb.SslPolicy
		name     string
		features int
		status   int
	}{
		{name: "compatible default", pol: &computepb.SslPolicy{}, features: 15},
		{
			name:     "restricted",
			pol:      &computepb.SslPolicy{Profile: s("RESTRICTED"), MinTlsVersion: s("TLS_1_3")},
			features: 6,
		},
		{
			name:   "tls13 needs restricted",
			pol:    &computepb.SslPolicy{MinTlsVersion: s("TLS_1_3")},
			status: http.StatusBadRequest,
		},
		{name: "custom", pol: &computepb.SslPolicy{
			Profile: s(
				"CUSTOM",
			),
			CustomFeatures: []string{"TLS_RSA_WITH_AES_128_GCM_SHA256", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"},
		}, features: 2},
		{name: "custom needs features", pol: &computepb.SslPolicy{Profile: s("CUSTOM")}, status: http.StatusBadRequest},
		{
			name:   "custom unknown feature",
			pol:    &computepb.SslPolicy{Profile: s("CUSTOM"), CustomFeatures: []string{"NOPE"}},
			status: http.StatusBadRequest,
		},
		{
			name:   "features without custom",
			pol:    &computepb.SslPolicy{CustomFeatures: []string{"TLS_RSA_WITH_AES_128_GCM_SHA256"}},
			status: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			pols := client(t, e, compute.NewSslPoliciesRESTClient)
			tt.pol.Name = s("pol")

			op, err := pols.Insert(
				t.Context(),
				&computepb.InsertSslPolicyRequest{Project: project, SslPolicyResource: tt.pol},
			)
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "")

				return
			}

			e.done(t, op, err)

			got, gerr := pols.Get(t.Context(), &computepb.GetSslPolicyRequest{Project: project, SslPolicy: "pol"})
			require.NoError(t, gerr)
			assert.Len(t, got.GetEnabledFeatures(), tt.features)

			feats, ferr := pols.ListAvailableFeatures(
				t.Context(),
				&computepb.ListAvailableFeaturesSslPoliciesRequest{Project: project},
			)
			require.NoError(t, ferr)
			assert.Len(t, feats.GetFeatures(), 15)
		})
	}
}

func TestTargetProxies(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.lbStack(t)
	e.lbCert(t, "cert")

	httpsProxies := client(t, e, compute.NewTargetHttpsProxiesRESTClient)
	tcpProxies := client(t, e, compute.NewTargetTcpProxiesRESTClient)
	sslProxies := client(t, e, compute.NewTargetSslProxiesRESTClient)
	pols := client(t, e, compute.NewSslPoliciesRESTClient)

	polOp, polErr := pols.Insert(t.Context(), &computepb.InsertSslPolicyRequest{
		Project: project, SslPolicyResource: &computepb.SslPolicy{Name: s("pol")},
	})
	e.done(t, polOp, polErr)

	t.Run("http", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, e.lbProxy(t, "px", umRef))
		assertAPIError(t, e.lbProxy(t, "px2", gl+"urlMaps/missing"), http.StatusNotFound, "notFound")
		assertAPIError(t, e.lbProxy(t, "px", umRef), http.StatusConflict, "alreadyExists")
	})

	t.Run("https needs certificates", func(t *testing.T) {
		t.Parallel()

		_, err := httpsProxies.Insert(t.Context(), &computepb.InsertTargetHttpsProxyRequest{
			Project: project,
			TargetHttpsProxyResource: &computepb.TargetHttpsProxy{Name: s("sp-nocert"), UrlMap: s(umRef)},
		})
		assertAPIError(t, err, http.StatusBadRequest, "required")
	})

	t.Run("https with certificate, policy and url map swap", func(t *testing.T) {
		t.Parallel()

		op, err := httpsProxies.Insert(t.Context(), &computepb.InsertTargetHttpsProxyRequest{
			Project: project, TargetHttpsProxyResource: &computepb.TargetHttpsProxy{
				Name: s(
					"sp",
				),
				UrlMap:          s(umRef),
				SslCertificates: []string{certRef},
				SslPolicy:       s(gl + "sslPolicies/pol"),
			},
		})
		e.done(t, op, err)

		got, err := httpsProxies.Get(
			t.Context(),
			&computepb.GetTargetHttpsProxyRequest{Project: project, TargetHttpsProxy: "sp"},
		)
		require.NoError(t, err)
		assert.Equal(t, "NONE", got.GetQuicOverride())
		assert.Contains(t, got.GetSslCertificates()[0], "/sslCertificates/cert")

		require.NoError(t, e.lbMap(t, &computepb.UrlMap{Name: s("um3"), DefaultService: s(bsRef)}))

		op, err = httpsProxies.SetUrlMap(t.Context(), &computepb.SetUrlMapTargetHttpsProxyRequest{
			Project: project, TargetHttpsProxy: "sp",
			UrlMapReferenceResource: &computepb.UrlMapReference{UrlMap: s(gl + "urlMaps/um3")},
		})
		e.done(t, op, err)

		got, err = httpsProxies.Get(
			t.Context(),
			&computepb.GetTargetHttpsProxyRequest{Project: project, TargetHttpsProxy: "sp"},
		)
		require.NoError(t, err)
		assert.Contains(t, got.GetUrlMap(), "/urlMaps/um3")

		_, err = httpsProxies.SetUrlMap(t.Context(), &computepb.SetUrlMapTargetHttpsProxyRequest{
			Project: project, TargetHttpsProxy: "sp",
			UrlMapReferenceResource: &computepb.UrlMapReference{UrlMap: s(gl + "urlMaps/none")},
		})
		assertAPIError(t, err, http.StatusNotFound, "notFound")
	})

	t.Run("tcp and ssl proxies", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, e.lbBS(t, &computepb.BackendService{
			Name: s("tcpbs"), Protocol: s("TCP"), HealthChecks: []string{hcRef},
		}))

		op, err := tcpProxies.Insert(t.Context(), &computepb.InsertTargetTcpProxyRequest{
			Project: project, TargetTcpProxyResource: &computepb.TargetTcpProxy{
				Name: s("tp"), Service: s(gl + "backendServices/tcpbs"), ProxyHeader: s("PROXY_V1"),
			},
		})
		e.done(t, op, err)

		_, err = tcpProxies.Insert(t.Context(), &computepb.InsertTargetTcpProxyRequest{
			Project:                project,
			TargetTcpProxyResource: &computepb.TargetTcpProxy{Name: s("tp2"), ProxyHeader: s("BOGUS")},
		})
		assertAPIError(t, err, http.StatusBadRequest, "invalid")

		op, err = sslProxies.Insert(t.Context(), &computepb.InsertTargetSslProxyRequest{
			Project: project, TargetSslProxyResource: &computepb.TargetSslProxy{
				Name: s("slp"), Service: s(gl + "backendServices/tcpbs"), SslCertificates: []string{certRef},
			},
		})
		e.done(t, op, err)

		got, err := sslProxies.Get(
			t.Context(),
			&computepb.GetTargetSslProxyRequest{Project: project, TargetSslProxy: "slp"},
		)
		require.NoError(t, err)
		assert.Equal(t, "NONE", got.GetProxyHeader())
	})
}
