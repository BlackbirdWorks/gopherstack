package gcpcompute_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/require"
)

const (
	gl         = "projects/" + project + "/global/"
	hcRef      = gl + "healthChecks/hc"
	igRef      = "zones/" + zone + "/instanceGroups/ig"
	bsRef      = gl + "backendServices/bs"
	umRef      = gl + "urlMaps/um"
	certRef    = gl + "sslCertificates/cert"
	proxyRef   = gl + "targetHttpProxies/px"
	regBase    = "projects/" + project + "/regions/" + region + "/"
	statusProv = "PROVISIONING"
)

func (e *env) finish(t *testing.T, op *compute.Operation, err error) error {
	t.Helper()

	if err != nil {
		return err
	}

	e.settle()

	return op.Wait(t.Context())
}

func (e *env) lbHC(t *testing.T, name string) {
	t.Helper()

	op, err := e.healthChecks(t).Insert(t.Context(), &computepb.InsertHealthCheckRequest{
		Project: project, HealthCheckResource: &computepb.HealthCheck{
			Name: s(name), HttpHealthCheck: &computepb.HTTPHealthCheck{},
		},
	})
	e.done(t, op, err)
}

func (e *env) lbBS(t *testing.T, bs *computepb.BackendService) error {
	t.Helper()

	op, err := client(t, e, compute.NewBackendServicesRESTClient).Insert(t.Context(),
		&computepb.InsertBackendServiceRequest{Project: project, BackendServiceResource: bs})

	return e.finish(t, op, err)
}

func (e *env) lbMap(t *testing.T, um *computepb.UrlMap) error {
	t.Helper()

	op, err := client(t, e, compute.NewUrlMapsRESTClient).Insert(t.Context(),
		&computepb.InsertUrlMapRequest{Project: project, UrlMapResource: um})

	return e.finish(t, op, err)
}

// lbStack creates hc, an unmanaged group ig, backend service bs and url map um.
func (e *env) lbStack(t *testing.T) {
	t.Helper()
	e.lbHC(t, "hc")
	e.group(t, "ig")
	require.NoError(t, e.lbBS(t, &computepb.BackendService{
		Name: s("bs"), HealthChecks: []string{hcRef},
		Backends: []*computepb.Backend{{Group: s(igRef)}},
	}))
	require.NoError(t, e.lbMap(t, &computepb.UrlMap{Name: s("um"), DefaultService: s(bsRef)}))
}

func selfSignedPEM(t *testing.T, dns ...string) (string, string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test"}, DNSNames: dns,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Date(2036, 1, 2, 3, 4, 5, 0, time.UTC),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}

func (e *env) lbCert(t *testing.T, name string) {
	t.Helper()

	crt, key := selfSignedPEM(t, "a.example.com")
	op, err := client(t, e, compute.NewSslCertificatesRESTClient).Insert(t.Context(),
		&computepb.InsertSslCertificateRequest{Project: project, SslCertificateResource: &computepb.SslCertificate{
			Name: s(name), Certificate: s(crt), PrivateKey: s(key),
		}})
	e.done(t, op, err)
}

func (e *env) lbProxy(t *testing.T, name, urlMap string) error {
	t.Helper()

	op, err := client(t, e, compute.NewTargetHttpProxiesRESTClient).Insert(t.Context(),
		&computepb.InsertTargetHttpProxyRequest{Project: project, TargetHttpProxyResource: &computepb.TargetHttpProxy{
			Name: s(name), UrlMap: s(urlMap),
		}})

	return e.finish(t, op, err)
}

func (e *env) lbRule(t *testing.T, r *computepb.ForwardingRule) error {
	t.Helper()

	op, err := client(t, e, compute.NewGlobalForwardingRulesRESTClient).Insert(t.Context(),
		&computepb.InsertGlobalForwardingRuleRequest{Project: project, ForwardingRuleResource: r})

	return e.finish(t, op, err)
}
