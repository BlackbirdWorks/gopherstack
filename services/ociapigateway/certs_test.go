package ociapigateway_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/apigateway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func selfSigned(t *testing.T, cn string) (string, string) {
	t.Helper()

	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: cn}, DNSNames: []string{cn, "alt." + cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
	}

	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &k.PublicKey, k)
	require.NoError(t, err)

	kb, err := x509.MarshalECPrivateKey(k)
	require.NoError(t, err)

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}))
}

func TestCertificateLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	certPEM, keyPEM := selfSigned(t, "api.example.test")

	created, err := e.api.CreateCertificate(t.Context(), apigateway.CreateCertificateRequest{
		CompartmentId: new(testCompartment), Certificate: &certPEM, PrivateKey: &keyPEM, DisplayName: new("c1"),
	})
	require.NoError(t, err)
	require.NotNil(t, created.OpcWorkRequestId)
	assert.Equal(t, []string{"api.example.test", "alt.api.example.test"}, created.SubjectNames)
	assert.True(t, created.TimeNotValidAfter.After(time.Now()))

	e.settle()

	got, err := e.api.GetCertificate(t.Context(), apigateway.GetCertificateRequest{CertificateId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, apigateway.CertificateLifecycleStateActive, got.LifecycleState)
	assert.Equal(t, certPEM, *got.Certificate.Certificate)

	list, err := e.api.ListCertificates(
		t.Context(),
		apigateway.ListCertificatesRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	gw, err := e.gw.CreateGateway(t.Context(), apigateway.CreateGatewayRequest{
		CompartmentId: new(testCompartment), EndpointType: apigateway.GatewayEndpointTypePublic,
		SubnetId: new(testSubnet), CertificateId: created.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, *created.Id, *gw.CertificateId)

	_, err = e.api.DeleteCertificate(t.Context(), apigateway.DeleteCertificateRequest{CertificateId: created.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "certificate in use")

	_, err = e.api.UpdateCertificate(t.Context(), apigateway.UpdateCertificateRequest{
		CertificateId: created.Id,
		DisplayName:   new("c2"),
	})
	require.NoError(t, err)

	e.settle()

	_, err = e.api.ChangeCertificateCompartment(t.Context(), apigateway.ChangeCertificateCompartmentRequest{
		CertificateId: created.Id,
		CompartmentId: new(otherCompart),
	})
	require.NoError(t, err)
}

func TestCertificateValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	certPEM, keyPEM := selfSigned(t, "a.example.test")
	_, otherKey := selfSigned(t, "b.example.test")

	tests := []struct {
		name         string
		cert, key    string
		intermediate string
		status       int
	}{
		{"mismatched key", certPEM, otherKey, "", 400},
		{"garbage cert", "nope", keyPEM, "", 400},
		{"missing key", certPEM, "", "", 400},
		{"bad intermediate", certPEM, keyPEM, "junk", 400},
		{"ok with intermediate", certPEM, keyPEM, certPEM, 202},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.api.CreateCertificate(t.Context(), apigateway.CreateCertificateRequest{
				CompartmentId: new(testCompartment), Certificate: &tt.cert, PrivateKey: &tt.key,
				IntermediateCertificates: &tt.intermediate, DisplayName: new(tt.name),
			})
			if tt.status == 202 {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}
