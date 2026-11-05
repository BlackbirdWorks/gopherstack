package ociapigateway_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/apigateway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const caBundleID = "ocid1.cabundle.oc1..clientca"

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  string
}

func newTestCA(t *testing.T, cn string) testCA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cn}, IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(48 * time.Hour),
	}

	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return testCA{cert: cert, key: key, pem: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))}
}

type issued struct {
	tls    tls.Certificate
	certPE string
	keyPE  string
}

func (ca testCA) issue(t *testing.T, tpl *x509.Certificate) issued {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tpl.SerialNumber = big.NewInt(time.Now().UnixNano())

	der, err := x509.CreateCertificate(rand.Reader, tpl, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)

	kb, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	out := issued{
		certPE: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		keyPE:  string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})),
	}
	out.tls, err = tls.X509KeyPair([]byte(out.certPE), []byte(out.keyPE))
	require.NoError(t, err)

	return out
}

func clientTemplate(san string, notAfter time.Time) *x509.Certificate {
	return &x509.Certificate{
		Subject: pkix.Name{CommonName: "client"}, DNSNames: []string{san},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: notAfter,
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
}

type tlsEnv struct {
	*env
	gw         apigateway.Gateway
	serverCA   testCA
	clientCA   testCA
	httpsBase  string
	serverName string
}

func newTLSEnv(t *testing.T) *tlsEnv {
	t.Helper()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "mtls")
	serverCA, clientCA := newTestCA(t, "server-ca"), newTestCA(t, "client-ca")

	srv := serverCA.issue(t, &x509.Certificate{
		Subject: pkix.Name{CommonName: *gw.Hostname}, DNSNames: []string{*gw.Hostname},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})

	cert, err := e.api.CreateCertificate(t.Context(), apigateway.CreateCertificateRequest{
		CompartmentId: new(testCompartment), Certificate: &srv.certPE, PrivateKey: &srv.keyPE,
	})
	require.NoError(t, err)
	e.settle()

	require.NoError(t, e.backend.RegisterCABundle(caBundleID, clientCA.pem))

	_, err = e.gw.UpdateGateway(t.Context(), apigateway.UpdateGatewayRequest{
		GatewayId:     gw.Id,
		CertificateId: cert.Id,
		CaBundles:     []apigateway.CaBundle{apigateway.CertificatesCaBundle{CaBundleId: new(caBundleID)}},
	})
	require.NoError(t, err)
	e.settle()

	return &tlsEnv{
		env: e, gw: gw, serverCA: serverCA, clientCA: clientCA,
		serverName: *gw.Hostname, httpsBase: "https://" + *gw.Hostname + ":" + strconv.Itoa(e.port),
	}
}

// get performs an HTTPS GET to the gateway listener, presenting certs when given.
func (e *tlsEnv) get(t *testing.T, path string, certs ...tls.Certificate) (int, string, error) {
	t.Helper()

	roots := x509.NewCertPool()
	roots.AddCert(e.serverCA.cert)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:      roots,
			ServerName:   e.serverName,
			Certificates: certs,
			MinVersion:   tls.VersionTLS12,
		},
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", "127.0.0.1:"+strconv.Itoa(e.port))
		},
	}
	t.Cleanup(tr.CloseIdleConnections)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, e.httpsBase+path, nil)
	require.NoError(t, err)

	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		return 0, "", err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(body), nil
}

func TestMutualTLS(t *testing.T) {
	t.Parallel()

	e := newTLSEnv(t)
	e.mkDeployment(t, *e.gw.Id, "/m", `{"requestPolicies":{"mutualTls":{"isVerifiedCertificateRequired":true,`+
		`"allowedSans":["*.EXAMPLE.com"]}},"routes":[`+stock("/hi", `"GET"`, "secure")+`]}`)
	e.mkDeployment(t, *e.gw.Id, "/open", `{"routes":[`+stock("/hi", `"GET"`, "open")+`]}`)

	otherCA := newTestCA(t, "rogue-ca")
	good := e.clientCA.issue(t, clientTemplate("client.example.com", time.Now().Add(time.Hour)))
	wrongSan := e.clientCA.issue(t, clientTemplate("client.other.test", time.Now().Add(time.Hour)))
	rogue := otherCA.issue(t, clientTemplate("client.example.com", time.Now().Add(time.Hour)))
	expired := e.clientCA.issue(t, clientTemplate("client.example.com", time.Now().Add(-time.Minute)))

	tests := []struct {
		name  string
		path  string
		certs []tls.Certificate
		want  int
	}{
		{"valid client certificate", "/m/hi", []tls.Certificate{good.tls}, http.StatusOK},
		{"no client certificate", "/m/hi", nil, http.StatusUnauthorized},
		{"certificate from an untrusted ca", "/m/hi", []tls.Certificate{rogue.tls}, http.StatusUnauthorized},
		{"san not allowed", "/m/hi", []tls.Certificate{wrongSan.tls}, http.StatusUnauthorized},
		{"expired certificate", "/m/hi", []tls.Certificate{expired.tls}, http.StatusUnauthorized},
		{"deployment without mtls needs no certificate", "/open/hi", nil, http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, body, err := e.get(t, tt.path, tt.certs...)
			require.NoError(t, err)
			assert.Equal(t, tt.want, status, body)
		})
	}

	t.Run("plain http still serves open deployments only", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, 200, e.call(t, *e.gw.Id, http.MethodGet, "/open/hi", nil, "").status)
		assert.Equal(
			t,
			401,
			e.call(t, *e.gw.Id, http.MethodGet, "/m/hi", nil, "").status,
			"no certificate over plain http",
		)
	})

	t.Run("unknown server name fails the handshake", func(t *testing.T) {
		t.Parallel()

		other := *e
		other.serverName = "nothing.apigateway.us-ashburn-1.oci.customer-oci.com"

		_, _, err := other.get(t, "/open/hi")
		require.Error(t, err)
	})
}

func TestMutualTLSWithoutTrustStoreRejectsEveryone(t *testing.T) {
	t.Parallel()

	e := newTLSEnv(t)
	require.True(t, e.backend.RemoveCABundle(caBundleID))
	e.mkDeployment(t, *e.gw.Id, "/m", `{"requestPolicies":{"mutualTls":{"isVerifiedCertificateRequired":true}},`+
		`"routes":[`+stock("/hi", `"GET"`, "secure")+`]}`)

	good := e.clientCA.issue(t, clientTemplate("client.example.com", time.Now().Add(time.Hour)))

	status, _, err := e.get(t, "/m/hi", good.tls)
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, status)
}

func TestMutualTLSSpecValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "mtls-spec")

	for name, sans := range map[string]string{
		"middle wildcard": `["a.*.com"]`,
		"only wildcard":   `["*"]`,
		"too many":        `["a","b","c","d","e","f","g","h","i","j","k"]`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			spec := `{"requestPolicies":{"mutualTls":{"allowedSans":` + sans + `}},"routes":[]}`
			_, err := e.createDeployment(t, *gw.Id, "/x", specFromJSON(t, spec))
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}
