package gcpsql_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"
)

func parseCert(t *testing.T, p string) *x509.Certificate {
	t.Helper()

	block, _ := pem.Decode([]byte(p))
	require.NotNil(t, block)

	c, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	return c
}

func TestSslCerts(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	inst := e.createInstance(t, "c1", "POSTGRES_16", nil)

	res, err := e.svc.SslCerts.Insert(project, "c1", &sqladmin.SslCertsInsertRequest{CommonName: "client-a"}).
		Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "sql#sslCertsInsert", res.Kind)
	assert.NotEmpty(t, res.ClientCert.CertPrivateKey)

	ca, client := parseCert(t, inst.ServerCaCert.Cert), parseCert(t, res.ClientCert.CertInfo.Cert)
	assert.True(t, ca.IsCA)
	require.NoError(t, client.CheckSignatureFrom(ca))
	assert.Equal(t, "client-a", client.Subject.CommonName)

	fp := res.ClientCert.CertInfo.Sha1Fingerprint
	require.NotEmpty(t, fp)

	got, err := e.svc.SslCerts.Get(project, "c1", fp).Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "client-a", got.CommonName)

	list, err := e.svc.SslCerts.List(project, "c1").Context(ctx).Do()
	require.NoError(t, err)
	assert.Len(t, list.Items, 1)

	op, err := e.svc.SslCerts.Delete(project, "c1", fp).Context(ctx).Do()
	e.mustWait(t, op, err)

	_, err = e.svc.SslCerts.Get(project, "c1", fp).Context(ctx).Do()
	code, _ := apiCode(t, err)
	assert.Equal(t, http.StatusNotFound, code)
}

func TestEphemeralCertAndConnectSettings(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	inst := e.createInstance(t, "c2", "MYSQL_8_0", nil)

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)

	pub := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))

	res, err := e.svc.Connect.GenerateEphemeralCert(project, "c2", &sqladmin.GenerateEphemeralCertRequest{
		PublicKey: pub, ValidDuration: "600s",
	}).Context(ctx).Do()
	require.NoError(t, err)

	cert := parseCert(t, res.EphemeralCert.Cert)
	require.NoError(t, cert.CheckSignatureFrom(parseCert(t, inst.ServerCaCert.Cert)))
	assert.Less(t, cert.NotAfter.Sub(cert.NotBefore).Minutes(), 15.0)

	_, err = e.svc.Connect.GenerateEphemeralCert(project, "c2", &sqladmin.GenerateEphemeralCertRequest{PublicKey: "junk"}).
		Context(ctx).
		Do()
	code, _ := apiCode(t, err)
	assert.Equal(t, http.StatusBadRequest, code)

	cs, err := e.svc.Connect.Get(project, "c2").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "sql#connectSettings", cs.Kind)
	assert.Equal(t, "MYSQL_8_0", cs.DatabaseVersion)
	assert.Equal(t, inst.ServerCaCert.Cert, cs.ServerCaCert.Cert)
	require.Len(t, cs.IpAddresses, 1)
	assert.Equal(t, "127.0.0.1", cs.IpAddresses[0].IpAddress)
}
