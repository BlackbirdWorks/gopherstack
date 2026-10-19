package azurekeyvault_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"math/big"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azcertificates"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pkcs12 "software.sslmate.com/src/go-pkcs12"
)

const (
	testPFXType = "application/x-pkcs12"
	testPEMType = "application/x-pem-file"
)

func makePFX(t *testing.T, password string) ([]byte, []byte) {
	t.Helper()

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(48 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	ca, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "leaf.test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	require.NoError(t, err)

	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	pfx, err := pkcs12.Modern.Encode(key, leaf, []*x509.Certificate{ca}, password)
	require.NoError(t, err)

	return pfx, der
}

func TestCertificates_PKCS12_SDK(t *testing.T) {
	t.Parallel()

	s := defaultServer(t)
	ctx := t.Context()
	certs := newCertsClient(t, s, "pfx")
	secrets := newSecretsClient(t, s, "pfx")

	tests := []struct {
		name         string
		password     string
		sendPassword string
		contentType  string
		wantType     string
		wantErr      bool
	}{
		{name: "nopass", wantType: testPFXType},
		{name: "withpass", password: "s3cret", sendPassword: "s3cret", wantType: testPFXType},
		{name: "badpass", password: "s3cret", sendPassword: "nope", wantErr: true},
		{name: "explicitpem", contentType: testPEMType, wantType: testPEMType},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pfx, der := makePFX(t, tt.password)
			params := azcertificates.ImportCertificateParameters{
				Base64EncodedCertificate: new(base64.StdEncoding.EncodeToString(pfx)),
			}

			if tt.sendPassword != "" {
				params.Password = new(tt.sendPassword)
			}

			if tt.contentType != "" {
				params.CertificatePolicy = &azcertificates.CertificatePolicy{
					SecretProperties: &azcertificates.SecretProperties{ContentType: new(tt.contentType)},
				}
			}

			imp, err := certs.ImportCertificate(ctx, tt.name, params, nil)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "400")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, der, imp.CER)
			assert.Equal(t, tt.wantType, *imp.Policy.SecretProperties.ContentType)

			sec, err := secrets.GetSecret(ctx, tt.name, "", nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, *sec.ContentType)

			if tt.wantType != testPFXType {
				return
			}

			raw, err := base64.StdEncoding.DecodeString(*sec.Value)
			require.NoError(t, err)

			_, leaf, chain, err := pkcs12.DecodeChain(raw, "")
			require.NoError(t, err)
			assert.Equal(t, der, leaf.Raw)
			assert.Len(t, chain, 1)
		})
	}
}

func TestCertificates_SelfSigned_SecretFormat_SDK(t *testing.T) {
	t.Parallel()

	s := defaultServer(t)
	ctx := t.Context()
	certs := newCertsClient(t, s, "fmt")
	secrets := newSecretsClient(t, s, "fmt")

	tests := []struct {
		name        string
		contentType string
		wantType    string
	}{
		{name: "pkcs12", contentType: testPFXType, wantType: testPFXType},
		{name: "pem", contentType: testPEMType, wantType: testPEMType},
		{name: "default", wantType: testPEMType},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := selfSignedPolicy("CN="+tt.name, azcertificates.KeyTypeRSA)
			if tt.contentType != "" {
				p.CertificatePolicy.SecretProperties = &azcertificates.SecretProperties{
					ContentType: new(tt.contentType),
				}
			}

			_, err := certs.CreateCertificate(ctx, tt.name, p, nil)
			require.NoError(t, err)

			cert, err := certs.GetCertificate(ctx, tt.name, "", nil)
			require.NoError(t, err)

			sec, err := secrets.GetSecret(ctx, tt.name, "", nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, *sec.ContentType)

			if tt.wantType != testPFXType {
				assert.Contains(t, *sec.Value, "-----BEGIN CERTIFICATE-----")

				return
			}

			raw, err := base64.StdEncoding.DecodeString(*sec.Value)
			require.NoError(t, err)

			priv, leaf, err := pkcs12.Decode(raw, "")
			require.NoError(t, err)
			assert.Equal(t, cert.CER, leaf.Raw)

			rk, ok := priv.(*rsa.PrivateKey)
			require.True(t, ok)
			assert.True(t, rk.PublicKey.Equal(leaf.PublicKey))
		})
	}
}
