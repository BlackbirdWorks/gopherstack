package iot_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func mustParseCert(t *testing.T, certPEM string) *x509.Certificate {
	t.Helper()

	block, _ := pem.Decode([]byte(certPEM))
	require.NotNil(t, block, "certificate PEM must decode")

	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	return cert
}

func TestCreateKeysAndCertificate_IssuesRealCertificate_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestIoTClient(t, iot.NewHandler(iot.NewInMemoryBackend(), nil))
	ctx := t.Context()

	first, err := client.CreateKeysAndCertificate(ctx, &iotsdk.CreateKeysAndCertificateInput{SetAsActive: true})
	require.NoError(t, err)

	second, err := client.CreateKeysAndCertificate(ctx, &iotsdk.CreateKeysAndCertificateInput{})
	require.NoError(t, err)

	cert := mustParseCert(t, aws.ToString(first.CertificatePem))
	assert.NotEqual(t, aws.ToString(first.CertificatePem), aws.ToString(second.CertificatePem))
	assert.NotEqual(t, aws.ToString(first.CertificateId), aws.ToString(second.CertificateId))

	sum := sha256.Sum256(cert.Raw)
	assert.Equal(t, hex.EncodeToString(sum[:]), aws.ToString(first.CertificateId))

	block, _ := pem.Decode([]byte(aws.ToString(first.KeyPair.PrivateKey)))
	require.NotNil(t, block)
	assert.Equal(t, "RSA PRIVATE KEY", block.Type)

	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	require.NoError(t, err)
	assert.True(t, key.PublicKey.Equal(cert.PublicKey), "certificate must carry the returned key's public half")

	pubBlock, _ := pem.Decode([]byte(aws.ToString(first.KeyPair.PublicKey)))
	require.NotNil(t, pubBlock)

	pub, err := x509.ParsePKIXPublicKey(pubBlock.Bytes)
	require.NoError(t, err)
	assert.True(t, key.PublicKey.Equal(pub))

	desc, err := client.DescribeCertificate(ctx, &iotsdk.DescribeCertificateInput{CertificateId: first.CertificateId})
	require.NoError(t, err)
	assert.Equal(t, cert.NotAfter.Unix(), desc.CertificateDescription.Validity.NotAfter.Unix())
	assert.Equal(t, aws.ToString(first.CertificatePem), aws.ToString(desc.CertificateDescription.CertificatePem))
}

func TestCreateCertificateFromCsr_SignsSubmittedKey_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		key  func(t *testing.T) any
		name string
	}{
		{name: "rsa", key: func(t *testing.T) any {
			t.Helper()

			k, err := rsa.GenerateKey(rand.Reader, 2048)
			require.NoError(t, err)

			return k
		}},
		{name: "ecdsa", key: func(t *testing.T) any {
			t.Helper()

			k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			require.NoError(t, err)

			return k
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key := tt.key(t)
			der, err := x509.CreateCertificateRequest(rand.Reader,
				&x509.CertificateRequest{Subject: pkix.Name{CommonName: "device"}}, key)
			require.NoError(t, err)

			csrPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}))

			client := newTestIoTClient(t, iot.NewHandler(iot.NewInMemoryBackend(), nil))
			out, err := client.CreateCertificateFromCsr(t.Context(), &iotsdk.CreateCertificateFromCsrInput{
				CertificateSigningRequest: aws.String(csrPEM), SetAsActive: true,
			})
			require.NoError(t, err)

			cert := mustParseCert(t, aws.ToString(out.CertificatePem))
			csr, err := x509.ParseCertificateRequest(der)
			require.NoError(t, err)

			switch pub := csr.PublicKey.(type) {
			case *rsa.PublicKey:
				assert.True(t, pub.Equal(cert.PublicKey))
			case *ecdsa.PublicKey:
				assert.True(t, pub.Equal(cert.PublicKey))
			}
		})
	}
}

func TestRegisterCertificate_RejectsDuplicate_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestIoTClient(t, iot.NewHandler(iot.NewInMemoryBackend(), nil))
	ctx := t.Context()

	other := newTestIoTClient(t, iot.NewHandler(iot.NewInMemoryBackend(), nil))

	made, err := other.CreateKeysAndCertificate(ctx, &iotsdk.CreateKeysAndCertificateInput{})
	require.NoError(t, err)

	reg, err := client.RegisterCertificateWithoutCA(ctx, &iotsdk.RegisterCertificateWithoutCAInput{
		CertificatePem: made.CertificatePem,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(made.CertificateId), aws.ToString(reg.CertificateId))

	_, err = client.RegisterCertificateWithoutCA(ctx, &iotsdk.RegisterCertificateWithoutCAInput{
		CertificatePem: made.CertificatePem,
	})

	var exists *types.ResourceAlreadyExistsException
	require.ErrorAs(t, err, &exists)
}

func TestRegisterCACertificate_IDIsDigestOfDER_RealClient(t *testing.T) {
	t.Parallel()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(7), Subject: pkix.Name{CommonName: "device-ca"}, IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	caPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	client := newTestIoTClient(t, iot.NewHandler(iot.NewInMemoryBackend(), nil))

	first, err := client.RegisterCACertificate(t.Context(), &iotsdk.RegisterCACertificateInput{
		CaCertificate: aws.String(caPEM), CertificateMode: types.CertificateModeSniOnly,
	})
	require.NoError(t, err)

	sum := sha256.Sum256(der)
	assert.Equal(t, hex.EncodeToString(sum[:]), aws.ToString(first.CertificateId))

	_, err = client.RegisterCACertificate(t.Context(), &iotsdk.RegisterCACertificateInput{
		CaCertificate: aws.String(caPEM), CertificateMode: types.CertificateModeSniOnly,
	})

	var exists *types.ResourceAlreadyExistsException
	require.ErrorAs(t, err, &exists)
}
