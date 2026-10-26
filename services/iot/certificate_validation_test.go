package iot_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func mintCSR(t *testing.T) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	der, err := x509.CreateCertificateRequest(rand.Reader,
		&x509.CertificateRequest{Subject: pkix.Name{CommonName: "device"}}, key)
	require.NoError(t, err)

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}))
}

func TestCertificateInputValidation_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call    func(t *testing.T, c *iotsdk.Client) error
		name    string
		wantErr string
	}{
		{
			name: "csr not pem", wantErr: "InvalidRequestException",
			call: func(t *testing.T, c *iotsdk.Client) error {
				_, err := c.CreateCertificateFromCsr(t.Context(), &iotsdk.CreateCertificateFromCsrInput{
					CertificateSigningRequest: aws.String("csr-data"),
				})

				return err
			},
		},
		{
			name: "csr pem garbage", wantErr: "InvalidRequestException",
			call: func(t *testing.T, c *iotsdk.Client) error {
				_, err := c.CreateCertificateFromCsr(t.Context(), &iotsdk.CreateCertificateFromCsrInput{
					CertificateSigningRequest: aws.String(
						"-----BEGIN CERTIFICATE REQUEST-----\nzz\n-----END CERTIFICATE REQUEST-----"),
				})

				return err
			},
		},
		{
			name: "register cert not pem", wantErr: "CertificateValidationException",
			call: func(t *testing.T, c *iotsdk.Client) error {
				_, err := c.RegisterCertificate(t.Context(), &iotsdk.RegisterCertificateInput{
					CertificatePem: aws.String("PEM"),
				})

				return err
			},
		},
		{
			name: "register without ca cert garbage", wantErr: "CertificateValidationException",
			call: func(t *testing.T, c *iotsdk.Client) error {
				_, err := c.RegisterCertificateWithoutCA(t.Context(), &iotsdk.RegisterCertificateWithoutCAInput{
					CertificatePem: aws.String("-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----"),
				})

				return err
			},
		},
		{
			name: "valid csr",
			call: func(t *testing.T, c *iotsdk.Client) error {
				_, err := c.CreateCertificateFromCsr(t.Context(), &iotsdk.CreateCertificateFromCsrInput{
					CertificateSigningRequest: aws.String(mintCSR(t)),
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestIoTClient(t, iot.NewHandler(iot.NewInMemoryBackend(), nil))
			err := tt.call(t, client)

			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestRegisterCertificate_IDIsDERDigest_RealClient(t *testing.T) {
	t.Parallel()

	certPEM, cert, _ := mintCert(t, "device", nil, nil)
	sum := sha256.Sum256(cert.Raw)

	client := newTestIoTClient(t, iot.NewHandler(iot.NewInMemoryBackend(), nil))
	out, err := client.RegisterCertificateWithoutCA(t.Context(), &iotsdk.RegisterCertificateWithoutCAInput{
		CertificatePem: aws.String(certPEM),
	})
	require.NoError(t, err)
	assert.Equal(t, hex.EncodeToString(sum[:]), aws.ToString(out.CertificateId))
}

func mintCertPEM(t *testing.T) string {
	t.Helper()

	certPEM, _, _ := mintCert(t, "device", nil, nil)

	return certPEM
}
