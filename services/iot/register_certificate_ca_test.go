package iot_test

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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

func mintCert(
	t *testing.T,
	cn string,
	parent *x509.Certificate,
	parentKey *ecdsa.PrivateKey,
) (string, *x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	if parent == nil {
		parent, parentKey = tmpl, key
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, parentKey)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), cert, key
}

func TestRegisterCertificate_CASignature(t *testing.T) {
	t.Parallel()

	caPEM, caCert, caKey := mintCert(t, "ca", nil, nil)
	otherPEM, _, _ := mintCert(t, "other-ca", nil, nil)
	leafPEM, _, _ := mintCert(t, "device", caCert, caKey)

	tests := []struct {
		name    string
		ca      string
		wantErr bool
	}{
		{"signed_by_ca", caPEM, false},
		{"signed_by_other", otherPEM, true},
		{"no_ca", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			_, err := b.RegisterCertificate(
				&iot.RegisterCertificateInput{CertificatePem: leafPEM, CACertificatePem: tt.ca},
			)

			if tt.wantErr {
				require.ErrorIs(t, err, iot.ErrCertificateValidation)

				return
			}

			assert.NoError(t, err)
		})
	}
}
