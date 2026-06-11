package acmpca //nolint:testpackage // needs access to the unexported region context key.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func pcaCtxRegion(region string) context.Context {
	return context.WithValue(context.Background(), regionContextKey{}, region)
}

func TestACMPCARegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := pcaCtxRegion("us-east-1")
	ctxWest := pcaCtxRegion("us-west-2")

	cfg := CertificateAuthorityConfiguration{
		Subject: CertificateAuthoritySubject{CommonName: "example.com"},
	}

	// 1. Create a ROOT CA in us-east-1 (auto-activated).
	eastCA, err := backend.CreateCertificateAuthority(ctxEast, "ROOT", cfg)
	require.NoError(t, err)
	assert.Contains(t, eastCA.ARN, "us-east-1")

	// 2. Create a ROOT CA in us-west-2.
	westCA, err := backend.CreateCertificateAuthority(ctxWest, "ROOT", cfg)
	require.NoError(t, err)
	assert.Contains(t, westCA.ARN, "us-west-2")

	// 3. us-east-1 sees only its own CA.
	eastList := backend.ListCertificateAuthorities(ctxEast, "", 0)
	require.Len(t, eastList.Data, 1)
	assert.Equal(t, eastCA.ARN, eastList.Data[0].ARN)

	// 4. us-west-2 sees only its own CA.
	westList := backend.ListCertificateAuthorities(ctxWest, "", 0)
	require.Len(t, westList.Data, 1)
	assert.Equal(t, westCA.ARN, westList.Data[0].ARN)

	// 5. A CA created in us-east-1 is not visible from us-west-2 (cross-region lookup fails).
	_, err = backend.DescribeCertificateAuthority(ctxWest, eastCA.ARN)
	require.Error(t, err)

	got, err := backend.DescribeCertificateAuthority(ctxEast, eastCA.ARN)
	require.NoError(t, err)
	assert.Equal(t, eastCA.ARN, got.ARN)
}

func TestACMPCACertificateRegionIsolation(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := pcaCtxRegion("us-east-1")
	ctxWest := pcaCtxRegion("us-west-2")

	cfg := CertificateAuthorityConfiguration{
		Subject: CertificateAuthoritySubject{CommonName: "example.com"},
	}

	eastCA, err := backend.CreateCertificateAuthority(ctxEast, "ROOT", cfg)
	require.NoError(t, err)

	csr, err := backend.GetCertificateAuthorityCsr(ctxEast, eastCA.ARN)
	require.NoError(t, err)

	cert, err := backend.IssueCertificate(ctxEast, eastCA.ARN, csr, 90)
	require.NoError(t, err)

	// Certificate is retrievable in us-east-1 but not in us-west-2.
	got, err := backend.GetCertificate(ctxEast, eastCA.ARN, cert.ARN)
	require.NoError(t, err)
	assert.Equal(t, cert.ARN, got.ARN)

	_, err = backend.GetCertificate(ctxWest, eastCA.ARN, cert.ARN)
	require.Error(t, err)
}
