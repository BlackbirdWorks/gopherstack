package acmpca_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

// TestInMemoryBackend_CreateCertificateAuthority_Idempotency verifies that
// repeated CreateCertificateAuthority calls bearing the same IdempotencyToken
// return the same CA ARN instead of creating a duplicate, matching real AWS's
// documented 5-minute idempotency window; a different (or absent) token
// creates a distinct CA.
func TestInMemoryBackend_CreateCertificateAuthority_Idempotency(t *testing.T) {
	t.Parallel()

	b := acmpca.NewInMemoryBackend(testAccountID, testRegion)
	cfg := acmpca.CertificateAuthorityConfiguration{
		Subject: acmpca.CertificateAuthoritySubject{CommonName: "Idempotent CA"},
	}

	first, err := b.CreateCertificateAuthority(
		context.Background(), "ROOT", cfg, acmpca.WithCreateCAIdempotencyToken("token-1"),
	)
	require.NoError(t, err)

	second, err := b.CreateCertificateAuthority(
		context.Background(), "ROOT", cfg, acmpca.WithCreateCAIdempotencyToken("token-1"),
	)
	require.NoError(t, err)
	assert.Equal(t, first.ARN, second.ARN, "same idempotency token must return the same CA")

	third, err := b.CreateCertificateAuthority(
		context.Background(), "ROOT", cfg, acmpca.WithCreateCAIdempotencyToken("token-2"),
	)
	require.NoError(t, err)
	assert.NotEqual(t, first.ARN, third.ARN, "different idempotency token must create a distinct CA")

	fourth, err := b.CreateCertificateAuthority(context.Background(), "ROOT", cfg)
	require.NoError(t, err)
	assert.NotEqual(t, first.ARN, fourth.ARN, "no idempotency token must always create a distinct CA")
}

// TestInMemoryBackend_IssueCertificate_Idempotency mirrors
// TestInMemoryBackend_CreateCertificateAuthority_Idempotency for IssueCertificate.
func TestInMemoryBackend_IssueCertificate_Idempotency(t *testing.T) {
	t.Parallel()

	b := acmpca.NewInMemoryBackend(testAccountID, testRegion)

	ca, err := b.CreateCertificateAuthority(context.Background(), "ROOT", acmpca.CertificateAuthorityConfiguration{
		Subject: acmpca.CertificateAuthoritySubject{CommonName: "Issuer CA"},
	})
	require.NoError(t, err)

	subCA, err := b.CreateCertificateAuthority(
		context.Background(), "SUBORDINATE", acmpca.CertificateAuthorityConfiguration{
			Subject: acmpca.CertificateAuthoritySubject{CommonName: "Leaf"},
		},
	)
	require.NoError(t, err)

	csr, err := b.GetCertificateAuthorityCsr(context.Background(), subCA.ARN)
	require.NoError(t, err)

	first, err := b.IssueCertificate(
		context.Background(), ca.ARN, csr, 365, acmpca.WithIssueCertIdempotencyToken("cert-token"),
	)
	require.NoError(t, err)

	second, err := b.IssueCertificate(
		context.Background(), ca.ARN, csr, 365, acmpca.WithIssueCertIdempotencyToken("cert-token"),
	)
	require.NoError(t, err)
	assert.Equal(t, first.ARN, second.ARN, "same idempotency token must return the same certificate")

	third, err := b.IssueCertificate(context.Background(), ca.ARN, csr, 365)
	require.NoError(t, err)
	assert.NotEqual(t, first.ARN, third.ARN, "no idempotency token must always issue a distinct certificate")
}
