package acmpca_test

import (
	"context"
	"math/big"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

// uuidShape matches the dashed-UUID resource ID format real ACM PCA ARNs use,
// e.g. "12345678-1234-1234-1234-123456789012" (see aws-sdk-go-v2's
// CreateCertificateAuthorityOutput doc comment).
var uuidShape = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// TestInMemoryBackend_CertificateAuthorityARN_UUIDShape verifies that a CA's
// ARN resource ID is a dashed UUID, matching real AWS's format -- gopherstack
// previously used a flat 32-char hex string with no dashes (PARITY.md gap).
func TestInMemoryBackend_CertificateAuthorityARN_UUIDShape(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	ca, err := b.CreateCertificateAuthority(context.Background(), "ROOT", acmpca.CertificateAuthorityConfiguration{
		Subject: acmpca.CertificateAuthoritySubject{CommonName: "UUID CA"},
	})
	require.NoError(t, err)

	id := ca.ARN[strings.LastIndex(ca.ARN, "/")+1:]
	assert.True(t, uuidShape.MatchString(id), "CA resource ID %q is not a dashed UUID", id)
}

// TestInMemoryBackend_IssuedCertificateARN_EmbedsDecimalSerial verifies that an
// issued certificate's ARN embeds its own serial number in decimal as the final
// path segment, matching aws-sdk-go-v2's IssueCertificateOutput doc comment
// example ("…/certificate/286535153982981100925020015808220737245"). gopherstack
// previously appended an unrelated random ID here instead (wire-shape bug found
// while diffing this pass -- see PARITY.md).
func TestInMemoryBackend_IssuedCertificateARN_EmbedsDecimalSerial(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
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

	cert, err := b.IssueCertificate(context.Background(), ca.ARN, csr, 365)
	require.NoError(t, err)

	wantSerialDecimal, ok := new(big.Int).SetString(cert.Serial, 16)
	require.True(t, ok, "cert.Serial %q must be valid hex", cert.Serial)

	certID := cert.ARN[strings.LastIndex(cert.ARN, "/")+1:]
	assert.Equal(t, wantSerialDecimal.String(), certID)
}
