package acmpca //nolint:testpackage // needs access to the unexported caGet accessor to backdate RestorableUntil.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRestorableUntil_PastWindowIsPermanentlyGone verifies that once a DELETED
// CA's RestorableUntil deadline has passed, it is invisible to Describe/List
// (ResourceNotFoundException / omitted from the list) and RestoreCertificateAuthority
// rejects the restore attempt -- matching real AWS, which permanently and
// irrevocably deletes a CA once its restoration window ends. Previously
// gopherstack tracked RestorableUntil but never enforced the deadline
// (PARITY.md gap).
func TestRestorableUntil_PastWindowIsPermanentlyGone(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1")

	ca, err := b.CreateCertificateAuthority(context.Background(), "ROOT", CertificateAuthorityConfiguration{
		Subject: CertificateAuthoritySubject{CommonName: "Expiring CA"},
	})
	require.NoError(t, err)

	require.NoError(t, b.UpdateCertificateAuthority(context.Background(), ca.ARN, caStatusDisabled))
	require.NoError(t, b.DeleteCertificateAuthority(context.Background(), ca.ARN, permanentDeletionMinDays))

	// There is no public API to backdate RestorableUntil, so reach into the
	// live backend row directly (same pattern as isolation_test.go).
	func() {
		b.mu.Lock("test backdate RestorableUntil")
		defer b.mu.Unlock()

		live, ok := b.cas.Get(regionKey(b.region, ca.ARN))
		require.True(t, ok)
		live.RestorableUntil = time.Now().UTC().Add(-time.Hour)
	}()

	_, err = b.DescribeCertificateAuthority(context.Background(), ca.ARN)
	require.ErrorIs(t, err, ErrCANotFound)

	p, err := b.ListCertificateAuthorities(context.Background(), "", 0, "")
	require.NoError(t, err)
	assert.Empty(t, p.Data)

	err = b.RestoreCertificateAuthority(context.Background(), ca.ARN)
	require.ErrorIs(t, err, ErrCANotFound)
}

// TestRestorableUntil_WithinWindowCanBeRestored is the control case: a CA
// still inside its restoration window restores successfully and clears
// RestorableUntil.
func TestRestorableUntil_WithinWindowCanBeRestored(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("000000000000", "us-east-1")

	ca, err := b.CreateCertificateAuthority(context.Background(), "ROOT", CertificateAuthorityConfiguration{
		Subject: CertificateAuthoritySubject{CommonName: "Restorable CA"},
	})
	require.NoError(t, err)

	require.NoError(t, b.UpdateCertificateAuthority(context.Background(), ca.ARN, caStatusDisabled))
	require.NoError(t, b.DeleteCertificateAuthority(context.Background(), ca.ARN, permanentDeletionMinDays))

	require.NoError(t, b.RestoreCertificateAuthority(context.Background(), ca.ARN))

	got, err := b.DescribeCertificateAuthority(context.Background(), ca.ARN)
	require.NoError(t, err)
	assert.Equal(t, caStatusDisabled, got.Status)
	assert.True(t, got.RestorableUntil.IsZero())
}
