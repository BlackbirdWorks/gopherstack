package transfer_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// TestCertificateCountExport verifies CertificateCount export.
func TestCertificateCountExport(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	assert.Equal(t, 0, transfer.CertificateCount(b))

	b.AddCertificateInternal("cert-test")

	assert.Equal(t, 1, transfer.CertificateCount(b))
}

func TestDeleteCertificate(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	certID := "cert-abc123"
	b.AddCertificateInternal(certID)

	require.NoError(t, b.DeleteCertificate(certID))

	// Double delete should fail
	err := b.DeleteCertificate(certID)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestDeleteCertificate_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	err := b.DeleteCertificate("cert-doesnotexist")
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

// TestImportCertificateFull_ActiveInactiveDates verifies that ActiveDate/InactiveDate
// are stored (real AWS ImportCertificateInput fields, previously accepted nowhere in
// gopherstack), that CertificateChain/PrivateKey round-trip, and that Status is
// computed the way AWS docs describe: ActiveDate/InactiveDate take precedence over
// the NotBefore/NotAfter (X.509) validity window when set.
func TestImportCertificateFull_ActiveInactiveDates(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	now := time.Now()

	// ActiveDate in the future -> INACTIVE despite NotBefore/NotAfter being valid now.
	c, err := b.ImportCertificateFull(&transfer.ImportCertificateInput{
		Usage:            "SIGNING",
		CertificateChain: "chain-pem",
		PrivateKey:       "private-key-pem",
		NotBefore:        now.Add(-time.Hour),
		NotAfter:         now.Add(time.Hour),
		ActiveDate:       now.Add(24 * time.Hour),
	})
	require.NoError(t, err)
	assert.Equal(t, "INACTIVE", c.Status)
	assert.Equal(t, "chain-pem", c.CertificateChain)
	assert.True(t, c.HasPrivateKey)

	// No ActiveDate/InactiveDate override -> falls back to NotBefore/NotAfter, which
	// are currently valid.
	c2, err := b.ImportCertificateFull(&transfer.ImportCertificateInput{
		Usage:     "SIGNING",
		NotBefore: now.Add(-time.Hour),
		NotAfter:  now.Add(time.Hour),
	})
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", c2.Status)
	assert.False(t, c2.HasPrivateKey)
}

// TestUpdateCertificateFull_RecomputesStatus verifies UpdateCertificate's real-AWS
// ActiveDate/InactiveDate fields are settable post-creation and affect Status.
func TestUpdateCertificateFull_RecomputesStatus(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	now := time.Now()

	c, err := b.ImportCertificateFull(&transfer.ImportCertificateInput{
		Usage:     "SIGNING",
		NotBefore: now.Add(-time.Hour),
		NotAfter:  now.Add(time.Hour),
	})
	require.NoError(t, err)
	require.Equal(t, "ACTIVE", c.Status)

	updated, err := b.UpdateCertificateFull(&transfer.UpdateCertificateInput{
		CertificateID: c.CertificateID,
		InactiveDate:  now.Add(-time.Minute),
	})
	require.NoError(t, err)
	assert.Equal(t, "INACTIVE", updated.Status, "InactiveDate in the past must flip Status to INACTIVE")
}
