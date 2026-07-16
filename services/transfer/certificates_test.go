package transfer_test

import (
	"testing"

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
