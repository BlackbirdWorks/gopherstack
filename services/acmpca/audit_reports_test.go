package acmpca_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

func TestInMemoryBackend_AuditReport(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	ca, err := b.CreateCertificateAuthority(
		context.Background(),
		"ROOT",
		acmpca.CertificateAuthorityConfiguration{
			Subject: acmpca.CertificateAuthoritySubject{CommonName: "Ops CA"},
		},
	)
	require.NoError(t, err)

	report, err := b.CreateCertificateAuthorityAuditReport(context.Background(), ca.ARN, "bucket", "JSON")
	require.NoError(t, err)
	assert.Equal(t, "SUCCESS", report.Status)
	assert.Contains(t, report.S3Key, ".json")

	got, err := b.DescribeCertificateAuthorityAuditReport(context.Background(), ca.ARN, report.AuditReportID)
	require.NoError(t, err)
	assert.Equal(t, report.AuditReportID, got.AuditReportID)

	require.NoError(t, b.UpdateCertificateAuthority(context.Background(), ca.ARN, "DISABLED"))
	require.NoError(t, b.DeleteCertificateAuthority(context.Background(), ca.ARN, 0))
	require.NoError(t, b.RestoreCertificateAuthority(context.Background(), ca.ARN))

	restored, err := b.DescribeCertificateAuthority(context.Background(), ca.ARN)
	require.NoError(t, err)
	assert.Equal(t, "DISABLED", restored.Status)
}

// TestInMemoryBackend_AuditReportValidation covers audit report validation edge cases.
func TestInMemoryBackend_AuditReportValidation(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	ca, err := b.CreateCertificateAuthority(
		context.Background(),
		"ROOT",
		acmpca.CertificateAuthorityConfiguration{
			Subject: acmpca.CertificateAuthoritySubject{CommonName: "Validate CA"},
		},
	)
	require.NoError(t, err)

	_, err = b.DescribeCertificateAuthorityAuditReport(context.Background(), ca.ARN, "")
	require.ErrorIs(t, err, acmpca.ErrInvalidArgs)
}
