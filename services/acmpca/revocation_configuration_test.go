package acmpca_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

func rootCACfg(name string) acmpca.CertificateAuthorityConfiguration {
	return acmpca.CertificateAuthorityConfiguration{Subject: acmpca.CertificateAuthoritySubject{CommonName: name}}
}

// TestInMemoryBackend_RevocationConfiguration covers CreateCertificateAuthority/
// UpdateCertificateAuthority accepting a CRL/OCSP RevocationConfiguration and
// DescribeCertificateAuthority reporting it back -- previously entirely
// unmodeled (PARITY.md gap: "RevocationConfiguration (CRL/OCSP) is not modeled
// at all").
func TestInMemoryBackend_RevocationConfiguration(t *testing.T) {
	t.Parallel()

	t.Run("create with CRL and OCSP enabled round-trips through Describe", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()
		rc := &acmpca.RevocationConfiguration{
			CrlConfiguration: &acmpca.CrlConfiguration{
				Enabled:      true,
				S3BucketName: "my-crl-bucket",
				CrlType:      "COMPLETE",
			},
			OcspConfiguration: &acmpca.OcspConfiguration{Enabled: true, OcspCustomCname: "ocsp.example.com"},
		}

		ca, err := b.CreateCertificateAuthority(
			context.Background(), "ROOT", rootCACfg("CRL CA"), acmpca.WithCreateCARevocationConfiguration(rc),
		)
		require.NoError(t, err)
		require.NotNil(t, ca.RevocationConfiguration)
		assert.True(t, ca.RevocationConfiguration.CrlConfiguration.Enabled)
		assert.Equal(t, "my-crl-bucket", ca.RevocationConfiguration.CrlConfiguration.S3BucketName)
		assert.True(t, ca.RevocationConfiguration.OcspConfiguration.Enabled)

		got, err := b.DescribeCertificateAuthority(context.Background(), ca.ARN)
		require.NoError(t, err)
		require.NotNil(t, got.RevocationConfiguration)
		assert.Equal(t, "my-crl-bucket", got.RevocationConfiguration.CrlConfiguration.S3BucketName)
	})

	t.Run("no RevocationConfiguration means unconfigured", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()
		ca, err := b.CreateCertificateAuthority(context.Background(), "ROOT", rootCACfg("Plain CA"))
		require.NoError(t, err)
		assert.Nil(t, ca.RevocationConfiguration)
	})

	t.Run("enabled CRL without S3BucketName is rejected", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()
		rc := &acmpca.RevocationConfiguration{CrlConfiguration: &acmpca.CrlConfiguration{Enabled: true}}

		_, err := b.CreateCertificateAuthority(
			context.Background(), "ROOT", rootCACfg("Bad CA"), acmpca.WithCreateCARevocationConfiguration(rc),
		)
		require.ErrorIs(t, err, acmpca.ErrInvalidArgs)
	})

	t.Run("disabled CRL with extra fields is rejected", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()
		rc := &acmpca.RevocationConfiguration{
			CrlConfiguration: &acmpca.CrlConfiguration{Enabled: false, S3BucketName: "should-not-be-set"},
		}

		_, err := b.CreateCertificateAuthority(
			context.Background(), "ROOT", rootCACfg("Bad CA"), acmpca.WithCreateCARevocationConfiguration(rc),
		)
		require.ErrorIs(t, err, acmpca.ErrInvalidArgs)
	})

	t.Run("unsupported CrlType is rejected", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()
		rc := &acmpca.RevocationConfiguration{
			CrlConfiguration: &acmpca.CrlConfiguration{Enabled: true, S3BucketName: "b", CrlType: "BOGUS"},
		}

		_, err := b.CreateCertificateAuthority(
			context.Background(), "ROOT", rootCACfg("Bad CA"), acmpca.WithCreateCARevocationConfiguration(rc),
		)
		require.ErrorIs(t, err, acmpca.ErrInvalidArgs)
	})

	t.Run("UpdateCertificateAuthority sets RevocationConfiguration", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()
		ca, err := b.CreateCertificateAuthority(context.Background(), "ROOT", rootCACfg("Updatable CA"))
		require.NoError(t, err)
		require.Nil(t, ca.RevocationConfiguration)

		rc := &acmpca.RevocationConfiguration{
			OcspConfiguration: &acmpca.OcspConfiguration{Enabled: true},
		}
		err = b.UpdateCertificateAuthority(
			context.Background(), ca.ARN, "", acmpca.WithUpdateCARevocationConfiguration(rc),
		)
		require.NoError(t, err)

		got, err := b.DescribeCertificateAuthority(context.Background(), ca.ARN)
		require.NoError(t, err)
		require.NotNil(t, got.RevocationConfiguration)
		assert.True(t, got.RevocationConfiguration.OcspConfiguration.Enabled)
	})

	t.Run("UpdateCertificateAuthority without the option leaves RevocationConfiguration unchanged", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend()
		rc := &acmpca.RevocationConfiguration{OcspConfiguration: &acmpca.OcspConfiguration{Enabled: true}}
		ca, err := b.CreateCertificateAuthority(
			context.Background(), "ROOT", rootCACfg("Untouched CA"), acmpca.WithCreateCARevocationConfiguration(rc),
		)
		require.NoError(t, err)

		require.NoError(t, b.UpdateCertificateAuthority(context.Background(), ca.ARN, "DISABLED"))

		got, err := b.DescribeCertificateAuthority(context.Background(), ca.ARN)
		require.NoError(t, err)
		require.NotNil(t, got.RevocationConfiguration)
		assert.True(t, got.RevocationConfiguration.OcspConfiguration.Enabled)
	})
}

// TestInMemoryBackend_UsageMode_ShortLivedCertificateValidityCap verifies that
// a SHORT_LIVED_CERTIFICATE-usage-mode CA enforces the real API's documented
// 7-day certificate validity cap.
func TestInMemoryBackend_UsageMode_ShortLivedCertificateValidityCap(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	ca, err := b.CreateCertificateAuthority(
		context.Background(), "ROOT", rootCACfg("Short-lived CA"),
		acmpca.WithCreateCAUsageMode("SHORT_LIVED_CERTIFICATE"),
	)
	require.NoError(t, err)
	assert.Equal(t, "SHORT_LIVED_CERTIFICATE", ca.UsageMode)

	subCA, err := b.CreateCertificateAuthority(context.Background(), "SUBORDINATE", rootCACfg("Leaf"))
	require.NoError(t, err)
	csr, err := b.GetCertificateAuthorityCsr(context.Background(), subCA.ARN)
	require.NoError(t, err)

	_, err = b.IssueCertificate(context.Background(), ca.ARN, csr, 30)
	require.ErrorIs(t, err, acmpca.ErrInvalidArgs)

	cert, err := b.IssueCertificate(context.Background(), ca.ARN, csr, 7)
	require.NoError(t, err)
	assert.NotEmpty(t, cert.ARN)
}

// TestInMemoryBackend_KeyStorageSecurityStandard_Default verifies the
// documented FIPS_140_2_LEVEL_3_OR_HIGHER default and enum validation.
func TestInMemoryBackend_KeyStorageSecurityStandard_Default(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	ca, err := b.CreateCertificateAuthority(context.Background(), "ROOT", rootCACfg("Default standard CA"))
	require.NoError(t, err)
	assert.Equal(t, "FIPS_140_2_LEVEL_3_OR_HIGHER", ca.KeyStorageSecurityStandard)

	_, err = b.CreateCertificateAuthority(
		context.Background(), "ROOT", rootCACfg("Bad standard CA"),
		acmpca.WithCreateCAKeyStorageSecurityStandard("NOT_A_REAL_STANDARD"),
	)
	require.ErrorIs(t, err, acmpca.ErrInvalidArgs)
}
