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

// TestInMemoryBackend_RevocationConfiguration_FieldValidation covers the
// CustomCname/OcspCustomCname (RFC2396 + no protocol prefix), S3BucketName
// (S3 bucket naming rules), ExpirationInDays (1-5000), and OmitExtension-vs-
// CustomCname constraints documented on CrlConfiguration/OcspConfiguration.
func TestInMemoryBackend_RevocationConfiguration_FieldValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rc      *acmpca.RevocationConfiguration
		name    string
		wantErr bool
	}{
		{
			name: "CustomCname with http scheme is rejected",
			rc: &acmpca.RevocationConfiguration{CrlConfiguration: &acmpca.CrlConfiguration{
				Enabled: true, S3BucketName: "my-bucket", CustomCname: "http://crl.example.com",
			}},
			wantErr: true,
		},
		{
			name: "CustomCname with https scheme is rejected",
			rc: &acmpca.RevocationConfiguration{CrlConfiguration: &acmpca.CrlConfiguration{
				Enabled: true, S3BucketName: "my-bucket", CustomCname: "https://crl.example.com",
			}},
			wantErr: true,
		},
		{
			name: "CustomCname with disallowed characters is rejected",
			rc: &acmpca.RevocationConfiguration{CrlConfiguration: &acmpca.CrlConfiguration{
				Enabled: true, S3BucketName: "my-bucket", CustomCname: "crl.example.com/<script>",
			}},
			wantErr: true,
		},
		{
			name: "valid CustomCname is accepted",
			rc: &acmpca.RevocationConfiguration{CrlConfiguration: &acmpca.CrlConfiguration{
				Enabled: true, S3BucketName: "my-bucket", CustomCname: "crl.example.com",
			}},
			wantErr: false,
		},
		{
			name: "OcspCustomCname with http scheme is rejected",
			rc: &acmpca.RevocationConfiguration{
				OcspConfiguration: &acmpca.OcspConfiguration{Enabled: true, OcspCustomCname: "http://ocsp.example.com"},
			},
			wantErr: true,
		},
		{
			name: "S3BucketName too short is rejected",
			rc: &acmpca.RevocationConfiguration{
				CrlConfiguration: &acmpca.CrlConfiguration{Enabled: true, S3BucketName: "ab"},
			},
			wantErr: true,
		},
		{
			name: "S3BucketName with uppercase is rejected (real S3 naming rules)",
			rc: &acmpca.RevocationConfiguration{
				CrlConfiguration: &acmpca.CrlConfiguration{Enabled: true, S3BucketName: "My-Bucket"},
			},
			wantErr: true,
		},
		{
			name: "S3BucketName with leading hyphen is rejected",
			rc: &acmpca.RevocationConfiguration{
				CrlConfiguration: &acmpca.CrlConfiguration{Enabled: true, S3BucketName: "-my-bucket"},
			},
			wantErr: true,
		},
		{
			name: "S3BucketName with consecutive periods is rejected",
			rc: &acmpca.RevocationConfiguration{
				CrlConfiguration: &acmpca.CrlConfiguration{Enabled: true, S3BucketName: "my..bucket"},
			},
			wantErr: true,
		},
		{
			name: "ExpirationInDays below 1 is rejected",
			rc: &acmpca.RevocationConfiguration{CrlConfiguration: &acmpca.CrlConfiguration{
				Enabled: true, S3BucketName: "my-bucket", ExpirationInDays: -1,
			}},
			wantErr: true,
		},
		{
			name: "ExpirationInDays above 5000 is rejected",
			rc: &acmpca.RevocationConfiguration{CrlConfiguration: &acmpca.CrlConfiguration{
				Enabled: true, S3BucketName: "my-bucket", ExpirationInDays: 5001,
			}},
			wantErr: true,
		},
		{
			name: "ExpirationInDays at the boundaries is accepted",
			rc: &acmpca.RevocationConfiguration{CrlConfiguration: &acmpca.CrlConfiguration{
				Enabled: true, S3BucketName: "my-bucket", ExpirationInDays: 5000,
			}},
			wantErr: false,
		},
		{
			name: "OmitExtension with CustomCname set is rejected",
			rc: &acmpca.RevocationConfiguration{CrlConfiguration: &acmpca.CrlConfiguration{
				Enabled: true, S3BucketName: "my-bucket", CustomCname: "crl.example.com", OmitExtension: true,
			}},
			wantErr: true,
		},
		{
			name: "OmitExtension without CustomCname is accepted",
			rc: &acmpca.RevocationConfiguration{CrlConfiguration: &acmpca.CrlConfiguration{
				Enabled: true, S3BucketName: "my-bucket", OmitExtension: true,
			}},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			_, err := b.CreateCertificateAuthority(
				context.Background(), "ROOT", rootCACfg("Field Validation CA"),
				acmpca.WithCreateCARevocationConfiguration(tt.rc),
			)

			if tt.wantErr {
				require.ErrorIs(t, err, acmpca.ErrInvalidArgs)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestInMemoryBackend_IssueCertificate_CRLDistributionPoint proves the
// issued certificate's cRLDistributionPoints extension (2.5.29.31) reflects
// the issuing CA's RevocationConfiguration: the custom CNAME when set,
// omitted entirely when OmitExtension is set or CRLs are disabled, and a
// RootCACertificate-templated cert never gets one at all (self-signed certs
// cannot be revoked).
func TestInMemoryBackend_IssueCertificate_CRLDistributionPoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rc          *acmpca.RevocationConfiguration
		check       func(t *testing.T, cdp []string)
		name        string
		templateArn string
	}{
		{
			name: "no RevocationConfiguration means no CDP",
			rc:   nil,
			check: func(t *testing.T, cdp []string) {
				t.Helper()
				assert.Empty(t, cdp)
			},
		},
		{
			name: "CRL enabled with S3BucketName only",
			rc: &acmpca.RevocationConfiguration{
				CrlConfiguration: &acmpca.CrlConfiguration{Enabled: true, S3BucketName: "my-crl-bucket"},
			},
			check: func(t *testing.T, cdp []string) {
				t.Helper()
				require.Len(t, cdp, 1)
				assert.Contains(t, cdp[0], "my-crl-bucket")
			},
		},
		{
			name: "CRL enabled with CustomCname uses the CNAME, not the bucket",
			rc: &acmpca.RevocationConfiguration{CrlConfiguration: &acmpca.CrlConfiguration{
				Enabled: true, S3BucketName: "my-crl-bucket", CustomCname: "crl.example.com",
			}},
			check: func(t *testing.T, cdp []string) {
				t.Helper()
				require.Len(t, cdp, 1)
				assert.Contains(t, cdp[0], "crl.example.com")
				assert.NotContains(t, cdp[0], "my-crl-bucket")
			},
		},
		{
			name: "OmitExtension suppresses the CDP even with CRLs enabled",
			rc: &acmpca.RevocationConfiguration{CrlConfiguration: &acmpca.CrlConfiguration{
				Enabled: true, S3BucketName: "my-crl-bucket", OmitExtension: true,
			}},
			check: func(t *testing.T, cdp []string) {
				t.Helper()
				assert.Empty(t, cdp)
			},
		},
		{
			name: "RootCACertificate template never gets a CDP even with CRLs enabled",
			rc: &acmpca.RevocationConfiguration{
				CrlConfiguration: &acmpca.CrlConfiguration{Enabled: true, S3BucketName: "my-crl-bucket"},
			},
			templateArn: "arn:aws:acm-pca:::template/RootCACertificate/V1",
			check: func(t *testing.T, cdp []string) {
				t.Helper()
				assert.Empty(t, cdp)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			var opts []acmpca.CreateCAOption
			if tt.rc != nil {
				opts = append(opts, acmpca.WithCreateCARevocationConfiguration(tt.rc))
			}

			ca, err := b.CreateCertificateAuthority(context.Background(), "ROOT", rootCACfg("CDP CA"), opts...)
			require.NoError(t, err)

			subCA, err := b.CreateCertificateAuthority(context.Background(), "SUBORDINATE", rootCACfg("Leaf"))
			require.NoError(t, err)

			csr, err := b.GetCertificateAuthorityCsr(context.Background(), subCA.ARN)
			require.NoError(t, err)

			var issueOpts []acmpca.IssueCertOption
			if tt.templateArn != "" {
				issueOpts = append(issueOpts, acmpca.WithIssueCertTemplateArn(tt.templateArn))
			}

			cert, err := b.IssueCertificate(context.Background(), ca.ARN, csr, 30, issueOpts...)
			require.NoError(t, err)

			parsed := parsePEMCert(t, cert.CertBody)
			tt.check(t, parsed.CRLDistributionPoints)
		})
	}
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
