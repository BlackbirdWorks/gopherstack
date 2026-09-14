package acmpca_test

import (
	"crypto/x509"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	acmpcasdk "github.com/aws/aws-sdk-go-v2/service/acmpca"
	acmpcatypes "github.com/aws/aws-sdk-go-v2/service/acmpca/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

// issueCertWithTemplate issues a certificate against templateArn (no
// ApiPassthrough) and returns the parsed result.
func issueCertWithTemplate(
	t *testing.T, client *acmpcasdk.Client, caARN, csrPEM, templateArn string,
) (*x509.Certificate, error) {
	t.Helper()

	issued, err := client.IssueCertificate(t.Context(), &acmpcasdk.IssueCertificateInput{
		CertificateAuthorityArn: aws.String(caARN),
		Csr:                     []byte(csrPEM),
		SigningAlgorithm:        acmpcatypes.SigningAlgorithmSha256withrsa,
		Validity:                &acmpcatypes.Validity{Type: acmpcatypes.ValidityPeriodTypeDays, Value: aws.Int64(30)},
		TemplateArn:             aws.String(templateArn),
	})
	if err != nil {
		return nil, err
	}

	got, err := client.GetCertificate(t.Context(), &acmpcasdk.GetCertificateInput{
		CertificateAuthorityArn: aws.String(caARN),
		CertificateArn:          issued.CertificateArn,
	})
	require.NoError(t, err)

	return parsePEMCert(t, aws.ToString(got.Certificate)), nil
}

// TestIssueCertificate_TemplateProfiles_FixedExtensions covers the
// documented per-template fixed KeyUsage/ExtendedKeyUsage/BasicConstraints
// profiles (template-definitions.md) across representative families: a plain
// end-entity split (Client/ServerAuth), the two Critical-EKU families
// (CodeSigning/OCSPSigning), the two CA families (Root/Subordinate), and a
// Blank family (non-critical BasicConstraints).
func TestIssueCertificate_TemplateProfiles_FixedExtensions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check       func(t *testing.T, cert *x509.Certificate)
		name        string
		templateArn string
	}{
		{
			name:        "default template is EndEntityCertificate/V1",
			templateArn: "",
			check: func(t *testing.T, cert *x509.Certificate) {
				t.Helper()

				assert.Equal(t, x509.KeyUsageDigitalSignature|x509.KeyUsageKeyEncipherment, cert.KeyUsage)
				assert.ElementsMatch(
					t,
					[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
					cert.ExtKeyUsage,
				)
				assert.False(t, cert.IsCA)
			},
		},
		{
			name:        "EndEntityClientAuthCertificate restricts EKU to ClientAuth",
			templateArn: "arn:aws:acm-pca:::template/EndEntityClientAuthCertificate/V1",
			check: func(t *testing.T, cert *x509.Certificate) {
				t.Helper()

				assert.Equal(t, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, cert.ExtKeyUsage)
			},
		},
		{
			name:        "EndEntityServerAuthCertificate restricts EKU to ServerAuth",
			templateArn: "arn:aws:acm-pca:::template/EndEntityServerAuthCertificate/V1",
			check: func(t *testing.T, cert *x509.Certificate) {
				t.Helper()

				assert.Equal(t, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, cert.ExtKeyUsage)
			},
		},
		{
			name:        "CodeSigningCertificate has a Critical EKU",
			templateArn: "arn:aws:acm-pca:::template/CodeSigningCertificate/V1",
			check: func(t *testing.T, cert *x509.Certificate) {
				t.Helper()

				assert.Equal(t, x509.KeyUsageDigitalSignature, cert.KeyUsage)
				assert.Equal(t, []x509.ExtKeyUsage{x509.ExtKeyUsageCodeSigning}, cert.ExtKeyUsage)

				ext := findExtension(cert.Extensions, "2.5.29.37")
				require.NotNil(t, ext)
				assert.True(t, ext.Critical)
			},
		},
		{
			name:        "OCSPSigningCertificate has a Critical EKU",
			templateArn: "arn:aws:acm-pca:::template/OCSPSigningCertificate/V1",
			check: func(t *testing.T, cert *x509.Certificate) {
				t.Helper()

				assert.Equal(t, []x509.ExtKeyUsage{x509.ExtKeyUsageOCSPSigning}, cert.ExtKeyUsage)

				ext := findExtension(cert.Extensions, "2.5.29.37")
				require.NotNil(t, ext)
				assert.True(t, ext.Critical)
			},
		},
		{
			name:        "RootCACertificate is a CA with no EKU and no path length",
			templateArn: "arn:aws:acm-pca:::template/RootCACertificate/V1",
			check: func(t *testing.T, cert *x509.Certificate) {
				t.Helper()

				assert.True(t, cert.IsCA)
				assert.Empty(t, cert.ExtKeyUsage)
				assert.Equal(t, x509.KeyUsageDigitalSignature|x509.KeyUsageCertSign|x509.KeyUsageCRLSign, cert.KeyUsage)
				assert.False(t, cert.MaxPathLenZero)
				assert.Equal(
					t,
					-1,
					cert.MaxPathLen,
					"no pathLenConstraint asserted -- crypto/x509 parses that as -1, not 0",
				)
				assert.Empty(
					t,
					cert.CRLDistributionPoints,
					"RootCACertificate: no CRL info (self-signed cannot be revoked)",
				)
			},
		},
		{
			name:        "SubordinateCACertificate_PathLen1 sets pathlen 1",
			templateArn: "arn:aws:acm-pca:::template/SubordinateCACertificate_PathLen1/V1",
			check: func(t *testing.T, cert *x509.Certificate) {
				t.Helper()

				assert.True(t, cert.IsCA)
				assert.Equal(t, 1, cert.MaxPathLen)
				assert.False(t, cert.MaxPathLenZero)
			},
		},
		{
			name:        "SubordinateCACertificate_PathLen0 sets pathlen 0 (MaxPathLenZero)",
			templateArn: "arn:aws:acm-pca:::template/SubordinateCACertificate_PathLen0/V1",
			check: func(t *testing.T, cert *x509.Certificate) {
				t.Helper()

				assert.True(t, cert.IsCA)
				assert.Equal(t, 0, cert.MaxPathLen)
				assert.True(t, cert.MaxPathLenZero)
			},
		},
		{
			name:        "BlankEndEntityCertificate_APIPassthrough has a non-critical BasicConstraints",
			templateArn: "arn:aws:acm-pca:::template/BlankEndEntityCertificate_APIPassthrough/V1",
			check: func(t *testing.T, cert *x509.Certificate) {
				t.Helper()

				assert.False(t, cert.IsCA)

				ext := findExtension(cert.Extensions, "2.5.29.19")
				require.NotNil(t, ext)
				assert.False(t, ext.Critical)
			},
		},
		{
			name:        "BlankEndEntityCertificate_CriticalBasicConstraints_APIPassthrough is critical",
			templateArn: "arn:aws:acm-pca:::template/BlankEndEntityCertificate_CriticalBasicConstraints_APIPassthrough/V1",
			check: func(t *testing.T, cert *x509.Certificate) {
				t.Helper()

				ext := findExtension(cert.Extensions, "2.5.29.19")
				require.NotNil(t, ext)
				assert.True(t, ext.Critical)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := acmpca.NewInMemoryBackend(testAccountID, testRegion)
			h := acmpca.NewHandler(backend)
			client := newTestACMPCASDKClient(t, h)

			ca, err := client.CreateCertificateAuthority(t.Context(), &acmpcasdk.CreateCertificateAuthorityInput{
				CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeRoot,
				CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
					KeyAlgorithm:     acmpcatypes.KeyAlgorithmEcPrime256v1,
					SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withecdsa,
					Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String("Template Issuer")},
				},
			})
			require.NoError(t, err)

			csrPEM := issueWithCSR(t, backend)

			cert, err := issueCertWithTemplate(
				t,
				client,
				aws.ToString(ca.CertificateAuthorityArn),
				csrPEM,
				tt.templateArn,
			)
			require.NoError(t, err)

			tt.check(t, cert)
		})
	}
}

// TestIssueCertificate_TemplateProfiles_APIPassthroughOverriddenByFixedEKU
// proves the documented precedence rule (template-order-of-operations.md):
// "the template definition has highest priority" -- an EndEntityCertificate_
// APIPassthrough template's fixed EKU wins even when ApiPassthrough supplies
// a conflicting one, while Subject (not template-fixed) still applies.
func TestIssueCertificate_TemplateProfiles_APIPassthroughOverriddenByFixedEKU(t *testing.T) {
	t.Parallel()

	backend := acmpca.NewInMemoryBackend(testAccountID, testRegion)
	h := acmpca.NewHandler(backend)
	client := newTestACMPCASDKClient(t, h)

	ca, err := client.CreateCertificateAuthority(t.Context(), &acmpcasdk.CreateCertificateAuthorityInput{
		CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeRoot,
		CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
			KeyAlgorithm:     acmpcatypes.KeyAlgorithmEcPrime256v1,
			SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withecdsa,
			Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String("Precedence Issuer")},
		},
	})
	require.NoError(t, err)

	csrPEM := issueWithCSR(t, backend)

	issued, err := client.IssueCertificate(t.Context(), &acmpcasdk.IssueCertificateInput{
		CertificateAuthorityArn: ca.CertificateAuthorityArn,
		Csr:                     []byte(csrPEM),
		SigningAlgorithm:        acmpcatypes.SigningAlgorithmSha256withrsa,
		Validity:                &acmpcatypes.Validity{Type: acmpcatypes.ValidityPeriodTypeDays, Value: aws.Int64(30)},
		TemplateArn: aws.String(
			"arn:aws:acm-pca:::template/EndEntityClientAuthCertificate_APIPassthrough/V1",
		),
		ApiPassthrough: &acmpcatypes.ApiPassthrough{
			Subject: &acmpcatypes.ASN1Subject{CommonName: aws.String("overridden.example.com")},
			Extensions: &acmpcatypes.Extensions{
				ExtendedKeyUsage: []acmpcatypes.ExtendedKeyUsage{
					{ExtendedKeyUsageType: acmpcatypes.ExtendedKeyUsageTypeCodeSigning},
				},
			},
		},
	})
	require.NoError(t, err)

	got, err := client.GetCertificate(t.Context(), &acmpcasdk.GetCertificateInput{
		CertificateAuthorityArn: ca.CertificateAuthorityArn,
		CertificateArn:          issued.CertificateArn,
	})
	require.NoError(t, err)

	cert := parsePEMCert(t, aws.ToString(got.Certificate))

	assert.Equal(
		t,
		"overridden.example.com",
		cert.Subject.CommonName,
		"Subject is not template-fixed, so ApiPassthrough applies",
	)
	assert.Equal(
		t, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, cert.ExtKeyUsage,
		"template's fixed EKU must win over the conflicting ApiPassthrough EKU",
	)
}

// TestIssueCertificate_TemplateProfiles_UnrecognizedTemplateArnRejected
// covers TemplateArn parsing failures.
func TestIssueCertificate_TemplateProfiles_UnrecognizedTemplateArnRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		templateArn string
	}{
		{name: "unrecognized family", templateArn: "arn:aws:acm-pca:::template/NotARealTemplate/V1"},
		{name: "missing PathLenN suffix", templateArn: "arn:aws:acm-pca:::template/SubordinateCACertificate/V1"},
		{
			name:        "PathLenN on a template that forbids it",
			templateArn: "arn:aws:acm-pca:::template/EndEntityCertificate_PathLen1/V1",
		},
		{name: "malformed ARN", templateArn: "not-an-arn"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := acmpca.NewInMemoryBackend(testAccountID, testRegion)
			h := acmpca.NewHandler(backend)
			client := newTestACMPCASDKClient(t, h)

			ca, err := client.CreateCertificateAuthority(t.Context(), &acmpcasdk.CreateCertificateAuthorityInput{
				CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeRoot,
				CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
					KeyAlgorithm:     acmpcatypes.KeyAlgorithmEcPrime256v1,
					SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withecdsa,
					Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String("Bad Template Issuer")},
				},
			})
			require.NoError(t, err)

			csrPEM := issueWithCSR(t, backend)

			_, err = issueCertWithTemplate(t, client, aws.ToString(ca.CertificateAuthorityArn), csrPEM, tt.templateArn)
			require.Error(t, err)

			var ia *acmpcatypes.InvalidArgsException
			require.ErrorAs(t, err, &ia)
		})
	}
}
