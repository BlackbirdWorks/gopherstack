package acmpca_test

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	acmpcasdk "github.com/aws/aws-sdk-go-v2/service/acmpca"
	acmpcatypes "github.com/aws/aws-sdk-go-v2/service/acmpca/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

// policyInformationDER mirrors RFC 5280 §4.2.1.4's PolicyInformation, used to
// asn1-decode the raw certificatePolicies extension this package hand-builds
// (see applyCertificatePolicies in certificate_extensions.go) -- a local,
// from-scratch decoder rather than reusing the package's own encoder, so the
// test cannot pass merely because both sides share a bug.
type policyInformationDER struct {
	PolicyIdentifier asn1.ObjectIdentifier
	PolicyQualifiers []policyQualifierInfoDER `asn1:"optional"`
}

//nolint:govet // ASN.1 SEQUENCE field order is wire-significant; fieldalignment must not reorder it
type policyQualifierInfoDER struct {
	PolicyQualifierID asn1.ObjectIdentifier
	Qualifier         string `asn1:"ia5"`
}

const oidCertificatePoliciesDotted = "2.5.29.32"

// issueRealCertWithPassthrough issues a certificate through the real
// aws-sdk-go-v2 client using a BlankEndEntityCertificate_APIPassthrough
// template (so ap is fully honored) and returns the parsed certificate.
func issueRealCertWithPassthrough(
	t *testing.T, client *acmpcasdk.Client, caARN string, csrPEM string, ap *acmpcatypes.ApiPassthrough,
) *x509.Certificate {
	t.Helper()

	issued, err := client.IssueCertificate(t.Context(), &acmpcasdk.IssueCertificateInput{
		CertificateAuthorityArn: aws.String(caARN),
		Csr:                     []byte(csrPEM),
		SigningAlgorithm:        acmpcatypes.SigningAlgorithmSha256withrsa,
		Validity:                &acmpcatypes.Validity{Type: acmpcatypes.ValidityPeriodTypeDays, Value: aws.Int64(30)},
		TemplateArn:             aws.String("arn:aws:acm-pca:::template/BlankEndEntityCertificate_APIPassthrough/V1"),
		ApiPassthrough:          ap,
	})
	require.NoError(t, err)

	got, err := client.GetCertificate(t.Context(), &acmpcasdk.GetCertificateInput{
		CertificateAuthorityArn: aws.String(caARN),
		CertificateArn:          issued.CertificateArn,
	})
	require.NoError(t, err)

	block, _ := pem.Decode([]byte(aws.ToString(got.Certificate)))
	require.NotNil(t, block)

	parsed, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	return parsed
}

// findExtension returns the raw extension in exts matching dottedOID, or nil.
func findExtension(exts []pkix.Extension, dottedOID string) *pkix.Extension {
	for i, e := range exts {
		if e.Id.String() == dottedOID {
			return &exts[i]
		}
	}

	return nil
}

func TestIssueCertificate_CertificatePolicies_RealClient(t *testing.T) {
	t.Parallel()

	backend := acmpca.NewInMemoryBackend(testAccountID, testRegion)
	h := acmpca.NewHandler(backend)
	client := newTestACMPCASDKClient(t, h)

	ca, err := client.CreateCertificateAuthority(t.Context(), &acmpcasdk.CreateCertificateAuthorityInput{
		CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeRoot,
		CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
			KeyAlgorithm:     acmpcatypes.KeyAlgorithmEcPrime256v1,
			SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withecdsa,
			Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String("Policy Issuer")},
		},
	})
	require.NoError(t, err)

	csrPEM := issueWithCSR(t, backend)

	ap := &acmpcatypes.ApiPassthrough{
		Extensions: &acmpcatypes.Extensions{
			CertificatePolicies: []acmpcatypes.PolicyInformation{
				{
					CertPolicyId: aws.String("1.2.3.4.5.6"),
					PolicyQualifiers: []acmpcatypes.PolicyQualifierInfo{
						{
							PolicyQualifierId: acmpcatypes.PolicyQualifierIdCps,
							Qualifier:         &acmpcatypes.Qualifier{CpsUri: aws.String("https://example.com/cps")},
						},
					},
				},
			},
		},
	}

	parsed := issueRealCertWithPassthrough(t, client, aws.ToString(ca.CertificateAuthorityArn), csrPEM, ap)

	wantOID := asn1.ObjectIdentifier{1, 2, 3, 4, 5, 6}

	var found bool

	for _, oid := range parsed.PolicyIdentifiers {
		if oid.Equal(wantOID) {
			found = true
		}
	}

	assert.True(
		t,
		found,
		"issued certificate's PolicyIdentifiers should contain 1.2.3.4.5.6, got %v",
		parsed.PolicyIdentifiers,
	)

	ext := findExtension(parsed.Extensions, oidCertificatePoliciesDotted)
	require.NotNil(t, ext, "certificatePolicies extension (2.5.29.32) not found")

	var policies []policyInformationDER

	_, err = asn1.Unmarshal(ext.Value, &policies)
	require.NoError(t, err)
	require.Len(t, policies, 1)
	assert.True(t, policies[0].PolicyIdentifier.Equal(wantOID))
	require.Len(t, policies[0].PolicyQualifiers, 1)
	assert.Equal(t, "https://example.com/cps", policies[0].PolicyQualifiers[0].Qualifier)
}

func TestIssueCertificate_CertificatePolicies_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy acmpcatypes.PolicyInformation
	}{
		{
			name: "unsupported PolicyQualifierId",
			policy: acmpcatypes.PolicyInformation{
				CertPolicyId: aws.String("1.2.3.4"),
				PolicyQualifiers: []acmpcatypes.PolicyQualifierInfo{
					{PolicyQualifierId: "UNOTICE", Qualifier: &acmpcatypes.Qualifier{CpsUri: aws.String("x")}},
				},
			},
		},
		{
			// The SDK client validates CpsUri == nil itself before sending
			// (validators.go's validateQualifier), so this uses an empty
			// (non-nil) CpsUri to reach gopherstack's own server-side check.
			name: "empty CpsUri",
			policy: acmpcatypes.PolicyInformation{
				CertPolicyId: aws.String("1.2.3.4"),
				PolicyQualifiers: []acmpcatypes.PolicyQualifierInfo{
					{
						PolicyQualifierId: acmpcatypes.PolicyQualifierIdCps,
						Qualifier:         &acmpcatypes.Qualifier{CpsUri: aws.String("")},
					},
				},
			},
		},
		{
			name:   "malformed CertPolicyId",
			policy: acmpcatypes.PolicyInformation{CertPolicyId: aws.String("not-an-oid")},
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
					Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String("Bad Policy Issuer")},
				},
			})
			require.NoError(t, err)

			csrPEM := issueWithCSR(t, backend)

			_, err = client.IssueCertificate(t.Context(), &acmpcasdk.IssueCertificateInput{
				CertificateAuthorityArn: ca.CertificateAuthorityArn,
				Csr:                     []byte(csrPEM),
				SigningAlgorithm:        acmpcatypes.SigningAlgorithmSha256withrsa,
				Validity: &acmpcatypes.Validity{
					Type: acmpcatypes.ValidityPeriodTypeDays, Value: aws.Int64(30),
				},
				TemplateArn: aws.String("arn:aws:acm-pca:::template/BlankEndEntityCertificate_APIPassthrough/V1"),
				ApiPassthrough: &acmpcatypes.ApiPassthrough{
					Extensions: &acmpcatypes.Extensions{
						CertificatePolicies: []acmpcatypes.PolicyInformation{tt.policy},
					},
				},
			})
			require.Error(t, err)

			var ia *acmpcatypes.InvalidArgsException
			require.ErrorAs(t, err, &ia)
		})
	}
}
