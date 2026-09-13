package acmpca_test

import (
	"crypto/x509/pkix"
	"encoding/asn1"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	acmpcasdk "github.com/aws/aws-sdk-go-v2/service/acmpca"
	acmpcatypes "github.com/aws/aws-sdk-go-v2/service/acmpca/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

// findRDNValue returns the string Value of the first AttributeTypeAndValue in
// rdns whose Type matches oid, or "" if none matches. Used to independently
// re-parse the issued certificate's raw Subject RDNSequence rather than
// trusting crypto/x509/pkix.Name's own (incomplete, for exotic OIDs) parsing.
func findRDNValue(rdns pkix.RDNSequence, oid []int) string {
	for _, rdnSet := range rdns {
		for _, atv := range rdnSet {
			if atv.Type.Equal(oid) {
				if s, ok := atv.Value.(string); ok {
					return s
				}
			}
		}
	}

	return ""
}

// TestIssueCertificate_ExoticSubjectRDNs_RealClient proves every
// APIPassthrough.Subject field -- including the exotic RDN types beyond
// CommonName/Country/Organization/OrganizationalUnit/State/Locality/SerialNumber
// -- lands in the issued certificate's Subject, at the RFC 5280 Appendix A.1 /
// RFC 4519 OIDs (pseudonym 2.5.4.65, generationQualifier 2.5.4.44, dnQualifier
// 2.5.4.46, title 2.5.4.12, initials 2.5.4.43, givenName 2.5.4.42, surname
// 2.5.4.4), plus an arbitrary CustomAttributes OID.
func TestIssueCertificate_ExoticSubjectRDNs_RealClient(t *testing.T) {
	t.Parallel()

	backend := acmpca.NewInMemoryBackend(testAccountID, testRegion)
	h := acmpca.NewHandler(backend)
	client := newTestACMPCASDKClient(t, h)

	ca, err := client.CreateCertificateAuthority(t.Context(), &acmpcasdk.CreateCertificateAuthorityInput{
		CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeRoot,
		CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
			KeyAlgorithm:     acmpcatypes.KeyAlgorithmEcPrime256v1,
			SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withecdsa,
			Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String("Exotic Subject Issuer")},
		},
	})
	require.NoError(t, err)

	csrPEM := issueWithCSR(t, backend)

	ap := &acmpcatypes.ApiPassthrough{
		Subject: &acmpcatypes.ASN1Subject{
			CommonName:                 aws.String("exotic.example.com"),
			DistinguishedNameQualifier: aws.String("DNQ1"),
			GenerationQualifier:        aws.String("Jr."),
			GivenName:                  aws.String("Ada"),
			Initials:                   aws.String("AL"),
			Pseudonym:                  aws.String("TheOracle"),
			Surname:                    aws.String("Lovelace"),
			Title:                      aws.String("Countess"),
			CustomAttributes: []acmpcatypes.CustomAttribute{
				{ObjectIdentifier: aws.String("1.2.3.4.5"), Value: aws.String("custom-value")},
			},
		},
	}

	parsed := issueRealCertWithPassthrough(t, client, aws.ToString(ca.CertificateAuthorityArn), csrPEM, ap)

	var rdns pkix.RDNSequence

	_, err = asn1.Unmarshal(parsed.RawSubject, &rdns)
	require.NoError(t, err)

	assert.Equal(t, "exotic.example.com", parsed.Subject.CommonName)
	assert.Equal(t, "DNQ1", findRDNValue(rdns, []int{2, 5, 4, 46}))
	assert.Equal(t, "Jr.", findRDNValue(rdns, []int{2, 5, 4, 44}))
	assert.Equal(t, "Ada", findRDNValue(rdns, []int{2, 5, 4, 42}))
	assert.Equal(t, "AL", findRDNValue(rdns, []int{2, 5, 4, 43}))
	assert.Equal(t, "TheOracle", findRDNValue(rdns, []int{2, 5, 4, 65}))
	assert.Equal(t, "Lovelace", findRDNValue(rdns, []int{2, 5, 4, 4}))
	assert.Equal(t, "Countess", findRDNValue(rdns, []int{2, 5, 4, 12}))
	assert.Equal(t, "custom-value", findRDNValue(rdns, []int{1, 2, 3, 4, 5}))
}

// TestIssueCertificate_ExoticSubjectRDNs_MalformedCustomAttributeRejected
// proves a malformed CustomAttributes OID is rejected with InvalidArgsException
// rather than silently dropped.
func TestIssueCertificate_ExoticSubjectRDNs_MalformedCustomAttributeRejected(t *testing.T) {
	t.Parallel()

	backend := acmpca.NewInMemoryBackend(testAccountID, testRegion)
	h := acmpca.NewHandler(backend)
	client := newTestACMPCASDKClient(t, h)

	ca, err := client.CreateCertificateAuthority(t.Context(), &acmpcasdk.CreateCertificateAuthorityInput{
		CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeRoot,
		CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
			KeyAlgorithm:     acmpcatypes.KeyAlgorithmEcPrime256v1,
			SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withecdsa,
			Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String("Bad Custom Attr Issuer")},
		},
	})
	require.NoError(t, err)

	csrPEM := issueWithCSR(t, backend)

	_, err = client.IssueCertificate(t.Context(), &acmpcasdk.IssueCertificateInput{
		CertificateAuthorityArn: ca.CertificateAuthorityArn,
		Csr:                     []byte(csrPEM),
		SigningAlgorithm:        acmpcatypes.SigningAlgorithmSha256withrsa,
		Validity:                &acmpcatypes.Validity{Type: acmpcatypes.ValidityPeriodTypeDays, Value: aws.Int64(30)},
		TemplateArn:             aws.String("arn:aws:acm-pca:::template/BlankEndEntityCertificate_APIPassthrough/V1"),
		ApiPassthrough: &acmpcatypes.ApiPassthrough{
			Subject: &acmpcatypes.ASN1Subject{
				CustomAttributes: []acmpcatypes.CustomAttribute{
					{ObjectIdentifier: aws.String("not-an-oid"), Value: aws.String("v")},
				},
			},
		},
	})
	require.Error(t, err)

	var ia *acmpcatypes.InvalidArgsException
	require.ErrorAs(t, err, &ia)
}
