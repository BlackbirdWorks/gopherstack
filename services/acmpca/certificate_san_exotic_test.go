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

const oidSubjectAltNameDotted = "2.5.29.17"

func decodeSAN(t *testing.T, der []byte) []asn1.RawValue {
	t.Helper()

	var raws []asn1.RawValue

	rest, err := asn1.Unmarshal(der, &raws)
	require.NoError(t, err)
	require.Empty(t, rest)

	return raws
}

func findGeneralName(t *testing.T, raws []asn1.RawValue, tag int) *asn1.RawValue {
	t.Helper()

	for i := range raws {
		if raws[i].Tag == tag && raws[i].Class == asn1.ClassContextSpecific {
			return &raws[i]
		}
	}

	return nil
}

// TestIssueCertificate_ExoticSAN_RealClient proves every GeneralName variant
// -- including the ones crypto/x509 cannot itself emit (OtherName,
// DirectoryName, EdiPartyName, RegisteredId) -- lands in the issued
// certificate's hand-built subjectAltName extension (2.5.29.17), alongside a
// simple DnsName entry, by asn1-decoding the raw extension from scratch.
func TestIssueCertificate_ExoticSAN_RealClient(t *testing.T) {
	t.Parallel()

	backend := acmpca.NewInMemoryBackend(testAccountID, testRegion)
	h := acmpca.NewHandler(backend)
	client := newTestACMPCASDKClient(t, h)

	ca, err := client.CreateCertificateAuthority(t.Context(), &acmpcasdk.CreateCertificateAuthorityInput{
		CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeRoot,
		CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
			KeyAlgorithm:     acmpcatypes.KeyAlgorithmEcPrime256v1,
			SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withecdsa,
			Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String("Exotic SAN Issuer")},
		},
	})
	require.NoError(t, err)

	csrPEM := issueWithCSR(t, backend)

	ap := &acmpcatypes.ApiPassthrough{
		Extensions: &acmpcatypes.Extensions{
			SubjectAlternativeNames: []acmpcatypes.GeneralName{
				{DnsName: aws.String("plain.example.com")},
				{RegisteredId: aws.String("1.2.3.4.5")},
				{
					OtherName: &acmpcatypes.OtherName{
						TypeId: aws.String("1.3.6.1.4.1.311.20.2.3"),
						Value:  aws.String("upn@example.com"),
					},
				},
				{
					EdiPartyName: &acmpcatypes.EdiPartyName{
						PartyName:    aws.String("party1"),
						NameAssigner: aws.String("assigner1"),
					},
				},
				{DirectoryName: &acmpcatypes.ASN1Subject{CommonName: aws.String("directory.example.com")}},
			},
		},
	}

	parsed := issueRealCertWithPassthrough(t, client, aws.ToString(ca.CertificateAuthorityArn), csrPEM, ap)

	ext := findExtension(parsed.Extensions, oidSubjectAltNameDotted)
	require.NotNil(t, ext, "subjectAltName extension (2.5.29.17) not found")

	raws := decodeSAN(t, ext.Value)
	require.Len(t, raws, 5)

	// DnsName: [2] IMPLICIT IA5String, primitive.
	dns := findGeneralName(t, raws, 2)
	require.NotNil(t, dns)
	assert.Equal(t, "plain.example.com", string(dns.Bytes))

	// RegisteredId: [8] IMPLICIT OBJECT IDENTIFIER, primitive.
	rid := findGeneralName(t, raws, 8)
	require.NotNil(t, rid)

	var ridOID asn1.ObjectIdentifier

	_, err = asn1.Unmarshal(append([]byte{asn1.TagOID, byte(len(rid.Bytes))}, rid.Bytes...), &ridOID)
	require.NoError(t, err)
	assert.True(t, ridOID.Equal(asn1.ObjectIdentifier{1, 2, 3, 4, 5}))

	// OtherName: [0] SEQUENCE { type-id OBJECT IDENTIFIER, value [0] EXPLICIT ANY }.
	other := findGeneralName(t, raws, 0)
	require.NotNil(t, other)
	assert.True(t, other.IsCompound)

	//nolint:govet // ASN.1 SEQUENCE field order is wire-significant; fieldalignment must not reorder it
	var otherSeq struct {
		TypeID asn1.ObjectIdentifier
		Value  string `asn1:"utf8,explicit,tag:0"`
	}

	_, err = asn1.Unmarshal(reencodeAsSequence(t, *other), &otherSeq)
	require.NoError(t, err)
	assert.True(t, otherSeq.TypeID.Equal(asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 311, 20, 2, 3}))
	assert.Equal(t, "upn@example.com", otherSeq.Value)

	// EdiPartyName: [5] SEQUENCE { nameAssigner [0] EXPLICIT ..., partyName [1] EXPLICIT ... }.
	edi := findGeneralName(t, raws, 5)
	require.NotNil(t, edi)
	assert.True(t, edi.IsCompound)

	var ediSeq struct {
		NameAssigner string `asn1:"utf8,optional,explicit,tag:0"`
		PartyName    string `asn1:"utf8,explicit,tag:1"`
	}

	_, err = asn1.Unmarshal(reencodeAsSequence(t, *edi), &ediSeq)
	require.NoError(t, err)
	assert.Equal(t, "party1", ediSeq.PartyName)
	assert.Equal(t, "assigner1", ediSeq.NameAssigner)

	// DirectoryName: [4] EXPLICIT Name (RDNSequence).
	dir := findGeneralName(t, raws, 4)
	require.NotNil(t, dir)
	assert.True(t, dir.IsCompound)

	var dirRDNs pkix.RDNSequence

	_, err = asn1.Unmarshal(reencodeAsSequence(t, *dir), &dirRDNs)
	require.NoError(t, err)
	assert.Equal(t, "directory.example.com", findRDNValue(dirRDNs, []int{2, 5, 4, 3}))
}

// reencodeAsSequence re-tags raw (a context-specific, constructed
// GeneralName alternative) as a universal SEQUENCE so the standard library's
// encoding/asn1 can decode its contents through an ordinary Go struct --
// mirroring, independently, what the package's own reTagImplicit does in
// reverse (certificate_extensions.go).
func reencodeAsSequence(t *testing.T, raw asn1.RawValue) []byte {
	t.Helper()

	raw.Class = asn1.ClassUniversal
	raw.Tag = asn1.TagSequence
	raw.FullBytes = nil

	out, err := asn1.Marshal(raw)
	require.NoError(t, err)

	return out
}

// TestIssueCertificate_ExoticSAN_Validation covers rejection of malformed or
// ambiguous SubjectAlternativeNames entries.
func TestIssueCertificate_ExoticSAN_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		gn   acmpcatypes.GeneralName
		name string
	}{
		{
			name: "two variants set on one entry",
			gn:   acmpcatypes.GeneralName{DnsName: aws.String("a.example.com"), RegisteredId: aws.String("1.2.3")},
		},
		{
			name: "malformed RegisteredId",
			gn:   acmpcatypes.GeneralName{RegisteredId: aws.String("not-an-oid")},
		},
		{
			name: "malformed OtherName TypeId",
			gn: acmpcatypes.GeneralName{
				OtherName: &acmpcatypes.OtherName{TypeId: aws.String("nope"), Value: aws.String("v")},
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
					Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String("Bad SAN Issuer")},
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
						SubjectAlternativeNames: []acmpcatypes.GeneralName{tt.gn},
					},
				},
			})
			require.Error(t, err)

			var ia *acmpcatypes.InvalidArgsException
			require.ErrorAs(t, err, &ia)
		})
	}
}
