package acmpca_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	acmpcasdk "github.com/aws/aws-sdk-go-v2/service/acmpca"
	acmpcatypes "github.com/aws/aws-sdk-go-v2/service/acmpca/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

const csrCRLURL = "http://crl.csr.example/ca.crl"

func contextTLV(tag int, content []byte) asn1.RawValue {
	return asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: tag, IsCompound: true, Bytes: content}
}

func sequenceTLV(content []byte) asn1.RawValue {
	return asn1.RawValue{Class: asn1.ClassUniversal, Tag: asn1.TagSequence, IsCompound: true, Bytes: content}
}

func csrExtensions(t *testing.T) []pkix.Extension {
	t.Helper()

	ku, err := asn1.Marshal(asn1.BitString{Bytes: []byte{0x88}, BitLength: 5})
	require.NoError(t, err)

	eku, err := asn1.Marshal([]asn1.ObjectIdentifier{{1, 3, 6, 1, 5, 5, 7, 3, 2}})
	require.NoError(t, err)

	uri, err := asn1.Marshal(asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 6, Bytes: []byte(csrCRLURL)})
	require.NoError(t, err)

	fullName, err := asn1.Marshal(contextTLV(0, uri))
	require.NoError(t, err)

	dpName, err := asn1.Marshal(contextTLV(0, fullName))
	require.NoError(t, err)

	dp, err := asn1.Marshal(sequenceTLV(dpName))
	require.NoError(t, err)

	list, err := asn1.Marshal(sequenceTLV(dp))
	require.NoError(t, err)

	return []pkix.Extension{
		{Id: asn1.ObjectIdentifier{2, 5, 29, 15}, Value: ku},
		{Id: asn1.ObjectIdentifier{2, 5, 29, 37}, Value: eku},
		{Id: asn1.ObjectIdentifier{2, 5, 29, 31}, Value: list},
	}
}

func makeExtensionCSR(t *testing.T) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	require.NoError(t, err)

	der, err := x509.CreateCertificateRequest(cryptorand.Reader, &x509.CertificateRequest{
		Subject:         pkix.Name{CommonName: "csr.example.com"},
		DNSNames:        []string{"csr.example.com", "alt.example.com"},
		ExtraExtensions: csrExtensions(t),
	}, key)
	require.NoError(t, err)

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}))
}

func newRootCA(t *testing.T, client *acmpcasdk.Client) *string {
	t.Helper()

	ca, err := client.CreateCertificateAuthority(t.Context(), &acmpcasdk.CreateCertificateAuthorityInput{
		CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeRoot,
		CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
			KeyAlgorithm:     acmpcatypes.KeyAlgorithmEcPrime256v1,
			SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withecdsa,
			Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String("Root")},
		},
	})
	require.NoError(t, err)

	return ca.CertificateAuthorityArn
}

func TestIssueCertificate_CSRPassthrough(t *testing.T) {
	t.Parallel()

	const tmplPrefix = "arn:aws:acm-pca:::template/"

	tests := []struct {
		check    func(t *testing.T, cert *x509.Certificate)
		name     string
		template string
	}{
		{
			name:     "csr passthrough honors csr extensions",
			template: tmplPrefix + "BlankEndEntityCertificate_CSRPassthrough/V1",
			check: func(t *testing.T, cert *x509.Certificate) {
				t.Helper()

				assert.Equal(t, x509.KeyUsageDigitalSignature|x509.KeyUsageKeyAgreement, cert.KeyUsage)
				assert.Equal(t, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, cert.ExtKeyUsage)
				assert.ElementsMatch(t, []string{"csr.example.com", "alt.example.com"}, cert.DNSNames)
				assert.Equal(t, []string{csrCRLURL}, cert.CRLDistributionPoints)
			},
		},
		{
			name:     "fixed profile beats csr",
			template: tmplPrefix + "EndEntityServerAuthCertificate_CSRPassthrough/V1",
			check: func(t *testing.T, cert *x509.Certificate) {
				t.Helper()

				assert.Equal(t, x509.KeyUsageDigitalSignature|x509.KeyUsageKeyEncipherment, cert.KeyUsage)
				assert.Equal(t, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, cert.ExtKeyUsage)
				assert.Equal(t, []string{csrCRLURL}, cert.CRLDistributionPoints)
			},
		},
		{
			name:     "no passthrough ignores csr extensions",
			template: tmplPrefix + "BlankEndEntityCertificate/V1",
			check: func(t *testing.T, cert *x509.Certificate) {
				t.Helper()

				assert.Zero(t, cert.KeyUsage)
				assert.Empty(t, cert.ExtKeyUsage)
				assert.Empty(t, cert.CRLDistributionPoints)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestACMPCASDKClient(t, acmpca.NewHandler(acmpca.NewInMemoryBackend(testAccountID, testRegion)))
			caARN := newRootCA(t, client)

			cert, err := issueCertWithTemplate(t, client, aws.ToString(caARN), makeExtensionCSR(t), tt.template)
			require.NoError(t, err)

			tt.check(t, cert)
		})
	}
}

func issueAndImportSubordinate(
	t *testing.T, client *acmpcasdk.Client, issuerARN, template string, validityDays int64,
) (*string, error) {
	t.Helper()

	sub, err := client.CreateCertificateAuthority(t.Context(), &acmpcasdk.CreateCertificateAuthorityInput{
		CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeSubordinate,
		CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
			KeyAlgorithm:     acmpcatypes.KeyAlgorithmEcPrime256v1,
			SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withecdsa,
			Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String("Sub")},
		},
	})
	require.NoError(t, err)

	csr, err := client.GetCertificateAuthorityCsr(t.Context(), &acmpcasdk.GetCertificateAuthorityCsrInput{
		CertificateAuthorityArn: sub.CertificateAuthorityArn,
	})
	require.NoError(t, err)

	issued, err := client.IssueCertificate(t.Context(), &acmpcasdk.IssueCertificateInput{
		CertificateAuthorityArn: aws.String(issuerARN),
		Csr:                     []byte(aws.ToString(csr.Csr)),
		SigningAlgorithm:        acmpcatypes.SigningAlgorithmSha256withecdsa,
		Validity: &acmpcatypes.Validity{
			Type:  acmpcatypes.ValidityPeriodTypeDays,
			Value: aws.Int64(validityDays),
		},
		TemplateArn: aws.String(template),
	})
	if err != nil {
		return nil, err
	}

	got, err := client.GetCertificate(t.Context(), &acmpcasdk.GetCertificateInput{
		CertificateAuthorityArn: aws.String(issuerARN),
		CertificateArn:          issued.CertificateArn,
	})
	require.NoError(t, err)

	_, err = client.ImportCertificateAuthorityCertificate(
		t.Context(),
		&acmpcasdk.ImportCertificateAuthorityCertificateInput{
			CertificateAuthorityArn: sub.CertificateAuthorityArn,
			Certificate:             []byte(aws.ToString(got.Certificate)),
			CertificateChain:        []byte(aws.ToString(got.CertificateChain)),
		},
	)
	require.NoError(t, err)

	return sub.CertificateAuthorityArn, nil
}

func TestIssueCertificate_IssuerPathLength(t *testing.T) {
	t.Parallel()

	const tmplPrefix = "arn:aws:acm-pca:::template/SubordinateCACertificate_"

	tests := []struct {
		name     string
		first    string
		second   string
		wantFail bool
	}{
		{name: "deeper than parent allows", first: "PathLen1", second: "PathLen1", wantFail: true},
		{name: "shallower than parent", first: "PathLen2", second: "PathLen1"},
		{name: "parent at zero issues no CA", first: "PathLen0", second: "PathLen0", wantFail: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestACMPCASDKClient(t, acmpca.NewHandler(acmpca.NewInMemoryBackend(testAccountID, testRegion)))
			rootARN := newRootCA(t, client)

			subARN, err := issueAndImportSubordinate(t, client, aws.ToString(rootARN), tmplPrefix+tt.first+"/V1", 30)
			require.NoError(t, err)

			_, err = issueAndImportSubordinate(t, client, aws.ToString(subARN), tmplPrefix+tt.second+"/V1", 10)
			if tt.wantFail {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "InvalidArgsException")

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestDescribeCertificateAuthority_Expired(t *testing.T) {
	t.Parallel()

	backend := acmpca.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestACMPCASDKClient(t, acmpca.NewHandler(backend))
	rootARN := newRootCA(t, client)

	sub, err := client.CreateCertificateAuthority(t.Context(), &acmpcasdk.CreateCertificateAuthorityInput{
		CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeSubordinate,
		CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
			KeyAlgorithm:     acmpcatypes.KeyAlgorithmEcPrime256v1,
			SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withecdsa,
			Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String("Short")},
		},
	})
	require.NoError(t, err)

	csr, err := backend.GetCertificateAuthorityCsr(t.Context(), aws.ToString(sub.CertificateAuthorityArn))
	require.NoError(t, err)

	issued, err := backend.IssueCertificate(
		t.Context(), aws.ToString(rootARN), csr, 1,
		acmpca.WithIssueCertTemplateArn("arn:aws:acm-pca:::template/SubordinateCACertificate_PathLen0/V1"),
		acmpca.WithIssueCertValidityNotBefore(time.Now().Add(-72*time.Hour)),
	)
	require.NoError(t, err)

	require.NoError(t, backend.ImportCertificateAuthorityCertificate(
		t.Context(), aws.ToString(sub.CertificateAuthorityArn), issued.CertBody, "",
	))

	desc, err := client.DescribeCertificateAuthority(t.Context(), &acmpcasdk.DescribeCertificateAuthorityInput{
		CertificateAuthorityArn: sub.CertificateAuthorityArn,
	})
	require.NoError(t, err)
	assert.Equal(t, acmpcatypes.CertificateAuthorityStatusExpired, desc.CertificateAuthority.Status)

	_, err = client.IssueCertificate(t.Context(), &acmpcasdk.IssueCertificateInput{
		CertificateAuthorityArn: sub.CertificateAuthorityArn,
		Csr:                     []byte(makeExtensionCSR(t)),
		SigningAlgorithm:        acmpcatypes.SigningAlgorithmSha256withecdsa,
		Validity:                &acmpcatypes.Validity{Type: acmpcatypes.ValidityPeriodTypeDays, Value: aws.Int64(1)},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidStateException")
}
