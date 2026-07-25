package acmpca_test

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

// issueWithCSR creates a leaf CSR from a throwaway SUBORDINATE CA and returns
// it base64-encoded (the wire form Csr expects), matching the pattern used
// throughout handler_certificates_test.go.
func issueWithCSR(t *testing.T, b *acmpca.InMemoryBackend) string {
	t.Helper()

	subCA, err := b.CreateCertificateAuthority(
		context.Background(), "SUBORDINATE", acmpca.CertificateAuthorityConfiguration{
			Subject: acmpca.CertificateAuthoritySubject{CommonName: "Leaf"},
		},
	)
	require.NoError(t, err)

	csrPEM, err := b.GetCertificateAuthorityCsr(context.Background(), subCA.ARN)
	require.NoError(t, err)

	return csrPEM
}

// TestACMPCAHandler_IssueCertificate_ApiPassthrough covers ApiPassthrough's
// implemented sub-fields (KeyUsage, ExtendedKeyUsage, SubjectAlternativeNames,
// CustomExtensions, Subject) actually altering the issued certificate's X.509
// extensions when TemplateArn selects an APIPassthrough variant -- previously
// ApiPassthrough/TemplateArn were both silently ignored entirely (PARITY.md
// deferred items).
func TestACMPCAHandler_IssueCertificate_ApiPassthrough(t *testing.T) {
	t.Parallel()

	b := acmpca.NewInMemoryBackend(testAccountID, testRegion)
	h := acmpca.NewHandler(b)

	ca, err := b.CreateCertificateAuthority(context.Background(), "ROOT", acmpca.CertificateAuthorityConfiguration{
		Subject: acmpca.CertificateAuthoritySubject{CommonName: "Passthrough Issuer"},
	})
	require.NoError(t, err)

	csrPEM := issueWithCSR(t, b)

	rec := doACMPCARequest(t, h, "IssueCertificate", map[string]any{
		"CertificateAuthorityArn": ca.ARN,
		"Csr":                     b64(csrPEM),
		"SigningAlgorithm":        "SHA256WITHRSA",
		"Validity":                map[string]any{"Type": "DAYS", "Value": 30},
		"TemplateArn":             "arn:aws:acm-pca:::template/BlankEndEntityCertificate_APIPassthrough/V1",
		"ApiPassthrough": map[string]any{
			"Subject": map[string]any{"CommonName": "overridden.example.com"},
			"Extensions": map[string]any{
				"KeyUsage": map[string]any{"DigitalSignature": true, "KeyCertSign": true},
				"ExtendedKeyUsage": []map[string]any{
					{"ExtendedKeyUsageType": "CODE_SIGNING"},
				},
				"SubjectAlternativeNames": []map[string]any{
					{"DnsName": "alt.example.com"},
					{"IpAddress": "10.0.0.1"},
					{"Rfc822Name": "user@example.com"},
				},
				"CustomExtensions": []map[string]any{
					{"ObjectIdentifier": "2.5.29.99", "Value": b64("hello"), "Critical": false},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	resp := parseACMPCAResponse(t, rec)
	certARN, _ := resp["CertificateArn"].(string)
	require.NotEmpty(t, certARN)

	cert, err := b.GetCertificate(context.Background(), ca.ARN, certARN)
	require.NoError(t, err)

	block, _ := pem.Decode([]byte(cert.CertBody))
	require.NotNil(t, block)
	parsed, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	assert.Equal(t, "overridden.example.com", parsed.Subject.CommonName)
	assert.NotZero(t, parsed.KeyUsage&x509.KeyUsageDigitalSignature)
	assert.NotZero(t, parsed.KeyUsage&x509.KeyUsageCertSign)
	assert.Contains(t, parsed.ExtKeyUsage, x509.ExtKeyUsageCodeSigning)
	assert.Contains(t, parsed.DNSNames, "alt.example.com")
	require.Len(t, parsed.IPAddresses, 1)
	assert.Equal(t, "10.0.0.1", parsed.IPAddresses[0].String())
	assert.Contains(t, parsed.EmailAddresses, "user@example.com")

	var foundCustomExt bool

	for _, ext := range parsed.Extensions {
		if ext.Id.String() == "2.5.29.99" {
			foundCustomExt = true

			assert.Equal(t, "hello", string(ext.Value))
		}
	}

	assert.True(t, foundCustomExt, "custom extension 2.5.29.99 not found on issued certificate")
}

// TestACMPCAHandler_IssueCertificate_ApiPassthrough_IgnoredWithoutPassthroughTemplate
// verifies the real API's documented behavior: ApiPassthrough is silently
// ignored unless TemplateArn selects an APIPassthrough/APICSRPassthrough
// variant -- a default-template request with ApiPassthrough set must issue
// normally, without applying the override.
func TestACMPCAHandler_IssueCertificate_ApiPassthrough_IgnoredWithoutPassthroughTemplate(t *testing.T) {
	t.Parallel()

	b := acmpca.NewInMemoryBackend(testAccountID, testRegion)
	h := acmpca.NewHandler(b)

	ca, err := b.CreateCertificateAuthority(context.Background(), "ROOT", acmpca.CertificateAuthorityConfiguration{
		Subject: acmpca.CertificateAuthoritySubject{CommonName: "Default Template Issuer"},
	})
	require.NoError(t, err)

	csrPEM := issueWithCSR(t, b)

	rec := doACMPCARequest(t, h, "IssueCertificate", map[string]any{
		"CertificateAuthorityArn": ca.ARN,
		"Csr":                     b64(csrPEM),
		"SigningAlgorithm":        "SHA256WITHRSA",
		"Validity":                map[string]any{"Type": "DAYS", "Value": 30},
		// No TemplateArn -> defaults to EndEntityCertificate/V1, not a passthrough variant.
		"ApiPassthrough": map[string]any{
			"Subject": map[string]any{"CommonName": "should-be-ignored.example.com"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	resp := parseACMPCAResponse(t, rec)
	certARN, _ := resp["CertificateArn"].(string)

	cert, err := b.GetCertificate(context.Background(), ca.ARN, certARN)
	require.NoError(t, err)

	block, _ := pem.Decode([]byte(cert.CertBody))
	require.NotNil(t, block)
	parsed, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	assert.NotEqual(t, "should-be-ignored.example.com", parsed.Subject.CommonName)
}

// TestACMPCAHandler_IssueCertificate_ApiPassthrough_UnsupportedFieldsRejected
// verifies that ApiPassthrough sub-fields gopherstack does not implement
// (CertificatePolicies, exotic ASN1Subject RDNs, exotic GeneralName variants)
// are rejected with a clear InvalidParameterException instead of being
// silently dropped -- per parity-principles.md's no-silent-gaps rule.
func TestACMPCAHandler_IssueCertificate_ApiPassthrough_UnsupportedFieldsRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		apiPassthrough map[string]any
		name           string
	}{
		{
			name: "CertificatePolicies",
			apiPassthrough: map[string]any{
				"Extensions": map[string]any{
					"CertificatePolicies": []map[string]any{{"CertPolicyId": "2.5.29.32.0"}},
				},
			},
		},
		{
			name: "Subject.Title",
			apiPassthrough: map[string]any{
				"Subject": map[string]any{"Title": "Dr."},
			},
		},
		{
			name: "SubjectAlternativeNames.RegisteredId",
			apiPassthrough: map[string]any{
				"Extensions": map[string]any{
					"SubjectAlternativeNames": []map[string]any{{"RegisteredId": "1.2.3.4"}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acmpca.NewInMemoryBackend(testAccountID, testRegion)
			h := acmpca.NewHandler(b)

			ca, err := b.CreateCertificateAuthority(
				context.Background(), "ROOT", acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Rejecting Issuer"},
				},
			)
			require.NoError(t, err)

			csrPEM := issueWithCSR(t, b)

			rec := doACMPCARequest(t, h, "IssueCertificate", map[string]any{
				"CertificateAuthorityArn": ca.ARN,
				"Csr":                     b64(csrPEM),
				"SigningAlgorithm":        "SHA256WITHRSA",
				"Validity":                map[string]any{"Type": "DAYS", "Value": 30},
				"TemplateArn":             "arn:aws:acm-pca:::template/BlankEndEntityCertificate_APIPassthrough/V1",
				"ApiPassthrough":          tt.apiPassthrough,
			})
			require.Equal(t, http.StatusBadRequest, rec.Code)

			resp := parseACMPCAResponse(t, rec)
			assert.Equal(t, "InvalidParameterException", resp["__type"])
		})
	}
}
