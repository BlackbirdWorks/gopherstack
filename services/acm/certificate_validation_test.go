package acm_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestACMHandler_MalformedArn_ReturnsInvalidArnException verifies that a
// non-empty CertificateArn which does not match ACM's ARN shape is rejected
// with InvalidArnException (400), distinct from the ResourceNotFoundException
// returned for a well-formed but nonexistent ARN.
func TestACMHandler_MalformedArn_ReturnsInvalidArnException(t *testing.T) {
	t.Parallel()

	const badArn = "not-an-arn-at-all"

	tests := []struct {
		name   string
		target string
		body   string
	}{
		{name: "DescribeCertificate", target: "DescribeCertificate", body: `{"CertificateArn":"` + badArn + `"}`},
		{name: "DeleteCertificate", target: "DeleteCertificate", body: `{"CertificateArn":"` + badArn + `"}`},
		{name: "GetCertificate", target: "GetCertificate", body: `{"CertificateArn":"` + badArn + `"}`},
		{
			name: "ExportCertificate", target: "ExportCertificate",
			body: `{"CertificateArn":"` + badArn + `","Passphrase":"dGVzdA=="}`,
		},
		{name: "RenewCertificate", target: "RenewCertificate", body: `{"CertificateArn":"` + badArn + `"}`},
		{
			name: "RevokeCertificate", target: "RevokeCertificate",
			body: `{"CertificateArn":"` + badArn + `","RevocationReason":"UNSPECIFIED"}`,
		},
		{
			name: "ResendValidationEmail", target: "ResendValidationEmail",
			body: `{"CertificateArn":"` + badArn + `","Domain":"x.com","ValidationDomain":"x.com"}`,
		},
		{
			name:   "UpdateCertificateOptions",
			target: "UpdateCertificateOptions",
			body: `{"CertificateArn":"` + badArn +
				`","Options":{"CertificateTransparencyLoggingPreference":"ENABLED"}}`,
		},
		{
			name: "ListTagsForCertificate", target: "ListTagsForCertificate",
			body: `{"CertificateArn":"` + badArn + `"}`,
		},
		{
			name: "AddTagsToCertificate", target: "AddTagsToCertificate",
			body: `{"CertificateArn":"` + badArn + `","Tags":[{"Key":"k","Value":"v"}]}`,
		},
		{
			name: "RemoveTagsFromCertificate", target: "RemoveTagsFromCertificate",
			body: `{"CertificateArn":"` + badArn + `","Tags":[{"Key":"k"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			rec := postACMJSON(t, h, tt.target, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "InvalidArnException")
		})
	}
}

// TestACMHandler_WellFormedNonexistentArn_ReturnsResourceNotFound verifies
// that a well-formed but nonexistent ARN still falls through to
// ResourceNotFoundException, not InvalidArnException -- the two error paths
// must not be conflated.
func TestACMHandler_WellFormedNonexistentArn_ReturnsResourceNotFound(t *testing.T) {
	t.Parallel()

	const wellFormedArn = "arn:aws:acm:us-east-1:000000000000:certificate/does-not-exist"

	h := newACMHandler()
	rec := postACMJSON(t, h, "DescribeCertificate", `{"CertificateArn":"`+wellFormedArn+`"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ResourceNotFoundException")
	assert.NotContains(t, rec.Body.String(), "InvalidArnException")
}

// TestACMHandler_RequestCertificate_TooManySANs_ReturnsLimitExceeded verifies
// that exceeding the domain-per-certificate quota returns
// LimitExceededException, matching real ACM's quota-error semantics (not a
// generic ValidationException).
func TestACMHandler_RequestCertificate_TooManySANs_ReturnsLimitExceeded(t *testing.T) {
	t.Parallel()

	sans := make([]string, 0, 12)
	for i := range 12 {
		sans = append(sans, "san"+string(rune('a'+i))+".example.com")
	}

	body, err := json.Marshal(map[string]any{
		"DomainName":              "toomany.example.com",
		"SubjectAlternativeNames": sans,
	})
	require.NoError(t, err)

	h := newACMHandler()
	rec := postACMJSON(t, h, "RequestCertificate", string(body))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "LimitExceededException")
}

// TestACMHandler_TooManyTags_ReturnsTooManyTagsException verifies that
// exceeding the 50-tags-per-certificate quota (via AddTagsToCertificate)
// returns TooManyTagsException, matching real AWS tagging error semantics.
func TestACMHandler_TooManyTags_ReturnsTooManyTagsException(t *testing.T) {
	t.Parallel()

	h := newACMHandler()
	reqRec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"manytags.example.com"}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	var reqOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

	tags := make([]map[string]string, 0, 51)
	for i := range 51 {
		tags = append(tags, map[string]string{"Key": "k" + string(rune('a'+i)), "Value": "v"})
	}

	body, err := json.Marshal(map[string]any{
		"CertificateArn": reqOut.CertificateArn,
		"Tags":           tags,
	})
	require.NoError(t, err)

	rec := postACMJSON(t, h, "AddTagsToCertificate", string(body))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "TooManyTagsException")
}

// TestACMHandler_ReservedTagPrefix_ReturnsInvalidTagException verifies that a
// tag key beginning with the AWS-reserved "aws:" prefix is rejected with
// InvalidTagException, matching real AWS tagging validation.
func TestACMHandler_ReservedTagPrefix_ReturnsInvalidTagException(t *testing.T) {
	t.Parallel()

	h := newACMHandler()
	reqRec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"reservedtag.example.com"}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	var reqOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

	body, err := json.Marshal(map[string]any{
		"CertificateArn": reqOut.CertificateArn,
		"Tags":           []map[string]string{{"Key": "aws:reserved", "Value": "v"}},
	})
	require.NoError(t, err)

	rec := postACMJSON(t, h, "AddTagsToCertificate", string(body))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidTagException")
}

// TestACMHandler_RequestCertificate_RSA1024_ReturnsValidationException
// locks in the fix for a bug where requesting RSA_1024 (a weak-key rejection
// path) escaped handleOpError's known-error switch and was reported as a 500
// InternalFailure instead of a 400 ValidationException.
func TestACMHandler_RequestCertificate_RSA1024_ReturnsValidationException(t *testing.T) {
	t.Parallel()

	h := newACMHandler()
	body := `{"DomainName":"weakkey.example.com","KeyAlgorithm":"RSA_1024"}`
	rec := postACMJSON(t, h, "RequestCertificate", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
	assert.NotContains(t, rec.Body.String(), "InternalFailure")
}

// TestACMHandler_RequestCertificate_DomainValidationOptions_Applied verifies
// that a caller-supplied DomainValidationOptions entry (custom EMAIL
// ValidationDomain) is validated, stored, and reflected back on
// DescribeCertificate -- including the derived validation email addresses.
func TestACMHandler_RequestCertificate_DomainValidationOptions_Applied(t *testing.T) {
	t.Parallel()

	h := newACMHandler()
	body := `{
		"DomainName":"sub.example.com",
		"ValidationMethod":"EMAIL",
		"DomainValidationOptions":[{"DomainName":"sub.example.com","ValidationDomain":"example.com"}]
	}`
	reqRec := postACMJSON(t, h, "RequestCertificate", body)
	require.Equal(t, http.StatusOK, reqRec.Code)

	var reqOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

	descBody, err := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
	require.NoError(t, err)

	descRec := postACMJSON(t, h, "DescribeCertificate", string(descBody))
	require.Equal(t, http.StatusOK, descRec.Code)
	assert.Contains(t, descRec.Body.String(), `"ValidationDomain":"example.com"`)
	assert.Contains(t, descRec.Body.String(), "admin@example.com")
}

// TestACMHandler_RequestCertificate_DomainValidationOptions_InvalidDomain
// verifies that a DomainValidationOptions entry naming a domain not in the
// request is rejected with InvalidDomainValidationOptionsException, and that
// no certificate is created as a side effect.
func TestACMHandler_RequestCertificate_DomainValidationOptions_InvalidDomain(t *testing.T) {
	t.Parallel()

	h := newACMHandler()
	body := `{
		"DomainName":"onlythis.example.com",
		"ValidationMethod":"EMAIL",
		"DomainValidationOptions":[{"DomainName":"not-requested.example.com","ValidationDomain":"example.com"}]
	}`
	rec := postACMJSON(t, h, "RequestCertificate", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidDomainValidationOptionsException")

	listRec := postACMJSON(t, h, "ListCertificates", `{}`)
	require.Equal(t, http.StatusOK, listRec.Code)
	assert.NotContains(t, listRec.Body.String(), "onlythis.example.com",
		"a rejected DomainValidationOptions request must not create a certificate")
}

// TestACMHandler_RequestCertificate_DomainValidationOptions_NotSuperdomain
// verifies that a ValidationDomain which is neither the same as nor a
// superdomain of its DomainName is rejected with
// InvalidDomainValidationOptionsException.
func TestACMHandler_RequestCertificate_DomainValidationOptions_NotSuperdomain(t *testing.T) {
	t.Parallel()

	h := newACMHandler()
	body := `{
		"DomainName":"sub.example.com",
		"ValidationMethod":"EMAIL",
		"DomainValidationOptions":[{"DomainName":"sub.example.com","ValidationDomain":"unrelated.org"}]
	}`
	rec := postACMJSON(t, h, "RequestCertificate", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidDomainValidationOptionsException")
}
