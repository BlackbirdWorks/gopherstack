package integration_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func acmPost(t *testing.T, action string, body any) *http.Response {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint,
		bytes.NewReader(bodyBytes))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "CertificateManager."+action)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func acmReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func acmReadJSON(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer resp.Body.Close()

	var m map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&m))

	return m
}

func TestIntegration_ACM_RequestCertificate(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := acmPost(t, "RequestCertificate", map[string]any{
		"DomainName":       "integ-acm.example.com",
		"ValidationMethod": "DNS",
	})
	body := acmReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "arn:aws:acm:")
	assert.Contains(t, body, "CertificateArn")
}

func TestIntegration_ACM_ListCertificates(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	acmPost(t, "RequestCertificate", map[string]any{
		"DomainName":       "list-acm.example.com",
		"ValidationMethod": "DNS",
	}).Body.Close()

	resp := acmPost(t, "ListCertificates", map[string]any{})
	m := acmReadJSON(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, m, "CertificateSummaryList")
}

func TestIntegration_ACM_DescribeCertificate(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := acmPost(t, "RequestCertificate", map[string]any{
		"DomainName":       "describe-acm.example.com",
		"ValidationMethod": "DNS",
	})
	m := acmReadJSON(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	arn, _ := m["CertificateArn"].(string)
	require.NotEmpty(t, arn, "CertificateArn should be present")

	descResp := acmPost(t, "DescribeCertificate", map[string]any{"CertificateArn": arn})
	descBody := acmReadBody(t, descResp)

	assert.Equal(t, http.StatusOK, descResp.StatusCode, "body: %s", descBody)
	assert.Contains(t, descBody, "describe-acm.example.com")
}

func TestIntegration_ACM_DescribeCertificate_DNSValidation(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := acmPost(t, "RequestCertificate", map[string]any{
		"DomainName":       "dns-validation.example.com",
		"ValidationMethod": "DNS",
	})
	m := acmReadJSON(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	arn, _ := m["CertificateArn"].(string)
	require.NotEmpty(t, arn)

	descResp := acmPost(t, "DescribeCertificate", map[string]any{"CertificateArn": arn})
	descBody := acmReadBody(t, descResp)
	assert.Equal(t, http.StatusOK, descResp.StatusCode, "body: %s", descBody)

	// Should contain DomainValidationOptions with ResourceRecord for DNS validation
	assert.Contains(t, descBody, "DomainValidationOptions")
	// Status may be PENDING_VALIDATION or ISSUED (auto-validated)
	assert.True(t,
		strings.Contains(descBody, "PENDING_VALIDATION") ||
			strings.Contains(descBody, "ISSUED"),
		"expected PENDING_VALIDATION or ISSUED in body: %s", descBody)
}

func TestIntegration_ACM_DeleteCertificate(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := acmPost(t, "RequestCertificate", map[string]any{
		"DomainName":       "delete-acm.example.com",
		"ValidationMethod": "DNS",
	})
	m := acmReadJSON(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	arn, _ := m["CertificateArn"].(string)
	require.NotEmpty(t, arn, "CertificateArn should be present")

	delResp := acmPost(t, "DeleteCertificate", map[string]any{"CertificateArn": arn})
	delBody := acmReadBody(t, delResp)
	assert.Equal(t, http.StatusOK, delResp.StatusCode, "body: %s", delBody)
}

func TestIntegration_ACM_ImportCertificate(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// First request a cert to get a valid PEM body from the mock
	reqResp := acmPost(t, "RequestCertificate", map[string]any{
		"DomainName": "import-source.example.com",
	})
	reqM := acmReadJSON(t, reqResp)
	require.Equal(t, http.StatusOK, reqResp.StatusCode)
	sourceARN, _ := reqM["CertificateArn"].(string)
	require.NotEmpty(t, sourceARN)

	// Get the PEM body of the source cert
	getCertResp := acmPost(t, "GetCertificate", map[string]any{"CertificateArn": sourceARN})
	getCertM := acmReadJSON(t, getCertResp)
	require.Equal(t, http.StatusOK, getCertResp.StatusCode)
	certPEM, _ := getCertM["Certificate"].(string)
	require.NotEmpty(t, certPEM, "Certificate PEM should be present")

	// DescribeCertificate to get the private key from the full cert detail
	// Use ExportCertificate would fail (AMAZON_ISSUED), so import using cert+key from RequestCertificate
	// The backend stores the PrivateKey but the SDK doesn't expose it directly.
	// We'll use the raw HTTP endpoint for ImportCertificate with the source cert fields.
	importResp := acmPost(t, "ImportCertificate", map[string]any{
		"Certificate": certPEM,
		"PrivateKey":  certPEM, // for integration test, provide any non-empty string
	})
	importBody := acmReadBody(t, importResp)

	// Note: The server will try to parse the PrivateKey PEM; using certPEM as key may fail parsing.
	// The mock doesn't validate key format beyond non-empty, so this should succeed.
	assert.Equal(t, http.StatusOK, importResp.StatusCode, "body: %s", importBody)
	assert.Contains(t, importBody, "CertificateArn")
	assert.Contains(t, importBody, "arn:aws:acm:")

	importedARN := struct {
		CertificateArn string `json:"CertificateArn"`
	}{}
	require.NoError(t, json.Unmarshal([]byte(importBody), &importedARN))

	t.Cleanup(func() {
		acmPost(t, "DeleteCertificate", map[string]any{"CertificateArn": importedARN.CertificateArn}).Body.Close()
	})

	// Describe the imported cert
	descImportedResp := acmPost(t, "DescribeCertificate", map[string]any{"CertificateArn": importedARN.CertificateArn})
	descImportedBody := acmReadBody(t, descImportedResp)
	assert.Equal(t, http.StatusOK, descImportedResp.StatusCode, "body: %s", descImportedBody)
	assert.Contains(t, descImportedBody, "IMPORTED")
}

func TestIntegration_ACM_RenewCertificate(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Create a cert
	reqResp := acmPost(t, "RequestCertificate", map[string]any{
		"DomainName": "renew.example.com",
	})
	reqM := acmReadJSON(t, reqResp)
	require.Equal(t, http.StatusOK, reqResp.StatusCode)
	certARN, _ := reqM["CertificateArn"].(string)
	require.NotEmpty(t, certARN)

	// Renew
	renewResp := acmPost(t, "RenewCertificate", map[string]any{"CertificateArn": certARN})
	renewBody := acmReadBody(t, renewResp)
	assert.Equal(t, http.StatusOK, renewResp.StatusCode, "body: %s", renewBody)
}

func TestIntegration_ACM_GetCertificate(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Create a cert
	reqResp := acmPost(t, "RequestCertificate", map[string]any{
		"DomainName": "getcert.example.com",
	})
	reqM := acmReadJSON(t, reqResp)
	require.Equal(t, http.StatusOK, reqResp.StatusCode)
	certARN, _ := reqM["CertificateArn"].(string)
	require.NotEmpty(t, certARN)

	// Get certificate PEM
	getResp := acmPost(t, "GetCertificate", map[string]any{"CertificateArn": certARN})
	getBody := acmReadBody(t, getResp)
	assert.Equal(t, http.StatusOK, getResp.StatusCode, "body: %s", getBody)
	assert.Contains(t, getBody, "Certificate")
	assert.Contains(t, getBody, "BEGIN CERTIFICATE")
}
