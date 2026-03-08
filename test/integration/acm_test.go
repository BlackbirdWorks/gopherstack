package integration_test

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"

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

// acmGenerateSelfSignedCert creates a self-signed ECDSA P-256 cert+key pair for
// the given domain and returns PEM-encoded certificate and private key strings.
func acmGenerateSelfSignedCert(t *testing.T, domainName string) (string, string) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err, "generate ECDSA key")

	serialBytes := make([]byte, 16)
	_, err = rand.Read(serialBytes)
	require.NoError(t, err)

	serial := new(big.Int).SetBytes(serialBytes)
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: domainName},
		DNSNames:     []string{domainName},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	require.NoError(t, err, "create certificate")

	certPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}))

	keyDER, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err, "marshal EC key")

	keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))

	return certPEM, keyPEM
}

func TestIntegration_ACM_ImportCertificate(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Generate a real self-signed cert+key pair to import.
	certPEM, keyPEM := acmGenerateSelfSignedCert(t, "import-test.example.com")

	importResp := acmPost(t, "ImportCertificate", map[string]any{
		"Certificate": certPEM,
		"PrivateKey":  keyPEM,
	})
	importBody := acmReadBody(t, importResp)

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
	assert.Contains(t, descImportedBody, "import-test.example.com")
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
