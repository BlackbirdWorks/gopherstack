package integration_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
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
