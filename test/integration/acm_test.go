package integration_test

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func acmPost(t *testing.T, form url.Values) *http.Response {
	t.Helper()

	form.Set("Version", "2015-12-08")

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint,
		strings.NewReader(form.Encode()))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

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

func TestIntegration_ACM_RequestCertificate(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := acmPost(t, url.Values{
		"Action":     {"RequestCertificate"},
		"DomainName": {"integ-acm.example.com"},
	})
	body := acmReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "RequestCertificateResponse")
	assert.Contains(t, body, "arn:aws:acm:")
}

func TestIntegration_ACM_ListCertificates(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	acmPost(t, url.Values{
		"Action":     {"RequestCertificate"},
		"DomainName": {"list-acm.example.com"},
	})

	resp := acmPost(t, url.Values{
		"Action": {"ListCertificates"},
	})
	body := acmReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "ListCertificatesResponse")
}

func TestIntegration_ACM_DescribeCertificate(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := acmPost(t, url.Values{
		"Action":     {"RequestCertificate"},
		"DomainName": {"describe-acm.example.com"},
	})
	body := acmReadBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Extract ARN from response body
	start := strings.Index(body, "<CertificateArn>")
	end := strings.Index(body, "</CertificateArn>")
	require.True(t, start >= 0 && end > start, "could not find CertificateArn in: %s", body)
	arn := body[start+len("<CertificateArn>") : end]

	// Describe the certificate
	descResp := acmPost(t, url.Values{
		"Action":         {"DescribeCertificate"},
		"CertificateArn": {arn},
	})
	descBody := acmReadBody(t, descResp)

	assert.Equal(t, http.StatusOK, descResp.StatusCode, "body: %s", descBody)
	assert.Contains(t, descBody, "DescribeCertificateResponse")
	assert.Contains(t, descBody, "describe-acm.example.com")
}

func TestIntegration_ACM_DeleteCertificate(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	reqResp := acmPost(t, url.Values{
		"Action":     {"RequestCertificate"},
		"DomainName": {"delete-acm.example.com"},
	})
	body := acmReadBody(t, reqResp)
	require.Equal(t, http.StatusOK, reqResp.StatusCode)

	start := strings.Index(body, "<CertificateArn>")
	end := strings.Index(body, "</CertificateArn>")
	require.True(t, start >= 0 && end > start, "could not find CertificateArn in: %s", body)
	arn := body[start+len("<CertificateArn>") : end]

	delResp := acmPost(t, url.Values{
		"Action":         {"DeleteCertificate"},
		"CertificateArn": {arn},
	})
	delBody := acmReadBody(t, delResp)
	assert.Equal(t, http.StatusOK, delResp.StatusCode, "body: %s", delBody)
	assert.Contains(t, delBody, "DeleteCertificateResponse")
}
