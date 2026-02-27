package integration_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const s3controlPublicAccessBlockPath = "/v20180820/configuration/publicAccessBlock"

func s3controlRequest(t *testing.T, method, accountID, body string) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(
		t.Context(),
		method,
		endpoint+s3controlPublicAccessBlockPath,
		strings.NewReader(body),
	)
	require.NoError(t, err)

	if accountID != "" {
		req.Header.Set("X-Amz-Account-Id", accountID)
	}

	if body != "" {
		req.Header.Set("Content-Type", "application/xml")
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func s3controlReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_S3Control_PutAndGetPublicAccessBlock(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	accountID := "s3ctrl-test-put-get"
	putBody := `<PublicAccessBlockConfiguration>
		<BlockPublicAcls>true</BlockPublicAcls>
		<IgnorePublicAcls>true</IgnorePublicAcls>
		<BlockPublicPolicy>false</BlockPublicPolicy>
		<RestrictPublicBuckets>false</RestrictPublicBuckets>
	</PublicAccessBlockConfiguration>`

	putResp := s3controlRequest(t, http.MethodPut, accountID, putBody)
	putRespBody := s3controlReadBody(t, putResp)
	assert.Equal(t, http.StatusCreated, putResp.StatusCode, "body: %s", putRespBody)

	getResp := s3controlRequest(t, http.MethodGet, accountID, "")
	getBody := s3controlReadBody(t, getResp)
	require.Equal(t, http.StatusOK, getResp.StatusCode, "body: %s", getBody)
	assert.Contains(t, getBody, "BlockPublicAcls")
}

func TestIntegration_S3Control_DeletePublicAccessBlock(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	accountID := "s3ctrl-test-delete"
	putBody := `<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls></PublicAccessBlockConfiguration>`
	s3controlRequest(t, http.MethodPut, accountID, putBody)

	delResp := s3controlRequest(t, http.MethodDelete, accountID, "")
	delBody := s3controlReadBody(t, delResp)
	assert.Equal(t, http.StatusNoContent, delResp.StatusCode, "body: %s", delBody)
}

func TestIntegration_S3Control_GetNotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	getResp := s3controlRequest(
		t,
		http.MethodGet,
		"nonexistent-account-99999",
		"",
	)
	getBody := s3controlReadBody(t, getResp)
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode, "body: %s", getBody)
}
