package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// doOpenSearchRequest performs a raw HTTP request against the OpenSearch API.
func doOpenSearchRequest(t *testing.T, method, path string, body any) (int, map[string]any) {
	t.Helper()

	url := endpoint + path

	var reqBody io.Reader

	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)

		reqBody = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(t.Context(), method, url, reqBody)
	require.NoError(t, err)

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	var result map[string]any

	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		result = nil
	}

	return resp.StatusCode, result
}

// TestIntegration_OpenSearch_DomainLifecycle tests create, describe, list, and delete.
func TestIntegration_OpenSearch_DomainLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	domainName := "test-domain"
	basePath := "/2021-01-01/opensearch/domain"

	// CreateDomain
	statusCode, body := doOpenSearchRequest(t, http.MethodPost, basePath, map[string]any{
		"DomainName":    domainName,
		"EngineVersion": "OpenSearch_2.11",
	})
	require.Equal(t, http.StatusOK, statusCode)

	status, ok := body["DomainStatus"].(map[string]any)
	require.True(t, ok, "expected DomainStatus key in response")
	assert.Equal(t, domainName, status["DomainName"])
	assert.Equal(t, "OpenSearch_2.11", status["EngineVersion"])
	assert.NotEmpty(t, status["ARN"])

	endpoint, ok := status["Endpoint"].(string)
	require.True(t, ok, "expected Endpoint in DomainStatus")
	assert.NotEmpty(t, endpoint, "Endpoint should be set")

	// DescribeDomain
	descCode, descBody := doOpenSearchRequest(t, http.MethodGet, fmt.Sprintf("%s/%s", basePath, domainName), nil)
	require.Equal(t, http.StatusOK, descCode)

	descStatus, ok := descBody["DomainStatus"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, domainName, descStatus["DomainName"])

	// ListDomainNames
	listCode, listBody := doOpenSearchRequest(t, http.MethodGet, basePath, nil)
	require.Equal(t, http.StatusOK, listCode)

	names, ok := listBody["DomainNames"].([]any)
	require.True(t, ok)

	found := false

	for _, entry := range names {
		e, ok := entry.(map[string]any)
		if !ok {
			continue
		}

		if e["DomainName"] == domainName {
			found = true

			break
		}
	}

	assert.True(t, found, "domain should appear in ListDomainNames")

	// DeleteDomain
	delCode, _ := doOpenSearchRequest(t, http.MethodDelete, fmt.Sprintf("%s/%s", basePath, domainName), nil)
	assert.Equal(t, http.StatusOK, delCode)

	// Confirm deleted
	notFoundCode, _ := doOpenSearchRequest(t, http.MethodGet, fmt.Sprintf("%s/%s", basePath, domainName), nil)
	assert.Equal(t, http.StatusNotFound, notFoundCode)
}
