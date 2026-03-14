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

// doElasticsearchRequest performs a raw HTTP request against the Elasticsearch API.
func doElasticsearchRequest(t *testing.T, method, path string, body any) (int, map[string]any) {
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

// TestIntegration_Elasticsearch_Domain_CRUD tests create, describe, list, update, and delete.
func TestIntegration_Elasticsearch_Domain_CRUD(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	domainName := "es-crud-domain"
	basePath := "/2015-01-01/es/domain"

	// CreateDomain
	statusCode, body := doElasticsearchRequest(t, http.MethodPost, basePath, map[string]any{
		"DomainName":           domainName,
		"ElasticsearchVersion": "7.10",
		"ClusterConfig": map[string]any{
			"InstanceType":  "t3.small.elasticsearch",
			"InstanceCount": 1,
		},
		"EBSOptions": map[string]any{
			"EBSEnabled": true,
			"VolumeSize": 10,
			"VolumeType": "gp2",
		},
	})
	require.Equal(t, http.StatusOK, statusCode)

	status, ok := body["DomainStatus"].(map[string]any)
	require.True(t, ok, "expected DomainStatus key in response")
	assert.Equal(t, domainName, status["DomainName"])
	assert.Equal(t, "7.10", status["ElasticsearchVersion"])
	assert.NotEmpty(t, status["ARN"])

	domainEndpoint, ok := status["Endpoint"].(string)
	require.True(t, ok, "expected Endpoint in DomainStatus")
	assert.NotEmpty(t, domainEndpoint, "Endpoint should be set")

	// DescribeDomain
	descCode, descBody := doElasticsearchRequest(t, http.MethodGet, fmt.Sprintf("%s/%s", basePath, domainName), nil)
	require.Equal(t, http.StatusOK, descCode)

	descStatus, ok := descBody["DomainStatus"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, domainName, descStatus["DomainName"])

	// ListDomainNames
	listCode, listBody := doElasticsearchRequest(t, http.MethodGet, basePath, nil)
	require.Equal(t, http.StatusOK, listCode)

	names, ok := listBody["DomainNames"].([]any)
	require.True(t, ok)

	found := false

	for _, entry := range names {
		e, eok := entry.(map[string]any)
		if !eok {
			continue
		}

		if e["DomainName"] == domainName {
			found = true

			break
		}
	}

	assert.True(t, found, "domain should appear in ListDomainNames")

	// UpdateDomainConfig
	updateCode, updateBody := doElasticsearchRequest(
		t,
		http.MethodPost,
		fmt.Sprintf("%s/%s/config", basePath, domainName),
		map[string]any{
			"ClusterConfig": map[string]any{
				"InstanceType":  "r5.large.elasticsearch",
				"InstanceCount": 2,
			},
		},
	)
	require.Equal(t, http.StatusOK, updateCode)
	assert.NotNil(t, updateBody["DomainConfig"])

	// DeleteDomain
	delCode, _ := doElasticsearchRequest(t, http.MethodDelete, fmt.Sprintf("%s/%s", basePath, domainName), nil)
	assert.Equal(t, http.StatusOK, delCode)

	// Confirm deleted
	notFoundCode, _ := doElasticsearchRequest(t, http.MethodGet, fmt.Sprintf("%s/%s", basePath, domainName), nil)
	assert.Equal(t, http.StatusNotFound, notFoundCode)
}

// TestIntegration_Elasticsearch_Domain_Tags tests adding, listing, and removing tags.
func TestIntegration_Elasticsearch_Domain_Tags(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	domainName := "es-tags-domain"
	basePath := "/2015-01-01/es/domain"

	// CreateDomain
	createCode, createBody := doElasticsearchRequest(t, http.MethodPost, basePath, map[string]any{
		"DomainName": domainName,
	})
	require.Equal(t, http.StatusOK, createCode)

	status, ok := createBody["DomainStatus"].(map[string]any)
	require.True(t, ok)

	domainARN, ok := status["ARN"].(string)
	require.True(t, ok)
	require.NotEmpty(t, domainARN)

	// AddTags
	addCode, _ := doElasticsearchRequest(t, http.MethodPost, "/2015-01-01/tags", map[string]any{
		"ARN": domainARN,
		"TagList": []map[string]string{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "platform"},
		},
	})
	assert.Equal(t, http.StatusOK, addCode)

	// ListTags
	listCode, listBody := doElasticsearchRequest(t, http.MethodGet, "/2015-01-01/tags?arn="+domainARN, nil)
	require.Equal(t, http.StatusOK, listCode)

	tagList, ok := listBody["TagList"].([]any)
	require.True(t, ok)
	assert.Len(t, tagList, 2)

	// RemoveTags
	removeCode, _ := doElasticsearchRequest(t, http.MethodPost, "/2015-01-01/tags-removal", map[string]any{
		"ARN":     domainARN,
		"TagKeys": []string{"env"},
	})
	assert.Equal(t, http.StatusOK, removeCode)

	// Verify tag removed
	listCode2, listBody2 := doElasticsearchRequest(t, http.MethodGet, "/2015-01-01/tags?arn="+domainARN, nil)
	require.Equal(t, http.StatusOK, listCode2)

	tagList2, ok := listBody2["TagList"].([]any)
	require.True(t, ok)
	assert.Len(t, tagList2, 1)

	// Cleanup
	delCode, _ := doElasticsearchRequest(t, http.MethodDelete, fmt.Sprintf("%s/%s", basePath, domainName), nil)
	assert.Equal(t, http.StatusOK, delCode)
}

// TestIntegration_Elasticsearch_Domain_NotFound tests describe of a non-existent domain.
func TestIntegration_Elasticsearch_Domain_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	code, _ := doElasticsearchRequest(t, http.MethodGet, "/2015-01-01/es/domain/nonexistent-domain", nil)
	assert.Equal(t, http.StatusNotFound, code)
}

// TestIntegration_Elasticsearch_Domain_Duplicate tests creating a domain that already exists.
func TestIntegration_Elasticsearch_Domain_Duplicate(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	domainName := "es-dup-domain"
	basePath := "/2015-01-01/es/domain"

	// First create succeeds
	code1, _ := doElasticsearchRequest(t, http.MethodPost, basePath, map[string]any{
		"DomainName": domainName,
	})
	require.Equal(t, http.StatusOK, code1)

	// Second create returns conflict
	code2, _ := doElasticsearchRequest(t, http.MethodPost, basePath, map[string]any{
		"DomainName": domainName,
	})
	assert.Equal(t, http.StatusConflict, code2)

	// Cleanup
	delCode, _ := doElasticsearchRequest(t, http.MethodDelete, fmt.Sprintf("%s/%s", basePath, domainName), nil)
	assert.Equal(t, http.StatusOK, delCode)
}

// TestIntegration_Elasticsearch_DescribeElasticsearchDomains tests the bulk describe endpoint.
func TestIntegration_Elasticsearch_DescribeElasticsearchDomains(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	basePath := "/2015-01-01/es/domain"
	domains := []string{"es-bulk-a", "es-bulk-b"}

	for _, name := range domains {
		code, _ := doElasticsearchRequest(t, http.MethodPost, basePath, map[string]any{
			"DomainName": name,
		})
		require.Equal(t, http.StatusOK, code)
	}

	code, body := doElasticsearchRequest(t, http.MethodPost, "/2015-01-01/es/domain-info", map[string]any{
		"DomainNames": domains,
	})
	require.Equal(t, http.StatusOK, code)

	list, ok := body["DomainStatusList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 2)

	for _, name := range domains {
		delCode, _ := doElasticsearchRequest(t, http.MethodDelete, fmt.Sprintf("%s/%s", basePath, name), nil)
		assert.Equal(t, http.StatusOK, delCode)
	}
}
