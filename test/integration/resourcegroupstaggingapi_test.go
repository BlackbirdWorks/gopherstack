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

// taggingAPIPost sends a POST to the Resource Groups Tagging API with the given
// X-Amz-Target action and JSON body.
func taggingAPIPost(t *testing.T, action string, body any) *http.Response {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "ResourceGroupsTaggingAPI_20170126."+action)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func taggingAPIBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_TaggingAPI_GetResources_Empty(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := taggingAPIPost(t, "GetResources", map[string]any{})
	body := taggingAPIBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "ResourceTagMappingList")
}

func TestIntegration_TaggingAPI_GetTagKeys(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := taggingAPIPost(t, "GetTagKeys", map[string]any{})
	body := taggingAPIBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "TagKeys")
}

func TestIntegration_TaggingAPI_GetTagValues(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := taggingAPIPost(t, "GetTagValues", map[string]any{"Key": "env"})
	body := taggingAPIBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "TagValues")
}

func TestIntegration_TaggingAPI_TagResources(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// First create an SQS queue to tag.
	sqsResp := taggingAPIPost(t, "GetResources", map[string]any{
		"ResourceTypeFilters": []string{"sqs:queue"},
	})
	defer sqsResp.Body.Close()

	resp := taggingAPIPost(t, "TagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:sqs:us-east-1:000000000000:nonexistent"},
		"Tags":            map[string]string{"test-key": "test-value"},
	})
	body := taggingAPIBody(t, resp)

	// FailedResourcesMap is expected since queue does not exist; the API still returns 200.
	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
}

func TestIntegration_TaggingAPI_UntagResources(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := taggingAPIPost(t, "UntagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:sqs:us-east-1:000000000000:nonexistent"},
		"TagKeys":         []string{"test-key"},
	})
	body := taggingAPIBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
}

func TestIntegration_TaggingAPI_GetResources_ByResourceType(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := taggingAPIPost(t, "GetResources", map[string]any{
		"ResourceTypeFilters": []string{"sqs:queue"},
	})
	body := taggingAPIBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "ResourceTagMappingList")
}

func TestIntegration_TaggingAPI_GetResources_ByTagFilter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := taggingAPIPost(t, "GetResources", map[string]any{
		"TagFilters": []map[string]any{
			{"Key": "env", "Values": []string{"prod"}},
		},
	})
	body := taggingAPIBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "ResourceTagMappingList")
}

func TestIntegration_TaggingAPI_UnknownOperation(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := taggingAPIPost(t, "BogusOperation", map[string]any{})
	body := taggingAPIBody(t, resp)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "body: %s", body)
}
