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

func resourceGroupsPost(t *testing.T, action string, body any) *http.Response {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "ResourceGroups."+action)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func resourceGroupsReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_ResourceGroups_CreateGroup(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := resourceGroupsPost(t, "CreateGroup", map[string]any{
		"Name":        "integ-test-group",
		"Description": "integration test group",
	})
	body := resourceGroupsReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "Group")
}

func TestIntegration_ResourceGroups_ListGroups(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resourceGroupsPost(t, "CreateGroup", map[string]any{"Name": "rg-list-test"})

	resp := resourceGroupsPost(t, "ListGroups", map[string]any{})
	body := resourceGroupsReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "GroupIdentifiers")
}

func TestIntegration_ResourceGroups_GetGroup(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	createResp := resourceGroupsPost(t, "CreateGroup", map[string]any{"Name": "rg-get-test"})
	require.Equal(t, http.StatusOK, createResp.StatusCode)
	createBody := resourceGroupsReadBody(t, createResp)
	assert.Contains(t, createBody, "rg-get-test")

	resp := resourceGroupsPost(t, "GetGroup", map[string]any{"GroupName": "rg-get-test"})
	body := resourceGroupsReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "rg-get-test")
}

func TestIntegration_ResourceGroups_DeleteGroup(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resourceGroupsPost(t, "CreateGroup", map[string]any{"Name": "rg-delete-test"})

	resp := resourceGroupsPost(t, "DeleteGroup", map[string]any{"GroupName": "rg-delete-test"})
	body := resourceGroupsReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
}
