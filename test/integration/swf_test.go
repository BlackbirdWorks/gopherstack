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

func swfPost(t *testing.T, action string, body any) *http.Response {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "SimpleWorkflowService."+action)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func swfReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_SWF_RegisterDomain(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := swfPost(t, "RegisterDomain", map[string]any{
		"name":        "integ-swf-domain",
		"description": "integration test domain",
	})
	body := swfReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
}

func TestIntegration_SWF_ListDomains(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	swfPost(t, "RegisterDomain", map[string]any{"name": "swf-list-test"})

	resp := swfPost(t, "ListDomains", map[string]any{"registrationStatus": "REGISTERED"})
	body := swfReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "domainInfos")
}

func TestIntegration_SWF_RegisterWorkflowType(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	swfPost(t, "RegisterDomain", map[string]any{"name": "swf-wf-domain"})

	resp := swfPost(t, "RegisterWorkflowType", map[string]any{
		"domain":  "swf-wf-domain",
		"name":    "my-workflow",
		"version": "1.0",
	})
	body := swfReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
}

func TestIntegration_SWF_StartWorkflowExecution(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	swfPost(t, "RegisterDomain", map[string]any{"name": "swf-exec-domain"})

	resp := swfPost(t, "StartWorkflowExecution", map[string]any{
		"domain":     "swf-exec-domain",
		"workflowId": "exec-001",
	})
	body := swfReadBody(t, resp)

	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "runId")
}

func TestIntegration_SWF_DescribeWorkflowExecution(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	swfPost(t, "RegisterDomain", map[string]any{"name": "swf-desc-domain"})
	swfPost(t, "StartWorkflowExecution", map[string]any{"domain": "swf-desc-domain", "workflowId": "exec-desc-001"})

	resp := swfPost(t, "DescribeWorkflowExecution", map[string]any{
		"domain": "swf-desc-domain",
		"execution": map[string]any{"workflowId": "exec-desc-001"},
	})
	body := swfReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "executionInfo")
}
