package swf_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

func createSWFDomain(t *testing.T, h *swf.Handler, name string) {
	t.Helper()

	rec := doSWFRequest(t, h, "RegisterDomain", map[string]any{
		"name":                                   name,
		"workflowExecutionRetentionPeriodInDays": "10",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func createSWFWorkflowType(t *testing.T, h *swf.Handler, domain, name string) {
	t.Helper()

	rec := doSWFRequest(t, h, "RegisterWorkflowType", map[string]any{
		"domain":  domain,
		"name":    name,
		"version": "1.0",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func startSWFExecution(t *testing.T, h *swf.Handler, domain, workflowType, execID string) string {
	t.Helper()

	rec := doSWFRequest(t, h, "StartWorkflowExecution", map[string]any{
		"domain":     domain,
		"workflowId": execID,
		"workflowType": map[string]any{
			"name":    workflowType,
			"version": "1.0",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseSWFResp(t, rec)
	runID, _ := resp["runId"].(string)
	require.NotEmpty(t, runID)

	return runID
}

func parseSWFResp(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

func TestSWF_WorkflowExecutionHistory(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)
	domain := "hist-domain"
	createSWFDomain(t, h, domain)
	createSWFWorkflowType(t, h, domain, "hist-wf")
	runID := startSWFExecution(t, h, domain, "hist-wf", "exec-001")

	rec := doSWFRequest(t, h, "GetWorkflowExecutionHistory", map[string]any{
		"domain": domain,
		"execution": map[string]any{
			"workflowId": "exec-001",
			"runId":      runID,
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestSWF_ListWorkflowExecutions(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)
	domain := "list-exec-domain"
	createSWFDomain(t, h, domain)
	createSWFWorkflowType(t, h, domain, "list-wf")
	startSWFExecution(t, h, domain, "list-wf", "list-exec-001")

	// ListOpenWorkflowExecutions
	rec := doSWFRequest(t, h, "ListOpenWorkflowExecutions", map[string]any{
		"domain": domain,
		"startTimeFilter": map[string]any{
			"oldestDate": 1.0,
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListClosedWorkflowExecutions
	rec = doSWFRequest(t, h, "ListClosedWorkflowExecutions", map[string]any{
		"domain": domain,
		"startTimeFilter": map[string]any{
			"oldestDate": 1.0,
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestSWF_Tags(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)
	domain := "tag-domain"
	createSWFDomain(t, h, domain)

	arn := "arn:aws:swf:us-east-1:123456789012:/domain/" + domain

	// TagResource
	rec := doSWFRequest(t, h, "TagResource", map[string]any{
		"resourceArn": arn,
		"tags": []map[string]any{
			{"key": "env", "value": "test"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListTagsForResource
	rec = doSWFRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": arn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UntagResource
	rec = doSWFRequest(t, h, "UntagResource", map[string]any{
		"resourceArn": arn,
		"tagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
