package swf_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandler_GetWorkflowExecutionHistory(t *testing.T) {
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
