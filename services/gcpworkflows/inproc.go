package gcpworkflows

import (
	"cloud.google.com/go/workflows/apiv1/workflowspb"
	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"
)

// API exposes the Workflows service for in-process callers.
func (h *Handler) API() workflowspb.WorkflowsServer { return &workflowsServer{b: h.Backend} }

// ExecutionsAPI exposes the Executions service for in-process callers.
func (h *Handler) ExecutionsAPI() executionspb.ExecutionsServer {
	return &executionsServer{b: h.Backend}
}
