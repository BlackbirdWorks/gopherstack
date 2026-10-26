package dashboard

import (
	"context"
	"net/http"

	"cloud.google.com/go/workflows/apiv1/workflowspb"
	"cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"google.golang.org/protobuf/proto"
)

const (
	gcpWorkflowsProxyPrefix = "/dashboard/api/v1/gcp/workflows"

	wfListSegs       = 5
	wfItemSegs       = 6
	wfExecListSegs   = 7
	wfExecItemSegs   = 8
	wfLocationsIdx   = 2
	wfKindIdx        = 4
	wfExecKindIdx    = 6
	wfLocationsKind  = "locations"
	wfWorkflowsKind  = "workflows"
	wfExecutionsKind = "executions"
	wfVerbRevisions  = "listRevisions"
	wfVerbCancel     = "cancel"
)

// registerGCPWorkflowsRoutes exposes Workflows and its executions to the SPA, calling the backend in-process.
func (h *DashboardHandler) registerGCPWorkflowsRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPWorkflowsOps; ops != nil {
		route = (&gcpWorkflowsAPI{wf: ops.API(), ex: ops.ExecutionsAPI()}).route
	}

	h.registerGCPRESTProxy(gcpWorkflowsProxyPrefix, route)
}

type gcpWorkflowsAPI struct {
	wf workflowspb.WorkflowsServer
	ex executionspb.ExecutionsServer
}

func (a *gcpWorkflowsAPI) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)
	if n < wfListSegs || n > wfExecItemSegs || r.segs[0] != gcpRESTProjectsSeg ||
		r.segs[wfLocationsIdx] != wfLocationsKind || r.segs[wfKindIdx] != wfWorkflowsKind {
		return nil, errGCPUnsupported()
	}

	if n >= wfExecListSegs && r.segs[wfExecKindIdx] != wfExecutionsKind {
		return nil, errGCPUnsupported()
	}

	switch n {
	case wfListSegs:
		if r.method == http.MethodGet && r.verb == "" {
			return a.wf.ListWorkflows(ctx, &workflowspb.ListWorkflowsRequest{
				Parent: r.name(wfListSegs - 1), PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
			})
		}
	case wfItemSegs:
		return a.workflow(ctx, r)
	case wfExecListSegs:
		return a.executions(ctx, r)
	case wfExecItemSegs:
		return a.execution(ctx, r)
	}

	return nil, errGCPUnsupported()
}

func (a *gcpWorkflowsAPI) workflow(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(wfItemSegs)

	switch {
	case r.method == http.MethodGet && r.verb == "":
		return a.wf.GetWorkflow(ctx, &workflowspb.GetWorkflowRequest{Name: name, RevisionId: r.query.Get("revisionId")})
	case r.method == http.MethodGet && r.verb == wfVerbRevisions:
		return a.wf.ListWorkflowRevisions(ctx, &workflowspb.ListWorkflowRevisionsRequest{
			Name: name, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpWorkflowsAPI) executions(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	parent := r.name(wfExecListSegs - 1)

	switch {
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.ex.ListExecutions(ctx, &executionspb.ListExecutionsRequest{
			Parent: parent, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
			View: executionspb.ExecutionView_FULL, OrderBy: r.query.Get("orderBy"),
		})
	case r.method == http.MethodPost:
		e := &executionspb.Execution{}
		if err := r.unmarshal(e); err != nil {
			return nil, err
		}

		return a.ex.CreateExecution(ctx, &executionspb.CreateExecutionRequest{Parent: parent, Execution: e})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpWorkflowsAPI) execution(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(wfExecItemSegs)

	switch {
	case r.method == http.MethodGet && r.verb == "":
		return a.ex.GetExecution(
			ctx,
			&executionspb.GetExecutionRequest{Name: name, View: executionspb.ExecutionView_FULL},
		)
	case r.method == http.MethodPost && r.verb == wfVerbCancel:
		return a.ex.CancelExecution(ctx, &executionspb.CancelExecutionRequest{Name: name})
	}

	return nil, errGCPUnsupported()
}
