package dashboard

import (
	"context"
	"net/http"

	"cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/services/gcprun"
)

const (
	gcpRunProxyPrefix = "/dashboard/api/v1/gcp/run"

	runKindIdx      = 4
	runChildIdx     = 6
	runCollSegs     = 5
	runItemSegs     = 6
	runChildSegs    = 7
	runChildItem    = 8
	runGrandChild   = 9
	runKindServices = "services"
	runKindJobs     = "jobs"
	runRevisions    = "revisions"
	runExecutions   = "executions"
	runTasks        = "tasks"
	runVerbCancel   = "cancel"
	runVerbRun      = "run"
)

// registerGCPRunRoutes exposes the Cloud Run Admin API to the SPA as REST JSON, calling the backend in-process.
func (h *DashboardHandler) registerGCPRunRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPRunOps; ops != nil {
		route = (&gcpRunAPI{api: ops.API()}).route
	}

	h.registerGCPRESTProxy(gcpRunProxyPrefix, route)
}

type gcpRunAPI struct {
	api *gcprun.API
}

func (a *gcpRunAPI) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)
	if n < runCollSegs || n > runGrandChild || r.segs[0] != gcpRESTProjectsSeg ||
		r.segs[schedLocationsIdx] != schedLocationsKind {
		return nil, errGCPUnsupported()
	}

	switch r.segs[runKindIdx] {
	case runKindServices:
		return a.services(ctx, r)
	case runKindJobs:
		return a.jobs(ctx, r)
	}

	return nil, errGCPUnsupported()
}

func (a *gcpRunAPI) services(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	switch len(r.segs) {
	case runCollSegs:
		return a.serviceColl(ctx, r)
	case runItemSegs:
		return a.service(ctx, r)
	case runChildSegs:
		if r.segs[runChildIdx] == runRevisions && r.method == http.MethodGet && r.verb == "" {
			return a.api.Revisions.ListRevisions(ctx, &runpb.ListRevisionsRequest{
				Parent: r.name(runItemSegs), PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
			})
		}
	case runChildItem:
		if r.segs[runChildIdx] == runRevisions && r.method == http.MethodDelete && r.verb == "" {
			return a.api.Revisions.DeleteRevision(ctx, &runpb.DeleteRevisionRequest{Name: r.name(runChildItem)})
		}
	}

	return nil, errGCPUnsupported()
}

func (a *gcpRunAPI) serviceColl(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	parent := r.name(runKindIdx)

	switch {
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.api.Services.ListServices(ctx, &runpb.ListServicesRequest{
			Parent: parent, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	case r.method == http.MethodPost:
		s := &runpb.Service{}
		if err := r.unmarshal(s); err != nil {
			return nil, err
		}

		return a.api.Services.CreateService(ctx, &runpb.CreateServiceRequest{
			Parent: parent, ServiceId: r.query.Get("serviceId"), Service: s,
		})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpRunAPI) service(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(runItemSegs)

	switch {
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.api.Services.GetService(ctx, &runpb.GetServiceRequest{Name: name})
	case r.method == http.MethodDelete:
		return a.api.Services.DeleteService(ctx, &runpb.DeleteServiceRequest{Name: name})
	case r.method == http.MethodPatch:
		s := &runpb.Service{}
		if err := r.unmarshal(s); err != nil {
			return nil, err
		}

		mask, err := r.mask()
		if err != nil {
			return nil, err
		}

		s.Name = name

		return a.api.Services.UpdateService(ctx, &runpb.UpdateServiceRequest{Service: s, UpdateMask: mask})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpRunAPI) jobs(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	switch len(r.segs) {
	case runCollSegs:
		return a.jobColl(ctx, r)
	case runItemSegs:
		return a.job(ctx, r)
	case runChildSegs:
		if r.segs[runChildIdx] == runExecutions && r.method == http.MethodGet && r.verb == "" {
			return a.api.Executions.ListExecutions(ctx, &runpb.ListExecutionsRequest{
				Parent: r.name(runItemSegs), PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
			})
		}
	case runChildItem:
		if r.segs[runChildIdx] == runExecutions {
			return a.execution(ctx, r)
		}
	case runGrandChild:
		if r.segs[runChildIdx] == runExecutions && r.segs[runChildItem] == runTasks &&
			r.method == http.MethodGet && r.verb == "" {
			return a.api.Tasks.ListTasks(ctx, &runpb.ListTasksRequest{
				Parent: r.name(runChildItem), PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
			})
		}
	}

	return nil, errGCPUnsupported()
}

func (a *gcpRunAPI) jobColl(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	parent := r.name(runKindIdx)

	switch {
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.api.Jobs.ListJobs(ctx, &runpb.ListJobsRequest{
			Parent: parent, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	case r.method == http.MethodPost:
		j := &runpb.Job{}
		if err := r.unmarshal(j); err != nil {
			return nil, err
		}

		return a.api.Jobs.CreateJob(ctx, &runpb.CreateJobRequest{
			Parent: parent, JobId: r.query.Get("jobId"), Job: j,
		})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpRunAPI) job(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(runItemSegs)

	switch {
	case r.verb == runVerbRun && r.method == http.MethodPost:
		return a.api.Jobs.RunJob(ctx, &runpb.RunJobRequest{Name: name})
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.api.Jobs.GetJob(ctx, &runpb.GetJobRequest{Name: name})
	case r.method == http.MethodDelete:
		return a.api.Jobs.DeleteJob(ctx, &runpb.DeleteJobRequest{Name: name})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpRunAPI) execution(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(runChildItem)

	switch {
	case r.verb == runVerbCancel && r.method == http.MethodPost:
		return a.api.Executions.CancelExecution(ctx, &runpb.CancelExecutionRequest{Name: name})
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.api.Executions.GetExecution(ctx, &runpb.GetExecutionRequest{Name: name})
	case r.method == http.MethodDelete:
		return a.api.Executions.DeleteExecution(ctx, &runpb.DeleteExecutionRequest{Name: name})
	}

	return nil, errGCPUnsupported()
}
