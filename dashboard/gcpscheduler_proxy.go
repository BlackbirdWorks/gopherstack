package dashboard

import (
	"context"
	"net/http"

	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"google.golang.org/protobuf/proto"
)

const (
	gcpSchedulerProxyPrefix = "/dashboard/api/v1/gcp/scheduler"

	schedLocationSegs  = 4
	schedJobsSegs      = 5
	schedJobSegs       = 6
	schedLocationsIdx  = 2
	schedJobsIdx       = 4
	schedLocationsKind = "locations"
	schedJobsKind      = "jobs"
	schedVerbPause     = "pause"
	schedVerbResume    = "resume"
	schedVerbRun       = "run"
)

// registerGCPSchedulerRoutes exposes Cloud Scheduler jobs to the SPA as the REST JSON shapes of
// projects.locations.jobs, calling the backend in-process.
func (h *DashboardHandler) registerGCPSchedulerRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPSchedulerOps; ops != nil {
		route = (&gcpSchedulerAPI{cs: ops.API()}).route
	}

	h.registerGCPRESTProxy(gcpSchedulerProxyPrefix, route)
}

type gcpSchedulerAPI struct {
	cs schedulerpb.CloudSchedulerServer
}

func (a *gcpSchedulerAPI) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)
	if n < schedJobsSegs || n > schedJobSegs || r.segs[0] != gcpRESTProjectsSeg ||
		r.segs[schedLocationsIdx] != schedLocationsKind || r.segs[schedJobsIdx] != schedJobsKind {
		return nil, errGCPUnsupported()
	}

	if n == schedJobsSegs {
		return a.jobs(ctx, r)
	}

	return a.job(ctx, r)
}

func (a *gcpSchedulerAPI) jobs(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	parent := r.name(schedLocationSegs)

	switch {
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.cs.ListJobs(ctx, &schedulerpb.ListJobsRequest{
			Parent: parent, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	case r.method == http.MethodPost:
		j := &schedulerpb.Job{}
		if err := r.unmarshal(j); err != nil {
			return nil, err
		}

		return a.cs.CreateJob(ctx, &schedulerpb.CreateJobRequest{Parent: parent, Job: j})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpSchedulerAPI) job(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(schedJobSegs)

	switch {
	case r.verb == schedVerbPause && r.method == http.MethodPost:
		return a.cs.PauseJob(ctx, &schedulerpb.PauseJobRequest{Name: name})
	case r.verb == schedVerbResume && r.method == http.MethodPost:
		return a.cs.ResumeJob(ctx, &schedulerpb.ResumeJobRequest{Name: name})
	case r.verb == schedVerbRun && r.method == http.MethodPost:
		return a.cs.RunJob(ctx, &schedulerpb.RunJobRequest{Name: name})
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.cs.GetJob(ctx, &schedulerpb.GetJobRequest{Name: name})
	case r.method == http.MethodDelete:
		return a.cs.DeleteJob(ctx, &schedulerpb.DeleteJobRequest{Name: name})
	case r.method == http.MethodPatch:
		j := &schedulerpb.Job{}
		if err := r.unmarshal(j); err != nil {
			return nil, err
		}

		mask, err := r.mask()
		if err != nil {
			return nil, err
		}

		j.Name = name

		return a.cs.UpdateJob(ctx, &schedulerpb.UpdateJobRequest{Job: j, UpdateMask: mask})
	}

	return nil, errGCPUnsupported()
}
