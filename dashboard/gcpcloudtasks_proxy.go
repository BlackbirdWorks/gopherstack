package dashboard

import (
	"context"
	"net/http"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/protobuf/proto"
)

const (
	gcpCloudTasksProxyPrefix = "/dashboard/api/v1/gcp/cloudtasks"

	ctLocationSegs  = 4
	ctQueuesSegs    = 5
	ctQueueSegs     = 6
	ctTasksSegs     = 7
	ctTaskSegs      = 8
	ctLocationsIdx  = 2
	ctQueuesIdx     = 4
	ctTasksIdx      = 6
	ctLocationsKind = "locations"
	ctQueuesKind    = "queues"
	ctTasksKind     = "tasks"
	ctVerbPause     = "pause"
	ctVerbResume    = "resume"
	ctVerbPurge     = "purge"
	ctVerbRun       = "run"
)

// registerGCPCloudTasksRoutes exposes Cloud Tasks to the SPA as the REST JSON shapes of
// projects.locations.queues(.tasks), calling the backend in-process.
func (h *DashboardHandler) registerGCPCloudTasksRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPCloudTasksOps; ops != nil {
		route = (&gcpCloudTasksAPI{ct: ops.API()}).route
	}

	h.registerGCPRESTProxy(gcpCloudTasksProxyPrefix, route)
}

type gcpCloudTasksAPI struct {
	ct cloudtaskspb.CloudTasksServer
}

func (a *gcpCloudTasksAPI) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)
	if n < ctQueuesSegs || n > ctTaskSegs || r.segs[0] != gcpRESTProjectsSeg ||
		r.segs[ctLocationsIdx] != ctLocationsKind || r.segs[ctQueuesIdx] != ctQueuesKind {
		return nil, errGCPUnsupported()
	}

	if n >= ctTasksSegs && r.segs[ctTasksIdx] != ctTasksKind {
		return nil, errGCPUnsupported()
	}

	switch n {
	case ctQueuesSegs:
		return a.queues(ctx, r)
	case ctQueueSegs:
		return a.queue(ctx, r)
	case ctTasksSegs:
		return a.tasks(ctx, r)
	default:
		return a.task(ctx, r)
	}
}

func (a *gcpCloudTasksAPI) queues(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	parent := r.name(ctLocationSegs)

	switch {
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.ct.ListQueues(ctx, &cloudtaskspb.ListQueuesRequest{
			Parent: parent, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	case r.method == http.MethodPost:
		q := &cloudtaskspb.Queue{}
		if err := r.unmarshal(q); err != nil {
			return nil, err
		}

		return a.ct.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{Parent: parent, Queue: q})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpCloudTasksAPI) queue(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(ctQueueSegs)

	switch {
	case r.verb == ctVerbPause && r.method == http.MethodPost:
		return a.ct.PauseQueue(ctx, &cloudtaskspb.PauseQueueRequest{Name: name})
	case r.verb == ctVerbResume && r.method == http.MethodPost:
		return a.ct.ResumeQueue(ctx, &cloudtaskspb.ResumeQueueRequest{Name: name})
	case r.verb == ctVerbPurge && r.method == http.MethodPost:
		return a.ct.PurgeQueue(ctx, &cloudtaskspb.PurgeQueueRequest{Name: name})
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.ct.GetQueue(ctx, &cloudtaskspb.GetQueueRequest{Name: name})
	case r.method == http.MethodDelete:
		return a.ct.DeleteQueue(ctx, &cloudtaskspb.DeleteQueueRequest{Name: name})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpCloudTasksAPI) tasks(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	parent := r.name(ctQueueSegs)

	switch {
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.ct.ListTasks(ctx, &cloudtaskspb.ListTasksRequest{
			Parent: parent, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
			ResponseView: taskView(r),
		})
	case r.method == http.MethodPost:
		req := &cloudtaskspb.CreateTaskRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Parent = parent
		req.ResponseView = taskView(r)

		return a.ct.CreateTask(ctx, req)
	}

	return nil, errGCPUnsupported()
}

func (a *gcpCloudTasksAPI) task(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(ctTaskSegs)

	switch {
	case r.verb == ctVerbRun && r.method == http.MethodPost:
		return a.ct.RunTask(ctx, &cloudtaskspb.RunTaskRequest{Name: name, ResponseView: taskView(r)})
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.ct.GetTask(ctx, &cloudtaskspb.GetTaskRequest{Name: name, ResponseView: taskView(r)})
	case r.method == http.MethodDelete:
		return a.ct.DeleteTask(ctx, &cloudtaskspb.DeleteTaskRequest{Name: name})
	}

	return nil, errGCPUnsupported()
}

func taskView(r gcpRESTCall) cloudtaskspb.Task_View {
	if r.query.Get("responseView") == cloudtaskspb.Task_FULL.String() {
		return cloudtaskspb.Task_FULL
	}

	return cloudtaskspb.Task_BASIC
}
