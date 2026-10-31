package dashboard

import (
	"context"
	"net/http"
	"regexp"
	"strings"

	"cloud.google.com/go/batch/apiv1/batchpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/services/gcpbatch"
)

const (
	gcpBatchPrefix     = "/dashboard/api/v1/gcp/batch"
	gcpBatchListSegs   = 5
	gcpBatchItemSegs   = 6
	gcpBatchTasksSegs  = 9
	gcpBatchLocIdx     = 3
	gcpBatchKindIdx    = 4
	gcpBatchGroupIdx   = 7
	gcpBatchMaxCmds    = 32
	gcpBatchMaxCmdLen  = 2048
	gcpBatchMaxVars    = 32
	gcpBatchMaxVarLen  = 4096
	gcpBatchMaxTasks   = 1000
	gcpBatchGroup      = "group0"
	gcpBatchOrder      = "create_time desc"
	gcpBatchVerbCancel = "cancel"
)

var gcpBatchIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// registerGCPBatchRoutes serves the Cloud Batch page by calling the control plane in-process. Reads cover jobs and the
// tasks of the single task group; writes are creating a job (body rebuilt from the image, commands, task count,
// parallelism, labels and plain variables only), cancelling and deleting one. Secret variables are never forwarded.
func (h *DashboardHandler) registerGCPBatchRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPBatchOps; ops != nil {
		route = (&gcpBatchProxy{api: ops.API(), be: ops.Backend}).route
	}

	h.registerGCPRESTProxy(gcpBatchPrefix, route)
}

type gcpBatchProxy struct {
	api batchpb.BatchServiceServer
	be  *gcpbatch.InMemoryBackend
}

func gcpBatchIDs(ids ...string) bool {
	for _, id := range ids {
		if id != "-" && !gcpBatchIDRE.MatchString(id) {
			return false
		}
	}

	return true
}

func (p *gcpBatchProxy) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	if len(r.segs) == 1 && r.segs[0] == gcpRESTProjectsSeg && r.method == http.MethodGet && r.verb == "" {
		return structOf(map[string]any{gcpRESTProjectsSeg: p.be.ProjectIDs()})
	}

	n := len(r.segs)
	if n < gcpBatchListSegs || n > gcpBatchTasksSegs || !gcpBatchScoped(r.segs) {
		return nil, errGCPUnsupported()
	}

	switch {
	case n == gcpBatchListSegs:
		return p.collection(ctx, r)
	case !gcpBatchIDs(r.segs[gcpBatchListSegs]):
		return nil, errGCPUnsupported()
	case n == gcpBatchItemSegs:
		return p.job(ctx, r)
	case gcpBatchTasksPath(r):
		return p.api.ListTasks(ctx, &batchpb.ListTasksRequest{
			Parent: r.name(gcpBatchGroupIdx + 1), PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	}

	return nil, errGCPUnsupported()
}

func (p *gcpBatchProxy) collection(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	parent := r.name(gcpBatchKindIdx)

	switch {
	case r.verb == "" && r.method == http.MethodGet:
		return p.api.ListJobs(ctx, &batchpb.ListJobsRequest{
			Parent: parent, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"), OrderBy: gcpBatchOrder,
		})
	case r.verb == "" && r.method == http.MethodPost:
		return p.createJob(ctx, r, parent)
	}

	return nil, errGCPUnsupported()
}

func gcpBatchScoped(segs []string) bool {
	return segs[0] == gcpRESTProjectsSeg && segs[2] == cbLocationsKind && segs[gcpBatchKindIdx] == "jobs" &&
		gcpBatchIDs(segs[1], segs[gcpBatchLocIdx])
}

func gcpBatchTasksPath(r gcpRESTCall) bool {
	n := len(r.segs)

	return n == gcpBatchTasksSegs && r.verb == "" && r.method == http.MethodGet &&
		r.segs[gcpBatchGroupIdx-1] == "taskGroups" && r.segs[gcpBatchGroupIdx] == gcpBatchGroup &&
		r.segs[n-1] == "tasks"
}

func (p *gcpBatchProxy) job(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(gcpBatchItemSegs)

	switch {
	case r.verb == "" && r.method == http.MethodGet:
		return p.api.GetJob(ctx, &batchpb.GetJobRequest{Name: name})
	case r.verb == "" && r.method == http.MethodDelete:
		return p.api.DeleteJob(ctx, &batchpb.DeleteJobRequest{Name: name})
	case r.verb == gcpBatchVerbCancel && r.method == http.MethodPost:
		return p.api.CancelJob(ctx, &batchpb.CancelJobRequest{Name: name})
	}

	return nil, errGCPUnsupported()
}

func gcpBatchBad(msg string) error { return status.Error(codes.InvalidArgument, msg) }

// createJob forwards one container runnable with plain variables; every other field of the caller's body is dropped.
func (p *gcpBatchProxy) createJob(ctx context.Context, r gcpRESTCall, parent string) (proto.Message, error) {
	in := &batchpb.Job{}
	if err := r.unmarshal(in); err != nil {
		return nil, err
	}

	id := r.query.Get("jobId")
	if id == "" || !gcpBatchIDs(id) {
		return nil, gcpBatchBad("jobId is required")
	}

	g, box, err := gcpBatchSingle(in)
	if err != nil {
		return nil, err
	}

	vars := g.GetTaskSpec().GetEnvironment().GetVariables()
	if err = gcpBatchLimits(g, box, vars); err != nil {
		return nil, err
	}

	job := gcpBatchRebuild(in.GetLabels(), g, box, vars)

	return p.api.CreateJob(ctx, &batchpb.CreateJobRequest{Parent: parent, JobId: id, Job: job})
}

func gcpBatchRebuild(
	labels map[string]string, g *batchpb.TaskGroup, box *batchpb.Runnable_Container, vars map[string]string,
) *batchpb.Job {
	run := &batchpb.Runnable_Container{ImageUri: box.GetImageUri(), Commands: box.GetCommands()}

	return &batchpb.Job{
		Labels: labels,
		TaskGroups: []*batchpb.TaskGroup{{
			TaskCount: g.GetTaskCount(), Parallelism: g.GetParallelism(),
			TaskSpec: &batchpb.TaskSpec{
				Runnables:   []*batchpb.Runnable{{Executable: &batchpb.Runnable_Container_{Container: run}}},
				Environment: &batchpb.Environment{Variables: vars},
			},
		}},
		LogsPolicy: &batchpb.LogsPolicy{Destination: batchpb.LogsPolicy_CLOUD_LOGGING},
	}
}

func gcpBatchSingle(in *batchpb.Job) (*batchpb.TaskGroup, *batchpb.Runnable_Container, error) {
	if len(in.GetTaskGroups()) != 1 {
		return nil, nil, gcpBatchBad("exactly one task group is required")
	}

	g := in.GetTaskGroups()[0]
	rs := g.GetTaskSpec().GetRunnables()

	if len(rs) != 1 || rs[0].GetContainer() == nil {
		return nil, nil, gcpBatchBad("exactly one container runnable is required")
	}

	return g, rs[0].GetContainer(), nil
}

func gcpBatchLimits(g *batchpb.TaskGroup, box *batchpb.Runnable_Container, vars map[string]string) error {
	switch {
	case box.GetImageUri() == "" || len(box.GetImageUri()) > gcpBatchMaxCmdLen,
		len(box.GetCommands()) > gcpBatchMaxCmds, len(vars) > gcpBatchMaxVars,
		g.GetTaskCount() < 0, g.GetTaskCount() > gcpBatchMaxTasks, g.GetParallelism() < 0:
		return gcpBatchBad("image, commands, variables, taskCount or parallelism out of range")
	case tooLong(box.GetCommands(), gcpBatchMaxCmdLen), tooLongVars(vars):
		return gcpBatchBad("command or variable too long")
	}

	return nil
}

func tooLong(ss []string, limit int) bool {
	for _, s := range ss {
		if len(s) > limit {
			return true
		}
	}

	return false
}

func tooLongVars(vars map[string]string) bool {
	for k, v := range vars {
		if len(k) > gcpBatchMaxCmdLen || len(v) > gcpBatchMaxVarLen || strings.ContainsRune(k, 0) {
			return true
		}
	}

	return false
}
