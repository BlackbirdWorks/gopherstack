package dashboard

import (
	"context"
	"net/http"
	"regexp"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	gcpcloudbuildbackend "github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
)

const (
	gcpCloudBuildProxyPrefix = "/dashboard/api/v1/gcp/cloudbuild"

	cbListSegs      = 5
	cbItemSegs      = 6
	cbLogSegs       = 7
	cbLocationsIdx  = 2
	cbKindIdx       = 4
	cbLocationsKind = "locations"
	cbBuildsKind    = "builds"
	cbTriggersKind  = "triggers"
	cbPoolsKind     = "workerPools"
	cbLogLeaf       = "log"
	cbVerbCancel    = "cancel"
	cbVerbRetry     = "retry"
	cbVerbRun       = "run"
	cbVerbApprove   = "approve"

	cbMaxSubs       = 20
	cbMaxSubValue   = 1024
	cbMaxComment    = 1000
	cbMaxRevisionID = 256
)

var cbSubKeyRE = regexp.MustCompile(`^_[A-Z0-9_]{1,64}$`)

// registerGCPCloudBuildRoutes exposes Cloud Build builds, logs and triggers to the SPA.
func (h *DashboardHandler) registerGCPCloudBuildRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPCloudBuildOps; ops != nil {
		route = (&gcpCloudBuildAPI{api: ops.API()}).route
	}

	h.registerGCPRESTProxy(gcpCloudBuildProxyPrefix, route)
}

type gcpCloudBuildAPI struct {
	api *gcpcloudbuildbackend.API
}

// route dispatches an allow-listed request and hides secret material in whatever comes back.
func (a *gcpCloudBuildAPI) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	out, err := a.dispatch(ctx, r)
	if err != nil {
		return nil, err
	}

	if out == nil {
		return nil, nil //nolint:nilnil // void result renders as an empty object
	}

	return cbMaskMessage(out), nil
}

func (a *gcpCloudBuildAPI) dispatch(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)
	if n < cbListSegs || n > cbLogSegs || r.segs[0] != gcpRESTProjectsSeg ||
		r.segs[cbLocationsIdx] != cbLocationsKind {
		return nil, errGCPUnsupported()
	}

	switch r.segs[cbKindIdx] {
	case cbBuildsKind:
		return a.builds(ctx, r)
	case cbTriggersKind:
		if n <= cbItemSegs {
			return a.triggers(ctx, r)
		}
	case cbPoolsKind:
		if n <= cbItemSegs {
			return a.pools(ctx, r)
		}
	}

	return nil, errGCPUnsupported()
}

func (a *gcpCloudBuildAPI) builds(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	switch len(r.segs) {
	case cbListSegs:
		if r.method != http.MethodGet || r.verb != "" {
			return nil, errGCPUnsupported()
		}

		return a.api.ListBuilds(ctx, &cloudbuildpb.ListBuildsRequest{
			Parent: r.name(cbListSegs - 1), PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
			Filter: r.query.Get("filter"),
		})
	case cbItemSegs:
		return a.build(ctx, r)
	}

	return a.buildLog(ctx, r)
}

func (a *gcpCloudBuildAPI) build(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(cbItemSegs)

	switch {
	case r.method == http.MethodGet && r.verb == "":
		return a.api.GetBuild(ctx, &cloudbuildpb.GetBuildRequest{Name: name})
	case r.method == http.MethodPost && r.verb == cbVerbCancel:
		return a.api.CancelBuild(ctx, &cloudbuildpb.CancelBuildRequest{Name: name})
	case r.method == http.MethodPost && r.verb == cbVerbRetry:
		return a.api.RetryBuild(ctx, &cloudbuildpb.RetryBuildRequest{Name: name})
	case r.method == http.MethodPost && r.verb == cbVerbApprove:
		return a.approve(ctx, r, name)
	}

	return nil, errGCPUnsupported()
}

// approve rebuilds the request from the decision and comment alone.
func (a *gcpCloudBuildAPI) approve(ctx context.Context, r gcpRESTCall, name string) (proto.Message, error) {
	var in cloudbuildpb.ApproveBuildRequest
	if err := r.unmarshal(&in); err != nil {
		return nil, err
	}

	decision := in.GetApprovalResult().GetDecision()
	if decision != cloudbuildpb.ApprovalResult_APPROVED && decision != cloudbuildpb.ApprovalResult_REJECTED {
		return nil, status.Error(codes.InvalidArgument, "decision must be APPROVED or REJECTED")
	}

	comment := in.GetApprovalResult().GetComment()
	if len(comment) > cbMaxComment {
		return nil, status.Error(codes.InvalidArgument, "comment is too long")
	}

	return a.api.ApproveBuild(ctx, &cloudbuildpb.ApproveBuildRequest{
		Name:           name,
		ApprovalResult: &cloudbuildpb.ApprovalResult{Decision: decision, Comment: comment},
	})
}

// pools is read-only: worker pools are listed and fetched, never changed from the dashboard.
func (a *gcpCloudBuildAPI) pools(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	if r.method != http.MethodGet || r.verb != "" {
		return nil, errGCPUnsupported()
	}

	if len(r.segs) == cbListSegs {
		return a.api.ListWorkerPools(ctx, &cloudbuildpb.ListWorkerPoolsRequest{
			Parent: r.name(cbListSegs - 1), PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	}

	return a.api.GetWorkerPool(ctx, &cloudbuildpb.GetWorkerPoolRequest{Name: r.name(cbItemSegs)})
}

// runRequest rebuilds the run request from the revision and plain substitutions alone.
func cbRunRequest(r gcpRESTCall, name string) (*cloudbuildpb.RunBuildTriggerRequest, error) {
	var in cloudbuildpb.RunBuildTriggerRequest
	if err := r.unmarshal(&in); err != nil {
		return nil, err
	}

	src := in.GetSource()
	out := &cloudbuildpb.RunBuildTriggerRequest{Name: name}

	if src == nil {
		return out, nil
	}

	if len(src.GetSubstitutions()) > cbMaxSubs {
		return nil, status.Error(codes.InvalidArgument, "too many substitutions")
	}

	subs := map[string]string{}

	for k, v := range src.GetSubstitutions() {
		if !cbSubKeyRE.MatchString(k) || len(v) > cbMaxSubValue {
			return nil, status.Error(codes.InvalidArgument, "invalid substitution "+k)
		}

		subs[k] = v
	}

	rs := &cloudbuildpb.RepoSource{Substitutions: subs}

	switch {
	case src.GetBranchName() != "":
		rs.Revision = &cloudbuildpb.RepoSource_BranchName{BranchName: src.GetBranchName()}
	case src.GetTagName() != "":
		rs.Revision = &cloudbuildpb.RepoSource_TagName{TagName: src.GetTagName()}
	case src.GetCommitSha() != "":
		rs.Revision = &cloudbuildpb.RepoSource_CommitSha{CommitSha: src.GetCommitSha()}
	}

	if len(src.GetBranchName()) > cbMaxRevisionID || len(src.GetTagName()) > cbMaxRevisionID ||
		len(src.GetCommitSha()) > cbMaxRevisionID {
		return nil, status.Error(codes.InvalidArgument, "revision is too long")
	}

	out.Source = rs

	return out, nil
}

func (a *gcpCloudBuildAPI) buildLog(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	if r.method != http.MethodGet || r.verb != "" || r.segs[cbItemSegs] != cbLogLeaf {
		return nil, errGCPUnsupported()
	}

	build, err := a.api.GetBuild(ctx, &cloudbuildpb.GetBuildRequest{Name: r.name(cbItemSegs)})
	if err != nil {
		return nil, err //nolint:wrapcheck // backend status passes through
	}

	data, ok := a.api.BuildLog(build)

	out, sErr := structpb.NewStruct(map[string]any{"available": ok, "log": string(data)})
	if sErr != nil {
		return nil, status.Error(codes.Internal, sErr.Error())
	}

	return out, nil
}

func (a *gcpCloudBuildAPI) triggers(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	if len(r.segs) == cbListSegs {
		if r.method != http.MethodGet || r.verb != "" {
			return nil, errGCPUnsupported()
		}

		return a.api.ListBuildTriggers(ctx, &cloudbuildpb.ListBuildTriggersRequest{
			Parent: r.name(cbListSegs - 1), PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	}

	name := r.name(cbItemSegs)

	switch {
	case r.method == http.MethodGet && r.verb == "":
		return a.api.GetBuildTrigger(ctx, &cloudbuildpb.GetBuildTriggerRequest{Name: name})
	case r.method == http.MethodDelete && r.verb == "":
		return a.api.DeleteBuildTrigger(ctx, &cloudbuildpb.DeleteBuildTriggerRequest{Name: name})
	case r.method == http.MethodPost && r.verb == cbVerbRun:
		req, err := cbRunRequest(r, name)
		if err != nil {
			return nil, err
		}

		return a.api.RunBuildTrigger(ctx, req)
	}

	return nil, errGCPUnsupported()
}
