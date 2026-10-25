package dashboard

import (
	"context"
	"net/http"

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
	cbLogLeaf       = "log"
	cbVerbCancel    = "cancel"
	cbVerbRetry     = "retry"
	cbVerbRun       = "run"
)

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

func (a *gcpCloudBuildAPI) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
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
	}

	return nil, errGCPUnsupported()
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
		req := &cloudbuildpb.RunBuildTriggerRequest{Name: name}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		req.Name = name

		return a.api.RunBuildTrigger(ctx, req)
	}

	return nil, errGCPUnsupported()
}
