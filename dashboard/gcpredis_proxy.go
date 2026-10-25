package dashboard

import (
	"context"
	"net/http"

	"cloud.google.com/go/redis/apiv1/redispb"
	"google.golang.org/protobuf/proto"
)

const (
	gcpRedisProxyPrefix = "/dashboard/api/v1/gcp/redis"

	rdListSegs      = 5
	rdItemSegs      = 6
	rdAuthSegs      = 7
	rdLocationsIdx  = 2
	rdKindIdx       = 4
	rdLocationsKind = "locations"
	rdInstancesKind = "instances"
	rdAuthLeaf      = "authString"
)

// registerGCPRedisRoutes exposes Memorystore for Redis instances to the SPA.
func (h *DashboardHandler) registerGCPRedisRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPRedisOps; ops != nil {
		route = (&gcpRedisAPI{api: ops.API()}).route
	}

	h.registerGCPRESTProxy(gcpRedisProxyPrefix, route)
}

type gcpRedisAPI struct {
	api redispb.CloudRedisServer
}

func (a *gcpRedisAPI) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)
	if n < rdListSegs || n > rdAuthSegs || r.segs[0] != gcpRESTProjectsSeg ||
		r.segs[rdLocationsIdx] != rdLocationsKind || r.segs[rdKindIdx] != rdInstancesKind || r.verb != "" {
		return nil, errGCPUnsupported()
	}

	switch n {
	case rdListSegs:
		if r.method == http.MethodGet {
			return a.api.ListInstances(ctx, &redispb.ListInstancesRequest{
				Parent: r.name(rdListSegs - 1), PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
			})
		}
	case rdItemSegs:
		switch r.method {
		case http.MethodGet:
			return a.api.GetInstance(ctx, &redispb.GetInstanceRequest{Name: r.name(rdItemSegs)})
		case http.MethodDelete:
			return a.api.DeleteInstance(ctx, &redispb.DeleteInstanceRequest{Name: r.name(rdItemSegs)})
		}
	default:
		if r.method == http.MethodGet && r.segs[rdItemSegs] == rdAuthLeaf {
			return a.api.GetInstanceAuthString(ctx, &redispb.GetInstanceAuthStringRequest{Name: r.name(rdItemSegs)})
		}
	}

	return nil, errGCPUnsupported()
}
