package dashboard

import (
	"context"
	"net/http"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/protobuf/proto"

	gcpspannerbackend "github.com/blackbirdworks/gopherstack/services/gcpspanner"
)

const (
	gcpSpannerProxyPrefix = "/dashboard/api/v1/gcp/spanner"

	spProjectSegs   = 2
	spInstancesSegs = 3
	spInstanceSegs  = 4
	spChildSegs     = 5
	spDBSegs        = 6
	spDDLSegs       = 7
	spKindIdx       = 2
	spChildIdx      = 4
	spDBChildIdx    = 6
	spInstancesKind = "instances"
	spConfigsKind   = "instanceConfigs"
	spDatabasesKind = "databases"
	spDDLKind       = "ddl"
)

// registerGCPSpannerRoutes exposes Spanner instances and databases (admin plane only) to the SPA.
func (h *DashboardHandler) registerGCPSpannerRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPSpannerOps; ops != nil {
		route = (&gcpSpannerAPI{api: ops.API()}).route
	}

	h.registerGCPRESTProxy(gcpSpannerProxyPrefix, route)
}

type gcpSpannerAPI struct {
	api *gcpspannerbackend.API
}

func (a *gcpSpannerAPI) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)
	if n < spInstancesSegs || r.segs[0] != gcpRESTProjectsSeg || r.verb != "" {
		return nil, errGCPUnsupported()
	}

	token := r.query.Get("pageToken")
	parent := r.name(spProjectSegs)

	switch {
	case n == spInstancesSegs && r.method == http.MethodGet && r.segs[spKindIdx] == spInstancesKind:
		return a.api.Instances.ListInstances(ctx, &instancepb.ListInstancesRequest{
			Parent: parent, PageSize: r.pageSize(), PageToken: token,
		})
	case n == spInstancesSegs && r.method == http.MethodGet && r.segs[spKindIdx] == spConfigsKind:
		return a.api.Instances.ListInstanceConfigs(ctx, &instancepb.ListInstanceConfigsRequest{
			Parent: parent, PageSize: r.pageSize(), PageToken: token,
		})
	case n == spInstancesSegs || r.segs[spKindIdx] != spInstancesKind:
		return nil, errGCPUnsupported()
	}

	return a.instance(ctx, r)
}

func (a *gcpSpannerAPI) instance(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	inst := r.name(spInstanceSegs)

	switch len(r.segs) {
	case spInstanceSegs:
		switch r.method {
		case http.MethodGet:
			return a.api.Instances.GetInstance(ctx, &instancepb.GetInstanceRequest{Name: inst})
		case http.MethodDelete:
			return a.api.Instances.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{Name: inst})
		}
	case spChildSegs:
		if r.method == http.MethodGet && r.segs[spChildIdx] == spDatabasesKind {
			return a.api.Databases.ListDatabases(ctx, &databasepb.ListDatabasesRequest{
				Parent: inst, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
			})
		}
	case spDBSegs:
		return a.database(ctx, r)
	case spDDLSegs:
		if r.method == http.MethodGet && r.segs[spChildIdx] == spDatabasesKind && r.segs[spDBChildIdx] == spDDLKind {
			return a.api.Databases.GetDatabaseDdl(ctx, &databasepb.GetDatabaseDdlRequest{Database: r.name(spDBSegs)})
		}
	}

	return nil, errGCPUnsupported()
}

func (a *gcpSpannerAPI) database(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	if r.segs[spChildIdx] != spDatabasesKind {
		return nil, errGCPUnsupported()
	}

	name := r.name(spDBSegs)

	switch r.method {
	case http.MethodGet:
		return a.api.Databases.GetDatabase(ctx, &databasepb.GetDatabaseRequest{Name: name})
	case http.MethodDelete:
		return a.api.Databases.DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: name})
	}

	return nil, errGCPUnsupported()
}
