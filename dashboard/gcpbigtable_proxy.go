package dashboard

import (
	"context"
	"encoding/json"
	"net/http"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	gcpbigtablebackend "github.com/blackbirdworks/gopherstack/services/gcpbigtable"
)

const (
	gcpBigtableProxyPrefix = "/dashboard/api/v1/gcp/bigtable"

	btProjectSegs     = 2
	btInstanceSegs    = 4
	btInstanceSub     = 5
	btTableSegs       = 6
	btInstancesIdx    = 2
	btSubIdx          = 4
	btTablesKind      = "tables"
	btClustersKind    = "clusters"
	btProfilesKind    = "appProfiles"
	btInstancesKind   = "instances"
	btVerbReadRows    = "readRows"
	btDefaultRowLimit = 100
	btMaxRowLimit     = 500
)

// registerGCPBigtableRoutes exposes Bigtable instances, clusters, tables and row reads to the SPA.
func (h *DashboardHandler) registerGCPBigtableRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPBigtableOps; ops != nil {
		route = (&gcpBigtableAPI{api: ops.API()}).route
	}

	h.registerGCPRESTProxy(gcpBigtableProxyPrefix, route)
}

type gcpBigtableAPI struct {
	api *gcpbigtablebackend.API
}

func (a *gcpBigtableAPI) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)
	if n < btInstanceSegs-1 || r.segs[0] != gcpRESTProjectsSeg || r.segs[btInstancesIdx] != btInstancesKind {
		return nil, errGCPUnsupported()
	}

	switch n {
	case btInstanceSegs - 1:
		return a.listInstances(ctx, r)
	case btInstanceSegs:
		return a.instance(ctx, r)
	case btInstanceSub:
		return a.instanceChildren(ctx, r)
	case btTableSegs:
		return a.table(ctx, r)
	}

	return nil, errGCPUnsupported()
}

func (a *gcpBigtableAPI) listInstances(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	if r.method != http.MethodGet || r.verb != "" {
		return nil, errGCPUnsupported()
	}

	return a.api.Instances.ListInstances(ctx, &adminpb.ListInstancesRequest{
		Parent: r.name(btProjectSegs), PageToken: r.query.Get("pageToken"),
	})
}

func (a *gcpBigtableAPI) instance(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(btInstanceSegs)

	switch {
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.api.Instances.GetInstance(ctx, &adminpb.GetInstanceRequest{Name: name})
	case r.method == http.MethodDelete:
		return a.api.Instances.DeleteInstance(ctx, &adminpb.DeleteInstanceRequest{Name: name})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpBigtableAPI) instanceChildren(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	parent := r.name(btInstanceSegs)

	if r.method != http.MethodGet || r.verb != "" {
		return nil, errGCPUnsupported()
	}

	switch r.segs[btSubIdx] {
	case btClustersKind:
		return a.api.Instances.ListClusters(ctx, &adminpb.ListClustersRequest{
			Parent: parent, PageToken: r.query.Get("pageToken"),
		})
	case btProfilesKind:
		return a.api.Instances.ListAppProfiles(ctx, &adminpb.ListAppProfilesRequest{
			Parent: parent, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	case btTablesKind:
		return a.api.Tables.ListTables(ctx, &adminpb.ListTablesRequest{
			Parent: parent, View: tableView(r), PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	}

	return nil, errGCPUnsupported()
}

func tableView(r gcpRESTCall) adminpb.Table_View {
	return adminpb.Table_View(adminpb.Table_View_value[r.query.Get("view")])
}

func (a *gcpBigtableAPI) table(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	if r.segs[btSubIdx] != btTablesKind {
		return nil, errGCPUnsupported()
	}

	name := r.name(btTableSegs)

	switch {
	case r.verb == btVerbReadRows && r.method == http.MethodPost:
		return a.readRows(r, name)
	case r.verb != "":
		return nil, errGCPUnsupported()
	case r.method == http.MethodGet:
		return a.api.Tables.GetTable(ctx, &adminpb.GetTableRequest{Name: name, View: tableView(r)})
	case r.method == http.MethodDelete:
		return a.api.Tables.DeleteTable(ctx, &adminpb.DeleteTableRequest{Name: name})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpBigtableAPI) readRows(r gcpRESTCall, table string) (proto.Message, error) {
	req := &btpb.ReadRowsRequest{}
	if err := r.unmarshal(req); err != nil {
		return nil, err
	}

	req.TableName = table
	req.Reversed = false

	if req.GetRowsLimit() <= 0 || req.GetRowsLimit() > btMaxRowLimit {
		req.RowsLimit = btDefaultRowLimit
	}

	rows, err := a.api.ReadRows(req)
	if err != nil {
		return nil, err //nolint:wrapcheck // backend status passes through
	}

	items := make([]any, 0, len(rows))

	for _, row := range rows {
		b, mErr := protojson.Marshal(row)
		if mErr != nil {
			return nil, status.Error(codes.Internal, mErr.Error())
		}

		var v any
		if uErr := json.Unmarshal(b, &v); uErr != nil {
			return nil, status.Error(codes.Internal, uErr.Error())
		}

		items = append(items, v)
	}

	out, err := structpb.NewStruct(map[string]any{"rows": items})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return out, nil
}
