package dashboard

import (
	"context"
	"net/http"

	"cloud.google.com/go/datastore/admin/apiv1/adminpb"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/services/gcpdatastore"
)

const (
	gcpDatastoreProxyPrefix = "/dashboard/api/v1/gcp/datastore"

	dsProjectSegs  = 2
	dsIndexesSegs  = 3
	dsIndexItem    = 4
	dsIndexesKind  = "indexes"
	dsVerbQuery    = "runQuery"
	dsVerbLookup   = "lookup"
	dsVerbAggQuery = "runAggregationQuery"
)

// registerGCPDatastoreRoutes exposes the Datastore entity browser to the SPA: read-only queries, lookups
// and the composite-index listing, called in-process against the backend.
func (h *DashboardHandler) registerGCPDatastoreRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPDatastoreOps; ops != nil {
		route = (&gcpDatastoreAPI{b: ops.Backend}).route
	}

	h.registerGCPRESTProxy(gcpDatastoreProxyPrefix, route)
}

type gcpDatastoreAPI struct {
	b *gcpdatastore.InMemoryBackend
}

func (a *gcpDatastoreAPI) route(_ context.Context, r gcpRESTCall) (proto.Message, error) {
	if len(r.segs) < dsProjectSegs || r.segs[0] != gcpRESTProjectsSeg {
		return nil, errGCPUnsupported()
	}

	project := r.segs[1]

	switch len(r.segs) {
	case dsProjectSegs:
		if r.method == http.MethodPost {
			return a.project(project, r)
		}
	case dsIndexesSegs:
		if r.method == http.MethodGet && r.verb == "" && r.segs[2] == dsIndexesKind {
			return a.b.ListIndexes(&adminpb.ListIndexesRequest{
				ProjectId: project, PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
			})
		}
	case dsIndexItem:
		if r.method == http.MethodGet && r.verb == "" && r.segs[2] == dsIndexesKind {
			return a.b.GetIndex(&adminpb.GetIndexRequest{ProjectId: project, IndexId: r.segs[3]})
		}
	}

	return nil, errGCPUnsupported()
}

func (a *gcpDatastoreAPI) project(project string, r gcpRESTCall) (proto.Message, error) {
	switch r.verb {
	case dsVerbQuery:
		in := &datastorepb.RunQueryRequest{}
		if err := r.unmarshal(in); err != nil {
			return nil, err
		}

		in.ProjectId = project

		return a.b.RunQuery(in)
	case dsVerbAggQuery:
		in := &datastorepb.RunAggregationQueryRequest{}
		if err := r.unmarshal(in); err != nil {
			return nil, err
		}

		in.ProjectId = project

		return a.b.RunAggregationQuery(in)
	case dsVerbLookup:
		in := &datastorepb.LookupRequest{}
		if err := r.unmarshal(in); err != nil {
			return nil, err
		}

		in.ProjectId = project

		return a.b.Lookup(in)
	}

	return nil, errGCPUnsupported()
}
