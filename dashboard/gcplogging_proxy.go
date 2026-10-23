package dashboard

import (
	"context"
	"net/http"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/protobuf/proto"

	gcploggingbackend "github.com/blackbirdworks/gopherstack/services/gcplogging"
)

const (
	gcpLoggingProxyPrefix = "/dashboard/api/v1/gcp/logging"
	logSinksSegs          = 3
	logSinksIdx           = 2
	logParentSegs         = 2
	logEntriesSeg         = "entries"
	logVerbList           = "list"
	logVerbWrite          = "write"
)

// registerGCPLoggingRoutes exposes entries.list/entries.write and projects.sinks.list
// as REST JSON, calling the Cloud Logging backend in-process.
func (h *DashboardHandler) registerGCPLoggingRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPLoggingOps; ops != nil {
		route = (&gcpLoggingAPI{b: ops.Backend}).route
	}

	h.registerGCPRESTProxy(gcpLoggingProxyPrefix, route)
}

type gcpLoggingAPI struct {
	b *gcploggingbackend.InMemoryBackend
}

func (a *gcpLoggingAPI) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	switch {
	case len(r.segs) == 1 && r.segs[0] == logEntriesSeg && r.method == http.MethodPost:
		return a.entries(ctx, r)
	case len(r.segs) == logSinksSegs && r.segs[0] == gcpRESTProjectsSeg && r.segs[logSinksIdx] == "sinks" &&
		r.method == http.MethodGet && r.verb == "":
		return a.sinks(r)
	}

	return nil, errGCPUnsupported()
}

func (a *gcpLoggingAPI) entries(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	switch r.verb {
	case logVerbList:
		req := &loggingpb.ListLogEntriesRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		items, next, err := a.b.ListLogEntries(req)
		if err != nil {
			return nil, err
		}

		return &loggingpb.ListLogEntriesResponse{Entries: items, NextPageToken: next}, nil
	case logVerbWrite:
		req := &loggingpb.WriteLogEntriesRequest{}
		if err := r.unmarshal(req); err != nil {
			return nil, err
		}

		return nil, a.b.WriteLogEntries(ctx, req)
	}

	return nil, errGCPUnsupported()
}

func (a *gcpLoggingAPI) sinks(r gcpRESTCall) (proto.Message, error) {
	items, next, err := a.b.ListSinks(&loggingpb.ListSinksRequest{
		Parent: r.name(logParentSegs), PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
	})
	if err != nil {
		return nil, err
	}

	return &loggingpb.ListSinksResponse{Sinks: items, NextPageToken: next}, nil
}
