package dashboard

import (
	"context"
	"net/http"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"google.golang.org/protobuf/proto"
)

const (
	gcpEventarcProxyPrefix = "/dashboard/api/v1/gcp/eventarc"

	earcLocationSegs  = 4
	earcListSegs      = 5
	earcItemSegs      = 6
	earcLocationsIdx  = 2
	earcKindIdx       = 4
	earcLocationsKind = "locations"
	earcTriggersKind  = "triggers"
	earcChannelsKind  = "channels"
	earcProvidersKind = "providers"
)

// registerGCPEventarcRoutes exposes Eventarc triggers, channels and providers to the SPA.
func (h *DashboardHandler) registerGCPEventarcRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPEventarcOps; ops != nil {
		route = (&gcpEventarcAPI{ea: ops.API()}).route
	}

	h.registerGCPRESTProxy(gcpEventarcProxyPrefix, route)
}

type gcpEventarcAPI struct {
	ea eventarcpb.EventarcServer
}

func (a *gcpEventarcAPI) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)
	if n < earcListSegs || n > earcItemSegs || r.segs[0] != gcpRESTProjectsSeg ||
		r.segs[earcLocationsIdx] != earcLocationsKind || r.verb != "" {
		return nil, errGCPUnsupported()
	}

	if n == earcListSegs {
		return a.list(ctx, r)
	}

	return a.item(ctx, r)
}

func (a *gcpEventarcAPI) list(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	if r.method != http.MethodGet {
		return nil, errGCPUnsupported()
	}

	parent := r.name(earcLocationSegs)
	page, token := r.pageSize(), r.query.Get("pageToken")

	switch r.segs[earcKindIdx] {
	case earcTriggersKind:
		return a.ea.ListTriggers(ctx, &eventarcpb.ListTriggersRequest{
			Parent: parent, PageSize: page, PageToken: token,
		})
	case earcChannelsKind:
		return a.ea.ListChannels(ctx, &eventarcpb.ListChannelsRequest{
			Parent: parent, PageSize: page, PageToken: token,
		})
	case earcProvidersKind:
		return a.ea.ListProviders(ctx, &eventarcpb.ListProvidersRequest{
			Parent: parent, PageSize: page, PageToken: token,
		})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpEventarcAPI) item(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(earcItemSegs)

	switch {
	case r.segs[earcKindIdx] == earcTriggersKind && r.method == http.MethodGet:
		return a.ea.GetTrigger(ctx, &eventarcpb.GetTriggerRequest{Name: name})
	case r.segs[earcKindIdx] == earcTriggersKind && r.method == http.MethodDelete:
		return a.ea.DeleteTrigger(ctx, &eventarcpb.DeleteTriggerRequest{Name: name})
	case r.segs[earcKindIdx] == earcChannelsKind && r.method == http.MethodGet:
		return a.ea.GetChannel(ctx, &eventarcpb.GetChannelRequest{Name: name})
	case r.segs[earcKindIdx] == earcChannelsKind && r.method == http.MethodDelete:
		return a.ea.DeleteChannel(ctx, &eventarcpb.DeleteChannelRequest{Name: name})
	case r.segs[earcKindIdx] == earcProvidersKind && r.method == http.MethodGet:
		return a.ea.GetProvider(ctx, &eventarcpb.GetProviderRequest{Name: name})
	}

	return nil, errGCPUnsupported()
}
