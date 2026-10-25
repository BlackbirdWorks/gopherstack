package dashboard

import (
	"context"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	gcpmonitoringbackend "github.com/blackbirdworks/gopherstack/services/gcpmonitoring"
)

const (
	gcpMonitoringProxyPrefix = "/dashboard/api/v1/gcp/monitoring"

	mnListSegs         = 3
	mnItemSegs         = 4
	mnKindIdx          = 2
	mnDescriptorsKind  = "metricDescriptors"
	mnTimeSeriesKind   = "timeSeries"
	mnAlertPoliciyKind = "alertPolicies"
	mnUptimeKind       = "uptimeCheckConfigs"
	mnSnoozesKind      = "snoozes"
	mnChannelsKind     = "notificationChannels"
	mnMaxTimeSeriesPg  = 500
)

// registerGCPMonitoringRoutes exposes Cloud Monitoring descriptors, series, policies, checks and snoozes.
func (h *DashboardHandler) registerGCPMonitoringRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPMonitoringOps; ops != nil {
		route = (&gcpMonitoringAPI{api: ops.API()}).route
	}

	h.registerGCPRESTProxyVersion(gcpMonitoringProxyPrefix, "v3", route)
}

type gcpMonitoringAPI struct {
	api *gcpmonitoringbackend.API
}

func (a *gcpMonitoringAPI) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)
	if n < mnListSegs || r.segs[0] != gcpRESTProjectsSeg || r.verb != "" {
		return nil, errGCPUnsupported()
	}

	kind := r.segs[mnKindIdx]

	if kind == mnDescriptorsKind {
		return a.descriptors(ctx, r)
	}

	if n > mnItemSegs {
		return nil, errGCPUnsupported()
	}

	if n == mnListSegs {
		if r.method != http.MethodGet {
			return nil, errGCPUnsupported()
		}

		return a.list(ctx, r, kind)
	}

	return a.item(ctx, r, kind)
}

func (a *gcpMonitoringAPI) descriptors(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	if r.method != http.MethodGet {
		return nil, errGCPUnsupported()
	}

	if len(r.segs) == mnListSegs {
		return a.api.Metrics.ListMetricDescriptors(ctx, &monitoringpb.ListMetricDescriptorsRequest{
			Name: r.name(mnKindIdx), Filter: r.query.Get("filter"),
			PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
		})
	}

	return a.api.Metrics.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{
		Name: strings.Join(r.segs, "/"),
	})
}

func (a *gcpMonitoringAPI) list(ctx context.Context, r gcpRESTCall, kind string) (proto.Message, error) {
	parent, page, token := r.name(mnKindIdx), r.pageSize(), r.query.Get("pageToken")

	switch kind {
	case mnTimeSeriesKind:
		return a.timeSeries(ctx, r, parent)
	case mnAlertPoliciyKind:
		return a.api.Alerts.ListAlertPolicies(ctx, &monitoringpb.ListAlertPoliciesRequest{
			Name: parent, Filter: r.query.Get("filter"), PageSize: page, PageToken: token,
		})
	case mnUptimeKind:
		return a.api.Uptime.ListUptimeCheckConfigs(ctx, &monitoringpb.ListUptimeCheckConfigsRequest{
			Parent: parent, PageSize: page, PageToken: token,
		})
	case mnSnoozesKind:
		return a.api.Snoozes.ListSnoozes(ctx, &monitoringpb.ListSnoozesRequest{
			Parent: parent, Filter: r.query.Get("filter"), PageSize: page, PageToken: token,
		})
	case mnChannelsKind:
		return a.api.Channels.ListNotificationChannels(ctx, &monitoringpb.ListNotificationChannelsRequest{
			Name: parent, PageSize: page, PageToken: token,
		})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpMonitoringAPI) item(ctx context.Context, r gcpRESTCall, kind string) (proto.Message, error) {
	name := r.name(mnItemSegs)

	switch {
	case kind == mnAlertPoliciyKind && r.method == http.MethodGet:
		return a.api.Alerts.GetAlertPolicy(ctx, &monitoringpb.GetAlertPolicyRequest{Name: name})
	case kind == mnAlertPoliciyKind && r.method == http.MethodDelete:
		return a.api.Alerts.DeleteAlertPolicy(ctx, &monitoringpb.DeleteAlertPolicyRequest{Name: name})
	case kind == mnUptimeKind && r.method == http.MethodGet:
		return a.api.Uptime.GetUptimeCheckConfig(ctx, &monitoringpb.GetUptimeCheckConfigRequest{Name: name})
	case kind == mnUptimeKind && r.method == http.MethodDelete:
		return a.api.Uptime.DeleteUptimeCheckConfig(ctx, &monitoringpb.DeleteUptimeCheckConfigRequest{Name: name})
	case kind == mnSnoozesKind && r.method == http.MethodGet:
		return a.api.Snoozes.GetSnooze(ctx, &monitoringpb.GetSnoozeRequest{Name: name})
	case kind == mnChannelsKind && r.method == http.MethodGet:
		return a.api.Channels.GetNotificationChannel(ctx, &monitoringpb.GetNotificationChannelRequest{Name: name})
	}

	return nil, errGCPUnsupported()
}

func parseMonitoringTime(v string) (*timestamppb.Timestamp, error) {
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid timestamp "+v)
	}

	return timestamppb.New(t), nil
}

func (a *gcpMonitoringAPI) timeSeries(ctx context.Context, r gcpRESTCall, parent string) (proto.Message, error) {
	start, err := parseMonitoringTime(r.query.Get("interval.startTime"))
	if err != nil && r.query.Get("interval.startTime") != "" {
		return nil, err
	}

	end, err := parseMonitoringTime(r.query.Get("interval.endTime"))
	if err != nil {
		return nil, err
	}

	if r.query.Get("interval.startTime") == "" {
		start = nil
	}

	req := &monitoringpb.ListTimeSeriesRequest{
		Name: parent, Filter: r.query.Get("filter"),
		Interval:  &monitoringpb.TimeInterval{StartTime: start, EndTime: end},
		View:      monitoringpb.ListTimeSeriesRequest_FULL,
		PageSize:  min(r.pageSize(), mnMaxTimeSeriesPg),
		PageToken: r.query.Get("pageToken"),
	}

	if align := r.query.Get("aggregation.alignmentPeriod"); align != "" {
		d, dErr := time.ParseDuration(align)
		if dErr != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid aggregation.alignmentPeriod")
		}

		req.Aggregation = &monitoringpb.Aggregation{
			AlignmentPeriod: durationpb.New(d),
			PerSeriesAligner: monitoringpb.Aggregation_Aligner(
				monitoringpb.Aggregation_Aligner_value[r.query.Get("aggregation.perSeriesAligner")]),
		}
	}

	return a.api.Metrics.ListTimeSeries(ctx, req)
}
