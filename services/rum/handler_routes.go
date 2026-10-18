package rum

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	paramName           = "Name"
	paramAppMonitorName = "AppMonitorName"
	paramID             = "Id"
	paramResourceARN    = "ResourceArn"

	pathMonitor      = "/appmonitor/{Name}"
	pathMonitorData  = pathMonitor + "/data"
	pathMonitorPol   = pathMonitor + "/policy"
	pathMetrics      = "/rummetrics/{AppMonitorName}/metrics"
	pathDestinations = "/rummetrics/{AppMonitorName}/metricsdestination"
	pathTags         = "/tags/{ResourceArn}"
)

func routes() []restroute.Route {
	return []restroute.Route{
		{Method: http.MethodPost, Path: "/appmonitor", Op: "CreateAppMonitor"},
		{Method: http.MethodGet, Path: pathMonitor, Op: "GetAppMonitor"},
		{Method: http.MethodPatch, Path: pathMonitor, Op: "UpdateAppMonitor"},
		{Method: http.MethodDelete, Path: pathMonitor, Op: "DeleteAppMonitor"},
		{Method: http.MethodPost, Path: "/appmonitors", Op: "ListAppMonitors"},
		{Method: http.MethodPost, Path: "/appmonitors/{Id}", Op: "PutRumEvents"},
		{Method: http.MethodPost, Path: pathMonitorData, Op: "GetAppMonitorData"},
		{Method: http.MethodPut, Path: pathMonitorPol, Op: "PutResourcePolicy"},
		{Method: http.MethodGet, Path: pathMonitorPol, Op: "GetResourcePolicy"},
		{Method: http.MethodDelete, Path: pathMonitorPol, Op: "DeleteResourcePolicy"},
		{Method: http.MethodPost, Path: pathMetrics, Op: "BatchCreateRumMetricDefinitions"},
		{Method: http.MethodDelete, Path: pathMetrics, Op: "BatchDeleteRumMetricDefinitions"},
		{Method: http.MethodGet, Path: pathMetrics, Op: "BatchGetRumMetricDefinitions"},
		{Method: http.MethodPatch, Path: pathMetrics, Op: "UpdateRumMetricDefinition"},
		{Method: http.MethodPost, Path: pathDestinations, Op: "PutRumMetricsDestination"},
		{Method: http.MethodDelete, Path: pathDestinations, Op: "DeleteRumMetricsDestination"},
		{Method: http.MethodGet, Path: pathDestinations, Op: "ListRumMetricsDestinations"},
		{Method: http.MethodGet, Path: pathTags, Op: "ListTagsForResource"},
		{Method: http.MethodPost, Path: pathTags, Op: "TagResource"},
		{Method: http.MethodDelete, Path: pathTags, Op: "UntagResource"},
	}
}

func (h *Handler) ops() map[string]restroute.OpFunc {
	ops := map[string]restroute.OpFunc{}
	h.monitorOps(ops)
	h.dataOps(ops)
	h.metricOps(ops)
	h.policyOps(ops)
	h.tagOps(ops)

	return ops
}
