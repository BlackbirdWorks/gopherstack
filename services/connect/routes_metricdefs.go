package connect

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func metricDefinitionRoutes() []restroute.Route {
	const (
		list = "/metrics/definitions/{InstanceId}"
		one  = list + "/{MetricId}"
	)

	return []restroute.Route{
		{Method: http.MethodPut, Path: list, Op: "CreateMetric"},
		{Method: http.MethodGet, Path: list, Op: "ListMetrics"},
		{Method: http.MethodGet, Path: one, Op: "DescribeMetric"},
		{Method: http.MethodDelete, Path: one, Op: "DeleteMetric"},
		{Method: http.MethodPost, Path: one + "/content", Op: "UpdateMetricContent"},
		{Method: http.MethodPost, Path: one + "/metadata", Op: "UpdateMetricMetadata"},
	}
}
