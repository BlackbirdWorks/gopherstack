package ocimonitoring

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Operation names, matching the OCI SDK operation names.
const (
	opCreateAlarm      = "CreateAlarm"
	opGetAlarm         = "GetAlarm"
	opListAlarms       = "ListAlarms"
	opListAlarmsStatus = "ListAlarmsStatus"
	opUpdateAlarm      = "UpdateAlarm"
	opDeleteAlarm      = "DeleteAlarm"
	opChangeAlarmComp  = "ChangeAlarmCompartment"
	opGetAlarmHistory  = "GetAlarmHistory"
	opPostMetricData   = "PostMetricData"
	opListMetrics      = "ListMetrics"
	opSummarizeMetrics = "SummarizeMetricsData"
	opAdminKeys        = "ManageAPIKeys"
	opUnknown          = "Unknown"
	adminPrefix        = "_gopherstack"
	base               = "/20180401"
	alarmPath          = base + "/alarms/{alarmId}"
)

//nolint:gochecknoglobals // immutable route table
var router = restroute.New([]restroute.Route{
	{Method: http.MethodPost, Path: base + "/alarms", Op: opCreateAlarm},
	{Method: http.MethodGet, Path: base + "/alarms", Op: opListAlarms},
	{Method: http.MethodGet, Path: base + "/alarms/status", Op: opListAlarmsStatus},
	{Method: http.MethodGet, Path: alarmPath, Op: opGetAlarm},
	{Method: http.MethodPut, Path: alarmPath, Op: opUpdateAlarm},
	{Method: http.MethodDelete, Path: alarmPath, Op: opDeleteAlarm},
	{Method: http.MethodPost, Path: alarmPath + "/actions/changeCompartment", Op: opChangeAlarmComp},
	{Method: http.MethodGet, Path: alarmPath + "/history", Op: opGetAlarmHistory},
	{Method: http.MethodPost, Path: base + "/metrics", Op: opPostMetricData},
	{Method: http.MethodPost, Path: base + "/metrics/actions/listMetrics", Op: opListMetrics},
	{Method: http.MethodPost, Path: base + "/metrics/actions/summarizeMetricsData", Op: opSummarizeMetrics},
})

type route struct {
	params map[string]string
	op     string
}

// parseRoute maps method and path to an operation.
func parseRoute(r *http.Request) route {
	path := r.URL.EscapedPath()
	if strings.HasPrefix(path, "/"+adminPrefix+"/") {
		return route{op: opAdminKeys}
	}

	op, params := router.Match(r.Method, path)
	if op == "" {
		op = opUnknown
	}

	return route{op: op, params: params}
}
