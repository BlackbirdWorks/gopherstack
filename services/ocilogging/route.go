package ocilogging

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Operation names, matching the OCI SDK operation names.
const (
	opCreateLogGroup    = "CreateLogGroup"
	opGetLogGroup       = "GetLogGroup"
	opListLogGroups     = "ListLogGroups"
	opUpdateLogGroup    = "UpdateLogGroup"
	opDeleteLogGroup    = "DeleteLogGroup"
	opChangeGroupComp   = "ChangeLogGroupCompartment"
	opCreateLog         = "CreateLog"
	opGetLog            = "GetLog"
	opListLogs          = "ListLogs"
	opUpdateLog         = "UpdateLog"
	opDeleteLog         = "DeleteLog"
	opChangeLogGroup    = "ChangeLogLogGroup"
	opCreateAgentConfig = "CreateUnifiedAgentConfiguration"
	opGetAgentConfig    = "GetUnifiedAgentConfiguration"
	opListAgentConfigs  = "ListUnifiedAgentConfigurations"
	opUpdateAgentConfig = "UpdateUnifiedAgentConfiguration"
	opDeleteAgentConfig = "DeleteUnifiedAgentConfiguration"
	opChangeAgentComp   = "ChangeUnifiedAgentConfigurationCompartment"
	opListServices      = "ListServices"
	opGetWorkRequest    = "GetWorkRequest"
	opListWorkRequests  = "ListWorkRequests"
	opDeleteWorkRequest = "DeleteWorkRequest"
	opListWorkReqErrors = "ListWorkRequestErrors"
	opListWorkReqLogs   = "ListWorkRequestLogs"
	opPutLogs           = "PutLogs"
	opSearchLogs        = "SearchLogs"
	opAdminKeys         = "ManageAPIKeys"
	opUnknown           = "Unknown"
	adminPrefix         = "_gopherstack"
	mgmtBase            = "/20200531"
	ingestBase          = "/20200831"
	searchBase          = "/20190909"
	groupPath           = mgmtBase + "/logGroups/{logGroupId}"
	logPath             = groupPath + "/logs/{logId}"
	agentPath           = mgmtBase + "/unifiedAgentConfigurations/{unifiedAgentConfigurationId}"
	workRequestPath     = mgmtBase + "/workRequests/{workRequestId}"
)

//nolint:gochecknoglobals // immutable route table
var router = restroute.New([]restroute.Route{
	{Method: http.MethodPost, Path: mgmtBase + "/logGroups", Op: opCreateLogGroup},
	{Method: http.MethodGet, Path: mgmtBase + "/logGroups", Op: opListLogGroups},
	{Method: http.MethodGet, Path: groupPath, Op: opGetLogGroup},
	{Method: http.MethodPut, Path: groupPath, Op: opUpdateLogGroup},
	{Method: http.MethodDelete, Path: groupPath, Op: opDeleteLogGroup},
	{Method: http.MethodPost, Path: groupPath + "/actions/changeCompartment", Op: opChangeGroupComp},
	{Method: http.MethodPost, Path: groupPath + "/logs", Op: opCreateLog},
	{Method: http.MethodGet, Path: groupPath + "/logs", Op: opListLogs},
	{Method: http.MethodGet, Path: logPath, Op: opGetLog},
	{Method: http.MethodPut, Path: logPath, Op: opUpdateLog},
	{Method: http.MethodDelete, Path: logPath, Op: opDeleteLog},
	{Method: http.MethodPost, Path: logPath + "/actions/changeLogGroup", Op: opChangeLogGroup},
	{Method: http.MethodPost, Path: mgmtBase + "/unifiedAgentConfigurations", Op: opCreateAgentConfig},
	{Method: http.MethodGet, Path: mgmtBase + "/unifiedAgentConfigurations", Op: opListAgentConfigs},
	{Method: http.MethodGet, Path: agentPath, Op: opGetAgentConfig},
	{Method: http.MethodPut, Path: agentPath, Op: opUpdateAgentConfig},
	{Method: http.MethodDelete, Path: agentPath, Op: opDeleteAgentConfig},
	{Method: http.MethodPost, Path: agentPath + "/actions/changeCompartment", Op: opChangeAgentComp},
	{Method: http.MethodGet, Path: mgmtBase + "/v2/registry/services", Op: opListServices},
	{Method: http.MethodGet, Path: mgmtBase + "/workRequests", Op: opListWorkRequests},
	{Method: http.MethodGet, Path: workRequestPath, Op: opGetWorkRequest},
	{Method: http.MethodDelete, Path: workRequestPath, Op: opDeleteWorkRequest},
	{Method: http.MethodGet, Path: workRequestPath + "/errors", Op: opListWorkReqErrors},
	{Method: http.MethodGet, Path: workRequestPath + "/logs", Op: opListWorkReqLogs},
	{Method: http.MethodPost, Path: ingestBase + "/logs/{logId}/actions/push", Op: opPutLogs},
	{Method: http.MethodPost, Path: searchBase + "/search", Op: opSearchLogs},
	{Method: http.MethodPost, Path: searchBase + "/actions/search", Op: opSearchLogs},
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
