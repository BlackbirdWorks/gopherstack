package ocicontainerinstances

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Operation names, matching the OCI SDK operation names.
const (
	opCreate        = "CreateContainerInstance"
	opGet           = "GetContainerInstance"
	opList          = "ListContainerInstances"
	opUpdate        = "UpdateContainerInstance"
	opDelete        = "DeleteContainerInstance"
	opStart         = "StartContainerInstance"
	opStop          = "StopContainerInstance"
	opRestart       = "RestartContainerInstance"
	opChangeComp    = "ChangeContainerInstanceCompartment"
	opListShapes    = "ListContainerInstanceShapes"
	opGetContainer  = "GetContainer"
	opListContainer = "ListContainers"
	opUpdContainer  = "UpdateContainer"
	opRetrieveLogs  = "RetrieveLogs"
	opGetWR         = "GetWorkRequest"
	opListWR        = "ListWorkRequests"
	opListWRErrors  = "ListWorkRequestErrors"
	opListWRLogs    = "ListWorkRequestLogs"
	opAdminKeys     = "ManageAPIKeys"
	opUnknown       = "Unknown"
	adminPrefix     = "/_gopherstack/"
	instPath        = apiBase + "/containerInstances/{containerInstanceId}"
	ctrPath         = apiBase + "/containers/{containerId}"
	wrPath          = apiBase + "/workRequests/{workRequestId}"
)

//nolint:gochecknoglobals // immutable route table
var router = restroute.New([]restroute.Route{
	{Method: http.MethodPost, Path: apiBase + "/containerInstances", Op: opCreate},
	{Method: http.MethodGet, Path: apiBase + "/containerInstances", Op: opList},
	{Method: http.MethodGet, Path: instPath, Op: opGet},
	{Method: http.MethodPut, Path: instPath, Op: opUpdate},
	{Method: http.MethodDelete, Path: instPath, Op: opDelete},
	{Method: http.MethodPost, Path: instPath + "/actions/start", Op: opStart},
	{Method: http.MethodPost, Path: instPath + "/actions/stop", Op: opStop},
	{Method: http.MethodPost, Path: instPath + "/actions/restart", Op: opRestart},
	{Method: http.MethodPost, Path: instPath + "/actions/changeCompartment", Op: opChangeComp},
	{Method: http.MethodGet, Path: apiBase + "/containerInstanceShapes", Op: opListShapes},
	{Method: http.MethodGet, Path: apiBase + "/containers", Op: opListContainer},
	{Method: http.MethodGet, Path: ctrPath, Op: opGetContainer},
	{Method: http.MethodPut, Path: ctrPath, Op: opUpdContainer},
	{Method: http.MethodPost, Path: ctrPath + "/actions/retrieveLogs", Op: opRetrieveLogs},
	{Method: http.MethodGet, Path: apiBase + "/workRequests", Op: opListWR},
	{Method: http.MethodGet, Path: wrPath, Op: opGetWR},
	{Method: http.MethodGet, Path: wrPath + "/errors", Op: opListWRErrors},
	{Method: http.MethodGet, Path: wrPath + "/logs", Op: opListWRLogs},
})

type route struct {
	params map[string]string
	op     string
}

// parseRoute maps method and path to an operation.
func parseRoute(r *http.Request) route {
	path := r.URL.EscapedPath()
	if strings.HasPrefix(path, adminPrefix) {
		return route{op: opAdminKeys}
	}

	op, params := router.Match(r.Method, path)
	if op == "" {
		op = opUnknown
	}

	return route{op: op, params: params}
}
