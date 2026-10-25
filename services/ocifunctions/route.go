package ocifunctions

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Operation names, matching the OCI SDK operation names.
const (
	opCreateApplication  = "CreateApplication"
	opGetApplication     = "GetApplication"
	opListApplications   = "ListApplications"
	opUpdateApplication  = "UpdateApplication"
	opDeleteApplication  = "DeleteApplication"
	opChangeAppComp      = "ChangeApplicationCompartment"
	opCreateFunction     = "CreateFunction"
	opGetFunction        = "GetFunction"
	opListFunctions      = "ListFunctions"
	opUpdateFunction     = "UpdateFunction"
	opDeleteFunction     = "DeleteFunction"
	opInvokeFunction     = "InvokeFunction"
	opGetWorkRequest     = "GetWorkRequest"
	opListWorkRequests   = "ListWorkRequests"
	opListWorkReqErrors  = "ListWorkRequestErrors"
	opListWorkReqLogs    = "ListWorkRequestLogs"
	opAdminKeys          = "ManageAPIKeys"
	opUnknown            = "Unknown"
	adminPrefix          = "_gopherstack"
	appPath              = "/applications/{applicationId}"
	fnPath               = "/functions/{functionId}"
	workRequestPath      = "/workRequests/{workRequestId}"
	changeCompartmentSfx = "/actions/changeCompartment"
)

const routesPerBase = 16

// apiBases are the version path prefixes served: 20181201 is the documented API version, 20260325 is
// the one the pinned SDK sends.
//
//nolint:gochecknoglobals // immutable
var apiBases = []string{"/20181201", "/20260325"}

func routesFor(base string) []restroute.Route {
	return []restroute.Route{
		{Method: http.MethodPost, Path: base + "/applications", Op: opCreateApplication},
		{Method: http.MethodGet, Path: base + "/applications", Op: opListApplications},
		{Method: http.MethodGet, Path: base + appPath, Op: opGetApplication},
		{Method: http.MethodPut, Path: base + appPath, Op: opUpdateApplication},
		{Method: http.MethodDelete, Path: base + appPath, Op: opDeleteApplication},
		{Method: http.MethodPost, Path: base + appPath + changeCompartmentSfx, Op: opChangeAppComp},
		{Method: http.MethodPost, Path: base + "/functions", Op: opCreateFunction},
		{Method: http.MethodGet, Path: base + "/functions", Op: opListFunctions},
		{Method: http.MethodGet, Path: base + fnPath, Op: opGetFunction},
		{Method: http.MethodPut, Path: base + fnPath, Op: opUpdateFunction},
		{Method: http.MethodDelete, Path: base + fnPath, Op: opDeleteFunction},
		{Method: http.MethodPost, Path: base + fnPath + "/actions/invoke", Op: opInvokeFunction},
		{Method: http.MethodGet, Path: base + "/workRequests", Op: opListWorkRequests},
		{Method: http.MethodGet, Path: base + workRequestPath, Op: opGetWorkRequest},
		{Method: http.MethodGet, Path: base + workRequestPath + "/errors", Op: opListWorkReqErrors},
		{Method: http.MethodGet, Path: base + workRequestPath + "/logs", Op: opListWorkReqLogs},
	}
}

//nolint:gochecknoglobals // immutable route table
var router = restroute.New(func() []restroute.Route {
	all := make([]restroute.Route, 0, len(apiBases)*routesPerBase)
	for _, b := range apiBases {
		all = append(all, routesFor(b)...)
	}

	return all
}())

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
