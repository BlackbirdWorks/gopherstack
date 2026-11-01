package ocinosql

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Operation names, matching the OCI SDK operation names.
const (
	opCreateTable       = "CreateTable"
	opGetTable          = "GetTable"
	opListTables        = "ListTables"
	opUpdateTable       = "UpdateTable"
	opDeleteTable       = "DeleteTable"
	opChangeTableComp   = "ChangeTableCompartment"
	opListTableUsage    = "ListTableUsage"
	opCreateIndex       = "CreateIndex"
	opGetIndex          = "GetIndex"
	opListIndexes       = "ListIndexes"
	opDeleteIndex       = "DeleteIndex"
	opUpdateRow         = "UpdateRow"
	opGetRow            = "GetRow"
	opDeleteRow         = "DeleteRow"
	opQuery             = "Query"
	opPrepareStatement  = "PrepareStatement"
	opSummarizeStmt     = "SummarizeStatement"
	opGetWorkRequest    = "GetWorkRequest"
	opListWorkRequests  = "ListWorkRequests"
	opListWorkReqErrors = "ListWorkRequestErrors"
	opListWorkReqLogs   = "ListWorkRequestLogs"
	opAdminKeys         = "ManageAPIKeys"
	opUnknown           = "Unknown"
	adminPrefix         = "_gopherstack"
	base                = "/20190828"
	tablePath           = base + "/tables/{tableNameOrId}"
	indexPath           = tablePath + "/indexes/{indexName}"
	workRequestPath     = base + "/workRequests/{workRequestId}"
)

//nolint:gochecknoglobals // immutable route table
var router = restroute.New([]restroute.Route{
	{Method: http.MethodPost, Path: base + "/tables", Op: opCreateTable},
	{Method: http.MethodGet, Path: base + "/tables", Op: opListTables},
	{Method: http.MethodGet, Path: tablePath, Op: opGetTable},
	{Method: http.MethodPut, Path: tablePath, Op: opUpdateTable},
	{Method: http.MethodDelete, Path: tablePath, Op: opDeleteTable},
	{Method: http.MethodPost, Path: tablePath + "/actions/changeCompartment", Op: opChangeTableComp},
	{Method: http.MethodGet, Path: tablePath + "/usage", Op: opListTableUsage},
	{Method: http.MethodPost, Path: tablePath + "/indexes", Op: opCreateIndex},
	{Method: http.MethodGet, Path: tablePath + "/indexes", Op: opListIndexes},
	{Method: http.MethodGet, Path: indexPath, Op: opGetIndex},
	{Method: http.MethodDelete, Path: indexPath, Op: opDeleteIndex},
	{Method: http.MethodPut, Path: tablePath + "/rows", Op: opUpdateRow},
	{Method: http.MethodGet, Path: tablePath + "/rows", Op: opGetRow},
	{Method: http.MethodDelete, Path: tablePath + "/rows", Op: opDeleteRow},
	{Method: http.MethodPost, Path: base + "/query", Op: opQuery},
	{Method: http.MethodGet, Path: base + "/query/prepare", Op: opPrepareStatement},
	{Method: http.MethodGet, Path: base + "/query/summarize", Op: opSummarizeStmt},
	{Method: http.MethodGet, Path: base + "/workRequests", Op: opListWorkRequests},
	{Method: http.MethodGet, Path: workRequestPath, Op: opGetWorkRequest},
	{Method: http.MethodGet, Path: workRequestPath + "/errors", Op: opListWorkReqErrors},
	{Method: http.MethodGet, Path: workRequestPath + "/logs", Op: opListWorkReqLogs},
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
