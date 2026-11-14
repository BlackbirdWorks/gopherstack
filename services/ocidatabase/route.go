package ocidatabase

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	apiBase     = "/20160918"
	adminPrefix = "/_gopherstack/"
)

// Operation names, matching the OCI SDK operation names.
const (
	opCreate        = "CreateAutonomousDatabase"
	opGet           = "GetAutonomousDatabase"
	opList          = "ListAutonomousDatabases"
	opUpdate        = "UpdateAutonomousDatabase"
	opDelete        = "DeleteAutonomousDatabase"
	opStart         = "StartAutonomousDatabase"
	opStop          = "StopAutonomousDatabase"
	opRestart       = "RestartAutonomousDatabase"
	opRestore       = "RestoreAutonomousDatabase"
	opChangeComp    = "ChangeAutonomousDatabaseCompartment"
	opGenWallet     = "GenerateAutonomousDatabaseWallet"
	opGetWallet     = "GetAutonomousDatabaseWallet"
	opUpdateWallet  = "UpdateAutonomousDatabaseWallet"
	opCreateBackup  = "CreateAutonomousDatabaseBackup"
	opGetBackup     = "GetAutonomousDatabaseBackup"
	opListBackups   = "ListAutonomousDatabaseBackups"
	opDeleteBackup  = "DeleteAutonomousDatabaseBackup"
	opGetWR         = "GetWorkRequest"
	opListWR        = "ListWorkRequests"
	opListWRErrors  = "ListWorkRequestErrors"
	opListWRLogs    = "ListWorkRequestLogs"
	opAdminKeys     = "ManageAPIKeys"
	opUnknown       = "Unknown"
	dbPath          = apiBase + "/autonomousDatabases/{autonomousDatabaseId}"
	backupPath      = apiBase + "/autonomousDatabaseBackups/{autonomousDatabaseBackupId}"
	wrPath          = apiBase + "/workRequests/{workRequestId}"
	collectionPath  = apiBase + "/autonomousDatabases"
	backupsPath     = apiBase + "/autonomousDatabaseBackups"
	workRequestPath = apiBase + "/workRequests"
)

//nolint:gochecknoglobals // immutable route table
var router = restroute.New([]restroute.Route{
	{Method: http.MethodPost, Path: collectionPath, Op: opCreate},
	{Method: http.MethodGet, Path: collectionPath, Op: opList},
	{Method: http.MethodGet, Path: dbPath, Op: opGet},
	{Method: http.MethodPut, Path: dbPath, Op: opUpdate},
	{Method: http.MethodDelete, Path: dbPath, Op: opDelete},
	{Method: http.MethodPost, Path: dbPath + "/actions/start", Op: opStart},
	{Method: http.MethodPost, Path: dbPath + "/actions/stop", Op: opStop},
	{Method: http.MethodPost, Path: dbPath + "/actions/restart", Op: opRestart},
	{Method: http.MethodPost, Path: dbPath + "/actions/restore", Op: opRestore},
	{Method: http.MethodPost, Path: dbPath + "/actions/changeCompartment", Op: opChangeComp},
	{Method: http.MethodPost, Path: dbPath + "/actions/generateWallet", Op: opGenWallet},
	{Method: http.MethodGet, Path: dbPath + "/wallet", Op: opGetWallet},
	{Method: http.MethodPut, Path: dbPath + "/wallet", Op: opUpdateWallet},
	{Method: http.MethodPost, Path: backupsPath, Op: opCreateBackup},
	{Method: http.MethodGet, Path: backupsPath, Op: opListBackups},
	{Method: http.MethodGet, Path: backupPath, Op: opGetBackup},
	{Method: http.MethodDelete, Path: backupPath, Op: opDeleteBackup},
	{Method: http.MethodGet, Path: workRequestPath, Op: opListWR},
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
