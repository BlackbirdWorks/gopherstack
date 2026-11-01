package ocimysql

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Operation names, matching the OCI SDK operation names.
const (
	opCreateDBSystem    = "CreateDbSystem"
	opGetDBSystem       = "GetDbSystem"
	opListDBSystems     = "ListDbSystems"
	opUpdateDBSystem    = "UpdateDbSystem"
	opDeleteDBSystem    = "DeleteDbSystem"
	opStartDBSystem     = "StartDbSystem"
	opStopDBSystem      = "StopDbSystem"
	opRestartDBSystem   = "RestartDbSystem"
	opCreateBackup      = "CreateBackup"
	opGetBackup         = "GetBackup"
	opListBackups       = "ListBackups"
	opUpdateBackup      = "UpdateBackup"
	opDeleteBackup      = "DeleteBackup"
	opChangeBackupComp  = "ChangeBackupCompartment"
	opCreateChannel     = "CreateChannel"
	opGetChannel        = "GetChannel"
	opListChannels      = "ListChannels"
	opUpdateChannel     = "UpdateChannel"
	opDeleteChannel     = "DeleteChannel"
	opResumeChannel     = "ResumeChannel"
	opResetChannel      = "ResetChannel"
	opCreateConfig      = "CreateConfiguration"
	opGetConfig         = "GetConfiguration"
	opListConfigs       = "ListConfigurations"
	opUpdateConfig      = "UpdateConfiguration"
	opDeleteConfig      = "DeleteConfiguration"
	opListShapes        = "ListShapes"
	opListVersions      = "ListVersions"
	opGetWorkRequest    = "GetWorkRequest"
	opListWorkRequests  = "ListWorkRequests"
	opListWorkReqErrors = "ListWorkRequestErrors"
	opListWorkReqLogs   = "ListWorkRequestLogs"
	opAdminKeys         = "ManageAPIKeys"
	opUnknown           = "Unknown"
	adminPrefix         = "_gopherstack"
	base                = "/20190415"
	dbSystemPath        = base + "/dbSystems/{dbSystemId}"
	backupPath          = base + "/backups/{backupId}"
	channelPath         = base + "/channels/{channelId}"
	configPath          = base + "/configurations/{configurationId}"
	workRequestPath     = base + "/workRequests/{workRequestId}"
)

const (
	keyCompartment = "compartmentId"
	keyDisplayName = "displayName"
	keyTimeCreated = "timeCreated"
	keyTimeUpdated = "timeUpdated"
	keyLifecycle   = "lifecycleState"
	keySourceType  = "sourceType"
	keyIsEnabled   = "isEnabled"
	paramDBSystem  = "dbSystemId"
)

//nolint:gochecknoglobals // immutable route table
var router = restroute.New([]restroute.Route{
	{Method: http.MethodPost, Path: base + "/dbSystems", Op: opCreateDBSystem},
	{Method: http.MethodGet, Path: base + "/dbSystems", Op: opListDBSystems},
	{Method: http.MethodGet, Path: dbSystemPath, Op: opGetDBSystem},
	{Method: http.MethodPut, Path: dbSystemPath, Op: opUpdateDBSystem},
	{Method: http.MethodDelete, Path: dbSystemPath, Op: opDeleteDBSystem},
	{Method: http.MethodPost, Path: dbSystemPath + "/actions/start", Op: opStartDBSystem},
	{Method: http.MethodPost, Path: dbSystemPath + "/actions/stop", Op: opStopDBSystem},
	{Method: http.MethodPost, Path: dbSystemPath + "/actions/restart", Op: opRestartDBSystem},
	{Method: http.MethodPost, Path: base + "/backups", Op: opCreateBackup},
	{Method: http.MethodGet, Path: base + "/backups", Op: opListBackups},
	{Method: http.MethodGet, Path: backupPath, Op: opGetBackup},
	{Method: http.MethodPut, Path: backupPath, Op: opUpdateBackup},
	{Method: http.MethodDelete, Path: backupPath, Op: opDeleteBackup},
	{Method: http.MethodPost, Path: backupPath + "/actions/changeCompartment", Op: opChangeBackupComp},
	{Method: http.MethodPost, Path: base + "/channels", Op: opCreateChannel},
	{Method: http.MethodGet, Path: base + "/channels", Op: opListChannels},
	{Method: http.MethodGet, Path: channelPath, Op: opGetChannel},
	{Method: http.MethodPut, Path: channelPath, Op: opUpdateChannel},
	{Method: http.MethodDelete, Path: channelPath, Op: opDeleteChannel},
	{Method: http.MethodPost, Path: channelPath + "/actions/resume", Op: opResumeChannel},
	{Method: http.MethodPost, Path: channelPath + "/actions/reset", Op: opResetChannel},
	{Method: http.MethodPost, Path: base + "/configurations", Op: opCreateConfig},
	{Method: http.MethodGet, Path: base + "/configurations", Op: opListConfigs},
	{Method: http.MethodGet, Path: configPath, Op: opGetConfig},
	{Method: http.MethodPut, Path: configPath, Op: opUpdateConfig},
	{Method: http.MethodDelete, Path: configPath, Op: opDeleteConfig},
	{Method: http.MethodGet, Path: base + "/shapes", Op: opListShapes},
	{Method: http.MethodGet, Path: base + "/versions", Op: opListVersions},
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
