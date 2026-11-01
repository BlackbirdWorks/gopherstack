package gcpsql

import (
	"net/http"
	"net/url"
	"strings"
)

// Operation names, used for routing, metrics and GetSupportedOperations.
const (
	opListFlags          = "ListFlags"
	opListTiers          = "ListTiers"
	opInsertInstance     = "InsertInstance"
	opGetInstance        = "GetInstance"
	opListInstances      = "ListInstances"
	opPatchInstance      = "PatchInstance"
	opUpdateInstance     = "UpdateInstance"
	opDeleteInstance     = "DeleteInstance"
	opRestartInstance    = "RestartInstance"
	opCloneInstance      = "CloneInstance"
	opPromoteReplica     = "PromoteReplica"
	opStartReplica       = "StartReplica"
	opStopReplica        = "StopReplica"
	opLatestRecovery     = "GetLatestRecoveryTime"
	opExportInstance     = "ExportInstance"
	opImportInstance     = "ImportInstance"
	opRestoreBackup      = "RestoreBackup"
	opInsertDatabase     = "InsertDatabase"
	opGetDatabase        = "GetDatabase"
	opListDatabases      = "ListDatabases"
	opPatchDatabase      = "PatchDatabase"
	opUpdateDatabase     = "UpdateDatabase"
	opDeleteDatabase     = "DeleteDatabase"
	opInsertUser         = "InsertUser"
	opGetUser            = "GetUser"
	opListUsers          = "ListUsers"
	opUpdateUser         = "UpdateUser"
	opDeleteUser         = "DeleteUser"
	opInsertBackupRun    = "InsertBackupRun"
	opGetBackupRun       = "GetBackupRun"
	opListBackupRuns     = "ListBackupRuns"
	opDeleteBackupRun    = "DeleteBackupRun"
	opInsertSslCert      = "InsertSslCert"
	opGetSslCert         = "GetSslCert"
	opListSslCerts       = "ListSslCerts"
	opDeleteSslCert      = "DeleteSslCert"
	opCreateEphemeral    = "CreateEphemeral"
	opGenerateEphemeral  = "GenerateEphemeralCert"
	opGetConnectSettings = "GetConnectSettings"
	opGetOperation       = "GetOperation"
	opListOperations     = "ListOperations"
	opCancelOperation    = "CancelOperation"
	unknownOperation     = "Unknown"

	segProjects   = "projects"
	segInstances  = "instances"
	minInstance   = 4
	instanceSub   = 5
	instanceItem  = 6
	ephemeralTail = ":generateEphemeralCert"
)

type route struct {
	op       string
	project  string
	instance string
	name     string
}

var apiPrefixes = []string{"sql/v1beta4/", "sql/v1/", "v1beta4/", "v1/"} //nolint:gochecknoglobals // immutable

func stripAPIPrefix(path string) string {
	trimmed := strings.TrimPrefix(path, "/")

	for _, p := range apiPrefixes {
		if rest, ok := strings.CutPrefix(trimmed, p); ok {
			return rest
		}
	}

	return trimmed
}

func splitSegments(escaped string) []string {
	escaped = strings.Trim(escaped, "/")
	if escaped == "" {
		return nil
	}

	segs := strings.Split(escaped, "/")
	for i, s := range segs {
		if u, err := url.PathUnescape(s); err == nil {
			segs[i] = u
		}
	}

	return segs
}

func pick(method string, ops map[string]string) string {
	if op, ok := ops[method]; ok {
		return op
	}

	return unknownOperation
}

func parseRoute(r *http.Request) route {
	segs := splitSegments(stripAPIPrefix(r.URL.EscapedPath()))
	if len(segs) == 1 && segs[0] == "flags" {
		return route{op: pick(r.Method, map[string]string{http.MethodGet: opListFlags})}
	}

	if len(segs) < minInstance-1 || segs[0] != segProjects {
		return route{op: unknownOperation}
	}

	rt := route{project: segs[1], op: unknownOperation}

	switch segs[2] {
	case "tiers":
		rt.op = pick(r.Method, map[string]string{http.MethodGet: opListTiers})
	case "operations":
		return operationRoute(r.Method, segs, rt)
	case segInstances:
		return instanceRoute(r.Method, segs, rt)
	}

	return rt
}

func operationRoute(method string, s []string, rt route) route {
	switch len(s) {
	case minInstance - 1:
		rt.op = pick(method, map[string]string{http.MethodGet: opListOperations})
	case minInstance:
		rt.name = s[3]
		rt.op = pick(method, map[string]string{http.MethodGet: opGetOperation})
	case instanceSub:
		rt.name = s[3]
		if s[4] == "cancel" {
			rt.op = pick(method, map[string]string{http.MethodPost: opCancelOperation})
		}
	}

	return rt
}

func instanceRoute(method string, s []string, rt route) route {
	if len(s) == minInstance-1 {
		rt.op = pick(method, map[string]string{http.MethodGet: opListInstances, http.MethodPost: opInsertInstance})

		return rt
	}

	if id, ok := strings.CutSuffix(s[3], ephemeralTail); ok && len(s) == minInstance {
		rt.instance = id
		rt.op = pick(method, map[string]string{http.MethodPost: opGenerateEphemeral})

		return rt
	}

	rt.instance = s[3]

	switch len(s) {
	case minInstance:
		rt.op = pick(method, map[string]string{
			http.MethodGet: opGetInstance, http.MethodPatch: opPatchInstance,
			http.MethodPut: opUpdateInstance, http.MethodDelete: opDeleteInstance,
		})
	case instanceSub:
		rt.op = subCollectionOp(method, s[4])
	case instanceItem:
		rt.name = s[5]
		rt.op = subItemOp(method, s[4])
	}

	return rt
}

func subCollectionOp(method, sub string) string {
	table := map[string]map[string]string{
		"restart":               {http.MethodPost: opRestartInstance},
		"clone":                 {http.MethodPost: opCloneInstance},
		"promoteReplica":        {http.MethodPost: opPromoteReplica},
		"startReplica":          {http.MethodPost: opStartReplica},
		"stopReplica":           {http.MethodPost: opStopReplica},
		"getLatestRecoveryTime": {http.MethodGet: opLatestRecovery},
		"export":                {http.MethodPost: opExportInstance},
		"import":                {http.MethodPost: opImportInstance},
		"restoreBackup":         {http.MethodPost: opRestoreBackup},
		"connectSettings":       {http.MethodGet: opGetConnectSettings},
		"createEphemeral":       {http.MethodPost: opCreateEphemeral},
		"databases":             {http.MethodGet: opListDatabases, http.MethodPost: opInsertDatabase},
		"users": {
			http.MethodGet: opListUsers, http.MethodPost: opInsertUser,
			http.MethodPut: opUpdateUser, http.MethodDelete: opDeleteUser,
		},
		"backupRuns": {http.MethodGet: opListBackupRuns, http.MethodPost: opInsertBackupRun},
		"sslCerts":   {http.MethodGet: opListSslCerts, http.MethodPost: opInsertSslCert},
	}

	return pick(method, table[sub])
}

func subItemOp(method, sub string) string {
	table := map[string]map[string]string{
		"databases": {
			http.MethodGet: opGetDatabase, http.MethodPatch: opPatchDatabase,
			http.MethodPut: opUpdateDatabase, http.MethodDelete: opDeleteDatabase,
		},
		"users":      {http.MethodGet: opGetUser},
		"backupRuns": {http.MethodGet: opGetBackupRun, http.MethodDelete: opDeleteBackupRun},
		"sslCerts":   {http.MethodGet: opGetSslCert, http.MethodDelete: opDeleteSslCert},
	}

	return pick(method, table[sub])
}
