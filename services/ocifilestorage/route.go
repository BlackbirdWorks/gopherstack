package ocifilestorage

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	apiBase     = "/20171215"
	adminPrefix = "/_gopherstack/"
	opAdminKeys = "ManageAPIKeys"
	opUnknown   = "Unknown"
)

// Operation names, matching the OCI SDK operation names.
const (
	opCreateFileSystem                          = "CreateFileSystem"
	opGetFileSystem                             = "GetFileSystem"
	opListFileSystems                           = "ListFileSystems"
	opUpdateFileSystem                          = "UpdateFileSystem"
	opDeleteFileSystem                          = "DeleteFileSystem"
	opChangeFileSystemCompartment               = "ChangeFileSystemCompartment"
	opDetachClone                               = "DetachClone"
	opEstimateReplication                       = "EstimateReplication"
	opToggleQuotaRules                          = "ToggleQuotaRules"
	opAddFileSystemLock                         = "AddFileSystemLock"
	opRemoveFileSystemLock                      = "RemoveFileSystemLock"
	opCreateQuotaRule                           = "CreateQuotaRule"
	opGetQuotaRule                              = "GetQuotaRule"
	opListQuotaRules                            = "ListQuotaRules"
	opUpdateQuotaRule                           = "UpdateQuotaRule"
	opDeleteQuotaRule                           = "DeleteQuotaRule"
	opCreateMountTarget                         = "CreateMountTarget"
	opGetMountTarget                            = "GetMountTarget"
	opListMountTargets                          = "ListMountTargets"
	opUpdateMountTarget                         = "UpdateMountTarget"
	opDeleteMountTarget                         = "DeleteMountTarget"
	opChangeMountTargetCompartment              = "ChangeMountTargetCompartment"
	opUpgradeShapeMountTarget                   = "UpgradeShapeMountTarget"
	opScheduleDowngradeShapeMountTarget         = "ScheduleDowngradeShapeMountTarget"
	opCancelDowngradeShapeMountTarget           = "CancelDowngradeShapeMountTarget"
	opAddMountTargetLock                        = "AddMountTargetLock"
	opRemoveMountTargetLock                     = "RemoveMountTargetLock"
	opValidateKeyTabs                           = "ValidateKeyTabs"
	opGetExportSet                              = "GetExportSet"
	opListExportSets                            = "ListExportSets"
	opUpdateExportSet                           = "UpdateExportSet"
	opCreateExport                              = "CreateExport"
	opGetExport                                 = "GetExport"
	opListExports                               = "ListExports"
	opUpdateExport                              = "UpdateExport"
	opDeleteExport                              = "DeleteExport"
	opAddExportLock                             = "AddExportLock"
	opRemoveExportLock                          = "RemoveExportLock"
	opCreateSnapshot                            = "CreateSnapshot"
	opGetSnapshot                               = "GetSnapshot"
	opListSnapshots                             = "ListSnapshots"
	opUpdateSnapshot                            = "UpdateSnapshot"
	opDeleteSnapshot                            = "DeleteSnapshot"
	opAddSnapshotLock                           = "AddSnapshotLock"
	opRemoveSnapshotLock                        = "RemoveSnapshotLock"
	opCreateFilesystemSnapshotPolicy            = "CreateFilesystemSnapshotPolicy"
	opGetFilesystemSnapshotPolicy               = "GetFilesystemSnapshotPolicy"
	opListFilesystemSnapshotPolicies            = "ListFilesystemSnapshotPolicies"
	opUpdateFilesystemSnapshotPolicy            = "UpdateFilesystemSnapshotPolicy"
	opDeleteFilesystemSnapshotPolicy            = "DeleteFilesystemSnapshotPolicy"
	opPauseFilesystemSnapshotPolicy             = "PauseFilesystemSnapshotPolicy"
	opUnpauseFilesystemSnapshotPolicy           = "UnpauseFilesystemSnapshotPolicy"
	opChangeFilesystemSnapshotPolicyCompartment = "ChangeFilesystemSnapshotPolicyCompartment"
	opAddFilesystemSnapshotPolicyLock           = "AddFilesystemSnapshotPolicyLock"
	opRemoveFilesystemSnapshotPolicyLock        = "RemoveFilesystemSnapshotPolicyLock"
	opCreateReplication                         = "CreateReplication"
	opGetReplication                            = "GetReplication"
	opListReplications                          = "ListReplications"
	opUpdateReplication                         = "UpdateReplication"
	opDeleteReplication                         = "DeleteReplication"
	opChangeReplicationCompartment              = "ChangeReplicationCompartment"
	opAddReplicationLock                        = "AddReplicationLock"
	opRemoveReplicationLock                     = "RemoveReplicationLock"
	opGetReplicationTarget                      = "GetReplicationTarget"
	opListReplicationTargets                    = "ListReplicationTargets"
	opDeleteReplicationTarget                   = "DeleteReplicationTarget"
	opCreateOutboundConnector                   = "CreateOutboundConnector"
	opGetOutboundConnector                      = "GetOutboundConnector"
	opListOutboundConnectors                    = "ListOutboundConnectors"
	opUpdateOutboundConnector                   = "UpdateOutboundConnector"
	opDeleteOutboundConnector                   = "DeleteOutboundConnector"
	opChangeOutboundConnectorCompartment        = "ChangeOutboundConnectorCompartment"
	opAddOutboundConnectorLock                  = "AddOutboundConnectorLock"
	opRemoveOutboundConnectorLock               = "RemoveOutboundConnectorLock"
)

// Resource paths shared by the routes.
const (
	fsPath     = apiBase + "/fileSystems/{fileSystemId}"
	mtPath     = apiBase + "/mountTargets/{mountTargetId}"
	setPath    = apiBase + "/exportSets/{exportSetId}"
	exportPath = apiBase + "/exports/{exportId}"
	snapPath   = apiBase + "/snapshots/{snapshotId}"
	polPath    = apiBase + "/filesystemSnapshotPolicies/{filesystemSnapshotPolicyId}"
	repPath    = apiBase + "/replications/{replicationId}"
	tgtPath    = apiBase + "/replicationTargets/{replicationTargetId}"
	connPath   = apiBase + "/outboundConnectors/{outboundConnectorId}"
)

func rt(method, path, op string) restroute.Route {
	return restroute.Route{Method: method, Path: path, Op: op}
}

func routeTable() []restroute.Route {
	return []restroute.Route{
		rt(http.MethodPost, exportPath+"/actions/addLock", opAddExportLock),
		rt(http.MethodPost, fsPath+"/actions/addLock", opAddFileSystemLock),
		rt(http.MethodPost, polPath+"/actions/addLock", opAddFilesystemSnapshotPolicyLock),
		rt(http.MethodPost, mtPath+"/actions/addLock", opAddMountTargetLock),
		rt(http.MethodPost, connPath+"/actions/addLock", opAddOutboundConnectorLock),
		rt(http.MethodPost, repPath+"/actions/addLock", opAddReplicationLock),
		rt(http.MethodPost, snapPath+"/actions/addLock", opAddSnapshotLock),
		rt(http.MethodPost, mtPath+"/actions/cancelShapeDowngrade", opCancelDowngradeShapeMountTarget),
		rt(http.MethodPost, fsPath+"/actions/changeCompartment", opChangeFileSystemCompartment),
		rt(http.MethodPost, polPath+"/actions/changeCompartment", opChangeFilesystemSnapshotPolicyCompartment),
		rt(http.MethodPost, mtPath+"/actions/changeCompartment", opChangeMountTargetCompartment),
		rt(http.MethodPost, connPath+"/actions/changeCompartment", opChangeOutboundConnectorCompartment),
		rt(http.MethodPost, repPath+"/actions/changeCompartment", opChangeReplicationCompartment),
		rt(http.MethodPost, apiBase+"/exports", opCreateExport),
		rt(http.MethodPost, apiBase+"/fileSystems", opCreateFileSystem),
		rt(http.MethodPost, apiBase+"/filesystemSnapshotPolicies", opCreateFilesystemSnapshotPolicy),
		rt(http.MethodPost, apiBase+"/mountTargets", opCreateMountTarget),
		rt(http.MethodPost, apiBase+"/outboundConnectors", opCreateOutboundConnector),
		rt(http.MethodPost, fsPath+"/quotaRules", opCreateQuotaRule),
		rt(http.MethodPost, apiBase+"/replications", opCreateReplication),
		rt(http.MethodPost, apiBase+"/snapshots", opCreateSnapshot),
		rt(http.MethodDelete, exportPath, opDeleteExport),
		rt(http.MethodDelete, fsPath, opDeleteFileSystem),
		rt(http.MethodDelete, polPath, opDeleteFilesystemSnapshotPolicy),
		rt(http.MethodDelete, mtPath, opDeleteMountTarget),
		rt(http.MethodDelete, connPath, opDeleteOutboundConnector),
		rt(http.MethodDelete, fsPath+"/quotaRules/{quotaRuleId}", opDeleteQuotaRule),
		rt(http.MethodDelete, repPath, opDeleteReplication),
		rt(http.MethodDelete, tgtPath, opDeleteReplicationTarget),
		rt(http.MethodDelete, snapPath, opDeleteSnapshot),
		rt(http.MethodPost, fsPath+"/actions/detachClone", opDetachClone),
		rt(http.MethodPost, fsPath+"/actions/estimateReplication", opEstimateReplication),
		rt(http.MethodGet, exportPath, opGetExport),
		rt(http.MethodGet, setPath, opGetExportSet),
		rt(http.MethodGet, fsPath, opGetFileSystem),
		rt(http.MethodGet, polPath, opGetFilesystemSnapshotPolicy),
		rt(http.MethodGet, mtPath, opGetMountTarget),
		rt(http.MethodGet, connPath, opGetOutboundConnector),
		rt(http.MethodGet, fsPath+"/quotaRules/{quotaRuleId}", opGetQuotaRule),
		rt(http.MethodGet, repPath, opGetReplication),
		rt(http.MethodGet, tgtPath, opGetReplicationTarget),
		rt(http.MethodGet, snapPath, opGetSnapshot),
		rt(http.MethodGet, apiBase+"/exportSets", opListExportSets),
		rt(http.MethodGet, apiBase+"/exports", opListExports),
		rt(http.MethodGet, apiBase+"/fileSystems", opListFileSystems),
		rt(http.MethodGet, apiBase+"/filesystemSnapshotPolicies", opListFilesystemSnapshotPolicies),
		rt(http.MethodGet, apiBase+"/mountTargets", opListMountTargets),
		rt(http.MethodGet, apiBase+"/outboundConnectors", opListOutboundConnectors),
		rt(http.MethodGet, fsPath+"/quotaRules", opListQuotaRules),
		rt(http.MethodGet, apiBase+"/replicationTargets", opListReplicationTargets),
		rt(http.MethodGet, apiBase+"/replications", opListReplications),
		rt(http.MethodGet, apiBase+"/snapshots", opListSnapshots),
		rt(http.MethodPost, polPath+"/actions/pause", opPauseFilesystemSnapshotPolicy),
		rt(http.MethodPost, exportPath+"/actions/removeLock", opRemoveExportLock),
		rt(http.MethodPost, fsPath+"/actions/removeLock", opRemoveFileSystemLock),
		rt(http.MethodPost, polPath+"/actions/removeLock", opRemoveFilesystemSnapshotPolicyLock),
		rt(http.MethodPost, mtPath+"/actions/removeLock", opRemoveMountTargetLock),
		rt(http.MethodPost, connPath+"/actions/removeLock", opRemoveOutboundConnectorLock),
		rt(http.MethodPost, repPath+"/actions/removeLock", opRemoveReplicationLock),
		rt(http.MethodPost, snapPath+"/actions/removeLock", opRemoveSnapshotLock),
		rt(http.MethodPost, mtPath+"/actions/scheduleShapeDowngrade", opScheduleDowngradeShapeMountTarget),
		rt(http.MethodPost, fsPath+"/actions/toggleQuotaRules", opToggleQuotaRules),
		rt(http.MethodPost, polPath+"/actions/unpause", opUnpauseFilesystemSnapshotPolicy),
		rt(http.MethodPut, exportPath, opUpdateExport),
		rt(http.MethodPut, setPath, opUpdateExportSet),
		rt(http.MethodPut, fsPath, opUpdateFileSystem),
		rt(http.MethodPut, polPath, opUpdateFilesystemSnapshotPolicy),
		rt(http.MethodPut, mtPath, opUpdateMountTarget),
		rt(http.MethodPut, connPath, opUpdateOutboundConnector),
		rt(http.MethodPut, fsPath+"/quotaRules/{quotaRuleId}", opUpdateQuotaRule),
		rt(http.MethodPut, repPath, opUpdateReplication),
		rt(http.MethodPut, snapPath, opUpdateSnapshot),
		rt(http.MethodPost, mtPath+"/actions/upgradeShape", opUpgradeShapeMountTarget),
		rt(http.MethodPost, apiBase+"/mountTargets/actions/validateKeyTabs", opValidateKeyTabs),
	}
}

//nolint:gochecknoglobals // immutable route table
var router = restroute.New(routeTable())

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
