package ocicontainerengine

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Operation names, matching the OCI SDK operation names.
const (
	opCreateCluster      = "CreateCluster"
	opGetCluster         = "GetCluster"
	opListClusters       = "ListClusters"
	opUpdateCluster      = "UpdateCluster"
	opDeleteCluster      = "DeleteCluster"
	opCreateKubeconfig   = "CreateKubeconfig"
	opMigrateNativeVcn   = "ClusterMigrateToNativeVcn"
	opMigrateStatus      = "GetClusterMigrateToNativeVcnStatus"
	opGetClusterOptions  = "GetClusterOptions"
	opCreateNodePool     = "CreateNodePool"
	opGetNodePool        = "GetNodePool"
	opListNodePools      = "ListNodePools"
	opUpdateNodePool     = "UpdateNodePool"
	opDeleteNodePool     = "DeleteNodePool"
	opDeleteNode         = "DeleteNode"
	opGetNodePoolOptions = "GetNodePoolOptions"
	opCreateVNP          = "CreateVirtualNodePool"
	opGetVNP             = "GetVirtualNodePool"
	opListVNPs           = "ListVirtualNodePools"
	opUpdateVNP          = "UpdateVirtualNodePool"
	opDeleteVNP          = "DeleteVirtualNodePool"
	opListVirtualNodes   = "ListVirtualNodes"
	opGetVirtualNode     = "GetVirtualNode"
	opInstallAddon       = "InstallAddon"
	opGetAddon           = "GetAddon"
	opListAddons         = "ListAddons"
	opUpdateAddon        = "UpdateAddon"
	opDisableAddon       = "DisableAddon"
	opListAddonOptions   = "ListAddonOptions"
	opCreateMapping      = "CreateWorkloadMapping"
	opGetMapping         = "GetWorkloadMapping"
	opListMappings       = "ListWorkloadMappings"
	opUpdateMapping      = "UpdateWorkloadMapping"
	opDeleteMapping      = "DeleteWorkloadMapping"
	opStartRotation      = "StartCredentialRotation"
	opCompleteRotation   = "CompleteCredentialRotation"
	opRotationStatus     = "GetCredentialRotationStatus"
	opStartDecommission  = "StartPublicApiEndpointDecommission"
	opRollbackDecomm     = "RollbackPublicApiEndpointDecommission"
	opExtendDecomm       = "ExtendEndpointDecommissionRollbackDeadline"
	opDecommStatus       = "GetPublicApiEndpointDecommissionStatus"
	opUpdateEndpointCfg  = "UpdateClusterEndpointConfig"
	opRebootNode         = "RebootClusterNode"
	opReplaceBootVolume  = "ReplaceBootVolumeClusterNode"
	opDeleteWorkRequest  = "DeleteWorkRequest"
	opGetWorkRequest     = "GetWorkRequest"
	opListWorkRequests   = "ListWorkRequests"
	opListWorkReqErrors  = "ListWorkRequestErrors"
	opListWorkReqLogs    = "ListWorkRequestLogs"
	opAdminKeys          = "ManageAPIKeys"
	opUnknown            = "Unknown"
	adminPrefix          = "_gopherstack"
	base                 = "/20180222"
	clusterPath          = base + "/clusters/{clusterId}"
	nodePoolPath         = base + "/nodePools/{nodePoolId}"
	vnpPath              = base + "/virtualNodePools/{virtualNodePoolId}"
	workRequestPath      = base + "/workRequests/{workRequestId}"
)

//nolint:gochecknoglobals // immutable route table
var router = restroute.New([]restroute.Route{
	{Method: http.MethodPost, Path: base + "/clusters", Op: opCreateCluster},
	{Method: http.MethodGet, Path: base + "/clusters", Op: opListClusters},
	{Method: http.MethodGet, Path: clusterPath, Op: opGetCluster},
	{Method: http.MethodPut, Path: clusterPath, Op: opUpdateCluster},
	{Method: http.MethodDelete, Path: clusterPath, Op: opDeleteCluster},
	{Method: http.MethodPost, Path: clusterPath + "/kubeconfig/content", Op: opCreateKubeconfig},
	{Method: http.MethodPost, Path: clusterPath + "/actions/migrateToNativeVcn", Op: opMigrateNativeVcn},
	{Method: http.MethodGet, Path: clusterPath + "/migrateToNativeVcnStatus", Op: opMigrateStatus},
	{Method: http.MethodGet, Path: base + "/clusterOptions/{clusterOptionId}", Op: opGetClusterOptions},
	{Method: http.MethodPost, Path: base + "/nodePools", Op: opCreateNodePool},
	{Method: http.MethodGet, Path: base + "/nodePools", Op: opListNodePools},
	{Method: http.MethodGet, Path: nodePoolPath, Op: opGetNodePool},
	{Method: http.MethodPut, Path: nodePoolPath, Op: opUpdateNodePool},
	{Method: http.MethodDelete, Path: nodePoolPath, Op: opDeleteNodePool},
	{Method: http.MethodDelete, Path: nodePoolPath + "/node/{nodeId}", Op: opDeleteNode},
	{Method: http.MethodGet, Path: base + "/nodePoolOptions/{nodePoolOptionId}", Op: opGetNodePoolOptions},
	{Method: http.MethodPost, Path: base + "/virtualNodePools", Op: opCreateVNP},
	{Method: http.MethodGet, Path: base + "/virtualNodePools", Op: opListVNPs},
	{Method: http.MethodGet, Path: vnpPath, Op: opGetVNP},
	{Method: http.MethodPut, Path: vnpPath, Op: opUpdateVNP},
	{Method: http.MethodDelete, Path: vnpPath, Op: opDeleteVNP},
	{Method: http.MethodGet, Path: vnpPath + "/virtualNodes", Op: opListVirtualNodes},
	{Method: http.MethodGet, Path: vnpPath + "/virtualNodes/{virtualNodeId}", Op: opGetVirtualNode},
	{Method: http.MethodPost, Path: clusterPath + "/addons", Op: opInstallAddon},
	{Method: http.MethodGet, Path: clusterPath + "/addons", Op: opListAddons},
	{Method: http.MethodGet, Path: clusterPath + "/addons/{addonName}", Op: opGetAddon},
	{Method: http.MethodPut, Path: clusterPath + "/addons/{addonName}", Op: opUpdateAddon},
	{Method: http.MethodDelete, Path: clusterPath + "/addons/{addonName}", Op: opDisableAddon},
	{Method: http.MethodGet, Path: base + "/addonOptions", Op: opListAddonOptions},
	{Method: http.MethodPost, Path: clusterPath + "/workloadMappings", Op: opCreateMapping},
	{Method: http.MethodGet, Path: clusterPath + "/workloadMappings", Op: opListMappings},
	{Method: http.MethodGet, Path: clusterPath + "/workloadMappings/{workloadMappingId}", Op: opGetMapping},
	{Method: http.MethodPut, Path: clusterPath + "/workloadMappings/{workloadMappingId}", Op: opUpdateMapping},
	{Method: http.MethodDelete, Path: clusterPath + "/workloadMappings/{workloadMappingId}", Op: opDeleteMapping},
	{Method: http.MethodPost, Path: clusterPath + "/actions/startCredentialRotation", Op: opStartRotation},
	{Method: http.MethodPost, Path: clusterPath + "/actions/completeCredentialRotation", Op: opCompleteRotation},
	{Method: http.MethodGet, Path: clusterPath + "/credentialRotationStatus", Op: opRotationStatus},
	{
		Method: http.MethodPost,
		Path:   clusterPath + "/actions/startPublicApiEndpointDecommission",
		Op:     opStartDecommission,
	},
	{
		Method: http.MethodPost,
		Path:   clusterPath + "/actions/rollbackPublicApiEndpointDecommission",
		Op:     opRollbackDecomm,
	},
	{
		Method: http.MethodPost,
		Path:   clusterPath + "/actions/extendEndpointDecommissionRollbackDeadline",
		Op:     opExtendDecomm,
	},
	{Method: http.MethodGet, Path: clusterPath + "/publicApiEndpointDecommissionStatus", Op: opDecommStatus},
	{Method: http.MethodPost, Path: clusterPath + "/actions/updateEndpointConfig", Op: opUpdateEndpointCfg},
	{Method: http.MethodPost, Path: clusterPath + "/nodes/{nodeId}/actions/reboot", Op: opRebootNode},
	{Method: http.MethodPost, Path: clusterPath + "/nodes/{nodeId}/actions/replaceBootVolume", Op: opReplaceBootVolume},
	{Method: http.MethodDelete, Path: workRequestPath, Op: opDeleteWorkRequest},
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
