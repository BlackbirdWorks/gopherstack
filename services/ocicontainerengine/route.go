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
