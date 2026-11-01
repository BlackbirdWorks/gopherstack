package ocicontainerengine

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func buildOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateCluster: (*Handler).createCluster, opGetCluster: (*Handler).getCluster,
		opListClusters: (*Handler).listClusters, opUpdateCluster: (*Handler).updateCluster,
		opDeleteCluster: (*Handler).deleteCluster, opCreateKubeconfig: (*Handler).createKubeconfig,
		opMigrateNativeVcn: (*Handler).migrateNativeVcn, opMigrateStatus: (*Handler).migrateStatus,
		opGetClusterOptions: (*Handler).getClusterOptions,
		opCreateNodePool:    (*Handler).createNodePool, opGetNodePool: (*Handler).getNodePool,
		opListNodePools: (*Handler).listNodePools, opUpdateNodePool: (*Handler).updateNodePool,
		opDeleteNodePool: (*Handler).deleteNodePool, opDeleteNode: (*Handler).deleteNode,
		opGetNodePoolOptions: (*Handler).getNodePoolOptions,
		opGetWorkRequest:     (*Handler).getWorkRequest, opListWorkRequests: (*Handler).listWorkRequests,
		opListWorkReqErrors: (*Handler).listWorkRequestErrors, opListWorkReqLogs: (*Handler).listWorkRequestLogs,
	}
}

func reply[T any](q *req, w T, etag string, err error) error {
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, w, etag)
}

func acceptedWR(q *req, wr string, err error) error {
	if err != nil {
		return err
	}

	return q.accepted(wr)
}

// collectionReply writes a bare JSON array, the shape the containerengine SDK decodes list bodies from.
func collectionReply[T any](q *req, pg page.Page[T], err error) error {
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	data := pg.Data
	if data == nil {
		data = []T{}
	}

	return q.json(http.StatusOK, data, "")
}
