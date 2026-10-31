//nolint:staticcheck // the API still serves fields the proto marks deprecated
package gcpcontainer_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func TestRESTClusterAndNodePoolLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	op, err := e.rc.CreateCluster(t.Context(), &containerpb.CreateClusterRequest{
		Parent: zoneLoc, Cluster: &containerpb.Cluster{
			Name: "rest", InitialNodeCount: 2, ResourceLabels: map[string]string{"via": "rest"},
			NodeConfig: &containerpb.NodeConfig{MachineType: "n2-standard-2"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, containerpb.Operation_CREATE_CLUSTER, op.GetOperationType())
	assert.Equal(t, containerpb.Operation_RUNNING, op.GetStatus())

	_, err = e.rc.CreateCluster(t.Context(), &containerpb.CreateClusterRequest{
		Parent: zoneLoc, Cluster: &containerpb.Cluster{Name: "rest", InitialNodeCount: 1},
	})
	assertCode(t, codes.AlreadyExists, err, "")

	e.settle()

	got, err := e.rc.GetCluster(t.Context(), &containerpb.GetClusterRequest{Name: clusterPath(zone, "rest")})
	require.NoError(t, err)
	assert.Equal(t, containerpb.Cluster_RUNNING, got.GetStatus())
	assert.Equal(t, map[string]string{"via": "rest"}, got.GetResourceLabels())
	assert.Equal(t, "n2-standard-2", got.GetNodePools()[0].GetConfig().GetMachineType())

	_, err = e.rc.GetCluster(t.Context(), &containerpb.GetClusterRequest{Name: clusterPath(zone, "ghost")})
	assertCode(t, codes.NotFound, err, "")

	pop, err := e.rc.CreateNodePool(t.Context(), &containerpb.CreateNodePoolRequest{
		Parent: clusterPath(zone, "rest"), NodePool: &containerpb.NodePool{Name: "extra", InitialNodeCount: 1},
	})
	require.NoError(t, err)
	assert.Equal(t, containerpb.Operation_CREATE_NODE_POOL, pop.GetOperationType())
	e.settle()

	sz, err := e.rc.SetNodePoolSize(
		t.Context(),
		&containerpb.SetNodePoolSizeRequest{Name: poolPath(zone, "rest", "extra"), NodeCount: 4},
	)
	require.NoError(t, err)
	assert.Equal(t, containerpb.Operation_SET_NODE_POOL_SIZE, sz.GetOperationType())
	e.settle()

	list, err := e.rc.ListNodePools(t.Context(), &containerpb.ListNodePoolsRequest{Parent: clusterPath(zone, "rest")})
	require.NoError(t, err)
	assert.Len(t, list.GetNodePools(), 2)

	lb, err := e.rc.SetLabels(t.Context(), &containerpb.SetLabelsRequest{
		Name: clusterPath(
			zone,
			"rest",
		),
		ResourceLabels:   map[string]string{"x": "y"},
		LabelFingerprint: got.GetLabelFingerprint(),
	})
	require.NoError(t, err)
	assert.Equal(t, containerpb.Operation_SET_LABELS, lb.GetOperationType())
	e.settle()

	up, err := e.rc.UpdateCluster(t.Context(), &containerpb.UpdateClusterRequest{
		Name: clusterPath(zone, "rest"), Update: &containerpb.ClusterUpdate{DesiredMonitoringService: "none"},
	})
	require.NoError(t, err)
	assert.Equal(t, containerpb.Operation_UPDATE_CLUSTER, up.GetOperationType())
	e.settle()

	fetched, err := e.rc.GetOperation(t.Context(), &containerpb.GetOperationRequest{
		Name: zoneLoc + "/operations/" + up.GetName(),
	})
	require.NoError(t, err)
	assert.Equal(t, containerpb.Operation_DONE, fetched.GetStatus())

	ops, err := e.rc.ListOperations(t.Context(), &containerpb.ListOperationsRequest{Parent: zoneLoc})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(ops.GetOperations()), 5)

	cfg, err := e.rc.GetServerConfig(t.Context(), &containerpb.GetServerConfigRequest{Name: zoneLoc})
	require.NoError(t, err)
	assert.Len(t, cfg.GetChannels(), 4)

	del, err := e.rc.DeleteCluster(t.Context(), &containerpb.DeleteClusterRequest{Name: clusterPath(zone, "rest")})
	require.NoError(t, err)
	assert.Equal(t, containerpb.Operation_DELETE_CLUSTER, del.GetOperationType())
	e.settle()

	listC, err := e.rc.ListClusters(t.Context(), &containerpb.ListClustersRequest{Parent: zoneLoc})
	require.NoError(t, err)
	assert.Empty(t, listC.GetClusters())
}

func (e *env) raw(t *testing.T, method, path string, body any) (int, map[string]any) {
	t.Helper()

	var rdr io.Reader

	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(t, err)

		rdr = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(t.Context(), method, "http://"+e.h.Addr()+path, rdr)
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	out := map[string]any{}
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	if len(raw) > 0 {
		require.NoError(t, json.Unmarshal(raw, &out), string(raw))
	}

	return resp.StatusCode, out
}

func TestRESTWireShape(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	status, op := e.raw(t, http.MethodPost, "/v1/projects/"+project+"/zones/"+zone+"/clusters", map[string]any{
		"cluster": map[string]any{"name": "wire", "initialNodeCount": 1},
	})
	require.Equal(t, http.StatusOK, status, op)
	assert.Equal(t, "CREATE_CLUSTER", op["operationType"])
	assert.Equal(t, "RUNNING", op["status"])
	assert.Equal(t, zone, op["zone"])
	assert.Contains(t, op["selfLink"], "/operations/")

	e.settle()

	status, cl := e.raw(t, http.MethodGet, "/v1/projects/"+project+"/zones/"+zone+"/clusters/wire", nil)
	require.Equal(t, http.StatusOK, status, cl)
	assert.Equal(t, "RUNNING", cl["status"])
	assert.Equal(t, "wire", cl["name"])
	assert.InDelta(t, 1, cl["currentNodeCount"], 0, "int32 counts are JSON numbers")

	pools, ok := cl["nodePools"].([]any)
	require.True(t, ok)

	pool, ok := pools[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "default-pool", pool["name"])

	status, list := e.raw(t, http.MethodGet, "/v1/projects/"+project+"/locations/-/clusters", nil)
	require.Equal(t, http.StatusOK, status)
	assert.Len(t, list["clusters"], 1)

	status, errBody := e.raw(t, http.MethodGet, "/v1/projects/"+project+"/zones/"+zone+"/clusters/ghost", nil)
	assert.Equal(t, http.StatusNotFound, status)

	errObj, ok := errBody["error"].(map[string]any)
	require.True(t, ok, errBody)
	assert.Equal(t, "NOT_FOUND", errObj["status"])
	assert.Contains(t, errObj["message"], "Not found: ")

	status, errBody = e.raw(t, http.MethodPost, "/v1/projects/"+project+"/zones/"+zone+"/clusters", map[string]any{
		"cluster": map[string]any{"name": "BAD"},
	})
	assert.Equal(t, http.StatusBadRequest, status)

	errObj, ok = errBody["error"].(map[string]any)
	require.True(t, ok, errBody)
	assert.Equal(t, "INVALID_ARGUMENT", errObj["status"])
	assert.NotEmpty(t, errObj["details"])
}
