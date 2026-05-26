package memorydb_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

// TestRefinement3_DescribeParameters_OK tests DescribeParameters returns parameters for known group.
func TestRefinement3_DescribeParameters_OK(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateParameterGroup", map[string]any{
		"ParameterGroupName": "my-pg",
		"Family":             "memorydb_redis7",
	})

	rec := doRequest(t, h, "DescribeParameters", map[string]any{"ParameterGroupName": "my-pg"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotNil(t, out["Parameters"])
}

// TestRefinement3_DescribeParameters_NotFound tests DescribeParameters returns 404 for unknown group.
func TestRefinement3_DescribeParameters_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeParameters", map[string]any{"ParameterGroupName": "no-such-pg"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement3_DescribeParameters_BadJSON tests DescribeParameters with bad JSON.
func TestRefinement3_DescribeParameters_BadJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequestRaw(t, h, "DescribeParameters", []byte("not-json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_ResetParameterGroup_OK tests ResetParameterGroup succeeds for known group.
func TestRefinement3_ResetParameterGroup_OK(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateParameterGroup", map[string]any{
		"ParameterGroupName": "reset-pg",
		"Family":             "memorydb_redis7",
	})

	rec := doRequest(t, h, "ResetParameterGroup", map[string]any{
		"ParameterGroupName": "reset-pg",
		"AllParameters":      true,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement3_ResetParameterGroup_NotFound tests ResetParameterGroup returns 404 for unknown group.
func TestRefinement3_ResetParameterGroup_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ResetParameterGroup", map[string]any{"ParameterGroupName": "no-such-pg"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement3_ResetParameterGroup_BadJSON tests ResetParameterGroup with bad JSON.
func TestRefinement3_ResetParameterGroup_BadJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequestRaw(t, h, "ResetParameterGroup", []byte("not-json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_FailoverShard_OK tests FailoverShard succeeds for known cluster.
func TestRefinement3_FailoverShard_OK(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "fs-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})

	rec := doRequest(t, h, "FailoverShard", map[string]any{"ClusterName": "fs-cluster"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement3_FailoverShard_MissingName tests FailoverShard returns 400 for missing ClusterName.
func TestRefinement3_FailoverShard_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "FailoverShard", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_FailoverShard_NotFound tests FailoverShard returns 404 for unknown cluster.
func TestRefinement3_FailoverShard_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "FailoverShard", map[string]any{"ClusterName": "no-such-cluster"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement3_FailoverShard_BadJSON tests FailoverShard with bad JSON.
func TestRefinement3_FailoverShard_BadJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequestRaw(t, h, "FailoverShard", []byte("not-json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_ListAllowedNodeTypeUpdates_OK tests the happy path.
func TestRefinement3_ListAllowedNodeTypeUpdates_OK(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "nt-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})

	rec := doRequest(t, h, "ListAllowedNodeTypeUpdates", map[string]any{"ClusterName": "nt-cluster"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotNil(t, out["ScaleUpNodeTypes"])
	assert.NotNil(t, out["ScaleDownNodeTypes"])
}

// TestRefinement3_ListAllowedNodeTypeUpdates_MissingName tests 400 for missing ClusterName.
func TestRefinement3_ListAllowedNodeTypeUpdates_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListAllowedNodeTypeUpdates", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_ListAllowedNodeTypeUpdates_NotFound tests 404 for unknown cluster.
func TestRefinement3_ListAllowedNodeTypeUpdates_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListAllowedNodeTypeUpdates", map[string]any{"ClusterName": "no-such-cluster"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement3_ListAllowedNodeTypeUpdates_BadJSON tests with bad JSON.
func TestRefinement3_ListAllowedNodeTypeUpdates_BadJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequestRaw(t, h, "ListAllowedNodeTypeUpdates", []byte("not-json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_ListAllowedMultiRegionClusterUpdates_OK tests the happy path.
func TestRefinement3_ListAllowedMultiRegionClusterUpdates_OK(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateMultiRegionCluster", map[string]any{
		"MultiRegionClusterNameSuffix": "mrc1",
		"NodeType":                     "db.r6g.large",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var mrcOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &mrcOut))
	mrc := mrcOut["MultiRegionCluster"].(map[string]any)
	mrcName := mrc["MultiRegionClusterName"].(string)

	rec2 := doRequest(t, h, "ListAllowedMultiRegionClusterUpdates", map[string]any{
		"MultiRegionClusterName": mrcName,
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// TestRefinement3_ListAllowedMultiRegionClusterUpdates_MissingName tests 400 for missing name.
func TestRefinement3_ListAllowedMultiRegionClusterUpdates_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListAllowedMultiRegionClusterUpdates", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_ListAllowedMultiRegionClusterUpdates_NotFound tests 404 for unknown cluster.
func TestRefinement3_ListAllowedMultiRegionClusterUpdates_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListAllowedMultiRegionClusterUpdates", map[string]any{
		"MultiRegionClusterName": "no-such-mrc",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement3_ListAllowedMultiRegionClusterUpdates_BadJSON tests with bad JSON.
func TestRefinement3_ListAllowedMultiRegionClusterUpdates_BadJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequestRaw(t, h, "ListAllowedMultiRegionClusterUpdates", []byte("not-json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_UpdateMultiRegionCluster_OK tests updating a multi-region cluster.
func TestRefinement3_UpdateMultiRegionCluster_OK(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateMultiRegionCluster", map[string]any{
		"MultiRegionClusterNameSuffix": "upd1",
		"NodeType":                     "db.r6g.large",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var mrcOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &mrcOut))
	mrc := mrcOut["MultiRegionCluster"].(map[string]any)
	mrcName := mrc["MultiRegionClusterName"].(string)

	rec2 := doRequest(t, h, "UpdateMultiRegionCluster", map[string]any{
		"MultiRegionClusterName": mrcName,
		"Description":            "updated description",
	})
	assert.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))
	updated := out["MultiRegionCluster"].(map[string]any)
	assert.Equal(t, "updated description", updated["Description"])
}

// TestRefinement3_UpdateMultiRegionCluster_MissingName tests 400 for missing name.
func TestRefinement3_UpdateMultiRegionCluster_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UpdateMultiRegionCluster", map[string]any{"Description": "desc"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_UpdateMultiRegionCluster_NotFound tests 404 for unknown cluster.
func TestRefinement3_UpdateMultiRegionCluster_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UpdateMultiRegionCluster", map[string]any{
		"MultiRegionClusterName": "no-such-mrc",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement3_UpdateMultiRegionCluster_BadJSON tests with bad JSON.
func TestRefinement3_UpdateMultiRegionCluster_BadJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequestRaw(t, h, "UpdateMultiRegionCluster", []byte("not-json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_DescribeServiceUpdates tests the DescribeServiceUpdates operation.
func TestRefinement3_DescribeServiceUpdates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeServiceUpdates", map[string]any{})

	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotNil(t, out["ServiceUpdates"])
}

// TestRefinement3_DescribeParameters_Backend tests DescribeParameters backend directly.
func TestRefinement3_DescribeParameters_Backend(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend()

	_, err := b.CreateParameterGroup("us-east-1", "123456789012", &memorydb.ExportedCreateParameterGroupRequest{
		ParameterGroupName: "test-pg",
		Family:             "memorydb_redis7",
	})
	require.NoError(t, err)

	params, err := b.DescribeParameters("test-pg")
	require.NoError(t, err)
	assert.NotNil(t, params)
}

// TestRefinement3_DescribeParameters_Backend_NotFound tests DescribeParameters returns error for unknown group.
func TestRefinement3_DescribeParameters_Backend_NotFound(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend()

	_, err := b.DescribeParameters("no-such-group")
	require.Error(t, err)
}

// TestRefinement3_ResetParameterGroup_Backend tests ResetParameterGroup backend directly.
func TestRefinement3_ResetParameterGroup_Backend(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend()

	_, err := b.CreateParameterGroup("us-east-1", "123456789012", &memorydb.ExportedCreateParameterGroupRequest{
		ParameterGroupName: "reset-pg",
		Family:             "memorydb_redis7",
	})
	require.NoError(t, err)

	pg, err := b.ResetParameterGroup("reset-pg", nil, true)
	require.NoError(t, err)
	assert.Equal(t, "reset-pg", pg.Name)
}

// TestRefinement3_FailoverShard_Backend tests FailoverShard backend directly.
func TestRefinement3_FailoverShard_Backend(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend()
	b.AddClusterInternal("fs-cluster", "db.r6g.large")

	cl, err := b.FailoverShard("fs-cluster", "")
	require.NoError(t, err)
	assert.Equal(t, "fs-cluster", cl.Name)
}

// TestRefinement3_ListAllowedNodeTypeUpdates_Backend tests backend directly.
func TestRefinement3_ListAllowedNodeTypeUpdates_Backend(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend()
	b.AddClusterInternal("nt-cluster", "db.r6g.large")

	types, err := b.ListAllowedNodeTypeUpdates("nt-cluster")
	require.NoError(t, err)
	assert.NotEmpty(t, types)
}

// TestRefinement3_ListAllowedMultiRegionClusterUpdates_Backend tests backend directly.
func TestRefinement3_ListAllowedMultiRegionClusterUpdates_Backend(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend()

	_, err := b.CreateMultiRegionCluster("us-east-1", "123456789012", &memorydb.ExportedCreateMultiRegionClusterRequest{
		MultiRegionClusterNameSuffix: "mrc-test",
		NodeType:                     "db.r6g.large",
	})
	require.NoError(t, err)

	types, err := b.ListAllowedMultiRegionClusterUpdates("virv-mrc-test")
	require.NoError(t, err)
	assert.NotEmpty(t, types)
}

// TestRefinement3_ListAllowedMultiRegionClusterUpdates_Backend_NotFound tests error for unknown cluster.
func TestRefinement3_ListAllowedMultiRegionClusterUpdates_Backend_NotFound(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend()

	_, err := b.ListAllowedMultiRegionClusterUpdates("no-such-mrc")
	require.Error(t, err)
}

// TestRefinement3_UpdateMultiRegionCluster_Backend tests UpdateMultiRegionCluster backend directly.
func TestRefinement3_UpdateMultiRegionCluster_Backend(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend()

	_, err := b.CreateMultiRegionCluster("us-east-1", "123456789012", &memorydb.ExportedCreateMultiRegionClusterRequest{
		MultiRegionClusterNameSuffix: "upd-test",
		NodeType:                     "db.r6g.large",
	})
	require.NoError(t, err)

	mrc, err := b.UpdateMultiRegionCluster(&memorydb.ExportedUpdateMultiRegionClusterRequest{
		MultiRegionClusterName: "virv-upd-test",
		Description:            "updated",
		NodeType:               "db.r6g.xlarge",
	})
	require.NoError(t, err)
	assert.Equal(t, "updated", mrc.Description)
	assert.Equal(t, "db.r6g.xlarge", mrc.NodeType)
}

// TestRefinement3_UpdateMultiRegionCluster_Backend_NotFound tests error for unknown cluster.
func TestRefinement3_UpdateMultiRegionCluster_Backend_NotFound(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend()

	_, err := b.UpdateMultiRegionCluster(&memorydb.ExportedUpdateMultiRegionClusterRequest{
		MultiRegionClusterName: "no-such-mrc",
	})
	require.Error(t, err)
}

// TestRefinement3_DeepCopyOnDescribeClusters verifies that mutating returned slices does not affect stored data.
func TestRefinement3_DeepCopyOnDescribeClusters(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend()
	b.AddClusterInternal("copy-cluster", "db.r6g.large")

	clusters1, err := b.DescribeClusters("")
	require.NoError(t, err)
	require.Len(t, clusters1, 1)

	// Mutate the returned copy
	clusters1[0].Name = "mutated"

	// Original should be unchanged
	clusters2, err := b.DescribeClusters("")
	require.NoError(t, err)
	require.Len(t, clusters2, 1)
	assert.Equal(t, "copy-cluster", clusters2[0].Name)
}

// TestRefinement3_DescribeParameters_Sorted verifies DescribeParameters returns sorted results.
func TestRefinement3_DescribeParameters_Sorted(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend()

	_, err := b.CreateParameterGroup("us-east-1", "123456789012", &memorydb.ExportedCreateParameterGroupRequest{
		ParameterGroupName: "sort-pg",
		Family:             "memorydb_redis7",
	})
	require.NoError(t, err)

	h := memorydb.NewHandler(b)

	rec := doRequest(t, h, "DescribeParameters", map[string]any{
		"ParameterGroupName": "sort-pg",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	params, _ := out["Parameters"].([]any)
	for i := 1; i < len(params); i++ {
		prev := params[i-1].(map[string]any)["Name"].(string)
		curr := params[i].(map[string]any)["Name"].(string)
		assert.LessOrEqual(t, prev, curr, "parameters should be sorted by name")
	}
}

// TestRefinement3_UpdateMultiRegionCluster_NodeType verifies NodeType is updated.
func TestRefinement3_UpdateMultiRegionCluster_NodeType(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend()

	_, err := b.CreateMultiRegionCluster("us-east-1", "123456789012", &memorydb.ExportedCreateMultiRegionClusterRequest{
		MultiRegionClusterNameSuffix: "nt-test",
		NodeType:                     "db.r6g.large",
	})
	require.NoError(t, err)

	mrc, err := b.UpdateMultiRegionCluster(&memorydb.ExportedUpdateMultiRegionClusterRequest{
		MultiRegionClusterName: "virv-nt-test",
		NodeType:               "db.r6g.2xlarge",
	})
	require.NoError(t, err)
	assert.Equal(t, "db.r6g.2xlarge", mrc.NodeType)
}

// TestRefinement3_ARNIndex_NewOps verifies the ARN index is properly maintained for new operations.
func TestRefinement3_ARNIndex_NewOps(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend()

	initialSize := memorydb.ARNIndexSize(b)

	b.AddClusterInternal("idx-cluster", "db.r6g.large")
	assert.Equal(t, initialSize+1, memorydb.ARNIndexSize(b))

	b.AddSnapshotInternal("idx-snap", "idx-cluster")
	assert.Equal(t, initialSize+2, memorydb.ARNIndexSize(b))
}
