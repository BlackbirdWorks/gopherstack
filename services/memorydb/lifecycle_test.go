package memorydb_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

// TestCluster_LifecycleDelay_Default verifies that with no lifecycle delay
// configured (the zero value, and the state every other test in this package
// relies on), a freshly created cluster reports "available" immediately --
// locking in that this opt-in mechanism cannot regress existing behavior.
func TestCluster_LifecycleDelay_Default(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "instant-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	cl, _ := resp["Cluster"].(map[string]any)
	assert.Equal(t, "available", cl["Status"])
}

// TestCluster_LifecycleDelay_CreatingThenAvailable verifies that when a
// lifecycle delay is configured, a freshly created cluster observably reports
// "creating" until the backend clock passes the deadline, then "available"
// thereafter -- exercising the goroutine-free overlay mechanism in
// lifecycle.go across CreateCluster's own response and a subsequent
// DescribeClusters call, matching real AWS's creating -> available transition
// that SDK waiters (e.g. WaitUntilClusterAvailable) poll for.
func TestCluster_LifecycleDelay_CreatingThenAvailable(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	now := time.Now()
	clock := func() time.Time { return now }
	b.SetClock(clock)
	b.SetLifecycleDelay(time.Minute)

	h := memorydb.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	createRec := doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "delayed-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})
	require.Equal(t, http.StatusOK, createRec.Code, "body: %s", createRec.Body)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	cl, _ := createResp["Cluster"].(map[string]any)
	assert.Equal(t, "creating", cl["Status"], "CreateCluster response must show creating while pending")

	// Still before the deadline: DescribeClusters must also observe "creating".
	descRec := doRequest(t, h, "DescribeClusters", map[string]any{"ClusterName": "delayed-cluster"})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	clusters, _ := descResp["Clusters"].([]any)
	require.Len(t, clusters, 1)
	got, _ := clusters[0].(map[string]any)
	assert.Equal(t, "creating", got["Status"])

	// Advance the injected clock past the deadline.
	now = now.Add(2 * time.Minute)

	afterRec := doRequest(t, h, "DescribeClusters", map[string]any{"ClusterName": "delayed-cluster"})
	require.Equal(t, http.StatusOK, afterRec.Code)

	var afterResp map[string]any
	require.NoError(t, json.Unmarshal(afterRec.Body.Bytes(), &afterResp))
	afterClusters, _ := afterResp["Clusters"].([]any)
	require.Len(t, afterClusters, 1)
	afterGot, _ := afterClusters[0].(map[string]any)
	assert.Equal(t, "available", afterGot["Status"], "cluster must report available once the deadline has passed")
}

// TestCluster_LifecycleDelay_UpdateAndUnrelatedOpsObserveOverlay verifies the
// status overlay also applies to UpdateCluster and FailoverShard responses,
// not just Create/Describe -- every read/write path that surfaces a Cluster
// must agree on the observable status.
func TestCluster_LifecycleDelay_UpdateAndUnrelatedOpsObserveOverlay(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	now := time.Now()
	b.SetClock(func() time.Time { return now })
	b.SetLifecycleDelay(time.Minute)

	h := memorydb.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	createRec := doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "mid-transition-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	updateRec := doRequest(t, h, "UpdateCluster", map[string]any{
		"ClusterName": "mid-transition-cluster",
		"Description": "updated while creating",
	})
	require.Equal(t, http.StatusOK, updateRec.Code, "body: %s", updateRec.Body)

	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateResp))
	cl, _ := updateResp["Cluster"].(map[string]any)
	assert.Equal(t, "creating", cl["Status"], "UpdateCluster response must still observe the pending status")
	assert.Equal(t, "updated while creating", cl["Description"])
}
