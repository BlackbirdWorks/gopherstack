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

func decodeBody(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out))

	return out
}

func TestDescribeSnapshots_ShowDetail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		showDetail  any
		name        string
		wantShards  int
		wantPresent bool
	}{
		{name: "detail_true", showDetail: true, wantPresent: true, wantShards: 2},
		{name: "detail_false", showDetail: false},
		{name: "detail_unset"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "sd-cluster", "NodeType": "db.r6g.large", "ACLName": "open-access",
				"NumShards": 2, "NumReplicasPerShard": 1,
			})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			rec = doRequest(
				t,
				h,
				"CreateSnapshot",
				map[string]any{"ClusterName": "sd-cluster", "SnapshotName": "sd-snap"},
			)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			req := map[string]any{"SnapshotName": "sd-snap"}
			if tt.showDetail != nil {
				req["ShowDetail"] = tt.showDetail
			}

			rec = doRequest(t, h, "DescribeSnapshots", req)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			snaps, _ := decodeBody(t, rec.Body.Bytes())["Snapshots"].([]any)
			require.Len(t, snaps, 1)
			cfg, _ := snaps[0].(map[string]any)["ClusterConfiguration"].(map[string]any)
			require.NotNil(t, cfg)

			shards, ok := cfg["Shards"].([]any)
			assert.Equal(t, tt.wantPresent, ok)

			if !tt.wantPresent {
				return
			}

			require.Len(t, shards, tt.wantShards)
			first, _ := shards[0].(map[string]any)
			assert.Equal(t, "sd-cluster-0001-0000", first["Name"])
			shardCfg, _ := first["Configuration"].(map[string]any)
			assert.Equal(t, "0-8191", shardCfg["Slots"])
			assert.InDelta(t, 1, shardCfg["ReplicaCount"], 0)
			assert.NotZero(t, first["SnapshotCreationTime"])
			assert.NotContains(t, first, "Size")
		})
	}
}

func TestUpdateCluster_ReshardingPendingUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		advance      time.Duration
		newShards    int
		wantPending  bool
		wantProgress float64
	}{
		{name: "midway", newShards: 4, advance: 30 * time.Second, wantPending: true, wantProgress: 50},
		{name: "done", newShards: 4, advance: 2 * time.Minute},
		{name: "same_count", newShards: 1, advance: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
			now := time.Now()
			b.SetClock(func() time.Time { return now })

			h := memorydb.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			rec := doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "rs-cluster", "NodeType": "db.r6g.large", "ACLName": "open-access",
			})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			b.SetLifecycleDelay(time.Minute)

			rec = doRequest(t, h, "UpdateCluster", map[string]any{
				"ClusterName":        "rs-cluster",
				"ShardConfiguration": map[string]any{"ShardCount": tt.newShards},
			})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			now = now.Add(tt.advance)

			rec = doRequest(t, h, "DescribeClusters", map[string]any{"ClusterName": "rs-cluster"})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			cl, _ := decodeBody(t, rec.Body.Bytes())["Clusters"].([]any)
			require.Len(t, cl, 1)
			cluster, _ := cl[0].(map[string]any)

			pending, ok := cluster["PendingUpdates"].(map[string]any)
			assert.Equal(t, tt.wantPending, ok)

			if !tt.wantPending {
				assert.Equal(t, "available", cluster["Status"])

				return
			}

			assert.Equal(t, "updating", cluster["Status"])
			rs, _ := pending["Resharding"].(map[string]any)
			sm, _ := rs["SlotMigration"].(map[string]any)
			assert.InDelta(t, tt.wantProgress, sm["ProgressPercentage"], 0.001)
		})
	}
}

func TestUpdateMultiRegionCluster_PropagatesShardCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateMultiRegionCluster", map[string]any{
		"MultiRegionClusterNameSuffix": "prop", "NodeType": "db.r6g.large",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	mrc, _ := decodeBody(t, rec.Body.Bytes())["MultiRegionCluster"].(map[string]any)
	mrcName, _ := mrc["MultiRegionClusterName"].(string)

	rec = doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "prop-member", "NodeType": "db.r6g.large", "ACLName": "open-access",
		"MultiRegionClusterName": mrcName,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = doRequest(t, h, "UpdateMultiRegionCluster", map[string]any{
		"MultiRegionClusterName": mrcName,
		"ShardConfiguration":     map[string]any{"ShardCount": 3},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = doRequest(t, h, "DescribeClusters", map[string]any{"ClusterName": "prop-member"})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	cl, _ := decodeBody(t, rec.Body.Bytes())["Clusters"].([]any)
	require.Len(t, cl, 1)
	cluster, _ := cl[0].(map[string]any)
	assert.InDelta(t, 3, cluster["NumberOfShards"], 0)
}
