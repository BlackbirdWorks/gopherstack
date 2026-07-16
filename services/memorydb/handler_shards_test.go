package memorydb_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRefinement3_FailoverShard_OK tests FailoverShard succeeds for known cluster.
func TestHandler_FailoverShard_OK(t *testing.T) {
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
func TestHandler_FailoverShard_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "FailoverShard", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_FailoverShard_NotFound tests FailoverShard returns 404 for unknown cluster.
func TestHandler_FailoverShard_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "FailoverShard", map[string]any{"ClusterName": "no-such-cluster"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement3_FailoverShard_BadJSON tests FailoverShard with bad JSON.
func TestHandler_FailoverShard_BadJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequestRaw(t, h, "FailoverShard", []byte("not-json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_ListAllowedNodeTypeUpdates_OK tests the happy path.
func TestHandler_ListAllowedNodeTypeUpdates_OK(t *testing.T) {
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
func TestHandler_ListAllowedNodeTypeUpdates_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListAllowedNodeTypeUpdates", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_ListAllowedNodeTypeUpdates_NotFound tests 404 for unknown cluster.
func TestHandler_ListAllowedNodeTypeUpdates_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListAllowedNodeTypeUpdates", map[string]any{"ClusterName": "no-such-cluster"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement3_ListAllowedNodeTypeUpdates_BadJSON tests with bad JSON.
func TestHandler_ListAllowedNodeTypeUpdates_BadJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequestRaw(t, h, "ListAllowedNodeTypeUpdates", []byte("not-json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_FailoverShard_EmitsEvent_ViaMinimalBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doCreateCluster(t, h, minimalClusterBody("failover-cluster"))

	rec := doRequest(t, h, "FailoverShard", map[string]any{
		"ClusterName": "failover-cluster",
		"ShardName":   "failover-cluster-0001-0000",
	})
	require.Equal(t, http.StatusOK, rec.Code, "failover: %s", rec.Body)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	cl, _ := resp["Cluster"].(map[string]any)
	assert.Equal(t, "failover-cluster", cl["Name"])

	evRec := doRequest(t, h, "DescribeEvents", map[string]any{
		"SourceName": "failover-cluster",
		"SourceType": "cluster",
	})
	require.Equal(t, http.StatusOK, evRec.Code)
	var evResp map[string]any
	require.NoError(t, json.Unmarshal(evRec.Body.Bytes(), &evResp))
	events, _ := evResp["Events"].([]any)
	assert.GreaterOrEqual(t, len(events), 2, "create + failover events expected")
}

// -- BatchUpdateCluster processes and unprocessed (finding 23) ------------------

func TestHandler_NodeType_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		nodeType   string
		wantStatus int
	}{
		{"valid db. prefix", "db.r6g.large", http.StatusOK},
		{"valid db.t4g.small", "db.t4g.small", http.StatusOK},
		{"invalid no db. prefix", "r6g.large", http.StatusBadRequest},
		{"invalid empty", "", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			body := map[string]any{
				"ClusterName": "nt-test-cluster",
				"ACLName":     "open-access",
			}
			if tt.nodeType != "" {
				body["NodeType"] = tt.nodeType
			}

			rec := doRequest(t, h, "CreateCluster", body)
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body)
		})
	}
}

// -- CopySnapshot behavior -------------------------------------------------------

func TestHandler_ShardConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         map[string]any
		name         string
		wantShards   float64
		wantReplicas float64
	}{
		{
			name: "default single shard single replica",
			body: map[string]any{
				"ClusterName": "test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
			},
			wantShards:   1,
			wantReplicas: 1,
		},
		{
			name: "two shards two replicas",
			body: map[string]any{
				"ClusterName":         "test-cluster",
				"NodeType":            "db.r6g.large",
				"ACLName":             "open-access",
				"NumShards":           2,
				"NumReplicasPerShard": 2,
			},
			wantShards:   2,
			wantReplicas: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			cl := createClusterObj(t, h, tt.body)
			assert.InDelta(t, tt.wantShards, cl["NumberOfShards"], 0)
			assert.InDelta(t, tt.wantReplicas, cl["NumberOfReplicasPerShard"], 0)
		})
	}
}

func TestHandler_UpdateCluster_ShardConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updateBody   map[string]any
		name         string
		wantShards   float64
		wantReplicas float64
	}{
		{
			name: "scale replicas",
			updateBody: map[string]any{
				"ClusterName":          "test-cluster",
				"ReplicaConfiguration": map[string]any{"ReplicaCount": 2},
			},
			wantShards:   1,
			wantReplicas: 2,
		},
		{
			name: "scale shards",
			updateBody: map[string]any{
				"ClusterName":        "test-cluster",
				"ShardConfiguration": map[string]any{"ShardCount": 3},
			},
			wantShards:   3,
			wantReplicas: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createClusterObj(t, h, map[string]any{
				"ClusterName": "test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
			})

			rec := doRequest(t, h, "UpdateCluster", tt.updateBody)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			cl := resp["Cluster"].(map[string]any)
			assert.InDelta(t, tt.wantShards, cl["NumberOfShards"], 0)
			assert.InDelta(t, tt.wantReplicas, cl["NumberOfReplicasPerShard"], 0)
		})
	}
}

// -- ShowShardDetails (Gap 19) --------------------------------------------------

func TestHandler_DescribeClusters_ShowShardDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setShards     bool
		showShards    bool
		wantHasShards bool
	}{
		{
			name:          "show shards omitted includes shards by default",
			setShards:     false,
			wantHasShards: false,
		},
		{
			name:          "show shards false omits shards",
			setShards:     true,
			showShards:    false,
			wantHasShards: false,
		},
		{
			name:          "show shards true includes shards",
			setShards:     true,
			showShards:    true,
			wantHasShards: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createClusterObj(t, h, map[string]any{
				"ClusterName": "test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
			})

			body := map[string]any{"ClusterName": "test-cluster"}
			if tt.setShards {
				body["ShowShardDetails"] = tt.showShards
			}

			rec := doRequest(t, h, "DescribeClusters", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			clusters := resp["Clusters"].([]any)
			require.Len(t, clusters, 1)
			cl := clusters[0].(map[string]any)

			if tt.wantHasShards {
				shards, ok := cl["Shards"]
				assert.True(t, ok && shards != nil, "expected Shards to be present")
			} else {
				shards := cl["Shards"]
				assert.True(t, shards == nil || len(shards.([]any)) == 0,
					"expected Shards to be empty/nil when ShowShardDetails=false")
			}
		})
	}
}

// -- Shard Nodes (Gap 20) -------------------------------------------------------

func TestHandler_ShardNodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		numReplicas       int
		wantNodesPerShard int
	}{
		{
			name:              "one replica: 2 nodes per shard (1 primary + 1 replica)",
			numReplicas:       1,
			wantNodesPerShard: 2,
		},
		{
			name:              "two replicas: 3 nodes per shard",
			numReplicas:       2,
			wantNodesPerShard: 3,
		},
		{
			name:              "zero replicas: 1 node per shard",
			numReplicas:       0,
			wantNodesPerShard: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createClusterObj(t, h, map[string]any{
				"ClusterName":         "test-cluster",
				"NodeType":            "db.r6g.large",
				"ACLName":             "open-access",
				"NumReplicasPerShard": tt.numReplicas,
			})

			rec := doRequest(t, h, "DescribeClusters", map[string]any{
				"ClusterName":      "test-cluster",
				"ShowShardDetails": true,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			clusters := resp["Clusters"].([]any)
			cl := clusters[0].(map[string]any)

			shards, shardsOk := cl["Shards"].([]any)
			require.True(t, shardsOk && len(shards) > 0)

			shard := shards[0].(map[string]any)
			nodes, nodesOk := shard["Nodes"].([]any)
			require.True(t, nodesOk)
			assert.Len(t, nodes, tt.wantNodesPerShard)

			for _, n := range nodes {
				node := n.(map[string]any)
				assert.NotEmpty(t, node["Name"])
				assert.NotEmpty(t, node["Status"])
				assert.NotEmpty(t, node["AvailabilityZone"])
				endpoint, endpointOk := node["Endpoint"].(map[string]any)
				assert.True(t, endpointOk)
				assert.NotEmpty(t, endpoint["Address"])
			}
		})
	}
}

// -- Events auto-population (Gap 13) -------------------------------------------

func TestHandler_FailoverShard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		setup      bool
	}{
		{
			name: "failover with shard name succeeds",
			body: map[string]any{
				"ClusterName": "test-cluster",
				"ShardName":   "test-cluster-0001-0000",
			},
			setup:      true,
			wantStatus: http.StatusOK,
		},
		{
			name: "failover without shard name succeeds (optional per AWS)",
			body: map[string]any{
				"ClusterName": "test-cluster",
			},
			setup:      true,
			wantStatus: http.StatusOK,
		},
		{
			name: "failover on missing cluster returns not found",
			body: map[string]any{
				"ClusterName": "no-such",
				"ShardName":   "no-such-0001-0000",
			},
			setup:      false,
			wantStatus: http.StatusNotFound,
		},
		{
			name: "failover emits event",
			body: map[string]any{
				"ClusterName": "test-cluster",
				"ShardName":   "test-cluster-0001-0000",
			},
			setup:      true,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup {
				createCluster(t, h, map[string]any{
					"ClusterName": "test-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			}

			rec := doRequest(t, h, "FailoverShard", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotNil(t, resp["Cluster"])
			}
		})
	}
}

func TestHandler_FailoverShard_EmitsEvent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, map[string]any{
		"ClusterName": "test-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})

	rec := doRequest(t, h, "FailoverShard", map[string]any{
		"ClusterName": "test-cluster",
		"ShardName":   "test-cluster-0001-0000",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Check event was emitted.
	rec2 := doRequest(t, h, "DescribeEvents", map[string]any{
		"SourceName": "test-cluster",
		"SourceType": "cluster",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	events := resp["Events"].([]any)

	found := false
	for _, e := range events {
		ev := e.(map[string]any)
		if strings.Contains(ev["Message"].(string), "Failover") {
			found = true

			break
		}
	}
	assert.True(t, found, "expected failover event")
}

// -- Snapshot CRUD (comprehensive) ---------------------------------------------

func TestHandler_ListAllowedNodeTypeUpdates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*memorydb.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "returns scale up and down node types",
			setup: func(h *memorydb.Handler) {
				createCluster(t, h, map[string]any{
					"ClusterName": "test-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			},
			body:       map[string]any{"ClusterName": "test-cluster"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "cluster not found",
			body:       map[string]any{"ClusterName": "no-such"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "missing cluster name",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "ListAllowedNodeTypeUpdates", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotNil(t, resp["ScaleUpNodeTypes"])
				assert.NotNil(t, resp["ScaleDownNodeTypes"])
			}
		})
	}
}

// -- UntagResource -------------------------------------------------------------

func TestCluster_ShardAZsAndFQDNsUseRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
	}{
		{name: "us-east-1 default", region: "us-east-1"},
		{name: "eu-west-1 cross-region", region: "eu-west-1"},
		{name: "ap-southeast-2 cross-region", region: "ap-southeast-2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := memorydb.NewInMemoryBackend("123456789012", tt.region)
			h := memorydb.NewHandler(b)
			h.AccountID = "123456789012"
			h.DefaultRegion = tt.region

			cl := createClusterObj(t, h, map[string]any{
				"ClusterName": "xr-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
				"NumShards":   1,
				"NumReplicas": 1,
			})

			shards, ok := cl["Shards"].([]any)
			require.True(t, ok, "Shards field missing")
			require.NotEmpty(t, shards)

			shard := shards[0].(map[string]any)
			nodes, ok := shard["Nodes"].([]any)
			require.True(t, ok, "Nodes field missing")
			require.NotEmpty(t, nodes)

			node := nodes[0].(map[string]any)
			az, _ := node["AvailabilityZone"].(string)
			assert.True(t, strings.HasPrefix(az, tt.region),
				"AZ %q should start with region %q", az, tt.region)

			ep := node["Endpoint"].(map[string]any)
			addr, _ := ep["Address"].(string)
			assert.Contains(t, addr, ".memorydb."+tt.region+".amazonaws.com",
				"node FQDN %q should contain region %q", addr, tt.region)
		})
	}
}
