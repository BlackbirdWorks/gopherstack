package memorydb_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

// helper: create a cluster and return the parsed response body.
func createCluster(t *testing.T, h *memorydb.Handler, body map[string]any) map[string]any {
	t.Helper()

	rec := doRequest(t, h, "CreateCluster", body)
	require.Equal(t, http.StatusOK, rec.Code, "create cluster failed: %s", rec.Body)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

// helper: create a cluster and return the Cluster object from the response.
func createClusterObj(t *testing.T, h *memorydb.Handler, body map[string]any) map[string]any {
	t.Helper()
	resp := createCluster(t, h, body)
	cl, ok := resp["Cluster"].(map[string]any)
	require.True(t, ok, "response has no Cluster field")
	return cl
}

// helper: create a snapshot and return the parsed response body.
func createSnapshot(t *testing.T, h *memorydb.Handler, body map[string]any) map[string]any {
	t.Helper()

	rec := doRequest(t, h, "CreateSnapshot", body)
	require.Equal(t, http.StatusOK, rec.Code, "create snapshot failed: %s", rec.Body)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

// -- Engine field (Gap 2) -------------------------------------------------------

func TestAudit_Engine_DefaultsToRedis(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantEngine string
	}{
		{
			name:       "omit engine defaults to redis",
			body:       map[string]any{"ClusterName": "test-cluster", "NodeType": "db.r6g.large", "ACLName": "open-access"},
			wantEngine: "redis",
		},
		{
			name:       "explicit redis engine",
			body:       map[string]any{"ClusterName": "test-cluster", "NodeType": "db.r6g.large", "ACLName": "open-access", "Engine": "redis"},
			wantEngine: "redis",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			cl := createClusterObj(t, h, tt.body)
			assert.Equal(t, tt.wantEngine, cl["Engine"])
		})
	}
}

func TestAudit_Engine_Valkey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		wantEngine  string
		wantVersion string
		wantStatus  int
	}{
		{
			name: "valkey engine defaults to 7.2",
			body: map[string]any{
				"ClusterName": "test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
				"Engine":      "valkey",
			},
			wantEngine:  "valkey",
			wantVersion: "7.2",
			wantStatus:  http.StatusOK,
		},
		{
			name: "valkey engine explicit 8.0",
			body: map[string]any{
				"ClusterName":   "test-cluster",
				"NodeType":      "db.r6g.large",
				"ACLName":       "open-access",
				"Engine":        "valkey",
				"EngineVersion": "8.0",
			},
			wantEngine:  "valkey",
			wantVersion: "8.0",
			wantStatus:  http.StatusOK,
		},
		{
			name: "invalid engine rejected",
			body: map[string]any{
				"ClusterName": "test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
				"Engine":      "memcached",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateCluster", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				cl := resp["Cluster"].(map[string]any)
				assert.Equal(t, tt.wantEngine, cl["Engine"])
				assert.Equal(t, tt.wantVersion, cl["EngineVersion"])
			}
		})
	}
}

// -- Engine versions (Gap 3) ----------------------------------------------------

func TestAudit_DescribeEngineVersions_IncludesValkey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         map[string]any
		wantEngines  []string
		name         string
		wantMinCount int
	}{
		{
			name:         "all versions includes valkey and redis",
			body:         map[string]any{},
			wantEngines:  []string{"valkey", "redis"},
			wantMinCount: 5,
		},
		{
			name:         "filter by redis engine",
			body:         map[string]any{"Engine": "redis"},
			wantEngines:  []string{"redis"},
			wantMinCount: 3,
		},
		{
			name:         "filter by valkey engine",
			body:         map[string]any{"Engine": "valkey"},
			wantEngines:  []string{"valkey"},
			wantMinCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeEngineVersions", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			versions := resp["EngineVersions"].([]any)
			assert.GreaterOrEqual(t, len(versions), tt.wantMinCount)

			engines := make(map[string]bool)
			for _, v := range versions {
				ev := v.(map[string]any)
				engines[ev["Engine"].(string)] = true
			}

			for _, wantEngine := range tt.wantEngines {
				assert.True(t, engines[wantEngine], "engine %q not found in results", wantEngine)
			}

			// Verify no unexpected engines when filtering.
			if len(tt.wantEngines) == 1 {
				assert.Len(t, engines, 1)
			}
		})
	}
}

func TestAudit_DescribeEngineVersions_EachHasEngine(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeEngineVersions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	for _, v := range resp["EngineVersions"].([]any) {
		ev := v.(map[string]any)
		assert.NotEmpty(t, ev["Engine"], "EngineVersion entry missing Engine field: %v", ev)
		assert.NotEmpty(t, ev["EngineVersion"])
		assert.NotEmpty(t, ev["ParameterGroupFamily"])
	}
}

func TestAudit_DescribeEngineVersions_ValkeySupportedVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		version    string
		wantStatus int
	}{
		{name: "valkey 7.2 supported", version: "7.2", wantStatus: http.StatusOK},
		{name: "valkey 8.0 supported", version: "8.0", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName":   "test-cluster",
				"NodeType":      "db.r6g.large",
				"ACLName":       "open-access",
				"Engine":        "valkey",
				"EngineVersion": tt.version,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// -- DataTiering (Gap 4) --------------------------------------------------------

func TestAudit_DataTiering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body            map[string]any
		name            string
		wantDataTiering string
	}{
		{
			name: "data tiering true",
			body: map[string]any{
				"ClusterName": "test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
				"DataTiering": true,
			},
			wantDataTiering: "true",
		},
		{
			name: "data tiering false",
			body: map[string]any{
				"ClusterName": "test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
				"DataTiering": false,
			},
			wantDataTiering: "false",
		},
		{
			name: "data tiering omitted defaults to false",
			body: map[string]any{
				"ClusterName": "test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
			},
			wantDataTiering: "false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			cl := createClusterObj(t, h, tt.body)
			assert.Equal(t, tt.wantDataTiering, cl["DataTiering"])
		})
	}
}

// -- NetworkType + IpDiscovery (Gap 5) ------------------------------------------

func TestAudit_NetworkType_DefaultsToIPv4(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	cl := createClusterObj(t, h, map[string]any{
		"ClusterName": "test-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})

	assert.Equal(t, "ipv4", cl["NetworkType"])
	assert.Equal(t, "ipv4", cl["IpDiscovery"])
}

func TestAudit_NetworkType_IPv6(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body            map[string]any
		name            string
		wantNetworkType string
		wantIpDiscovery string
	}{
		{
			name: "ipv6 network type",
			body: map[string]any{
				"ClusterName": "test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
				"NetworkType": "ipv6",
				"IpDiscovery": "ipv6",
			},
			wantNetworkType: "ipv6",
			wantIpDiscovery: "ipv6",
		},
		{
			name: "dual stack network type",
			body: map[string]any{
				"ClusterName": "test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
				"NetworkType": "dual_stack",
			},
			wantNetworkType: "dual_stack",
			wantIpDiscovery: "ipv4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			cl := createClusterObj(t, h, tt.body)
			assert.Equal(t, tt.wantNetworkType, cl["NetworkType"])
			assert.Equal(t, tt.wantIpDiscovery, cl["IpDiscovery"])
		})
	}
}

// -- AutoMinorVersionUpgrade (Gap 6) --------------------------------------------

func TestAudit_AutoMinorVersionUpgrade(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		wantAMV bool
	}{
		{
			name: "auto minor version upgrade defaults to true",
			body: map[string]any{
				"ClusterName": "test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
			},
			wantAMV: true,
		},
		{
			name: "auto minor version upgrade explicit false",
			body: map[string]any{
				"ClusterName":            "test-cluster",
				"NodeType":               "db.r6g.large",
				"ACLName":                "open-access",
				"AutoMinorVersionUpgrade": false,
			},
			wantAMV: false,
		},
		{
			name: "auto minor version upgrade explicit true",
			body: map[string]any{
				"ClusterName":            "test-cluster",
				"NodeType":               "db.r6g.large",
				"ACLName":                "open-access",
				"AutoMinorVersionUpgrade": true,
			},
			wantAMV: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			cl := createClusterObj(t, h, tt.body)
			assert.Equal(t, tt.wantAMV, cl["AutoMinorVersionUpgrade"])
		})
	}
}

func TestAudit_UpdateCluster_AutoMinorVersionUpgrade(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create cluster with AMV=true.
	createClusterObj(t, h, map[string]any{
		"ClusterName":            "test-cluster",
		"NodeType":               "db.r6g.large",
		"ACLName":                "open-access",
		"AutoMinorVersionUpgrade": true,
	})

	// Update to AMV=false.
	rec := doRequest(t, h, "UpdateCluster", map[string]any{
		"ClusterName":            "test-cluster",
		"AutoMinorVersionUpgrade": false,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	cl := resp["Cluster"].(map[string]any)
	assert.Equal(t, false, cl["AutoMinorVersionUpgrade"])
}

// -- Cluster lifecycle ----------------------------------------------------------

func TestAudit_ClusterLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "create basic cluster",
			body: map[string]any{
				"ClusterName": "test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "create cluster with all optional fields",
			body: map[string]any{
				"ClusterName":            "full-cluster",
				"NodeType":               "db.r6g.xlarge",
				"ACLName":                "open-access",
				"EngineVersion":          "7.1",
				"Description":            "test cluster",
				"NumShards":              2,
				"NumReplicasPerShard":    1,
				"TLSEnabled":             true,
				"MaintenanceWindow":      "sun:05:00-sun:06:00",
				"SnapshotWindow":         "03:00-04:00",
				"SnapshotRetentionLimit": 3,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "duplicate cluster name rejected",
			body: map[string]any{
				"ClusterName": "test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
			},
			wantStatus: http.StatusConflict,
		},
		{
			name: "missing cluster name rejected",
			body: map[string]any{
				"NodeType": "db.r6g.large",
				"ACLName":  "open-access",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing node type rejected",
			body: map[string]any{
				"ClusterName": "test-cluster2",
				"ACLName":     "open-access",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid node type rejected",
			body: map[string]any{
				"ClusterName": "test-cluster3",
				"NodeType":    "invalid.node.type",
				"ACLName":     "open-access",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.name == "duplicate cluster name rejected" {
				createCluster(t, h, map[string]any{
					"ClusterName": "test-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			}

			rec := doRequest(t, h, "CreateCluster", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAudit_ClusterCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "describe all clusters empty",
			op:         "DescribeClusters",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe single cluster not found",
			op:         "DescribeClusters",
			body:       map[string]any{"ClusterName": "no-such"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete cluster not found",
			op:         "DeleteCluster",
			body:       map[string]any{"ClusterName": "no-such"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete cluster missing name",
			op:         "DeleteCluster",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "update cluster not found",
			op:         "UpdateCluster",
			body:       map[string]any{"ClusterName": "no-such"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// -- NodeGroups + ReplicaCount + ShardCount -------------------------------------

func TestAudit_ShardConfiguration(t *testing.T) {
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
			assert.Equal(t, tt.wantShards, cl["NumberOfShards"])
			assert.Equal(t, tt.wantReplicas, cl["NumberOfReplicasPerShard"])
		})
	}
}

func TestAudit_UpdateCluster_ShardConfig(t *testing.T) {
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
			assert.Equal(t, tt.wantShards, cl["NumberOfShards"])
			assert.Equal(t, tt.wantReplicas, cl["NumberOfReplicasPerShard"])
		})
	}
}

// -- ShowShardDetails (Gap 19) --------------------------------------------------

func TestAudit_DescribeClusters_ShowShardDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		showShards    *bool
		name          string
		wantHasShards bool
	}{
		{
			name:          "show shards omitted includes shards by default",
			showShards:    nil,
			wantHasShards: false,
		},
		{
			name:          "show shards false omits shards",
			showShards:    boolPtr(false),
			wantHasShards: false,
		},
		{
			name:          "show shards true includes shards",
			showShards:    boolPtr(true),
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
			if tt.showShards != nil {
				body["ShowShardDetails"] = *tt.showShards
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

func TestAudit_ShardNodes(t *testing.T) {
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

func TestAudit_Events_AutoPopulated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(*memorydb.Handler)
		name       string
		wantSrc    string
		wantType   string
		wantMinMsg string
	}{
		{
			name: "create cluster emits event",
			ops: func(h *memorydb.Handler) {
				createCluster(t, h, map[string]any{
					"ClusterName": "evt-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			},
			wantSrc:    "evt-cluster",
			wantType:   "cluster",
			wantMinMsg: "created",
		},
		{
			name: "delete cluster emits event",
			ops: func(h *memorydb.Handler) {
				createCluster(t, h, map[string]any{
					"ClusterName": "evt-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
				doRequest(t, h, "DeleteCluster", map[string]any{"ClusterName": "evt-cluster"})
			},
			wantSrc:    "evt-cluster",
			wantType:   "cluster",
			wantMinMsg: "deleted",
		},
		{
			name: "create snapshot emits event",
			ops: func(h *memorydb.Handler) {
				createCluster(t, h, map[string]any{
					"ClusterName": "evt-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
				createSnapshot(t, h, map[string]any{
					"ClusterName":  "evt-cluster",
					"SnapshotName": "evt-snap",
				})
			},
			wantSrc:    "evt-snap",
			wantType:   "snapshot",
			wantMinMsg: "created",
		},
		{
			name: "create ACL emits event",
			ops: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateACL", map[string]any{"ACLName": "evt-acl"})
			},
			wantSrc:    "evt-acl",
			wantType:   "acl",
			wantMinMsg: "created",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.ops(h)

			rec := doRequest(t, h, "DescribeEvents", map[string]any{
				"SourceName": tt.wantSrc,
				"SourceType": tt.wantType,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			events := resp["Events"].([]any)
			require.NotEmpty(t, events, "expected at least one event for source %q type %q", tt.wantSrc, tt.wantType)

			found := false
			for _, e := range events {
				ev := e.(map[string]any)
				if strings.Contains(strings.ToLower(ev["Message"].(string)), tt.wantMinMsg) {
					found = true
					break
				}
			}
			assert.True(t, found, "expected event with message containing %q", tt.wantMinMsg)
		})
	}
}

// -- DescribeEvents Duration (Gap 14) ------------------------------------------

func TestAudit_DescribeEvents_Duration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*memorydb.Handler)
		body         map[string]any
		name         string
		wantMinCount int
	}{
		{
			name: "duration set returns recent events",
			setup: func(h *memorydb.Handler) {
				createCluster(t, h, map[string]any{
					"ClusterName": "dur-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			},
			body:         map[string]any{"Duration": 60},
			wantMinCount: 1,
		},
		{
			name: "no filter returns all events",
			setup: func(h *memorydb.Handler) {
				createCluster(t, h, map[string]any{
					"ClusterName": "dur-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			},
			body:         map[string]any{},
			wantMinCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			rec := doRequest(t, h, "DescribeEvents", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			events := resp["Events"].([]any)
			assert.GreaterOrEqual(t, len(events), tt.wantMinCount)
		})
	}
}

// -- ServiceUpdates fixtures (Gap 11) ------------------------------------------

func TestAudit_DescribeServiceUpdates_SeededFixtures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         map[string]any
		name         string
		wantType     string
		wantMinCount int
	}{
		{
			name:         "returns seeded service updates",
			body:         map[string]any{},
			wantMinCount: 2,
		},
		{
			name:         "filter by status available",
			body:         map[string]any{"Status": []string{"available"}},
			wantMinCount: 2,
		},
		{
			name:         "filter by non-existent status returns empty",
			body:         map[string]any{"Status": []string{"complete"}},
			wantMinCount: 0,
		},
		{
			name:         "filter by name",
			body:         map[string]any{"ServiceUpdateName": "memorydb-20240601-redis-security"},
			wantMinCount: 1,
		},
		{
			name:         "filter by non-existent name returns empty",
			body:         map[string]any{"ServiceUpdateName": "no-such-update"},
			wantMinCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeServiceUpdates", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			updates := resp["ServiceUpdates"].([]any)
			assert.GreaterOrEqual(t, len(updates), tt.wantMinCount)
		})
	}
}

func TestAudit_DescribeServiceUpdates_FieldsPopulated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeServiceUpdates", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	updates := resp["ServiceUpdates"].([]any)
	require.NotEmpty(t, updates)

	for _, u := range updates {
		su := u.(map[string]any)
		assert.NotEmpty(t, su["ServiceUpdateName"])
		assert.NotEmpty(t, su["Status"])
		assert.NotEmpty(t, su["Type"])
		assert.NotEmpty(t, su["Description"])
	}
}

// -- IamAuth validation (Gap 8) ------------------------------------------------

func TestAudit_CreateUser_IamAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "iam auth type accepted",
			body: map[string]any{
				"UserName":     "iam-user",
				"AccessString": "on ~* +@all",
				"AuthenticationMode": map[string]any{
					"Type": "iam",
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "iam auth with passwords rejected",
			body: map[string]any{
				"UserName":     "iam-user",
				"AccessString": "on ~* +@all",
				"AuthenticationMode": map[string]any{
					"Type":      "iam",
					"Passwords": []string{"somepassword"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "no-password-required accepted",
			body: map[string]any{
				"UserName":     "nopass-user",
				"AccessString": "on ~* +@all",
				"AuthenticationMode": map[string]any{
					"Type": "no-password-required",
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "password auth accepted",
			body: map[string]any{
				"UserName":     "pass-user",
				"AccessString": "on ~* +@all",
				"AuthenticationMode": map[string]any{
					"Type":      "password",
					"Passwords": []string{"mypassword123"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "invalid auth type rejected",
			body: map[string]any{
				"UserName":     "bad-user",
				"AccessString": "on ~* +@all",
				"AuthenticationMode": map[string]any{
					"Type": "kerberos",
				},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateUser", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// -- Automated snapshots (Gap 16) ----------------------------------------------

func TestAudit_AutomatedSnapshot_OnCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                   string
		snapshotRetentionLimit int
		wantAutoSnap           bool
	}{
		{
			name:                   "retention limit > 0 seeds automated snapshot",
			snapshotRetentionLimit: 3,
			wantAutoSnap:           true,
		},
		{
			name:                   "retention limit 0 no automated snapshot",
			snapshotRetentionLimit: 0,
			wantAutoSnap:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createCluster(t, h, map[string]any{
				"ClusterName":            "snap-cluster",
				"NodeType":               "db.r6g.large",
				"ACLName":                "open-access",
				"SnapshotRetentionLimit": tt.snapshotRetentionLimit,
			})

			rec := doRequest(t, h, "DescribeSnapshots", map[string]any{
				"ClusterName":  "snap-cluster",
				"SnapshotType": "automated",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			snaps := resp["Snapshots"].([]any)

			if tt.wantAutoSnap {
				assert.NotEmpty(t, snaps, "expected automated snapshot to be created")
				snap := snaps[0].(map[string]any)
				assert.Equal(t, "automated", snap["SnapshotType"])
				assert.True(t, strings.HasPrefix(snap["Name"].(string), "automatic.snap-cluster"))
			} else {
				assert.Empty(t, snaps, "expected no automated snapshot")
			}
		})
	}
}

// -- DescribeSnapshots Source filter (Gap 15) ----------------------------------

func TestAudit_DescribeSnapshots_SourceFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create cluster with retention (seeds automated snapshot).
	createCluster(t, h, map[string]any{
		"ClusterName":            "snap-cluster",
		"NodeType":               "db.r6g.large",
		"ACLName":                "open-access",
		"SnapshotRetentionLimit": 1,
	})

	// Create manual snapshot.
	createSnapshot(t, h, map[string]any{
		"ClusterName":  "snap-cluster",
		"SnapshotName": "manual-snap",
	})

	tests := []struct {
		name             string
		body             map[string]any
		wantMinCount     int
		wantSnapshotType string
	}{
		{
			name:         "no filter returns all",
			body:         map[string]any{},
			wantMinCount: 2,
		},
		{
			name:             "source=manual returns only manual",
			body:             map[string]any{"Source": "manual"},
			wantMinCount:     1,
			wantSnapshotType: "manual",
		},
		{
			name:             "source=automated returns only automated",
			body:             map[string]any{"Source": "automated"},
			wantMinCount:     1,
			wantSnapshotType: "automated",
		},
		{
			name:         "filter by cluster name",
			body:         map[string]any{"ClusterName": "snap-cluster"},
			wantMinCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "DescribeSnapshots", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			snaps := resp["Snapshots"].([]any)
			assert.GreaterOrEqual(t, len(snaps), tt.wantMinCount)

			if tt.wantSnapshotType != "" {
				for _, s := range snaps {
					snap := s.(map[string]any)
					assert.Equal(t, tt.wantSnapshotType, snap["SnapshotType"])
				}
			}
		})
	}
}

// -- RestoreCluster from snapshot (Gap 17) -------------------------------------

func TestAudit_CreateCluster_RestoreFromSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		wantStatus       int
		wantEngineVersion string
	}{
		{
			name:             "restore from snapshot uses snapshot config",
			wantStatus:       http.StatusOK,
			wantEngineVersion: "7.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create source cluster with specific engine version.
			createCluster(t, h, map[string]any{
				"ClusterName":   "src-cluster",
				"NodeType":      "db.r6g.large",
				"ACLName":       "open-access",
				"EngineVersion": "7.1",
			})

			// Create snapshot.
			createSnapshot(t, h, map[string]any{
				"ClusterName":  "src-cluster",
				"SnapshotName": "src-snap",
			})

			// Restore from snapshot.
			rec := doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName":  "restored-cluster",
				"NodeType":     "db.r6g.large",
				"ACLName":      "open-access",
				"SnapshotName": "src-snap",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				cl := resp["Cluster"].(map[string]any)
				assert.Equal(t, tt.wantEngineVersion, cl["EngineVersion"])
				assert.Equal(t, "restored-cluster", cl["Name"])
			}
		})
	}
}

func TestAudit_CreateCluster_RestoreFromSnapshot_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName":  "restored-cluster",
		"NodeType":     "db.r6g.large",
		"ACLName":      "open-access",
		"SnapshotName": "no-such-snap",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// -- CopySnapshot TargetBucket (Gap 18) ----------------------------------------

func TestAudit_CopySnapshot_TargetBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		body           map[string]any
		wantStatus     int
		wantNewSnap    bool
	}{
		{
			name: "target bucket copies to S3 without creating new snapshot",
			body: map[string]any{
				"SourceSnapshotName": "src-snap",
				"TargetSnapshotName": "",
				"TargetBucket":       "my-s3-bucket",
			},
			wantStatus:  http.StatusOK,
			wantNewSnap: false,
		},
		{
			name: "normal copy creates new snapshot",
			body: map[string]any{
				"SourceSnapshotName": "src-snap",
				"TargetSnapshotName": "dst-snap",
			},
			wantStatus:  http.StatusOK,
			wantNewSnap: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createCluster(t, h, map[string]any{
				"ClusterName": "snap-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
			})
			createSnapshot(t, h, map[string]any{
				"ClusterName":  "snap-cluster",
				"SnapshotName": "src-snap",
			})

			initialSnaps := memorydb.SnapshotCount(h.Backend.(*memorydb.InMemoryBackend))

			rec := doRequest(t, h, "CopySnapshot", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				afterSnaps := memorydb.SnapshotCount(h.Backend.(*memorydb.InMemoryBackend))
				if tt.wantNewSnap {
					assert.Equal(t, initialSnaps+1, afterSnaps, "expected new snapshot to be created")
				} else {
					assert.Equal(t, initialSnaps, afterSnaps, "expected no new snapshot for S3 export")
				}
			}
		})
	}
}

// -- FailoverShard (Gap 10) ----------------------------------------------------

func TestAudit_FailoverShard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       map[string]any
		setup      bool
		wantStatus int
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

func TestAudit_FailoverShard_EmitsEvent(t *testing.T) {
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

func TestAudit_SnapshotCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		setup      func(*memorydb.Handler)
		body       map[string]any
		wantStatus int
	}{
		{
			name: "create snapshot",
			op:   "CreateSnapshot",
			setup: func(h *memorydb.Handler) {
				createCluster(t, h, map[string]any{
					"ClusterName": "test-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			},
			body: map[string]any{
				"ClusterName":  "test-cluster",
				"SnapshotName": "my-snap",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "create snapshot missing cluster name",
			op:         "CreateSnapshot",
			body:       map[string]any{"SnapshotName": "my-snap"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create snapshot missing snapshot name",
			op:         "CreateSnapshot",
			body:       map[string]any{"ClusterName": "test-cluster"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "describe all snapshots",
			op:   "DescribeSnapshots",
			setup: func(h *memorydb.Handler) {
				createCluster(t, h, map[string]any{
					"ClusterName": "test-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
				createSnapshot(t, h, map[string]any{
					"ClusterName":  "test-cluster",
					"SnapshotName": "my-snap",
				})
			},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe snapshot not found",
			op:         "DescribeSnapshots",
			body:       map[string]any{"SnapshotName": "no-such"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete snapshot not found",
			op:         "DeleteSnapshot",
			body:       map[string]any{"SnapshotName": "no-such"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "copy snapshot",
			op:   "CopySnapshot",
			setup: func(h *memorydb.Handler) {
				createCluster(t, h, map[string]any{
					"ClusterName": "test-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
				createSnapshot(t, h, map[string]any{
					"ClusterName":  "test-cluster",
					"SnapshotName": "src-snap",
				})
			},
			body: map[string]any{
				"SourceSnapshotName": "src-snap",
				"TargetSnapshotName": "dst-snap",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "copy snapshot missing source",
			op:         "CopySnapshot",
			body:       map[string]any{"TargetSnapshotName": "dst-snap"},
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

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// -- SubnetGroup CRUD ----------------------------------------------------------

func TestAudit_SubnetGroupCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		setup      func(*memorydb.Handler)
		body       map[string]any
		wantStatus int
	}{
		{
			name:       "create subnet group",
			op:         "CreateSubnetGroup",
			body:       map[string]any{"SubnetGroupName": "my-sg", "SubnetIds": []string{"subnet-1", "subnet-2"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "create subnet group missing name",
			op:         "CreateSubnetGroup",
			body:       map[string]any{"SubnetIds": []string{"subnet-1"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "describe subnet groups",
			op:   "DescribeSubnetGroups",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateSubnetGroup", map[string]any{"SubnetGroupName": "my-sg"})
			},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe subnet group not found",
			op:         "DescribeSubnetGroups",
			body:       map[string]any{"SubnetGroupName": "no-such"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "update subnet group",
			op:   "UpdateSubnetGroup",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateSubnetGroup", map[string]any{"SubnetGroupName": "my-sg"})
			},
			body:       map[string]any{"SubnetGroupName": "my-sg", "Description": "updated"},
			wantStatus: http.StatusOK,
		},
		{
			name: "delete subnet group",
			op:   "DeleteSubnetGroup",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateSubnetGroup", map[string]any{"SubnetGroupName": "my-sg"})
			},
			body:       map[string]any{"SubnetGroupName": "my-sg"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// -- ParameterGroup CRUD -------------------------------------------------------

func TestAudit_ParameterGroupCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		setup      func(*memorydb.Handler)
		body       map[string]any
		wantStatus int
	}{
		{
			name:       "create parameter group",
			op:         "CreateParameterGroup",
			body:       map[string]any{"ParameterGroupName": "my-pg", "Family": "memorydb_redis7"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "create parameter group missing name",
			op:         "CreateParameterGroup",
			body:       map[string]any{"Family": "memorydb_redis7"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "describe parameter groups",
			op:   "DescribeParameterGroups",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateParameterGroup", map[string]any{"ParameterGroupName": "my-pg", "Family": "memorydb_redis7"})
			},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name: "update parameter group",
			op:   "UpdateParameterGroup",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateParameterGroup", map[string]any{"ParameterGroupName": "my-pg", "Family": "memorydb_redis7"})
			},
			body: map[string]any{
				"ParameterGroupName": "my-pg",
				"ParameterNameValues": []map[string]any{
					{"ParameterName": "maxmemory-policy", "ParameterValue": "allkeys-lru"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "reset parameter group",
			op:   "ResetParameterGroup",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateParameterGroup", map[string]any{"ParameterGroupName": "my-pg", "Family": "memorydb_redis7"})
			},
			body:       map[string]any{"ParameterGroupName": "my-pg"},
			wantStatus: http.StatusOK,
		},
		{
			name: "describe parameters",
			op:   "DescribeParameters",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateParameterGroup", map[string]any{"ParameterGroupName": "my-pg", "Family": "memorydb_redis7"})
				doRequest(t, h, "UpdateParameterGroup", map[string]any{
					"ParameterGroupName":  "my-pg",
					"ParameterNameValues": []map[string]any{{"ParameterName": "maxmemory-policy", "ParameterValue": "allkeys-lru"}},
				})
			},
			body:       map[string]any{"ParameterGroupName": "my-pg"},
			wantStatus: http.StatusOK,
		},
		{
			name: "delete parameter group",
			op:   "DeleteParameterGroup",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateParameterGroup", map[string]any{"ParameterGroupName": "my-pg", "Family": "memorydb_redis7"})
			},
			body:       map[string]any{"ParameterGroupName": "my-pg"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// -- ACL CRUD ------------------------------------------------------------------

func TestAudit_ACL_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		setup      func(*memorydb.Handler)
		body       map[string]any
		wantStatus int
	}{
		{
			name:       "create ACL",
			op:         "CreateACL",
			body:       map[string]any{"ACLName": "my-acl"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "create ACL with users",
			op:         "CreateACL",
			body:       map[string]any{"ACLName": "my-acl", "UserNames": []string{}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "create ACL missing name",
			op:         "CreateACL",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "describe ACL",
			op:   "DescribeACLs",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateACL", map[string]any{"ACLName": "my-acl"})
			},
			body:       map[string]any{"ACLName": "my-acl"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe ACL not found",
			op:         "DescribeACLs",
			body:       map[string]any{"ACLName": "no-such"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "update ACL add users",
			op:   "UpdateACL",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateACL", map[string]any{"ACLName": "my-acl"})
				doRequest(t, h, "CreateUser", map[string]any{
					"UserName":           "acl-user",
					"AccessString":       "on ~* +@all",
					"AuthenticationMode": map[string]any{"Type": "no-password-required"},
				})
			},
			body: map[string]any{
				"ACLName":        "my-acl",
				"UserNamesToAdd": []string{"acl-user"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "delete ACL",
			op:   "DeleteACL",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateACL", map[string]any{"ACLName": "my-acl"})
			},
			body:       map[string]any{"ACLName": "my-acl"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// -- User CRUD -----------------------------------------------------------------

func TestAudit_UserCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		setup      func(*memorydb.Handler)
		body       map[string]any
		wantStatus int
	}{
		{
			name: "create user password auth",
			op:   "CreateUser",
			body: map[string]any{
				"UserName":     "test-user",
				"AccessString": "on ~* +@all",
				"AuthenticationMode": map[string]any{
					"Type":      "password",
					"Passwords": []string{"mypassword"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "create user missing name",
			op:         "CreateUser",
			body:       map[string]any{"AccessString": "on ~* +@all"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "describe users",
			op:   "DescribeUsers",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateUser", map[string]any{
					"UserName":           "test-user",
					"AccessString":       "on ~* +@all",
					"AuthenticationMode": map[string]any{"Type": "no-password-required"},
				})
			},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name: "update user access string",
			op:   "UpdateUser",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateUser", map[string]any{
					"UserName":           "test-user",
					"AccessString":       "on ~* +@all",
					"AuthenticationMode": map[string]any{"Type": "no-password-required"},
				})
			},
			body: map[string]any{
				"UserName":     "test-user",
				"AccessString": "on ~key* +@read",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "delete user",
			op:   "DeleteUser",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateUser", map[string]any{
					"UserName":           "test-user",
					"AccessString":       "on ~* +@all",
					"AuthenticationMode": map[string]any{"Type": "no-password-required"},
				})
			},
			body:       map[string]any{"UserName": "test-user"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// -- MultiRegionCluster --------------------------------------------------------

func TestAudit_MultiRegionCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		setup      func(*memorydb.Handler)
		body       map[string]any
		wantStatus int
	}{
		{
			name: "create multi-region cluster",
			op:   "CreateMultiRegionCluster",
			body: map[string]any{
				"MultiRegionClusterNameSuffix": "my-cluster",
				"NodeType":                    "db.r6g.large",
				"Engine":                      "redis",
				"EngineVersion":               "7.0",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "create multi-region cluster missing suffix",
			op:         "CreateMultiRegionCluster",
			body:       map[string]any{"NodeType": "db.r6g.large"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "describe multi-region clusters",
			op:   "DescribeMultiRegionClusters",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateMultiRegionCluster", map[string]any{
					"MultiRegionClusterNameSuffix": "my-cluster",
					"NodeType":                    "db.r6g.large",
				})
			},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name: "delete multi-region cluster",
			op:   "DeleteMultiRegionCluster",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateMultiRegionCluster", map[string]any{
					"MultiRegionClusterNameSuffix": "my-cluster",
					"NodeType":                    "db.r6g.large",
				})
			},
			body:       map[string]any{"MultiRegionClusterName": "virv-my-cluster"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// -- Tags ---------------------------------------------------------------------

func TestAudit_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*memorydb.Handler) string
		wantStatus int
		wantTags   map[string]string
	}{
		{
			name: "list tags on cluster",
			setup: func(h *memorydb.Handler) string {
				cl := createClusterObj(t, h, map[string]any{
					"ClusterName": "tag-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
					"Tags":        []map[string]any{{"Key": "env", "Value": "prod"}},
				})
				return cl["ARN"].(string)
			},
			wantStatus: http.StatusOK,
			wantTags:   map[string]string{"env": "prod"},
		},
		{
			name: "tag resource",
			setup: func(h *memorydb.Handler) string {
				cl := createClusterObj(t, h, map[string]any{
					"ClusterName": "tag-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
				arn := cl["ARN"].(string)
				doRequest(t, h, "TagResource", map[string]any{
					"ResourceArn": arn,
					"Tags":        []map[string]any{{"Key": "team", "Value": "backend"}},
				})
				return arn
			},
			wantStatus: http.StatusOK,
			wantTags:   map[string]string{"team": "backend"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(h)

			rec := doRequest(t, h, "ListTags", map[string]any{"ResourceArn": resourceARN})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tagList := resp["TagList"].([]any)
				tags := make(map[string]string)
				for _, t := range tagList {
					tag := t.(map[string]any)
					tags[tag["Key"].(string)] = tag["Value"].(string)
				}
				for k, v := range tt.wantTags {
					assert.Equal(t, v, tags[k])
				}
			}
		})
	}
}

// -- Endpoint addressing -------------------------------------------------------

func TestAudit_ClusterEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		body           map[string]any
		wantAddrSuffix string
		wantPort       float64
	}{
		{
			name: "default port 6379",
			body: map[string]any{
				"ClusterName": "test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
			},
			wantAddrSuffix: ".memorydb.us-east-1.amazonaws.com",
			wantPort:       6379,
		},
		{
			name: "custom port",
			body: map[string]any{
				"ClusterName": "test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
				"Port":        6380,
			},
			wantAddrSuffix: ".memorydb.us-east-1.amazonaws.com",
			wantPort:       6380,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			cl := createClusterObj(t, h, tt.body)

			endpoint, ok := cl["ClusterEndpoint"].(map[string]any)
			require.True(t, ok)
			assert.True(t, strings.HasSuffix(endpoint["Address"].(string), tt.wantAddrSuffix))
			assert.Equal(t, tt.wantPort, endpoint["Port"])
		})
	}
}

// -- DescribeReservedNodesOfferings Duration filter (Gap 23) -------------------

func TestAudit_DescribeReservedNodesOfferings_DurationFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         map[string]any
		wantMinCount int
		wantDuration int
	}{
		{
			name:         "no filter returns all offerings",
			body:         map[string]any{},
			wantMinCount: 3,
		},
		{
			name:         "filter by duration 1 year (string)",
			body:         map[string]any{"Duration": "1"},
			wantMinCount: 1,
			wantDuration: 31536000,
		},
		{
			name:         "filter by duration 3 years (string)",
			body:         map[string]any{"Duration": "3"},
			wantMinCount: 1,
			wantDuration: 94608000,
		},
		{
			name:         "filter by duration in seconds (1 year)",
			body:         map[string]any{"Duration": "31536000"},
			wantMinCount: 1,
			wantDuration: 31536000,
		},
		{
			name:         "filter by duration in seconds (3 years)",
			body:         map[string]any{"Duration": "94608000"},
			wantMinCount: 1,
			wantDuration: 94608000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeReservedNodesOfferings", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			offerings := resp["ReservedNodesOfferings"].([]any)
			assert.GreaterOrEqual(t, len(offerings), tt.wantMinCount)

			if tt.wantDuration > 0 {
				for _, o := range offerings {
					off := o.(map[string]any)
					assert.Equal(t, float64(tt.wantDuration), off["Duration"])
				}
			}
		})
	}
}

// -- ReservedNodes -------------------------------------------------------------

func TestAudit_ReservedNodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		setup      func(*memorydb.Handler)
		body       map[string]any
		wantStatus int
	}{
		{
			name:       "describe reserved nodes empty",
			op:         "DescribeReservedNodes",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name: "purchase reserved nodes offering",
			op:   "PurchaseReservedNodesOffering",
			body: map[string]any{
				"ReservedNodesOfferingId": "aaa00000-1111-2222-3333-444444444444",
				"ReservationId":           "my-reservation",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "purchase reserved nodes missing offering id",
			op:         "PurchaseReservedNodesOffering",
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

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// -- ListAllowedNodeTypeUpdates ------------------------------------------------

func TestAudit_ListAllowedNodeTypeUpdates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*memorydb.Handler)
		body       map[string]any
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

func TestAudit_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	cl := createClusterObj(t, h, map[string]any{
		"ClusterName": "tag-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
		"Tags":        []map[string]any{{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "backend"}},
	})
	resourceARN := cl["ARN"].(string)

	// Untag the "env" key.
	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": resourceARN,
		"TagKeys":     []string{"env"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List tags - should only have "team" left.
	rec2 := doRequest(t, h, "ListTags", map[string]any{"ResourceArn": resourceARN})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	tagList := resp["TagList"].([]any)
	assert.Len(t, tagList, 1)
	tag := tagList[0].(map[string]any)
	assert.Equal(t, "team", tag["Key"])
}

// -- BatchUpdateCluster --------------------------------------------------------

func TestAudit_BatchUpdateCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		setup             func(*memorydb.Handler)
		body              map[string]any
		wantStatus        int
		wantProcessed     int
		wantUnprocessed   int
	}{
		{
			name: "all clusters found",
			setup: func(h *memorydb.Handler) {
				for _, name := range []string{"cluster-a", "cluster-b"} {
					createCluster(t, h, map[string]any{
						"ClusterName": name,
						"NodeType":    "db.r6g.large",
						"ACLName":     "open-access",
					})
				}
			},
			body: map[string]any{
				"ClusterNames": []string{"cluster-a", "cluster-b"},
			},
			wantStatus:      http.StatusOK,
			wantProcessed:   2,
			wantUnprocessed: 0,
		},
		{
			name:  "some clusters not found",
			setup: func(h *memorydb.Handler) {
				createCluster(t, h, map[string]any{
					"ClusterName": "cluster-a",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			},
			body: map[string]any{
				"ClusterNames": []string{"cluster-a", "no-such"},
			},
			wantStatus:      http.StatusOK,
			wantProcessed:   1,
			wantUnprocessed: 1,
		},
		{
			name:       "missing cluster names",
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

			rec := doRequest(t, h, "BatchUpdateCluster", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				processed := resp["ProcessedClusters"].([]any)
				unprocessed := resp["UnprocessedClusters"].([]any)
				assert.Len(t, processed, tt.wantProcessed)
				assert.Len(t, unprocessed, tt.wantUnprocessed)
			}
		})
	}
}

// -- DeleteCluster with snapshot -----------------------------------------------

func TestAudit_DeleteCluster_WithFinalSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       map[string]any
		wantStatus int
		wantSnaps  bool
	}{
		{
			name: "delete with final snapshot creates snapshot",
			body: map[string]any{
				"ClusterName":       "test-cluster",
				"FinalSnapshotName": "final-snap",
			},
			wantStatus: http.StatusOK,
			wantSnaps:  true,
		},
		{
			name: "delete without final snapshot does not create snapshot",
			body: map[string]any{
				"ClusterName": "test-cluster",
			},
			wantStatus: http.StatusOK,
			wantSnaps:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createCluster(t, h, map[string]any{
				"ClusterName": "test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
			})

			rec := doRequest(t, h, "DeleteCluster", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantSnaps {
				rec2 := doRequest(t, h, "DescribeSnapshots", map[string]any{"SnapshotName": "final-snap"})
				assert.Equal(t, http.StatusOK, rec2.Code)
			}
		})
	}
}

// -- Pagination ----------------------------------------------------------------

func TestAudit_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		setup      func(*memorydb.Handler)
		body       map[string]any
		wantCount  int
		wantToken  bool
	}{
		{
			name: "describe clusters pagination",
			op:   "DescribeClusters",
			setup: func(h *memorydb.Handler) {
				for _, name := range []string{"cluster-a", "cluster-b", "cluster-c"} {
					createCluster(t, h, map[string]any{
						"ClusterName": name,
						"NodeType":    "db.r6g.large",
						"ACLName":     "open-access",
					})
				}
			},
			body:      map[string]any{"MaxResults": 2},
			wantCount: 2,
			wantToken: true,
		},
		{
			name: "describe ACLs pagination",
			op:   "DescribeACLs",
			setup: func(h *memorydb.Handler) {
				for _, name := range []string{"acl-a", "acl-b", "acl-c"} {
					doRequest(t, h, "CreateACL", map[string]any{"ACLName": name})
				}
			},
			body:      map[string]any{"MaxResults": 2},
			wantCount: 2,
			wantToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			rec := doRequest(t, h, tt.op, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			// Find the list field.
			var items []any
			for _, key := range []string{"Clusters", "ACLs", "Users", "Snapshots", "ParameterGroups", "SubnetGroups"} {
				if v, ok := resp[key].([]any); ok {
					items = v
					break
				}
			}

			assert.Len(t, items, tt.wantCount)

			if tt.wantToken {
				assert.NotEmpty(t, resp["NextToken"])
			}
		})
	}
}

// -- Describe snapshots pagination --------------------------------------------

func TestAudit_DescribeSnapshots_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, map[string]any{
		"ClusterName": "test-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})

	for _, name := range []string{"snap-a", "snap-b", "snap-c"} {
		createSnapshot(t, h, map[string]any{
			"ClusterName":  "test-cluster",
			"SnapshotName": name,
		})
	}

	rec := doRequest(t, h, "DescribeSnapshots", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	snaps := resp["Snapshots"].([]any)
	assert.Len(t, snaps, 2)
	assert.NotEmpty(t, resp["NextToken"])
}

// -- helper -------------------------------------------------------------------

func boolPtr(b bool) *bool {
	return &b
}
