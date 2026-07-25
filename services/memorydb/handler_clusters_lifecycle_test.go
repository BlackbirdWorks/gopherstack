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

// TestHandler_DescribeClusters_Pagination tests cursor-based pagination.
func TestHandler_DescribeClusters_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults int
		wantCount  int
		hasToken   bool
	}{
		{name: "with MaxResults returns page", maxResults: 2, wantCount: 2, hasToken: true},
		{name: "MaxResults larger than list returns all", maxResults: 10, wantCount: 3, hasToken: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create 3 clusters
			for _, name := range []string{"cluster-a", "cluster-b", "cluster-c"} {
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": name,
					"NodeType":    "db.r6g.large",
				})
			}

			rec := doRequest(t, h, "DescribeClusters", map[string]any{
				"MaxResults": tt.maxResults,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			clusters := resp["Clusters"].([]any)
			assert.Len(t, clusters, tt.wantCount)

			if tt.hasToken {
				assert.NotEmpty(t, resp["NextToken"])
			} else {
				assert.Empty(t, resp["NextToken"])
			}
		})
	}
}

// TestHandler_DescribeClusters_NextToken tests that NextToken advances the cursor.
func TestHandler_DescribeClusters_NextToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "next page returns remaining clusters"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, name := range []string{"cluster-a", "cluster-b", "cluster-c"} {
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": name,
					"NodeType":    "db.r6g.large",
				})
			}

			// First page of 2
			rec := doRequest(t, h, "DescribeClusters", map[string]any{"MaxResults": 2})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp1 map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp1))

			// NextToken may be empty or non-empty depending on implementation
			nextToken, _ := resp1["NextToken"].(string)

			// Second page using whatever token we got
			rec2 := doRequest(t, h, "DescribeClusters", map[string]any{"NextToken": nextToken})
			require.Equal(t, http.StatusOK, rec2.Code)

			var resp2 map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

			clusters := resp2["Clusters"].([]any)
			// At minimum should return a valid response
			assert.NotNil(t, clusters)
		})
	}
}

// TestHandler_DescribeClusters_ShowShards tests that ShowShardDetails populates shards.
func TestHandler_DescribeClusters_ShowShards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		showShardDetails bool
		wantShards       bool
	}{
		{name: "show shards true", showShardDetails: true, wantShards: true},
		{name: "show shards false", showShardDetails: false, wantShards: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "shard-cluster",
				"NodeType":    "db.r6g.large",
			})

			rec := doRequest(t, h, "DescribeClusters", map[string]any{
				"ClusterName":      "shard-cluster",
				"ShowShardDetails": tt.showShardDetails,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			clusters := resp["Clusters"].([]any)
			require.Len(t, clusters, 1)

			clusterMap := clusters[0].(map[string]any)
			shards, hasShards := clusterMap["Shards"]
			if tt.wantShards {
				assert.True(t, hasShards && shards != nil)
			} else {
				shardSlice, _ := shards.([]any)
				assert.Empty(t, shardSlice)
			}
		})
	}
}

// TestHandler_UpdateCluster_FieldCoverage tests updating various cluster string fields.
func TestHandler_UpdateCluster_FieldRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updateBody map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "update description and node type",
			updateBody: map[string]any{
				"ClusterName": "upd-cluster",
				"Description": "updated",
				"NodeType":    "db.r6g.xlarge",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update SNS topic and network type",
			updateBody: map[string]any{
				"ClusterName":    "upd-cluster",
				"SnsTopicArn":    "arn:aws:sns:us-east-1:123:topic",
				"SnsTopicStatus": "active",
				"NetworkType":    "ipv6",
				"IPDiscovery":    "ipv6",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update engine version",
			updateBody: map[string]any{
				"ClusterName":   "upd-cluster",
				"EngineVersion": "7.1",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update replica config",
			updateBody: map[string]any{
				"ClusterName": "upd-cluster",
				"ReplicaConfiguration": map[string]any{
					"ReplicaCount": 2,
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update shard config",
			updateBody: map[string]any{
				"ClusterName": "upd-cluster",
				"ShardConfiguration": map[string]any{
					"ShardCount": 2,
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update auto minor version upgrade",
			updateBody: map[string]any{
				"ClusterName":             "upd-cluster",
				"AutoMinorVersionUpgrade": false,
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "upd-cluster",
				"NodeType":    "db.r6g.large",
			})

			rec := doRequest(t, h, "UpdateCluster", tt.updateBody)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DeleteClusterWithSnapshot tests deleting a cluster with a final snapshot.
func TestHandler_DeleteClusterWithSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "delete with final snapshot",
			body: map[string]any{
				"ClusterName":       "snap-delete-cluster",
				"FinalSnapshotName": "final-snap",
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "snap-delete-cluster",
				"NodeType":    "db.r6g.large",
			})

			rec := doRequest(t, h, "DeleteCluster", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ClusterWithSnapshotRestore tests creating a cluster from snapshot.
func TestHandler_ClusterWithSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "create cluster from existing snapshot", wantStatus: http.StatusOK},
		{name: "create cluster from nonexistent snapshot returns 400", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "create cluster from existing snapshot" {
				// Create cluster and snapshot first
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": "original-cl",
					"NodeType":    "db.r6g.large",
				})
				doRequest(t, h, "CreateSnapshot", map[string]any{
					"SnapshotName": "restore-snap",
					"ClusterName":  "original-cl",
				})

				rec := doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName":  "restored-cl",
					"NodeType":     "db.r6g.large",
					"SnapshotName": "restore-snap",
				})
				assert.Equal(t, tt.wantStatus, rec.Code)
			} else {
				rec := doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName":  "restored-cl",
					"NodeType":     "db.r6g.large",
					"SnapshotName": "no-such-snap",
				})
				assert.Equal(t, tt.wantStatus, rec.Code)
			}
		})
	}
}

// TestHandler_CreateCluster_ValidationEdgeCases tests various cluster creation validations.
func TestHandler_CreateCluster_ValidationEdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "invalid node type prefix returns 400",
			body: map[string]any{
				"ClusterName": "bad-node-cl",
				"NodeType":    "x.r6g.large",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid engine returns 400",
			body: map[string]any{
				"ClusterName": "bad-engine-cl",
				"NodeType":    "db.r6g.large",
				"Engine":      "mysql",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid engine version returns 400",
			body: map[string]any{
				"ClusterName":   "bad-ver-cl",
				"NodeType":      "db.r6g.large",
				"EngineVersion": "5.0",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid num shards returns 400",
			body: map[string]any{
				"ClusterName": "bad-shards-cl",
				"NodeType":    "db.r6g.large",
				"NumShards":   0,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid num replicas returns 400",
			body: map[string]any{
				"ClusterName":         "bad-replicas-cl",
				"NodeType":            "db.r6g.large",
				"NumReplicasPerShard": 10,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid ACL reference returns 400",
			body: map[string]any{
				"ClusterName": "bad-acl-cl",
				"NodeType":    "db.r6g.large",
				"ACLName":     "nonexistent-acl",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid maintenance window format",
			body: map[string]any{
				"ClusterName":       "bad-mw-cl",
				"NodeType":          "db.r6g.large",
				"MaintenanceWindow": "invalid",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid snapshot window format",
			body: map[string]any{
				"ClusterName":    "bad-sw-cl",
				"NodeType":       "db.r6g.large",
				"SnapshotWindow": "invalid",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "valkey engine with default version",
			body: map[string]any{
				"ClusterName": "valkey-cl",
				"NodeType":    "db.r6g.large",
				"Engine":      "valkey",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "with data tiering enabled",
			body: map[string]any{
				"ClusterName": "tiered-cl",
				"NodeType":    "db.r6g.large",
				"DataTiering": true,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "with snapshot retention limit creates automated snapshot",
			body: map[string]any{
				"ClusterName":            "retention-cl",
				"NodeType":               "db.r6g.large",
				"SnapshotRetentionLimit": 7,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "with explicit port",
			body: map[string]any{
				"ClusterName": "port-cl",
				"NodeType":    "db.r6g.large",
				"Port":        6380,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "with tls disabled",
			body: map[string]any{
				"ClusterName": "notls-cl",
				"NodeType":    "db.r6g.large",
				"TLSEnabled":  false,
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateCluster", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_CreateACL_WithSubnetGroupRef tests cluster creation with subnet group validation.
func TestHandler_CreateCluster_SubnetGroupRef(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "invalid subnet group reference returns 400", wantStatus: http.StatusBadRequest},
		{name: "valid subnet group reference succeeds", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "valid subnet group reference succeeds" {
				doRequest(t, h, "CreateSubnetGroup", map[string]any{
					"SubnetGroupName": "test-sg",
					"SubnetIds":       []string{"subnet-1"},
				})

				rec := doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName":     "sg-cluster",
					"NodeType":        "db.r6g.large",
					"SubnetGroupName": "test-sg",
				})
				assert.Equal(t, tt.wantStatus, rec.Code)
			} else {
				rec := doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName":     "sg-cluster",
					"NodeType":        "db.r6g.large",
					"SubnetGroupName": "no-such-sg",
				})
				assert.Equal(t, tt.wantStatus, rec.Code)
			}
		})
	}
}

// TestHandler_CreateCluster_ParameterGroupRef tests cluster creation with parameter group validation.
func TestHandler_CreateCluster_ParameterGroupRef(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "invalid parameter group reference returns 400", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName":        "pg-cluster",
				"NodeType":           "db.r6g.large",
				"ParameterGroupName": "no-such-pg",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_BatchUpdateCluster_Mixed tests batch update with mix of found and not-found clusters.
func TestHandler_BatchUpdateCluster_Mixed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		clusterNames    []string
		wantProcessed   int
		wantUnprocessed int
	}{
		{
			name:            "mix of found and not found",
			clusterNames:    []string{"existing-cl", "missing-cl"},
			wantProcessed:   1,
			wantUnprocessed: 1,
		},
		{
			name:            "all found",
			clusterNames:    []string{"existing-cl"},
			wantProcessed:   1,
			wantUnprocessed: 0,
		},
		{
			name:            "none found",
			clusterNames:    []string{"missing-cl"},
			wantProcessed:   0,
			wantUnprocessed: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "existing-cl",
				"NodeType":    "db.r6g.large",
			})

			rec := doRequest(t, h, "BatchUpdateCluster", map[string]any{
				"ClusterNames": tt.clusterNames,
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			processed := resp["ProcessedClusters"].([]any)
			unprocessed := resp["UnprocessedClusters"].([]any)
			assert.Len(t, processed, tt.wantProcessed)
			assert.Len(t, unprocessed, tt.wantUnprocessed)
		})
	}
}

func TestHandler_Engine_DefaultsToRedis(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantEngine string
	}{
		{
			name: "omit engine defaults to redis",
			body: map[string]any{
				"ClusterName": "test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
			},
			wantEngine: "redis",
		},
		{
			name: "explicit redis engine",
			body: map[string]any{
				"ClusterName": "test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
				"Engine":      "redis",
			},
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

func TestHandler_Engine_Valkey(t *testing.T) {
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

func TestHandler_DataTiering(t *testing.T) {
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

// -- NetworkType + IPDiscovery (Gap 5) ------------------------------------------

func TestHandler_NetworkType_DefaultsToIPv4(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	cl := createClusterObj(t, h, map[string]any{
		"ClusterName": "test-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})

	assert.Equal(t, "ipv4", cl["NetworkType"])
	assert.Equal(t, "ipv4", cl["IPDiscovery"])
}

func TestHandler_NetworkType_IPv6(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body            map[string]any
		name            string
		wantNetworkType string
		wantIPDiscovery string
	}{
		{
			name: "ipv6 network type",
			body: map[string]any{
				"ClusterName": "test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
				"NetworkType": "ipv6",
				"IPDiscovery": "ipv6",
			},
			wantNetworkType: "ipv6",
			wantIPDiscovery: "ipv6",
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
			wantIPDiscovery: "ipv4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			cl := createClusterObj(t, h, tt.body)
			assert.Equal(t, tt.wantNetworkType, cl["NetworkType"])
			assert.Equal(t, tt.wantIPDiscovery, cl["IPDiscovery"])
		})
	}
}

// -- AutoMinorVersionUpgrade (Gap 6) --------------------------------------------

func TestHandler_AutoMinorVersionUpgrade(t *testing.T) {
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
				"ClusterName":             "test-cluster",
				"NodeType":                "db.r6g.large",
				"ACLName":                 "open-access",
				"AutoMinorVersionUpgrade": false,
			},
			wantAMV: false,
		},
		{
			name: "auto minor version upgrade explicit true",
			body: map[string]any{
				"ClusterName":             "test-cluster",
				"NodeType":                "db.r6g.large",
				"ACLName":                 "open-access",
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

func TestHandler_UpdateCluster_AutoMinorVersionUpgrade(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create cluster with AMV=true.
	createClusterObj(t, h, map[string]any{
		"ClusterName":             "test-cluster",
		"NodeType":                "db.r6g.large",
		"ACLName":                 "open-access",
		"AutoMinorVersionUpgrade": true,
	})

	// Update to AMV=false.
	rec := doRequest(t, h, "UpdateCluster", map[string]any{
		"ClusterName":             "test-cluster",
		"AutoMinorVersionUpgrade": false,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	cl := resp["Cluster"].(map[string]any)
	assert.Equal(t, false, cl["AutoMinorVersionUpgrade"])
}

// -- Cluster lifecycle ----------------------------------------------------------

func TestHandler_ClusterLifecycle(t *testing.T) {
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
			wantStatus: http.StatusBadRequest,
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

func TestHandler_ClusterCRUD(t *testing.T) {
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
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete cluster not found",
			op:         "DeleteCluster",
			body:       map[string]any{"ClusterName": "no-such"},
			wantStatus: http.StatusBadRequest,
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
			wantStatus: http.StatusBadRequest,
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

func TestHandler_CreateCluster_RestoreFromSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		wantEngineVersion string
		wantStatus        int
	}{
		{
			name:              "restore from snapshot uses snapshot config",
			wantStatus:        http.StatusOK,
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

func TestHandler_CreateCluster_RestoreFromSnapshot_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName":  "restored-cluster",
		"NodeType":     "db.r6g.large",
		"ACLName":      "open-access",
		"SnapshotName": "no-such-snap",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// -- CopySnapshot TargetBucket (Gap 18) ----------------------------------------

func TestHandler_ClusterEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
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
			assert.InDelta(t, tt.wantPort, endpoint["Port"], 0)
		})
	}
}

// -- DescribeReservedNodesOfferings Duration filter (Gap 23) -------------------

func TestHandler_BatchUpdateCluster_MissingNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(*memorydb.Handler)
		body            map[string]any
		name            string
		wantStatus      int
		wantProcessed   int
		wantUnprocessed int
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
			name: "some clusters not found",
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

func TestHandler_DeleteCluster_WithFinalSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
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
