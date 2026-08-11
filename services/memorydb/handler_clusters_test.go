package memorydb_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		wantStatus  int
		wantCluster bool
	}{
		{
			name: "creates cluster",
			body: map[string]any{
				"ClusterName": "my-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
			},
			wantStatus:  http.StatusOK,
			wantCluster: true,
		},
		{
			name:       "missing cluster name",
			body:       map[string]any{"NodeType": "db.r6g.large"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing node type",
			body:       map[string]any{"ClusterName": "my-cluster"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing target header",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "missing target header" {
				e := echo.New()
				bodyBytes, _ := json.Marshal(tt.body)
				req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)
				err := h.Handler()(c)
				require.NoError(t, err)
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			rec := doRequest(t, h, "CreateCluster", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantCluster {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				clusterVal, ok := resp["Cluster"]
				require.True(t, ok, "response should contain Cluster field")

				clusterMap, ok := clusterVal.(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.body["ClusterName"], clusterMap["Name"])
				assert.NotEmpty(t, clusterMap["ARN"])
			}
		})
	}
}

func TestHandler_DescribeClusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*memorydb.Handler)
		body       map[string]any
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "describe all clusters",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": "cluster-a",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": "cluster-b",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "describe by name not found",
			body:       map[string]any{"ClusterName": "no-such-cluster"},
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

			rec := doRequest(t, h, "DescribeClusters", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantCount > 0 {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				clusters, ok := resp["Clusters"].([]any)
				require.True(t, ok)
				assert.Len(t, clusters, tt.wantCount)
			}
		})
	}
}

func TestHandler_DeleteCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "deletes existing cluster",
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete non-existent cluster",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantStatus == http.StatusOK {
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": "del-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			}

			clusterName := "del-cluster"
			if tt.wantStatus == http.StatusBadRequest {
				clusterName = "no-cluster"
			}

			rec := doRequest(t, h, "DeleteCluster", map[string]any{"ClusterName": clusterName})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestParity_ClusterResponse_ParameterGroupStatus verifies ParameterGroupStatus is present.
func TestHandler_ClusterResponse_ParameterGroupStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "pgstatus-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})
	require.Equal(t, http.StatusOK, createRec.Code, "create cluster: %s", createRec.Body)

	descRec := doRequest(t, h, "DescribeClusters", map[string]any{
		"ClusterName": "pgstatus-cluster",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
	clusters, _ := resp["Clusters"].([]any)
	require.Len(t, clusters, 1)
	cl, _ := clusters[0].(map[string]any)
	assert.NotEmpty(t, cl["ParameterGroupStatus"], "ParameterGroupStatus must be present")
}

// TestParity_ClusterResponse_MultiRegionClusterName verifies MultiRegionClusterName is in response.
func TestHandler_ClusterResponse_MultiRegionClusterName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "mrc-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	cl, _ := createResp["Cluster"].(map[string]any)

	// Field must be present (empty string is fine for a cluster not in multi-region).
	_, hasField := cl["MultiRegionClusterName"]
	assert.True(t, hasField, "MultiRegionClusterName field must be present in cluster response")
}

// TestParity_ClusterParameterGroupStatus_InSync verifies default value is "in-sync".
func TestHandler_ClusterParameterGroupStatus_InSync(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "insync-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &resp))
	cl, _ := resp["Cluster"].(map[string]any)
	assert.Equal(t, "in-sync", cl["ParameterGroupStatus"],
		"default ParameterGroupStatus must be in-sync")
}

func TestHandler_BatchUpdateCluster(t *testing.T) {
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
			name: "updates known clusters",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": "cluster-a",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": "cluster-b",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			},
			body: map[string]any{
				"ClusterNames": []string{"cluster-a", "cluster-b"},
				"ServiceUpdate": map[string]any{
					"ServiceUpdateNameToApply": "memorydb-20240601-redis-security",
				},
			},
			wantStatus:      http.StatusOK,
			wantProcessed:   2,
			wantUnprocessed: 0,
		},
		{
			name: "unknown cluster goes to unprocessed",
			body: map[string]any{
				"ClusterNames": []string{"no-such-cluster"},
			},
			wantStatus:      http.StatusOK,
			wantProcessed:   0,
			wantUnprocessed: 1,
		},
		{
			name:       "empty cluster names returns bad request",
			body:       map[string]any{"ClusterNames": []string{}},
			wantStatus: http.StatusBadRequest,
		},
		{
			// Proves the fix: BatchUpdateCluster must reject a
			// ServiceUpdateNameToApply that names no known service update
			// (real AWS fault: ServiceUpdateNotFoundFault) instead of silently
			// succeeding for every found cluster.
			name: "unknown service update name is rejected",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": "cluster-c",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			},
			body: map[string]any{
				"ClusterNames": []string{"cluster-c"},
				"ServiceUpdate": map[string]any{
					"ServiceUpdateNameToApply": "no-such-service-update",
				},
			},
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
				assert.Len(t, processed, tt.wantProcessed)

				unprocessed := resp["UnprocessedClusters"].([]any)
				assert.Len(t, unprocessed, tt.wantUnprocessed)
			}
		})
	}
}

// TestHandler_UpdateCluster tests UpdateCluster handler.
func TestHandler_UpdateCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "updates cluster",
			body: map[string]any{
				"ClusterName": "my-cluster",
				"Description": "updated",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing cluster name",
			body:       map[string]any{"Description": "updated"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "cluster not found",
			body:       map[string]any{"ClusterName": "no-such"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "updates cluster" {
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": "my-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			}

			rec := doRequest(t, h, "UpdateCluster", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				cluster := resp["Cluster"].(map[string]any)
				assert.Equal(t, "updated", cluster["Description"])
			}
		})
	}
}

func TestHandler_NumShardsBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		numShards  int
		wantStatus int
	}{
		{"zero shards rejected", 0, http.StatusBadRequest},
		{"501 shards rejected", 501, http.StatusBadRequest},
		{"1 shard accepted", 1, http.StatusOK},
		{"500 shards accepted", 500, http.StatusOK},
		{"250 shards accepted", 250, http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			body := minimalClusterBody("shardtest")
			body["NumShards"] = tt.numShards
			_, code := doCreateCluster(t, h, body)
			assert.Equal(t, tt.wantStatus, code, "NumShards=%d", tt.numShards)
		})
	}
}

func TestHandler_NumReplicasBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		numReplicas int
		wantStatus  int
	}{
		{"negative replicas rejected", -1, http.StatusBadRequest},
		{"6 replicas rejected", 6, http.StatusBadRequest},
		{"0 replicas accepted", 0, http.StatusOK},
		{"5 replicas accepted", 5, http.StatusOK},
		{"3 replicas accepted", 3, http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			body := minimalClusterBody("reptest")
			body["NumReplicasPerShard"] = tt.numReplicas
			_, code := doCreateCluster(t, h, body)
			assert.Equal(t, tt.wantStatus, code, "NumReplicasPerShard=%d", tt.numReplicas)
		})
	}
}

// -- Finding 13: Window format validation ----------------------------------------

func TestHandler_MaintenanceWindowValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		window     string
		wantStatus int
	}{
		{"empty window accepted", "", http.StatusOK},
		{"valid window accepted", "sun:05:00-sun:06:00", http.StatusOK},
		{"no dash rejected", "sun:05:00", http.StatusBadRequest},
		{"empty left part rejected", "-sun:06:00", http.StatusBadRequest},
		{"empty right part rejected", "sun:05:00-", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			body := minimalClusterBody("mwtest")
			if tt.window != "" {
				body["MaintenanceWindow"] = tt.window
			}
			_, code := doCreateCluster(t, h, body)
			assert.Equal(t, tt.wantStatus, code, "MaintenanceWindow=%q", tt.window)
		})
	}
}

func TestHandler_SnapshotWindowValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		window     string
		wantStatus int
	}{
		{"empty window accepted", "", http.StatusOK},
		{"valid window accepted", "05:00-06:00", http.StatusOK},
		{"no dash rejected", "05:00", http.StatusBadRequest},
		{"empty left part rejected", "-06:00", http.StatusBadRequest},
		{"empty right part rejected", "05:00-", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			body := minimalClusterBody("swtest")
			if tt.window != "" {
				body["SnapshotWindow"] = tt.window
			}
			_, code := doCreateCluster(t, h, body)
			assert.Equal(t, tt.wantStatus, code, "SnapshotWindow=%q", tt.window)
		})
	}
}

// -- Finding 14: SnsTopicStatus stored -------------------------------------------

func TestHandler_SnsTopicStatusStoredOnUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		snsTopicStatus string
		wantStatus     string
	}{
		{"status active stored", "active", "active"},
		{"status inactive stored", "inactive", "inactive"},
		{"status empty not overwritten", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// Create cluster first.
			resp, code := doCreateCluster(t, h, minimalClusterBody("sns-test"))
			require.Equal(t, http.StatusOK, code)
			_ = resp

			// Update with SnsTopicStatus.
			updateBody := map[string]any{
				"ClusterName": "sns-test",
			}
			if tt.snsTopicStatus != "" {
				updateBody["SnsTopicStatus"] = tt.snsTopicStatus
			}

			rec := doRequest(t, h, "UpdateCluster", updateBody)
			require.Equal(t, http.StatusOK, rec.Code)

			var updateResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))

			cluster, _ := updateResp["Cluster"].(map[string]any)
			gotStatus, _ := cluster["SnsTopicStatus"].(string)
			assert.Equal(t, tt.wantStatus, gotStatus)
		})
	}
}

// -- Finding 15: EnginePatchVersion lookup ---------------------------------------

func TestHandler_EnginePatchVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		engine           string
		engineVersion    string
		wantPatchVersion string
	}{
		{"redis 7.0 has patch version", "redis", "7.0", "7.0.7"},
		{"redis 6.2 has patch version", "redis", "6.2", "6.2.6"},
		{"redis 7.1 has patch version", "redis", "7.1", "7.1.0"},
		{"valkey 7.2 has patch version", "valkey", "7.2", "7.2.4"},
		{"valkey 8.0 has patch version", "valkey", "8.0", "8.0.1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			body := minimalClusterBody("patchtest")
			body["Engine"] = tt.engine
			body["EngineVersion"] = tt.engineVersion

			resp, code := doCreateCluster(t, h, body)
			require.Equal(t, http.StatusOK, code)

			cluster, _ := resp["Cluster"].(map[string]any)
			patchVersion, _ := cluster["EnginePatchVersion"].(string)
			assert.Equal(t, tt.wantPatchVersion, patchVersion,
				"Engine=%s EngineVersion=%s", tt.engine, tt.engineVersion)
		})
	}
}

// -- Finding 16: ACL fields ------------------------------------------------------

func TestHandler_UpdateCluster_ShardBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		shardCount int
		wantStatus int
	}{
		{"zero shards rejected", 0, http.StatusBadRequest},
		{"negative shards rejected", -1, http.StatusBadRequest},
		{"501 shards rejected", 501, http.StatusBadRequest},
		{"1 shard accepted", 1, http.StatusOK},
		{"500 shards accepted", 500, http.StatusOK},
		{"256 shards accepted", 256, http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			doCreateCluster(t, h, minimalClusterBody("shard-bounds-cluster"))

			rec := doRequest(t, h, "UpdateCluster", map[string]any{
				"ClusterName": "shard-bounds-cluster",
				"ShardConfiguration": map[string]any{
					"ShardCount": tt.shardCount,
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body)
		})
	}
}

func TestHandler_UpdateCluster_ReplicaBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		replicaCount int
		wantStatus   int
	}{
		{"negative replicas rejected", -1, http.StatusBadRequest},
		{"6 replicas rejected", 6, http.StatusBadRequest},
		{"100 replicas rejected", 100, http.StatusBadRequest},
		{"0 replicas accepted", 0, http.StatusOK},
		{"5 replicas accepted", 5, http.StatusOK},
		{"3 replicas accepted", 3, http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			doCreateCluster(t, h, minimalClusterBody("replica-bounds-cluster"))

			rec := doRequest(t, h, "UpdateCluster", map[string]any{
				"ClusterName": "replica-bounds-cluster",
				"ReplicaConfiguration": map[string]any{
					"ReplicaCount": tt.replicaCount,
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body)
		})
	}
}

// -- UpdateCluster: window format validation (finding 13 via update) -------------

func TestHandler_UpdateCluster_WindowValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		field      string
		value      string
		wantStatus int
	}{
		{"valid maintenance window", "MaintenanceWindow", "sun:05:00-sun:06:00", http.StatusOK},
		{"invalid maintenance window no dash", "MaintenanceWindow", "nodash", http.StatusBadRequest},
		{"invalid maintenance window empty parts", "MaintenanceWindow", "-", http.StatusBadRequest},
		{"valid snapshot window", "SnapshotWindow", "03:00-04:00", http.StatusOK},
		{"invalid snapshot window no dash", "SnapshotWindow", "nodash", http.StatusBadRequest},
		{"invalid snapshot window empty parts", "SnapshotWindow", "-", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			doCreateCluster(t, h, minimalClusterBody("window-update-cluster"))

			rec := doRequest(t, h, "UpdateCluster", map[string]any{
				"ClusterName": "window-update-cluster",
				tt.field:      tt.value,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body)
		})
	}
}

// -- DeleteCluster with FinalSnapshot emits events (finding 22) -----------------

func TestHandler_DeleteCluster_WithSnapshot_EmitsEvent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doCreateCluster(t, h, minimalClusterBody("del-snap-cluster"))

	rec := doRequest(t, h, "DeleteCluster", map[string]any{
		"ClusterName":       "del-snap-cluster",
		"FinalSnapshotName": "final-del-snap",
	})
	require.Equal(t, http.StatusOK, rec.Code, "delete: %s", rec.Body)

	// Event for cluster deletion should be emitted.
	evRec := doRequest(t, h, "DescribeEvents", map[string]any{
		"SourceName": "del-snap-cluster",
		"SourceType": "cluster",
	})
	require.Equal(t, http.StatusOK, evRec.Code)

	var evResp map[string]any
	require.NoError(t, json.Unmarshal(evRec.Body.Bytes(), &evResp))
	events, _ := evResp["Events"].([]any)
	// At least create + delete events.
	assert.GreaterOrEqual(t, len(events), 2, "expected create and delete events")
}

// -- Multi-region parameter groups seeded by default (finding 25) ---------------

func TestHandler_BatchUpdateCluster_Extended(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		clusterNames     []string
		existingClusters []string
		wantProcessed    int
		wantUnprocessed  int
	}{
		{
			name:             "all clusters found",
			existingClusters: []string{"buc-a", "buc-b"},
			clusterNames:     []string{"buc-a", "buc-b"},
			wantProcessed:    2,
			wantUnprocessed:  0,
		},
		{
			name:             "some clusters not found",
			existingClusters: []string{"buc-exists"},
			clusterNames:     []string{"buc-exists", "buc-missing"},
			wantProcessed:    1,
			wantUnprocessed:  1,
		},
		{
			name:             "all clusters not found",
			existingClusters: []string{},
			clusterNames:     []string{"buc-ghost-1", "buc-ghost-2"},
			wantProcessed:    0,
			wantUnprocessed:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for _, cn := range tt.existingClusters {
				doCreateCluster(t, h, minimalClusterBody(cn))
			}

			rec := doRequest(t, h, "BatchUpdateCluster", map[string]any{
				"ClusterNames": tt.clusterNames,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			processed, _ := resp["ProcessedClusters"].([]any)
			unprocessed, _ := resp["UnprocessedClusters"].([]any)
			assert.Len(t, processed, tt.wantProcessed)
			assert.Len(t, unprocessed, tt.wantUnprocessed)
		})
	}
}

// -- ReservedNodes: purchase and describe ----------------------------------------

func TestHandler_CreateCluster_EngineVersionCombinations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		engine        string
		engineVersion string
		wantStatus    int
	}{
		{"redis 7.0 valid", "redis", "7.0", http.StatusOK},
		{"redis 7.1 valid", "redis", "7.1", http.StatusOK},
		{"redis 7.2 valid", "redis", "7.2", http.StatusOK},
		{"redis 6.2 valid", "redis", "6.2", http.StatusOK},
		{"valkey 7.2 valid", "valkey", "7.2", http.StatusOK},
		{"valkey 8.0 valid", "valkey", "8.0", http.StatusOK},
		{"unknown version rejected", "redis", "5.0", http.StatusBadRequest},
		{"unknown engine rejected", "memcached", "7.0", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			body := map[string]any{
				"ClusterName":   "ev-test-cluster",
				"NodeType":      "db.r6g.large",
				"ACLName":       "open-access",
				"Engine":        tt.engine,
				"EngineVersion": tt.engineVersion,
			}

			rec := doRequest(t, h, "CreateCluster", body)
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body)
		})
	}
}

// -- DescribeEngineVersions filtering --------------------------------------------

func TestHandler_EnginePatchVersion_AllFamilies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		engine           string
		engineVersion    string
		wantPatchVersion string
	}{
		{"redis 6.2 patch", "redis", "6.2", "6.2.6"},
		{"redis 7.0 patch", "redis", "7.0", "7.0.7"},
		{"redis 7.1 patch", "redis", "7.1", "7.1.0"},
		{"valkey 7.2 patch", "valkey", "7.2", "7.2.4"},
		{"valkey 8.0 patch", "valkey", "8.0", "8.0.1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			body := map[string]any{
				"ClusterName":   "patch-test-cluster",
				"NodeType":      "db.r6g.large",
				"ACLName":       "open-access",
				"Engine":        tt.engine,
				"EngineVersion": tt.engineVersion,
			}

			rec := doRequest(t, h, "CreateCluster", body)
			require.Equal(t, http.StatusOK, rec.Code, "create: %s", rec.Body)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			cl, _ := resp["Cluster"].(map[string]any)

			assert.Equal(t, tt.wantPatchVersion, cl["EnginePatchVersion"],
				"engine=%s version=%s", tt.engine, tt.engineVersion)
			assert.NotEqual(t, cl["EngineVersion"], cl["EnginePatchVersion"],
				"EnginePatchVersion should differ from EngineVersion for most versions")
		})
	}
}

// -- UpdateCluster: SnsTopicStatus enum validation --------------------------------

func TestHandler_UpdateCluster_SnsTopicStatusValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		snsTopicStatus string
		wantStatus     int
	}{
		{"active accepted", "active", http.StatusOK},
		{"inactive accepted", "inactive", http.StatusOK},
		{"invalid value rejected", "enabled", http.StatusBadRequest},
		{"garbage value rejected", "notvalid", http.StatusBadRequest},
		{"empty string no-ops (no validation)", "", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			doCreateCluster(t, h, minimalClusterBody("sns-status-cluster"))

			rec := doRequest(t, h, "UpdateCluster", map[string]any{
				"ClusterName":    "sns-status-cluster",
				"SnsTopicStatus": tt.snsTopicStatus,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body)
		})
	}
}
