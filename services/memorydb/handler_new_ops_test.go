package memorydb_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

func TestHandler_CreateSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         map[string]any
		name         string
		wantStatus   int
		wantSnapshot bool
	}{
		{
			name: "creates snapshot",
			body: map[string]any{
				"SnapshotName": "my-snap",
				"ClusterName":  "my-cluster",
			},
			wantStatus:   http.StatusOK,
			wantSnapshot: true,
		},
		{
			name:       "missing snapshot name",
			body:       map[string]any{"ClusterName": "my-cluster"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing cluster name",
			body:       map[string]any{"SnapshotName": "my-snap"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate snapshot",
			body: map[string]any{
				"SnapshotName": "dup-snap",
				"ClusterName":  "my-cluster",
			},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Pre-create the cluster that the snapshot will reference.
			if clusterName, ok := tt.body["ClusterName"].(string); ok && clusterName != "" {
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": clusterName,
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			}

			if tt.name == "duplicate snapshot" {
				doRequest(t, h, "CreateSnapshot", tt.body)
			}

			rec := doRequest(t, h, "CreateSnapshot", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantSnapshot {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				snap, ok := resp["Snapshot"]
				require.True(t, ok)
				snapMap := snap.(map[string]any)
				assert.Equal(t, tt.body["SnapshotName"], snapMap["Name"])
				assert.NotEmpty(t, snapMap["ARN"])
			}
		})
	}
}

func TestHandler_CopySnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name: "copies snapshot",
			body: map[string]any{
				"SourceSnapshotName": "src-snap",
				"TargetSnapshotName": "dst-snap",
			},
			wantStatus: http.StatusOK,
			wantName:   "dst-snap",
		},
		{
			name:       "missing source name",
			body:       map[string]any{"TargetSnapshotName": "dst"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing target name",
			body:       map[string]any{"SourceSnapshotName": "src"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "source not found",
			body: map[string]any{
				"SourceSnapshotName": "no-such-snap",
				"TargetSnapshotName": "dst-snap",
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "copies snapshot" {
				// Pre-create the cluster, then the source snapshot.
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": "my-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
				doRequest(t, h, "CreateSnapshot", map[string]any{
					"SnapshotName": "src-snap",
					"ClusterName":  "my-cluster",
				})
			}

			rec := doRequest(t, h, "CopySnapshot", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				snap := resp["Snapshot"].(map[string]any)
				assert.Equal(t, tt.wantName, snap["Name"])
			}
		})
	}
}

func TestHandler_DeleteSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "deletes existing snapshot",
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete non-existent snapshot",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "missing snapshot name",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			switch tt.name {
			case "deletes existing snapshot":
				// Pre-create cluster then snapshot.
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": "my-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
				doRequest(t, h, "CreateSnapshot", map[string]any{
					"SnapshotName": "del-snap",
					"ClusterName":  "my-cluster",
				})
				rec := doRequest(t, h, "DeleteSnapshot", map[string]any{"SnapshotName": "del-snap"})
				assert.Equal(t, tt.wantStatus, rec.Code)
			case "delete non-existent snapshot":
				rec := doRequest(t, h, "DeleteSnapshot", map[string]any{"SnapshotName": "no-snap"})
				assert.Equal(t, tt.wantStatus, rec.Code)
			case "missing snapshot name":
				rec := doRequest(t, h, "DeleteSnapshot", map[string]any{})
				assert.Equal(t, tt.wantStatus, rec.Code)
			}
		})
	}
}

func TestHandler_DescribeEngineVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         map[string]any
		name         string
		wantStatus   int
		wantMinCount int
	}{
		{
			name:         "returns all versions",
			body:         map[string]any{},
			wantStatus:   http.StatusOK,
			wantMinCount: 1,
		},
		{
			name: "filter by family",
			body: map[string]any{
				"ParameterGroupFamily": "memorydb_redis7",
			},
			wantStatus:   http.StatusOK,
			wantMinCount: 1,
		},
		{
			name: "filter by unknown family returns empty",
			body: map[string]any{
				"ParameterGroupFamily": "memorydb_redis99",
			},
			wantStatus:   http.StatusOK,
			wantMinCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeEngineVersions", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			versions := resp["EngineVersions"].([]any)
			assert.GreaterOrEqual(t, len(versions), tt.wantMinCount)
		})
	}
}

func TestHandler_DescribeEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "returns empty events list",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name: "filter by source name returns empty when no match",
			body: map[string]any{
				"SourceName": "my-cluster",
				"SourceType": "cluster",
			},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeEvents", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			events := resp["Events"].([]any)
			assert.Len(t, events, tt.wantCount)
		})
	}
}

func TestHandler_CreateMultiRegionCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantMRC    bool
	}{
		{
			name: "creates multi-region cluster",
			body: map[string]any{
				"MultiRegionClusterNameSuffix": "my-mrc",
				"NodeType":                     "db.r6g.large",
			},
			wantStatus: http.StatusOK,
			wantMRC:    true,
		},
		{
			name:       "missing name suffix",
			body:       map[string]any{"NodeType": "db.r6g.large"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing node type",
			body:       map[string]any{"MultiRegionClusterNameSuffix": "my-mrc"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate cluster",
			body: map[string]any{
				"MultiRegionClusterNameSuffix": "dup-mrc",
				"NodeType":                     "db.r6g.large",
			},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate cluster" {
				doRequest(t, h, "CreateMultiRegionCluster", tt.body)
			}

			rec := doRequest(t, h, "CreateMultiRegionCluster", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantMRC {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				mrc, ok := resp["MultiRegionCluster"]
				require.True(t, ok)
				mrcMap := mrc.(map[string]any)
				assert.NotEmpty(t, mrcMap["MultiRegionClusterName"])
				assert.Equal(t, "available", mrcMap["Status"])
			}
		})
	}
}

func TestHandler_DeleteMultiRegionCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "deletes existing multi-region cluster",
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete non-existent cluster",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "missing cluster name",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			switch tt.name {
			case "deletes existing multi-region cluster":
				doRequest(t, h, "CreateMultiRegionCluster", map[string]any{
					"MultiRegionClusterNameSuffix": "del-mrc",
					"NodeType":                     "db.r6g.large",
				})

				var createResp map[string]any

				createRec := doRequest(t, h, "DescribeMultiRegionClusters", map[string]any{})
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				clusters := createResp["MultiRegionClusters"].([]any)
				require.Len(t, clusters, 1)
				clusterName := clusters[0].(map[string]any)["MultiRegionClusterName"].(string)

				rec := doRequest(t, h, "DeleteMultiRegionCluster", map[string]any{
					"MultiRegionClusterName": clusterName,
				})
				assert.Equal(t, tt.wantStatus, rec.Code)
			case "delete non-existent cluster":
				rec := doRequest(t, h, "DeleteMultiRegionCluster", map[string]any{
					"MultiRegionClusterName": "no-such-mrc",
				})
				assert.Equal(t, tt.wantStatus, rec.Code)
			case "missing cluster name":
				rec := doRequest(t, h, "DeleteMultiRegionCluster", map[string]any{})
				assert.Equal(t, tt.wantStatus, rec.Code)
			}
		})
	}
}

func TestHandler_DescribeMultiRegionClusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*memorydb.Handler)
		body       map[string]any
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "describe all",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateMultiRegionCluster", map[string]any{
					"MultiRegionClusterNameSuffix": "mrc-a",
					"NodeType":                     "db.r6g.large",
				})
				doRequest(t, h, "CreateMultiRegionCluster", map[string]any{
					"MultiRegionClusterNameSuffix": "mrc-b",
					"NodeType":                     "db.r6g.large",
				})
			},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "describe not found",
			body:       map[string]any{"MultiRegionClusterName": "no-such-mrc"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "DescribeMultiRegionClusters", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantCount > 0 {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				clusters := resp["MultiRegionClusters"].([]any)
				assert.Len(t, clusters, tt.wantCount)
			}
		})
	}
}

func TestHandler_DescribeMultiRegionParameterGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "describe all returns seeded defaults",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantCount:  4,
		},
		{
			name:       "describe not found",
			body:       map[string]any{"ParameterGroupName": "no-such-pg"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeMultiRegionParameterGroups", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				groups := resp["MultiRegionParameterGroups"].([]any)
				assert.Len(t, groups, tt.wantCount)
			}
		})
	}
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
					"ServiceUpdateNameToApply": "memorydb-redis6.2-0001",
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
