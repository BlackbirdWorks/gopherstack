package memorydb_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

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
			wantStatus: http.StatusNotFound,
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

// TestHandler_UpdateACL tests UpdateACL handler.
func TestHandler_UpdateACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "updates ACL",
			body: map[string]any{
				"ACLName":        "my-acl",
				"UserNamesToAdd": []string{"user1"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing ACL name",
			body:       map[string]any{"UserNamesToAdd": []string{"user1"}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "ACL not found",
			body:       map[string]any{"ACLName": "no-such-acl"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "updates ACL" {
				doRequest(t, h, "CreateUser", map[string]any{
					"UserName":           "user1",
					"AccessString":       "on ~* &* +@all",
					"AuthenticationMode": map[string]any{"Type": "no-password"},
				})
				doRequest(t, h, "CreateACL", map[string]any{"ACLName": "my-acl"})
			}

			rec := doRequest(t, h, "UpdateACL", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_SubnetGroup_CRUD tests full SubnetGroup lifecycle through the handler.
func TestHandler_SubnetGroup_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		setup      func(*memorydb.Handler)
		name       string
		op         string
		wantStatus int
	}{
		{
			name: "create subnet group",
			op:   "CreateSubnetGroup",
			body: map[string]any{
				"SubnetGroupName": "my-sg",
				"SubnetIds":       []string{"subnet-1"},
			},
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
				doRequest(t, h, "CreateSubnetGroup", map[string]any{
					"SubnetGroupName": "sg-x",
					"SubnetIds":       []string{"subnet-1"},
				})
			},
			body:       map[string]any{"SubnetGroupName": "sg-x"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe subnet group not found",
			op:         "DescribeSubnetGroups",
			body:       map[string]any{"SubnetGroupName": "no-such"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "delete subnet group",
			op:   "DeleteSubnetGroup",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateSubnetGroup", map[string]any{
					"SubnetGroupName": "del-sg",
					"SubnetIds":       []string{"subnet-1"},
				})
			},
			body:       map[string]any{"SubnetGroupName": "del-sg"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete subnet group missing name",
			op:         "DeleteSubnetGroup",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete subnet group not found",
			op:         "DeleteSubnetGroup",
			body:       map[string]any{"SubnetGroupName": "no-such"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "update subnet group",
			op:   "UpdateSubnetGroup",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateSubnetGroup", map[string]any{
					"SubnetGroupName": "upd-sg",
					"SubnetIds":       []string{"subnet-1"},
				})
			},
			body: map[string]any{
				"SubnetGroupName": "upd-sg",
				"Description":     "new desc",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "update subnet group missing name",
			op:         "UpdateSubnetGroup",
			body:       map[string]any{"Description": "new"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "update subnet group not found",
			op:         "UpdateSubnetGroup",
			body:       map[string]any{"SubnetGroupName": "no-such"},
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

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_User_CRUD tests full User lifecycle through the handler.
func TestHandler_User_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		setup      func(*memorydb.Handler)
		name       string
		op         string
		wantStatus int
	}{
		{
			name: "create user",
			op:   "CreateUser",
			body: map[string]any{
				"UserName":     "my-user",
				"AccessString": "on ~* +@all",
				"AuthenticationMode": map[string]any{
					"Type":      "password",
					"Passwords": []string{"mypassword123"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "create user missing name",
			op:         "CreateUser",
			body:       map[string]any{"AccessString": "on ~*"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "describe users",
			op:   "DescribeUsers",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateUser", map[string]any{
					"UserName":     "user-x",
					"AccessString": "on ~*",
					"AuthenticationMode": map[string]any{
						"Type":      "password",
						"Passwords": []string{"pass1"},
					},
				})
			},
			body:       map[string]any{"UserName": "user-x"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe users not found",
			op:         "DescribeUsers",
			body:       map[string]any{"UserName": "no-such"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "delete user",
			op:   "DeleteUser",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateUser", map[string]any{
					"UserName":     "del-user",
					"AccessString": "on ~*",
					"AuthenticationMode": map[string]any{
						"Type":      "password",
						"Passwords": []string{"pass1"},
					},
				})
			},
			body:       map[string]any{"UserName": "del-user"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete user missing name",
			op:         "DeleteUser",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete user not found",
			op:         "DeleteUser",
			body:       map[string]any{"UserName": "no-such"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "update user",
			op:   "UpdateUser",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateUser", map[string]any{
					"UserName":     "upd-user",
					"AccessString": "on ~*",
					"AuthenticationMode": map[string]any{
						"Type":      "password",
						"Passwords": []string{"pass1"},
					},
				})
			},
			body: map[string]any{
				"UserName":     "upd-user",
				"AccessString": "on ~* +@read",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "update user missing name",
			op:         "UpdateUser",
			body:       map[string]any{"AccessString": "on ~*"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "update user not found",
			op:         "UpdateUser",
			body:       map[string]any{"UserName": "no-such"},
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

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ParameterGroup_CRUD tests full ParameterGroup lifecycle through the handler.
func TestHandler_ParameterGroup_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		setup      func(*memorydb.Handler)
		name       string
		op         string
		wantStatus int
	}{
		{
			name: "create parameter group",
			op:   "CreateParameterGroup",
			body: map[string]any{
				"ParameterGroupName": "my-pg",
				"Family":             "memorydb_redis7",
			},
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
				doRequest(t, h, "CreateParameterGroup", map[string]any{
					"ParameterGroupName": "pg-x",
					"Family":             "memorydb_redis7",
				})
			},
			body:       map[string]any{"ParameterGroupName": "pg-x"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe parameter group not found",
			op:         "DescribeParameterGroups",
			body:       map[string]any{"ParameterGroupName": "no-such"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "delete parameter group",
			op:   "DeleteParameterGroup",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateParameterGroup", map[string]any{
					"ParameterGroupName": "del-pg",
					"Family":             "memorydb_redis7",
				})
			},
			body:       map[string]any{"ParameterGroupName": "del-pg"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete parameter group missing name",
			op:         "DeleteParameterGroup",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete parameter group not found",
			op:         "DeleteParameterGroup",
			body:       map[string]any{"ParameterGroupName": "no-such"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "update parameter group",
			op:   "UpdateParameterGroup",
			setup: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateParameterGroup", map[string]any{
					"ParameterGroupName": "upd-pg",
					"Family":             "memorydb_redis7",
				})
			},
			body: map[string]any{
				"ParameterGroupName": "upd-pg",
				"ParameterNameValues": []map[string]any{
					{"ParameterName": "maxmemory-policy", "ParameterValue": "allkeys-lru"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "update parameter group missing name",
			op:         "UpdateParameterGroup",
			body:       map[string]any{"ParameterNameValues": []map[string]any{}},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "update parameter group not found",
			op:         "UpdateParameterGroup",
			body:       map[string]any{"ParameterGroupName": "no-such"},
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

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_Tags_AllResources tests tag operations on all resource kinds.
func TestHandler_Tags_AllResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*memorydb.Handler) string // returns ARN
		name       string
		wantStatus int
	}{
		{
			name: "tags on subnet group",
			setup: func(h *memorydb.Handler) string {
				rec := doRequest(t, h, "CreateSubnetGroup", map[string]any{
					"SubnetGroupName": "tag-sg",
					"SubnetIds":       []string{"subnet-1"},
					"Tags":            []map[string]any{{"Key": "Env", "Value": "prod"}},
				})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["SubnetGroup"].(map[string]any)["ARN"].(string)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "tags on user",
			setup: func(h *memorydb.Handler) string {
				rec := doRequest(t, h, "CreateUser", map[string]any{
					"UserName":     "tag-user",
					"AccessString": "on ~*",
					"AuthenticationMode": map[string]any{
						"Type":      "password",
						"Passwords": []string{"pass1"},
					},
					"Tags": []map[string]any{{"Key": "Env", "Value": "prod"}},
				})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["User"].(map[string]any)["ARN"].(string)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "tags on parameter group",
			setup: func(h *memorydb.Handler) string {
				rec := doRequest(t, h, "CreateParameterGroup", map[string]any{
					"ParameterGroupName": "tag-pg",
					"Family":             "memorydb_redis7",
					"Tags":               []map[string]any{{"Key": "Env", "Value": "prod"}},
				})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["ParameterGroup"].(map[string]any)["ARN"].(string)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "tags on snapshot",
			setup: func(h *memorydb.Handler) string {
				// Pre-create the cluster first.
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": "my-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
				rec := doRequest(t, h, "CreateSnapshot", map[string]any{
					"SnapshotName": "tag-snap",
					"ClusterName":  "my-cluster",
					"Tags":         []map[string]any{{"Key": "Env", "Value": "prod"}},
				})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["Snapshot"].(map[string]any)["ARN"].(string)
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := tt.setup(h)

			rec := doRequest(t, h, "ListTags", map[string]any{"ResourceArn": arn})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			tagList := resp["TagList"].([]any)
			assert.NotEmpty(t, tagList)

			// Test TagResource
			tagRec := doRequest(t, h, "TagResource", map[string]any{
				"ResourceArn": arn,
				"Tags":        []map[string]any{{"Key": "Team", "Value": "ops"}},
			})
			assert.Equal(t, http.StatusOK, tagRec.Code)

			// Test UntagResource
			untagRec := doRequest(t, h, "UntagResource", map[string]any{
				"ResourceArn": arn,
				"TagKeys":     []string{"Team"},
			})
			assert.Equal(t, http.StatusOK, untagRec.Code)
		})
	}
}

// TestBackend_NewOps_Lifecycle tests the new backend operations end-to-end.
func TestBackend_NewOps_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "snapshot_lifecycle"},
		{name: "multi_region_cluster_lifecycle"},
		{name: "engine_versions"},
		{name: "events"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := memorydb.NewInMemoryBackend()

			switch tt.name {
			case "snapshot_lifecycle":
				// Pre-create the cluster the snapshot references.
				b.AddClusterInternal("my-cluster", "db.r6g.large")

				req := &memorydb.ExportedCreateSnapshotRequest{
					SnapshotName: "my-snap",
					ClusterName:  "my-cluster",
				}

				s, err := b.CreateSnapshot(testRegion, testAccountID, req)
				require.NoError(t, err)
				assert.Equal(t, "my-snap", s.Name)
				assert.Equal(t, "my-cluster", s.ClusterName)
				assert.NotEmpty(t, s.ARN)
				assert.Equal(t, "available", s.Status)

				// duplicate
				_, err = b.CreateSnapshot(testRegion, testAccountID, req)
				require.Error(t, err)

				// copy
				cp, err := b.CopySnapshot(testRegion, testAccountID, &memorydb.ExportedCopySnapshotRequest{
					SourceSnapshotName: "my-snap",
					TargetSnapshotName: "copy-snap",
				})
				require.NoError(t, err)
				assert.Equal(t, "copy-snap", cp.Name)
				assert.Equal(t, "my-cluster", cp.ClusterName)

				// delete
				deleted, err := b.DeleteSnapshot("my-snap")
				require.NoError(t, err)
				assert.Equal(t, "my-snap", deleted.Name)

				// delete again → error
				_, err = b.DeleteSnapshot("my-snap")
				require.Error(t, err)

			case "multi_region_cluster_lifecycle":
				req := &memorydb.ExportedCreateMultiRegionClusterRequest{
					MultiRegionClusterNameSuffix: "my-mrc",
					NodeType:                     "db.r6g.large",
					EngineVersion:                "7.0",
				}

				mrc, err := b.CreateMultiRegionCluster(testRegion, testAccountID, req)
				require.NoError(t, err)
				assert.Contains(t, mrc.MultiRegionClusterName, "my-mrc")
				assert.Equal(t, "available", mrc.Status)
				assert.Equal(t, "7.0", mrc.EngineVersion)

				// duplicate
				_, err = b.CreateMultiRegionCluster(testRegion, testAccountID, req)
				require.Error(t, err)

				// describe
				mrcs, err := b.DescribeMultiRegionClusters("")
				require.NoError(t, err)
				require.Len(t, mrcs, 1)

				// describe by name
				mrcs2, err := b.DescribeMultiRegionClusters(mrc.MultiRegionClusterName)
				require.NoError(t, err)
				require.Len(t, mrcs2, 1)

				// describe by bad name
				_, err = b.DescribeMultiRegionClusters("no-such")
				require.Error(t, err)

				// delete
				deleted, err := b.DeleteMultiRegionCluster(mrc.MultiRegionClusterName)
				require.NoError(t, err)
				assert.Equal(t, mrc.MultiRegionClusterName, deleted.MultiRegionClusterName)

				// delete again → error
				_, err = b.DeleteMultiRegionCluster(mrc.MultiRegionClusterName)
				require.Error(t, err)

			case "engine_versions":
				versions, err := b.DescribeEngineVersions(&memorydb.ExportedDescribeEngineVersionsRequest{})
				require.NoError(t, err)
				assert.NotEmpty(t, versions)

				// filter by family
				redis7, err := b.DescribeEngineVersions(&memorydb.ExportedDescribeEngineVersionsRequest{
					ParameterGroupFamily: "memorydb_redis7",
				})
				require.NoError(t, err)

				for _, ev := range redis7 {
					assert.Equal(t, "memorydb_redis7", ev.ParameterGroupFamily)
				}

				// filter unknown family
				none, err := b.DescribeEngineVersions(&memorydb.ExportedDescribeEngineVersionsRequest{
					ParameterGroupFamily: "memorydb_redis99",
				})
				require.NoError(t, err)
				assert.Empty(t, none)

			case "events":
				// empty initially
				events, err := b.DescribeEvents(&memorydb.ExportedDescribeEventsRequest{})
				require.NoError(t, err)
				assert.Empty(t, events)

				// add events
				b.AddEvent(&memorydb.ExportedEvent{
					SourceName: "my-cluster",
					SourceType: "cluster",
					Message:    "cluster created",
				})
				b.AddEvent(&memorydb.ExportedEvent{
					SourceName: "other-cluster",
					SourceType: "cluster",
					Message:    "other created",
				})

				// all events
				all, err := b.DescribeEvents(&memorydb.ExportedDescribeEventsRequest{})
				require.NoError(t, err)
				assert.Len(t, all, 2)

				// filter by source name
				filtered, err := b.DescribeEvents(&memorydb.ExportedDescribeEventsRequest{
					SourceName: "my-cluster",
				})
				require.NoError(t, err)
				assert.Len(t, filtered, 1)
				assert.Equal(t, "my-cluster", filtered[0].SourceName)

				// filter by source type
				byType, err := b.DescribeEvents(&memorydb.ExportedDescribeEventsRequest{
					SourceType: "cluster",
				})
				require.NoError(t, err)
				assert.Len(t, byType, 2)
			}
		})
	}
}

// TestBackend_MultiRegionParameterGroups tests DescribeMultiRegionParameterGroups.
func TestBackend_MultiRegionParameterGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filterName string
		wantErr    bool
		wantCount  int
	}{
		{
			name:      "list returns seeded defaults",
			wantCount: 4,
		},
		{
			name:       "not found by name",
			filterName: "no-such",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := memorydb.NewInMemoryBackend()
			groups, err := b.DescribeMultiRegionParameterGroups(tt.filterName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, groups, tt.wantCount)
		})
	}
}

// TestBackend_BatchUpdateCluster tests the BatchUpdateCluster backend method.
func TestBackend_BatchUpdateCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		clusterNames   []string
		wantFoundCount int
	}{
		{
			name:           "found clusters returned",
			clusterNames:   []string{"cluster-a", "cluster-b"},
			wantFoundCount: 2,
		},
		{
			name:           "unknown clusters omitted",
			clusterNames:   []string{"no-such"},
			wantFoundCount: 0,
		},
		{
			name:           "mixed found and not found",
			clusterNames:   []string{"cluster-a", "no-such"},
			wantFoundCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := memorydb.NewInMemoryBackend()

			// Seed clusters
			req := &memorydb.ExportedCreateClusterRequest{
				ClusterName: "cluster-a",
				NodeType:    "db.r6g.large",
				ACLName:     "open-access",
			}
			_, err := b.CreateCluster(testRegion, testAccountID, req)
			require.NoError(t, err)

			req2 := &memorydb.ExportedCreateClusterRequest{
				ClusterName: "cluster-b",
				NodeType:    "db.r6g.large",
				ACLName:     "open-access",
			}
			_, err = b.CreateCluster(testRegion, testAccountID, req2)
			require.NoError(t, err)

			found := b.BatchUpdateCluster(tt.clusterNames)
			assert.Len(t, found, tt.wantFoundCount)
		})
	}
}

// TestBackend_CopySnapshot_EdgeCases tests CopySnapshot edge cases.
func TestBackend_CopySnapshot_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "copy with no source",
			wantErr: true,
		},
		{
			name:    "copy to existing target",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := memorydb.NewInMemoryBackend()

			if tt.name == "copy to existing target" {
				// Pre-create the cluster that the snapshots reference.
				b.AddClusterInternal("cluster", "db.r6g.large")

				// Create source
				_, err := b.CreateSnapshot(testRegion, testAccountID, &memorydb.ExportedCreateSnapshotRequest{
					SnapshotName: "src",
					ClusterName:  "cluster",
				})
				require.NoError(t, err)

				// Create target first
				_, err = b.CreateSnapshot(testRegion, testAccountID, &memorydb.ExportedCreateSnapshotRequest{
					SnapshotName: "dst",
					ClusterName:  "cluster",
				})
				require.NoError(t, err)

				// Try to copy to same target name
				_, err = b.CopySnapshot(testRegion, testAccountID, &memorydb.ExportedCopySnapshotRequest{
					SourceSnapshotName: "src",
					TargetSnapshotName: "dst",
				})
				require.Error(t, err)
			} else {
				// Copy from non-existent source
				_, err := b.CopySnapshot(testRegion, testAccountID, &memorydb.ExportedCopySnapshotRequest{
					SourceSnapshotName: "no-such",
					TargetSnapshotName: "dst",
				})
				require.Error(t, err)
			}
		})
	}
}
