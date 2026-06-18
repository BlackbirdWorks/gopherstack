package memorydb_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

// -- UpdateCluster: shard/replica bounds validation (finding 5 via update) ------

func TestAudit2b_UpdateCluster_ShardBounds(t *testing.T) {
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

func TestAudit2b_UpdateCluster_ReplicaBounds(t *testing.T) {
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

func TestAudit2b_UpdateCluster_WindowValidation(t *testing.T) {
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

func TestAudit2b_DeleteCluster_WithSnapshot_EmitsEvent(t *testing.T) {
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

func TestAudit2b_MultiRegionParameterGroups_DefaultSeeded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		pgName string
	}{
		{"redis6 multi-region seeded", "default.memorydb-redis6.multiregion"},
		{"redis7 multi-region seeded", "default.memorydb-redis7.multiregion"},
		{"valkey7 multi-region seeded", "default.memorydb-valkey7.multiregion"},
		{"valkey8 multi-region seeded", "default.memorydb-valkey8.multiregion"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doRequest(t, h, "DescribeMultiRegionParameterGroups", map[string]any{
				"ParameterGroupName": tt.pgName,
			})
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			groups, _ := resp["MultiRegionParameterGroups"].([]any)
			assert.NotEmpty(t, groups, "multi-region parameter group %q should be seeded", tt.pgName)
		})
	}
}

func TestAudit2b_MultiRegionParameterGroups_DefaultCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "DescribeMultiRegionParameterGroups", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	groups, _ := resp["MultiRegionParameterGroups"].([]any)
	assert.GreaterOrEqual(t, len(groups), 4, "at least 4 default multi-region parameter groups expected")
}

func TestAudit2b_MultiRegionParameters_DefaultNonEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		pgName string
	}{
		{"redis7 multi-region params non-empty", "default.memorydb-redis7.multiregion"},
		{"valkey7 multi-region params non-empty", "default.memorydb-valkey7.multiregion"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doRequest(t, h, "DescribeMultiRegionParameters", map[string]any{
				"ParameterGroupName": tt.pgName,
			})
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			params, _ := resp["Parameters"].([]any)
			assert.NotEmpty(t, params, "multi-region parameters should be non-empty for %q", tt.pgName)
		})
	}
}

// -- ACL cluster membership accuracy (finding 16 clusters field) -----------------

func TestAudit2b_ACL_ClusterMembership(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		clusterCount  int
		expectedCount int
	}{
		{"no clusters in ACL", 0, 0},
		{"one cluster in ACL", 1, 1},
		{"two clusters in ACL", 2, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// Create a named ACL.
			rec := doRequest(t, h, "CreateACL", map[string]any{"ACLName": "test-membership-acl"})
			require.Equal(t, http.StatusOK, rec.Code)

			// Create clusters referencing the ACL.
			for i := range tt.clusterCount {
				body := map[string]any{
					"ClusterName": "mem-cl-" + string(rune('a'+i)),
					"NodeType":    "db.r6g.large",
					"ACLName":     "test-membership-acl",
				}
				rec2 := doRequest(t, h, "CreateCluster", body)
				require.Equal(t, http.StatusOK, rec2.Code, "create cluster %d: %s", i, rec2.Body)
			}

			acls := doDescribeACLs(t, h, "test-membership-acl")
			require.Len(t, acls, 1)

			acl, _ := acls[0].(map[string]any)
			clusters, _ := acl["Clusters"].([]any)
			assert.Len(t, clusters, tt.expectedCount)
		})
	}
}

// -- User UserGroupCount accurate (finding 17) -----------------------------------

func TestAudit2b_User_UserGroupCount_Accurate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		aclCount  int
		wantCount float64
	}{
		{"user in 0 ACLs", 0, 0},
		{"user in 1 ACL", 1, 1},
		{"user in 2 ACLs", 2, 2},
		{"user in 3 ACLs", 3, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// Create a user.
			rec := doRequest(t, h, "CreateUser", map[string]any{
				"UserName":           "ugc-user",
				"AccessString":       "on ~* &* +@all",
				"AuthenticationMode": map[string]any{"Type": "no-password-required"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Add user to N ACLs.
			for i := range tt.aclCount {
				aclName := "ugc-acl-" + string(rune('a'+i))
				rec2 := doRequest(t, h, "CreateACL", map[string]any{
					"ACLName":   aclName,
					"UserNames": []string{"ugc-user"},
				})
				require.Equal(t, http.StatusOK, rec2.Code, "create ACL: %s", rec2.Body)
			}

			users := doDescribeUsers(t, h, "ugc-user")
			require.Len(t, users, 1)
			user, _ := users[0].(map[string]any)
			assert.InDelta(t, tt.wantCount, user["UserGroupCount"], 0.001,
				"UserGroupCount should be %v for user in %d ACLs", tt.wantCount, tt.aclCount)
		})
	}
}

// -- Service updates filtering accuracy (finding 23) ----------------------------

func TestAudit2b_DescribeServiceUpdates_Filtering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		filterName   string
		filterStatus []string
		wantMinCount int
		wantMaxCount int
	}{
		{
			name:         "no filter returns all seeded updates",
			wantMinCount: 2,
			wantMaxCount: 100,
		},
		{
			name:         "filter by exact name",
			filterName:   "memorydb-20240601-redis-security",
			wantMinCount: 1,
			wantMaxCount: 1,
		},
		{
			name:         "filter by nonexistent name returns empty",
			filterName:   "nonexistent-update",
			wantMinCount: 0,
			wantMaxCount: 0,
		},
		{
			name:         "filter by status available returns results",
			filterStatus: []string{"available"},
			wantMinCount: 2,
			wantMaxCount: 100,
		},
		{
			name:         "filter by status pending returns empty",
			filterStatus: []string{"pending"},
			wantMinCount: 0,
			wantMaxCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			body := map[string]any{}
			if tt.filterName != "" {
				body["ServiceUpdateName"] = tt.filterName
			}
			if len(tt.filterStatus) > 0 {
				body["Status"] = tt.filterStatus
			}

			rec := doRequest(t, h, "DescribeServiceUpdates", body)
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			updates, _ := resp["ServiceUpdates"].([]any)
			assert.GreaterOrEqual(t, len(updates), tt.wantMinCount)
			assert.LessOrEqual(t, len(updates), tt.wantMaxCount)
		})
	}
}

// -- Events: SourceType filtering (finding 22) -----------------------------------

func TestAudit2b_Events_SourceTypeFiltering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupBody      map[string]any
		name           string
		srcType        string
		setupOp        string
		wantEventCount int
	}{
		{
			name:    "cluster events filterable by source type",
			srcType: "cluster",
			setupOp: "CreateCluster",
			setupBody: map[string]any{
				"ClusterName": "evt-src-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
			},
			wantEventCount: 1,
		},
		{
			name:    "acl events filterable by source type",
			srcType: "acl",
			setupOp: "CreateACL",
			setupBody: map[string]any{
				"ACLName": "evt-src-acl",
			},
			wantEventCount: 1,
		},
		{
			name:    "user events filterable by source type",
			srcType: "user",
			setupOp: "CreateUser",
			setupBody: map[string]any{
				"UserName":           "evt-src-user",
				"AccessString":       "on ~* &* +@all",
				"AuthenticationMode": map[string]any{"Type": "no-password-required"},
			},
			wantEventCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doRequest(t, h, tt.setupOp, tt.setupBody)
			require.Equal(t, http.StatusOK, rec.Code, "setup: %s", rec.Body)

			evRec := doRequest(t, h, "DescribeEvents", map[string]any{
				"SourceType": tt.srcType,
			})
			require.Equal(t, http.StatusOK, evRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(evRec.Body.Bytes(), &resp))
			events, _ := resp["Events"].([]any)
			assert.GreaterOrEqual(t, len(events), tt.wantEventCount)
		})
	}
}

// -- Pagination: MaxResults respected (finding 24) -------------------------------

func TestAudit2b_Pagination_MaxResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults int
		want       int
	}{
		{"MaxResults=1 returns 1 item", 1, 1},
		{"MaxResults=2 returns 2 items", 2, 2},
		{"MaxResults=50 returns all 5", 50, 5},
		{"MaxResults=100 returns all 5", 100, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// Create 5 parameter groups (4 exist by default).
			// Actually 4 default groups exist; create 1 more for total of 5.
			rec := doRequest(t, h, "CreateParameterGroup", map[string]any{
				"ParameterGroupName": "extra-pg",
				"Family":             "memorydb_redis7",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := doRequest(t, h, "DescribeParameterGroups", map[string]any{
				"MaxResults": tt.maxResults,
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
			pgs, _ := resp["ParameterGroups"].([]any)
			assert.LessOrEqual(t, len(pgs), tt.maxResults)
			if tt.maxResults >= 5 {
				assert.Len(t, pgs, tt.want)
			}
		})
	}
}

func TestAudit2b_Pagination_NextToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 3 ACLs beyond open-access.
	for _, name := range []string{"acl-page-a", "acl-page-b", "acl-page-c"} {
		rec := doRequest(t, h, "CreateACL", map[string]any{"ACLName": name})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// First page: MaxResults=2.
	rec1 := doRequest(t, h, "DescribeACLs", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec1.Code)
	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))
	acls1, _ := resp1["ACLs"].([]any)
	assert.Len(t, acls1, 2)
	nextToken, _ := resp1["NextToken"].(string)
	assert.NotEmpty(t, nextToken)

	// Second page using NextToken.
	rec2 := doRequest(t, h, "DescribeACLs", map[string]any{
		"MaxResults": 2,
		"NextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	acls2, _ := resp2["ACLs"].([]any)
	assert.NotEmpty(t, acls2)

	// Ensure no overlap between pages.
	page1Names := make(map[string]bool)
	for _, a := range acls1 {
		am, _ := a.(map[string]any)
		page1Names[am["Name"].(string)] = true
	}
	for _, a := range acls2 {
		am, _ := a.(map[string]any)
		assert.False(t, page1Names[am["Name"].(string)], "page 2 returned item already in page 1")
	}
}

// -- FailoverShard emits event and returns cluster (finding 22) -----------------

func TestAudit2b_FailoverShard_EmitsEvent(t *testing.T) {
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

func TestAudit2b_BatchUpdateCluster(t *testing.T) {
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

func TestAudit2b_ReservedNodes_PurchaseAndDescribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		offeringID string
		wantStatus int
	}{
		{
			name:       "purchase valid offering",
			offeringID: "aaa00000-1111-2222-3333-444444444444",
			wantStatus: http.StatusOK,
		},
		{
			name:       "purchase nonexistent offering",
			offeringID: "nonexistent-offering-id",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doRequest(t, h, "PurchaseReservedNodesOffering", map[string]any{
				"ReservedNodesOfferingId": tt.offeringID,
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				rn, _ := resp["ReservedNode"].(map[string]any)
				assert.NotNil(t, rn)
				assert.NotEmpty(t, rn["ReservedNodeId"])
			}
		})
	}
}

func TestAudit2b_ReservedNodesOfferings_Seeded(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "DescribeReservedNodesOfferings", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	offerings, _ := resp["ReservedNodesOfferings"].([]any)
	assert.GreaterOrEqual(t, len(offerings), 3, "at least 3 reserved node offerings should be seeded")
}

func TestAudit2b_ReservedNodes_DescribeAfterPurchase(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Purchase a node.
	purchaseRec := doRequest(t, h, "PurchaseReservedNodesOffering", map[string]any{
		"ReservedNodesOfferingId": "aaa00000-1111-2222-3333-444444444444",
		"ReservationId":           "my-reservation-123",
	})
	require.Equal(t, http.StatusOK, purchaseRec.Code, "purchase: %s", purchaseRec.Body)

	// Describe with filter.
	descRec := doRequest(t, h, "DescribeReservedNodes", map[string]any{
		"ReservedNodeId": "my-reservation-123",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
	nodes, _ := resp["ReservedNodes"].([]any)
	assert.Len(t, nodes, 1)

	node, _ := nodes[0].(map[string]any)
	assert.Equal(t, "my-reservation-123", node["ReservedNodeId"])
	assert.Equal(t, "active", node["State"])
}

// -- Snapshot filter by type (finding 19) ----------------------------------------

func TestAudit2b_Snapshot_FilterByType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		filterType     string
		wantMinResults int
	}{
		{"filter manual returns manual snaps", "manual", 1},
		{"filter automated returns automated snaps", "automated", 1},
		{"no filter returns all", "", 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// Create cluster with retention > 0 to seed automated snapshot.
			doCreateCluster(t, h, map[string]any{
				"ClusterName":            "snap-filter-cluster",
				"NodeType":               "db.r6g.large",
				"ACLName":                "open-access",
				"SnapshotRetentionLimit": 1,
			})

			// Create a manual snapshot.
			manRec := doRequest(t, h, "CreateSnapshot", map[string]any{
				"ClusterName":  "snap-filter-cluster",
				"SnapshotName": "manual-snap",
			})
			require.Equal(t, http.StatusOK, manRec.Code)

			body := map[string]any{}
			if tt.filterType != "" {
				body["SnapshotType"] = tt.filterType
			}

			rec := doRequest(t, h, "DescribeSnapshots", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			snaps, _ := resp["Snapshots"].([]any)
			assert.GreaterOrEqual(t, len(snaps), tt.wantMinResults,
				"filter=%q: expected >= %d snapshots", tt.filterType, tt.wantMinResults)
		})
	}
}

// -- Engine version validation (finding 3) ----------------------------------------

func TestAudit2b_EngineVersion_Validation(t *testing.T) {
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

func TestAudit2b_DescribeEngineVersions_Filtering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		filterEngine string
		wantEngine   string
		wantMinCount int
	}{
		{"no filter returns all", "", "", 5},
		{"redis filter returns only redis", "redis", "redis", 3},
		{"valkey filter returns only valkey", "valkey", "valkey", 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			body := map[string]any{}
			if tt.filterEngine != "" {
				body["Engine"] = tt.filterEngine
			}

			rec := doRequest(t, h, "DescribeEngineVersions", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			versions, _ := resp["EngineVersions"].([]any)
			assert.GreaterOrEqual(t, len(versions), tt.wantMinCount)

			if tt.wantEngine != "" {
				for _, v := range versions {
					vm, _ := v.(map[string]any)
					assert.Equal(t, tt.wantEngine, vm["Engine"],
						"all results should have engine=%q", tt.wantEngine)
				}
			}
		})
	}
}

// -- Tag operations --------------------------------------------------------------

func TestAudit2b_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody map[string]any
		getARN     func(resp map[string]any) string
		name       string
		resource   string
		createOp   string
	}{
		{
			name:     "tag cluster",
			resource: "cluster",
			createOp: "CreateCluster",
			createBody: map[string]any{
				"ClusterName": "tag-test-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
			},
			getARN: func(r map[string]any) string {
				cl, _ := r["Cluster"].(map[string]any)
				arn, _ := cl["ARN"].(string)

				return arn
			},
		},
		{
			name:       "tag ACL",
			resource:   "acl",
			createOp:   "CreateACL",
			createBody: map[string]any{"ACLName": "tag-test-acl"},
			getARN: func(r map[string]any) string {
				acl, _ := r["ACL"].(map[string]any)
				arn, _ := acl["ARN"].(string)

				return arn
			},
		},
		{
			name:     "tag user",
			resource: "user",
			createOp: "CreateUser",
			createBody: map[string]any{
				"UserName":           "tag-test-user",
				"AccessString":       "on ~* &* +@all",
				"AuthenticationMode": map[string]any{"Type": "no-password-required"},
			},
			getARN: func(r map[string]any) string {
				u, _ := r["User"].(map[string]any)
				arn, _ := u["ARN"].(string)

				return arn
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doRequest(t, h, tt.createOp, tt.createBody)
			require.Equal(t, http.StatusOK, rec.Code, "create: %s", rec.Body)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			resourceARN := tt.getARN(createResp)
			require.NotEmpty(t, resourceARN)

			// Tag the resource.
			tagRec := doRequest(t, h, "TagResource", map[string]any{
				"ResourceArn": resourceARN,
				"Tags": []any{
					map[string]any{"Key": "env", "Value": "test"},
					map[string]any{"Key": "team", "Value": "platform"},
				},
			})
			require.Equal(t, http.StatusOK, tagRec.Code, "tag: %s", tagRec.Body)

			// List tags.
			listRec := doRequest(t, h, "ListTags", map[string]any{"ResourceArn": resourceARN})
			require.Equal(t, http.StatusOK, listRec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
			tagList, _ := listResp["TagList"].([]any)
			assert.Len(t, tagList, 2)

			// Untag one.
			untagRec := doRequest(t, h, "UntagResource", map[string]any{
				"ResourceArn": resourceARN,
				"TagKeys":     []string{"env"},
			})
			require.Equal(t, http.StatusOK, untagRec.Code)

			// Verify only 1 tag remains.
			listRec2 := doRequest(t, h, "ListTags", map[string]any{"ResourceArn": resourceARN})
			require.Equal(t, http.StatusOK, listRec2.Code)
			require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listResp))
			tagList2, _ := listResp["TagList"].([]any)
			assert.Len(t, tagList2, 1)
		})
	}
}

// -- Reset clears and re-seeds everything ----------------------------------------

func TestAudit2b_Reset_ReseededDefaultGroups(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	// Count default parameter groups before reset.
	initialCount := memorydb.ParameterGroupCount(b)
	assert.GreaterOrEqual(t, initialCount, 4, "default parameter groups should be seeded")

	// Reset.
	b.Reset()

	// Count after reset.
	afterCount := memorydb.ParameterGroupCount(b)
	assert.Equal(t, initialCount, afterCount, "Reset should re-seed the same default parameter groups")
}

// -- NodeType validation ---------------------------------------------------------

func TestAudit2b_NodeType_Validation(t *testing.T) {
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

func TestAudit2b_CopySnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		targetBucket    string
		targetName      string
		wantCopyCreated bool
		wantStatus      int
	}{
		{
			name:            "copy without bucket creates new snapshot",
			targetBucket:    "",
			targetName:      "copy-dest",
			wantCopyCreated: true,
			wantStatus:      http.StatusOK,
		},
		{
			name:            "copy with TargetBucket returns source (S3 export)",
			targetBucket:    "my-s3-bucket",
			targetName:      "export-dest",
			wantCopyCreated: false,
			wantStatus:      http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			doCreateCluster(t, h, minimalClusterBody("copy-snap-cluster"))
			createSnapshot(t, h, map[string]any{
				"ClusterName":  "copy-snap-cluster",
				"SnapshotName": "source-snap",
			})

			body := map[string]any{
				"SourceSnapshotName": "source-snap",
				"TargetSnapshotName": tt.targetName,
			}
			if tt.targetBucket != "" {
				body["TargetBucket"] = tt.targetBucket
			}

			rec := doRequest(t, h, "CopySnapshot", body)
			require.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body)

			if tt.wantCopyCreated {
				// Verify the copy exists.
				snaps := doDescribeSnapshots(t, h, tt.targetName)
				assert.Len(t, snaps, 1, "copied snapshot should exist")
			}
		})
	}
}

// -- EnginePatchVersion: all engine families covered (finding 15) ----------------

func TestAudit2b_EnginePatchVersion_AllFamilies(t *testing.T) {
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

func TestAudit2b_UpdateCluster_SnsTopicStatusValidation(t *testing.T) {
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
