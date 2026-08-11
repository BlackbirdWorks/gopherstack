package memorydb_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createRedisCluster seeds a redis-engine cluster so DescribeServiceUpdates
// (which fans updates out per matching cluster, see service_updates.go) has
// something to report against.
func createRedisCluster(t *testing.T, h *memorydb.Handler, name string) {
	t.Helper()

	doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": name,
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})
}

func TestWireEpoch_DescribeServiceUpdates_DatesAreNumbers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRedisCluster(t, h, "epoch-cluster")

	rec := doRequest(t, h, "DescribeServiceUpdates", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	updates, _ := resp["ServiceUpdates"].([]any)
	require.NotEmpty(t, updates)

	for _, raw := range updates {
		su, _ := raw.(map[string]any)

		_, releaseIsNumber := su["ReleaseDate"].(float64)
		assert.True(t, releaseIsNumber, "ServiceUpdate.ReleaseDate must be a JSON number, got %T", su["ReleaseDate"])

		_, autoIsNumber := su["AutoUpdateStartDate"].(float64)
		assert.True(t, autoIsNumber,
			"ServiceUpdate.AutoUpdateStartDate must be a JSON number, got %T", su["AutoUpdateStartDate"])
	}
}

func TestHandler_DescribeServiceUpdates_Basic(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRedisCluster(t, h, "basic-cluster")

	rec := doRequest(t, h, "DescribeServiceUpdates", map[string]any{})

	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotNil(t, out["ServiceUpdates"])
}

func TestHandler_DescribeServiceUpdates_Filtering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		filterName   string
		filterStatus []string
		wantMinCount int
		wantMaxCount int
	}{
		{
			name:         "no filter returns all seeded updates for the cluster",
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
			name:         "filter by status complete returns empty (nothing applied yet)",
			filterStatus: []string{"complete"},
			wantMinCount: 0,
			wantMaxCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			createRedisCluster(t, h, "filter-cluster")

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

func TestHandler_DescribeServiceUpdates_Filtered(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      map[string]any
		name      string
		wantCount int
	}{
		{name: "all updates", body: map[string]any{}, wantCount: 2},
		{
			name:      "filter by name",
			body:      map[string]any{"ServiceUpdateName": "memorydb-20240601-redis-security"},
			wantCount: 1,
		},
		{
			name:      "filter by status",
			body:      map[string]any{"Status": []string{"available"}},
			wantCount: 2,
		},
		{
			name:      "filter by non-existent status",
			body:      map[string]any{"Status": []string{"complete"}},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createRedisCluster(t, h, "filtered-cluster")

			rec := doRequest(t, h, "DescribeServiceUpdates", tt.body)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			updates := resp["ServiceUpdates"].([]any)
			assert.Len(t, updates, tt.wantCount)
		})
	}
}

func TestHandler_DescribeServiceUpdates_SeededFixtures(t *testing.T) {
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
			body:         map[string]any{"Status": []string{"in-progress"}},
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
			createRedisCluster(t, h, "seeded-cluster")

			rec := doRequest(t, h, "DescribeServiceUpdates", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			updates := resp["ServiceUpdates"].([]any)
			assert.GreaterOrEqual(t, len(updates), tt.wantMinCount)
		})
	}
}

func TestHandler_DescribeServiceUpdates_FieldsPopulated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRedisCluster(t, h, "fields-cluster")

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
		assert.Equal(t, "fields-cluster", su["ClusterName"])
	}
}

// TestHandler_DescribeServiceUpdates_ClusterNamesFilter proves the ClusterNames
// filter now actually scopes the response instead of being parsed and ignored
// (pre-fix: a nonexistent cluster name still returned every seeded update).
func TestHandler_DescribeServiceUpdates_ClusterNamesFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRedisCluster(t, h, "cn-redis-cluster")
	doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "cn-valkey-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
		"Engine":      "valkey",
	})

	tests := []struct {
		name         string
		clusterNames []string
		wantClusters []string
		wantCount    int
	}{
		{
			name:         "unfiltered returns updates for every cluster whose engine matches",
			clusterNames: nil,
			wantCount:    2, // 2 seeded redis updates x 1 redis cluster; valkey cluster has no seeded updates
			wantClusters: []string{"cn-redis-cluster"},
		},
		{
			name:         "named nonexistent cluster returns nothing",
			clusterNames: []string{"no-such-cluster"},
			wantCount:    0,
		},
		{
			name:         "named redis cluster returns its updates",
			clusterNames: []string{"cn-redis-cluster"},
			wantCount:    2,
			wantClusters: []string{"cn-redis-cluster"},
		},
		{
			name:         "named valkey cluster returns none (no valkey updates seeded)",
			clusterNames: []string{"cn-valkey-cluster"},
			wantCount:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := map[string]any{}
			if tt.clusterNames != nil {
				body["ClusterNames"] = tt.clusterNames
			}

			rec := doRequest(t, h, "DescribeServiceUpdates", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			updates := resp["ServiceUpdates"].([]any)
			require.Len(t, updates, tt.wantCount)

			for _, u := range updates {
				su := u.(map[string]any)
				assert.Contains(t, tt.wantClusters, su["ClusterName"])
			}
		})
	}
}

// TestHandler_BatchUpdateCluster_ThenDescribe proves applying a service update
// via BatchUpdateCluster flips that cluster's status to "complete" in a
// subsequent DescribeServiceUpdates, instead of the operation being a no-op.
func TestHandler_BatchUpdateCluster_ThenDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRedisCluster(t, h, "apply-cluster")

	applyRec := doRequest(t, h, "BatchUpdateCluster", map[string]any{
		"ClusterNames": []string{"apply-cluster"},
		"ServiceUpdate": map[string]any{
			"ServiceUpdateNameToApply": "memorydb-20240601-redis-security",
		},
	})
	require.Equal(t, http.StatusOK, applyRec.Code)

	rec := doRequest(t, h, "DescribeServiceUpdates", map[string]any{
		"ServiceUpdateName": "memorydb-20240601-redis-security",
		"ClusterNames":      []string{"apply-cluster"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	updates := resp["ServiceUpdates"].([]any)
	require.Len(t, updates, 1)
	assert.Equal(t, "complete", updates[0].(map[string]any)["Status"])
}
