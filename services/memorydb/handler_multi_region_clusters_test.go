package memorydb_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_DescribeMultiRegionParameters verifies DescribeMultiRegionParameters.
func TestHandler_DescribeMultiRegionParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "missing parameter group name",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "non-existent parameter group",
			body:       map[string]any{"ParameterGroupName": "no-such"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeMultiRegionParameters", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DescribeMultiRegionParameters_WithGroup verifies parameters are returned for existing group.
func TestHandler_DescribeMultiRegionParameters_WithGroup(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	b.AddMultiRegionParameterGroupInternal("my-mr-pg", "memorydb_redis7")
	h := memorydb.NewHandler(b)

	rec := doRequest(t, h, "DescribeMultiRegionParameters", map[string]any{
		"ParameterGroupName": "my-mr-pg",
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Parameters"])
}

// TestRefinement3_ListAllowedMultiRegionClusterUpdates_OK tests the happy path.
func TestHandler_ListAllowedMultiRegionClusterUpdates_OK(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateMultiRegionCluster", map[string]any{
		"MultiRegionClusterNameSuffix": "mrc1",
		"NodeType":                     "db.r6g.large",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var mrcOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &mrcOut))
	mrc := mrcOut["MultiRegionCluster"].(map[string]any)
	mrcName := mrc["MultiRegionClusterName"].(string)

	rec2 := doRequest(t, h, "ListAllowedMultiRegionClusterUpdates", map[string]any{
		"MultiRegionClusterName": mrcName,
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// TestRefinement3_ListAllowedMultiRegionClusterUpdates_MissingName tests 400 for missing name.
func TestHandler_ListAllowedMultiRegionClusterUpdates_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListAllowedMultiRegionClusterUpdates", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_ListAllowedMultiRegionClusterUpdates_NotFound tests 400 for unknown cluster.
func TestHandler_ListAllowedMultiRegionClusterUpdates_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListAllowedMultiRegionClusterUpdates", map[string]any{
		"MultiRegionClusterName": "no-such-mrc",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_ListAllowedMultiRegionClusterUpdates_BadJSON tests with bad JSON.
func TestHandler_ListAllowedMultiRegionClusterUpdates_BadJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequestRaw(t, h, "ListAllowedMultiRegionClusterUpdates", []byte("not-json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_UpdateMultiRegionCluster_OK tests updating a multi-region cluster.
func TestHandler_UpdateMultiRegionCluster_OK(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateMultiRegionCluster", map[string]any{
		"MultiRegionClusterNameSuffix": "upd1",
		"NodeType":                     "db.r6g.large",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var mrcOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &mrcOut))
	mrc := mrcOut["MultiRegionCluster"].(map[string]any)
	mrcName := mrc["MultiRegionClusterName"].(string)

	rec2 := doRequest(t, h, "UpdateMultiRegionCluster", map[string]any{
		"MultiRegionClusterName": mrcName,
		"Description":            "updated description",
	})
	assert.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))
	updated := out["MultiRegionCluster"].(map[string]any)
	assert.Equal(t, "updated description", updated["Description"])
}

// TestRefinement3_UpdateMultiRegionCluster_MissingName tests 400 for missing name.
func TestHandler_UpdateMultiRegionCluster_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UpdateMultiRegionCluster", map[string]any{"Description": "desc"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_UpdateMultiRegionCluster_NotFound tests 400 for unknown cluster.
func TestHandler_UpdateMultiRegionCluster_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UpdateMultiRegionCluster", map[string]any{
		"MultiRegionClusterName": "no-such-mrc",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_UpdateMultiRegionCluster_BadJSON tests with bad JSON.
func TestHandler_UpdateMultiRegionCluster_BadJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequestRaw(t, h, "UpdateMultiRegionCluster", []byte("not-json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
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
			wantStatus: http.StatusBadRequest,
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
			wantStatus: http.StatusBadRequest,
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
			wantStatus: http.StatusBadRequest,
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

func TestHandler_MultiRegionParameterGroups_DefaultSeeded(t *testing.T) {
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

func TestHandler_MultiRegionParameterGroups_DefaultCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "DescribeMultiRegionParameterGroups", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	groups, _ := resp["MultiRegionParameterGroups"].([]any)
	assert.GreaterOrEqual(t, len(groups), 4, "at least 4 default multi-region parameter groups expected")
}

func TestHandler_MultiRegionParameters_DefaultNonEmpty(t *testing.T) {
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

// TestHandler_MultiRegionParameters_HaveSourceField verifies each returned
// parameter carries a "Source" field -- part of the real SDK's
// types.MultiRegionParameter (a distinct shape from types.Parameter that
// additionally carries Source: confirmed via types.go), which a prior pass
// omitted entirely by reusing the plain Parameter wire object for both
// DescribeParameters and DescribeMultiRegionParameters.
func TestHandler_MultiRegionParameters_HaveSourceField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "DescribeMultiRegionParameters", map[string]any{
		"ParameterGroupName": "default.memorydb-redis7.multiregion",
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	params, _ := resp["Parameters"].([]any)
	require.NotEmpty(t, params)

	for _, p := range params {
		pm, _ := p.(map[string]any)
		assert.NotEmpty(t, pm["Source"], "parameter %q must have a Source field", pm["Name"])
	}
}

// -- ACL cluster membership accuracy (finding 16 clusters field) -----------------

// TestHandler_DescribeMultiRegionClusters_NotFound tests MRC not found path.
func TestHandler_DescribeMultiRegionClusters_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "nonexistent MRC returns 400",
			body:       map[string]any{"MultiRegionClusterName": "virv-no-such"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeMultiRegionClusters", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DescribeMultiRegionParameters_Boost tests DescribeMultiRegionParameters paths.
func TestHandler_DescribeMultiRegionParameters_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "missing parameter group name returns 400",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "nonexistent group returns 400",
			body:       map[string]any{"ParameterGroupName": "no-such"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "valid multi-region parameter group returns 200",
			body: map[string]any{
				"ParameterGroupName": "default.memorydb-redis7.multiregion",
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeMultiRegionParameters", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DescribeMultiRegionParameterGroups_NotFound tests not found path.
func TestHandler_DescribeMultiRegionParameterGroups_FilteredAndNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "specific group by name",
			body:       map[string]any{"ParameterGroupName": "default.memorydb-redis7.multiregion"},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name:       "nonexistent group returns 400",
			body:       map[string]any{"ParameterGroupName": "no-such.multiregion"},
			wantStatus: http.StatusBadRequest,
			wantCount:  0,
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

func TestHandler_MultiRegionCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*memorydb.Handler)
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name: "create multi-region cluster",
			op:   "CreateMultiRegionCluster",
			body: map[string]any{
				"MultiRegionClusterNameSuffix": "my-cluster",
				"NodeType":                     "db.r6g.large",
				"Engine":                       "redis",
				"EngineVersion":                "7.0",
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
					"NodeType":                     "db.r6g.large",
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
					"NodeType":                     "db.r6g.large",
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
