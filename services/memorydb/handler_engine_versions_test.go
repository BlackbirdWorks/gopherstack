package memorydb_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestHandler_DescribeEngineVersions_Filtering(t *testing.T) {
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

// TestHandler_DescribeEngineVersions_Filtered tests engine version filtering.
func TestHandler_DescribeEngineVersions_Filtered(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantMin    int
	}{
		{
			name:       "all versions",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantMin:    1,
		},
		{
			name:       "filter by engine redis",
			body:       map[string]any{"Engine": "redis"},
			wantStatus: http.StatusOK,
			wantMin:    1,
		},
		{
			name:       "filter by family",
			body:       map[string]any{"ParameterGroupFamily": "memorydb_redis7"},
			wantStatus: http.StatusOK,
			wantMin:    1,
		},
		{
			name:       "default only returns first",
			body:       map[string]any{"DefaultOnly": true},
			wantStatus: http.StatusOK,
			wantMin:    1,
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
			assert.GreaterOrEqual(t, len(versions), tt.wantMin)
		})
	}
}

func TestHandler_DescribeEngineVersions_IncludesValkey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         map[string]any
		name         string
		wantEngines  []string
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

func TestHandler_DescribeEngineVersions_EachHasEngine(t *testing.T) {
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

func TestHandler_DescribeEngineVersions_ValkeySupportedVersions(t *testing.T) {
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
