package memorydb_test

import (
	"encoding/json"
	"net/http"
	"sync"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_DescribeParameterGroups_All tests DescribeParameterGroups with no filter.
func TestHandler_DescribeParameterGroups_All(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateParameterGroup", map[string]any{
		"ParameterGroupName": "pg-1",
		"Family":             "memorydb_redis7",
	})

	rec := doRequest(t, h, "DescribeParameterGroups", map[string]any{})
	assert.Equal(t, 200, rec.Code)
}

func TestRace_CreateParameterGroupVsUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateParameterGroup", map[string]any{
		"ParameterGroupName": "race-pg",
		"Family":             "memorydb_redis7",
	})

	var wg sync.WaitGroup

	for range 20 {
		wg.Go(func() {
			_, _ = doRequestAsync(h, "UpdateParameterGroup", map[string]any{
				"ParameterGroupName": "race-pg",
				"ParameterNameValues": []map[string]string{
					{"ParameterName": "maxmemory-policy", "ParameterValue": "allkeys-lru"},
				},
			})
		})
	}

	wg.Wait()
}

// TestRefinement3_DescribeParameters_OK tests DescribeParameters returns parameters for known group.
func TestHandler_DescribeParameters_OK(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateParameterGroup", map[string]any{
		"ParameterGroupName": "my-pg",
		"Family":             "memorydb_redis7",
	})

	rec := doRequest(t, h, "DescribeParameters", map[string]any{"ParameterGroupName": "my-pg"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotNil(t, out["Parameters"])
}

// TestRefinement3_DescribeParameters_NotFound tests DescribeParameters returns 404 for unknown group.
func TestHandler_DescribeParameters_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeParameters", map[string]any{"ParameterGroupName": "no-such-pg"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement3_DescribeParameters_BadJSON tests DescribeParameters with bad JSON.
func TestHandler_DescribeParameters_BadJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequestRaw(t, h, "DescribeParameters", []byte("not-json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement3_ResetParameterGroup_OK tests ResetParameterGroup succeeds for known group.
func TestHandler_ResetParameterGroup_OK(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateParameterGroup", map[string]any{
		"ParameterGroupName": "reset-pg",
		"Family":             "memorydb_redis7",
	})

	rec := doRequest(t, h, "ResetParameterGroup", map[string]any{
		"ParameterGroupName": "reset-pg",
		"AllParameters":      true,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement3_ResetParameterGroup_NotFound tests ResetParameterGroup returns 404 for unknown group.
func TestHandler_ResetParameterGroup_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ResetParameterGroup", map[string]any{"ParameterGroupName": "no-such-pg"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement3_ResetParameterGroup_BadJSON tests ResetParameterGroup with bad JSON.
func TestHandler_ResetParameterGroup_BadJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequestRaw(t, h, "ResetParameterGroup", []byte("not-json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
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

func TestHandler_DefaultParameterGroupsSeeded(t *testing.T) {
	t.Parallel()

	expectedGroups := []struct {
		name   string
		family string
	}{
		{"default.memorydb-redis6", "memorydb_redis6"},
		{"default.memorydb-redis7", "memorydb_redis7"},
		{"default.memorydb-valkey7", "memorydb_valkey7"},
		{"default.memorydb-valkey8", "memorydb_valkey8"},
	}

	for _, eg := range expectedGroups {
		t.Run(eg.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			pgs := doDescribeParameterGroups(t, h, eg.name)
			require.Len(t, pgs, 1, "default parameter group %q should exist", eg.name)

			pg, _ := pgs[0].(map[string]any)
			assert.Equal(t, eg.name, pg["Name"])
			assert.Equal(t, eg.family, pg["Family"])
		})
	}
}

func TestHandler_DescribeParameters_NonEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		pgName string
	}{
		{"redis6 default params non-empty", "default.memorydb-redis6"},
		{"redis7 default params non-empty", "default.memorydb-redis7"},
		{"valkey7 default params non-empty", "default.memorydb-valkey7"},
		{"valkey8 default params non-empty", "default.memorydb-valkey8"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			params := doDescribeParameters(t, h, tt.pgName)
			assert.NotEmpty(t, params, "default parameter group %q should have non-empty parameters", tt.pgName)

			// Verify a known parameter exists.
			found := false
			for _, p := range params {
				pm, _ := p.(map[string]any)
				if nm, _ := pm["Name"].(string); nm == "maxmemory-policy" {
					found = true
					val, _ := pm["Value"].(string)
					assert.Equal(t, "noeviction", val)

					break
				}
			}
			assert.True(t, found, "expected maxmemory-policy parameter in %q", tt.pgName)
		})
	}
}

// -- Finding 28: ResetParameterGroup honors ParameterNames -----------------------

func TestHandler_ResetParameterGroup_AllParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		paramNames    []string
		allParameters bool
	}{
		{"AllParameters=true resets all", nil, true},
		{"empty ParameterNames resets all", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// Create a parameter group and update some values.
			rec := doRequest(t, h, "CreateParameterGroup", map[string]any{
				"ParameterGroupName": "reset-test-pg",
				"Family":             "memorydb_redis7",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Change a value.
			rec = doRequest(t, h, "UpdateParameterGroup", map[string]any{
				"ParameterGroupName": "reset-test-pg",
				"ParameterNameValues": []map[string]any{
					{"ParameterName": "maxmemory-policy", "ParameterValue": "allkeys-lru"},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Verify the change.
			params := doDescribeParameters(t, h, "reset-test-pg")
			var currentVal string
			for _, p := range params {
				pm, _ := p.(map[string]any)
				if nm, _ := pm["Name"].(string); nm == "maxmemory-policy" {
					currentVal, _ = pm["Value"].(string)
				}
			}
			assert.Equal(t, "allkeys-lru", currentVal)

			// Reset.
			resetBody := map[string]any{
				"ParameterGroupName": "reset-test-pg",
				"AllParameters":      tt.allParameters,
			}
			if tt.paramNames != nil {
				resetBody["ParameterNames"] = tt.paramNames
			}
			rec = doRequest(t, h, "ResetParameterGroup", resetBody)
			require.Equal(t, http.StatusOK, rec.Code)

			// Verify reset back to default.
			params = doDescribeParameters(t, h, "reset-test-pg")
			var resetVal string
			for _, p := range params {
				pm, _ := p.(map[string]any)
				if nm, _ := pm["Name"].(string); nm == "maxmemory-policy" {
					resetVal, _ = pm["Value"].(string)
				}
			}
			assert.Equal(t, "noeviction", resetVal, "maxmemory-policy should be reset to default")
		})
	}
}

func TestHandler_ResetParameterGroup_SpecificNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		paramToChange string
		newValue      string
		paramToReset  string
		otherParam    string
		otherNewValue string
		wantReset     string
		wantOtherKept string
	}{
		{
			name:          "only named parameter is reset",
			paramToChange: "maxmemory-policy",
			newValue:      "allkeys-lru",
			paramToReset:  "maxmemory-policy",
			otherParam:    "hz",
			otherNewValue: "25",
			wantReset:     "noeviction",
			wantOtherKept: "25",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// Create parameter group.
			rec := doRequest(t, h, "CreateParameterGroup", map[string]any{
				"ParameterGroupName": "selective-reset-pg",
				"Family":             "memorydb_redis7",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Change two parameters.
			rec = doRequest(t, h, "UpdateParameterGroup", map[string]any{
				"ParameterGroupName": "selective-reset-pg",
				"ParameterNameValues": []map[string]any{
					{"ParameterName": tt.paramToChange, "ParameterValue": tt.newValue},
					{"ParameterName": tt.otherParam, "ParameterValue": tt.otherNewValue},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Reset only the first parameter.
			rec = doRequest(t, h, "ResetParameterGroup", map[string]any{
				"ParameterGroupName": "selective-reset-pg",
				"ParameterNames":     []string{tt.paramToReset},
				"AllParameters":      false,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Check the reset param.
			params := doDescribeParameters(t, h, "selective-reset-pg")
			vals := make(map[string]string)
			for _, p := range params {
				pm, _ := p.(map[string]any)
				name, _ := pm["Name"].(string)
				val, _ := pm["Value"].(string)
				vals[name] = val
			}

			assert.Equal(t, tt.wantReset, vals[tt.paramToReset], "reset parameter should be at default")
			assert.Equal(t, tt.wantOtherKept, vals[tt.otherParam], "other parameter should keep its value")
		})
	}
}

// -- Finding 22: Events generated ------------------------------------------------

func TestHandler_DescribeParameters_Metadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantField string
		wantValue string
	}{
		{"has ChangeType=immediate", "ChangeType", "immediate"},
		{"has Source=system", "Source", "system"},
		{"has MinimumEngineVersion=6.2", "MinimumEngineVersion", "6.2"},
		{"has DataType=string", "DataType", "string"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			params := doDescribeParameters(t, h, "default.memorydb-redis7")
			require.NotEmpty(t, params)

			// All parameters should have the expected field.
			for _, p := range params {
				pm, _ := p.(map[string]any)
				val, _ := pm[tt.wantField].(string)
				assert.Equal(t, tt.wantValue, val,
					"parameter %q should have %s=%q", pm["Name"], tt.wantField, tt.wantValue)
			}
		})
	}
}

// -- CreateParameterGroup seeds defaults -----------------------------------------

func TestHandler_CreateParameterGroup_SeedsDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		family string
	}{
		{"redis6 family seeded", "memorydb_redis6"},
		{"redis7 family seeded", "memorydb_redis7"},
		{"valkey7 family seeded", "memorydb_valkey7"},
		{"valkey8 family seeded", "memorydb_valkey8"},
	}

	// Each case uses a unique valid resource name (lowercase alphanumeric + hyphens).
	pgNames := map[string]string{
		"memorydb_redis6":  "my-pg-redis6",
		"memorydb_redis7":  "my-pg-redis7",
		"memorydb_valkey7": "my-pg-valkey7",
		"memorydb_valkey8": "my-pg-valkey8",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			pgName := pgNames[tt.family]
			rec := doRequest(t, h, "CreateParameterGroup", map[string]any{
				"ParameterGroupName": pgName,
				"Family":             tt.family,
			})
			require.Equal(t, http.StatusOK, rec.Code, "CreateParameterGroup failed: %s", rec.Body)

			params := doDescribeParameters(t, h, pgName)
			assert.NotEmpty(t, params, "newly created parameter group should have default parameters")

			// Spot-check a known default parameter.
			found := false
			for _, p := range params {
				pm, _ := p.(map[string]any)
				if nm, _ := pm["Name"].(string); nm == "maxmemory-policy" {
					found = true

					break
				}
			}
			assert.True(t, found, "maxmemory-policy should be seeded in new parameter group")
		})
	}
}

// -- Snapshot contains all expanded fields ---------------------------------------

func TestHandler_Pagination_MaxResults(t *testing.T) {
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

// TestHandler_DescribeParameters_Empty tests DescribeParameters with no group name.
func TestHandler_DescribeParameters_Empty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "empty parameter group name in request still dispatches",
			body:       map[string]any{"ParameterGroupName": ""},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "nonexistent parameter group returns 404",
			body:       map[string]any{"ParameterGroupName": "no-such-pg"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "valid parameter group returns 200",
			body:       map[string]any{"ParameterGroupName": "default.memorydb-redis7"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DescribeParameters", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_CreateParameterGroup_NoFamily tests creating parameter group without family.
func TestHandler_CreateParameterGroup_NoFamily(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "missing family returns 400",
			body:       map[string]any{"ParameterGroupName": "pg-no-family"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateParameterGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ResetParameterGroup_Variants tests various ResetParameterGroup invocations.
func TestHandler_ResetParameterGroup_Variants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "reset specific parameters",
			body: map[string]any{
				"ParameterGroupName": "my-pg",
				"ParameterNames":     []string{"timeout", "hz"},
				"AllParameters":      false,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "reset all parameters",
			body: map[string]any{
				"ParameterGroupName": "my-pg",
				"AllParameters":      true,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "reset nonexistent group returns 404",
			body: map[string]any{
				"ParameterGroupName": "no-such-pg",
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name != "reset nonexistent group returns 404" {
				doRequest(t, h, "CreateParameterGroup", map[string]any{
					"ParameterGroupName": "my-pg",
					"Family":             "memorydb_redis7",
				})
			}

			rec := doRequest(t, h, "ResetParameterGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ParameterGroupCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*memorydb.Handler)
		body       map[string]any
		name       string
		op         string
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
				doRequest(
					t,
					h,
					"CreateParameterGroup",
					map[string]any{"ParameterGroupName": "my-pg", "Family": "memorydb_redis7"},
				)
			},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name: "update parameter group",
			op:   "UpdateParameterGroup",
			setup: func(h *memorydb.Handler) {
				doRequest(
					t,
					h,
					"CreateParameterGroup",
					map[string]any{"ParameterGroupName": "my-pg", "Family": "memorydb_redis7"},
				)
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
				doRequest(
					t,
					h,
					"CreateParameterGroup",
					map[string]any{"ParameterGroupName": "my-pg", "Family": "memorydb_redis7"},
				)
			},
			body:       map[string]any{"ParameterGroupName": "my-pg"},
			wantStatus: http.StatusOK,
		},
		{
			name: "describe parameters",
			op:   "DescribeParameters",
			setup: func(h *memorydb.Handler) {
				doRequest(
					t,
					h,
					"CreateParameterGroup",
					map[string]any{"ParameterGroupName": "my-pg", "Family": "memorydb_redis7"},
				)
				doRequest(t, h, "UpdateParameterGroup", map[string]any{
					"ParameterGroupName": "my-pg",
					"ParameterNameValues": []map[string]any{
						{"ParameterName": "maxmemory-policy", "ParameterValue": "allkeys-lru"},
					},
				})
			},
			body:       map[string]any{"ParameterGroupName": "my-pg"},
			wantStatus: http.StatusOK,
		},
		{
			name: "delete parameter group",
			op:   "DeleteParameterGroup",
			setup: func(h *memorydb.Handler) {
				doRequest(
					t,
					h,
					"CreateParameterGroup",
					map[string]any{"ParameterGroupName": "my-pg", "Family": "memorydb_redis7"},
				)
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
