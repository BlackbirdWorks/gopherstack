package memorydb_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWireEpoch_DescribeServiceUpdates_DatesAreNumbers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

// TestRefinement3_DescribeServiceUpdates tests the DescribeServiceUpdates operation.
func TestHandler_DescribeServiceUpdates_Basic(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
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

// TestHandler_DescribeServiceUpdates_Filtered tests service update filtering.
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
			body:      map[string]any{"Status": []string{"pending"}},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
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

func TestHandler_DescribeServiceUpdates_FieldsPopulated(t *testing.T) {
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
