package ce_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ce"
)

// TestListCostAllocationTags_EmptyWithNoTags verifies empty list on fresh backend.
func TestListCostAllocationTags_EmptyWithNoTags(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	rec := doRequest(t, h, "ListCostAllocationTags", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		CostAllocationTags []any `json:"CostAllocationTags"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.NotNil(t, out.CostAllocationTags)
	assert.Empty(t, out.CostAllocationTags)
}

// TestUpdateCostAllocationTagsStatus_MultipleUpdates verifies error returns.
func TestUpdateCostAllocationTagsStatus_MultipleUpdates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		updates    []map[string]string
		wantErrors int
	}{
		{
			name: "all_valid",
			updates: []map[string]string{
				{"TagKey": "Env", "Status": "Active"},
				{"TagKey": "Team", "Status": "Inactive"},
			},
			wantErrors: 0,
		},
		{
			name: "one_invalid_status",
			updates: []map[string]string{
				{"TagKey": "Good", "Status": "Active"},
				{"TagKey": "Bad", "Status": "Unknown"},
			},
			wantErrors: 1,
		},
		{
			name: "all_invalid",
			updates: []map[string]string{
				{"TagKey": "A", "Status": "Enabled"},
				{"TagKey": "B", "Status": "Disabled"},
			},
			wantErrors: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))

			body := make([]any, 0, len(tt.updates))
			for _, u := range tt.updates {
				body = append(body, u)
			}

			rec := doRequest(t, h, "UpdateCostAllocationTagsStatus", map[string]any{
				"CostAllocationTagsStatus": body,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Errors []any `json:"Errors"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Len(t, out.Errors, tt.wantErrors)
		})
	}
}

// TestBackfillMultipleJobs verifies multiple backfill jobs tracked.
func TestBackfillMultipleJobs(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))

	// Start 3 backfill jobs
	for _, from := range []string{"2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z", "2024-03-01T00:00:00Z"} {
		rec := doRequest(t, h, "StartCostAllocationTagBackfill", map[string]any{
			"BackfillFrom": from,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	listRec := doRequest(t, h, "ListCostAllocationTagBackfillHistory", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		BackfillRequests []map[string]any `json:"BackfillRequests"`
	}
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&out))
	assert.Len(t, out.BackfillRequests, 3)
}

func TestCostAllocationTags_UpdateAndList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		updateBody     map[string]any
		filterStatus   string
		wantTagCount   int
		wantStatusCode int
	}{
		{
			name: "activate_two_tags_list_active",
			updateBody: map[string]any{
				"CostAllocationTagsStatus": []map[string]string{
					{"TagKey": "Env", "Status": "Active"},
					{"TagKey": "Team", "Status": "Active"},
				},
			},
			filterStatus:   "Active",
			wantTagCount:   2,
			wantStatusCode: http.StatusOK,
		},
		{
			name: "deactivate_one_tag",
			updateBody: map[string]any{
				"CostAllocationTagsStatus": []map[string]string{
					{"TagKey": "Project", "Status": "Inactive"},
				},
			},
			filterStatus:   "Inactive",
			wantTagCount:   1,
			wantStatusCode: http.StatusOK,
		},
		{
			name: "invalid_status_returns_errors",
			updateBody: map[string]any{
				"CostAllocationTagsStatus": []map[string]string{
					{"TagKey": "BadTag", "Status": "Enabled"},
				},
			},
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "UpdateCostAllocationTagsStatus", tt.updateBody)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			if tt.filterStatus == "" {
				return
			}

			listRec := doRequest(t, h, "ListCostAllocationTags", map[string]any{
				"Status": tt.filterStatus,
			})
			require.Equal(t, http.StatusOK, listRec.Code)

			var listOut struct {
				CostAllocationTags []struct {
					TagKey string `json:"TagKey"`
					Status string `json:"Status"`
					Type   string `json:"Type"`
				} `json:"CostAllocationTags"`
			}
			require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
			assert.Len(t, listOut.CostAllocationTags, tt.wantTagCount)

			for _, tag := range listOut.CostAllocationTags {
				assert.Equal(t, tt.filterStatus, tag.Status)
				assert.NotEmpty(t, tag.TagKey)
			}
		})
	}
}

func TestCostAllocationTags_ListFiltersWork(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create several tags
	doRequest(t, h, "UpdateCostAllocationTagsStatus", map[string]any{
		"CostAllocationTagsStatus": []map[string]string{
			{"TagKey": "Alpha", "Status": "Active"},
			{"TagKey": "Beta", "Status": "Active"},
			{"TagKey": "Gamma", "Status": "Inactive"},
		},
	})

	t.Run("filter_by_tag_keys", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, "ListCostAllocationTags", map[string]any{
			"TagKeys": []string{"Alpha", "Beta"},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			CostAllocationTags []struct {
				TagKey string `json:"TagKey"`
			} `json:"CostAllocationTags"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		assert.Len(t, out.CostAllocationTags, 2)
	})

	t.Run("filter_by_status_active", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, "ListCostAllocationTags", map[string]any{
			"Status": "Active",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			CostAllocationTags []struct {
				TagKey string `json:"TagKey"`
				Status string `json:"Status"`
			} `json:"CostAllocationTags"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		assert.Len(t, out.CostAllocationTags, 2)

		for _, tag := range out.CostAllocationTags {
			assert.Equal(t, "Active", tag.Status)
		}
	})
}

func TestCostAllocationTagBackfill_StateTracking(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Start backfill
	rec := doRequest(t, h, "StartCostAllocationTagBackfill", map[string]any{
		"BackfillFrom": "2024-01-01T00:00:00Z",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var startOut struct {
		BackfillRequest struct {
			BackfillFrom   string `json:"backfillFrom"`
			BackfillStatus string `json:"backfillStatus"`
			RequestedAt    string `json:"requestedAt"`
		} `json:"BackfillRequest"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&startOut))
	assert.Equal(t, "2024-01-01T00:00:00Z", startOut.BackfillRequest.BackfillFrom)
	assert.Equal(t, "PROCESSING", startOut.BackfillRequest.BackfillStatus)

	// List history
	listRec := doRequest(t, h, "ListCostAllocationTagBackfillHistory", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		BackfillRequests []struct {
			BackfillStatus string `json:"backfillStatus"`
		} `json:"BackfillRequests"`
	}
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	require.Len(t, listOut.BackfillRequests, 1)
	assert.Equal(t, "PROCESSING", listOut.BackfillRequests[0].BackfillStatus)
}

func TestCostAllocationTagBackfill_MissingFromReturns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "StartCostAllocationTagBackfill", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
