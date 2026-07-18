package codebuild_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCodeBuild_BuildBatch covers RetryBuildBatch, StopBuildBatch, ListBuildBatchesForProject.
func TestCodeBuild_BuildBatch(t *testing.T) {
	t.Parallel()

	t.Run("stop_build_batch", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "batch-proj")

		startRec := doRequest(t, h, "StartBuildBatch", map[string]any{"projectName": "batch-proj"})
		require.Equal(t, http.StatusOK, startRec.Code)

		var startOut struct {
			BuildBatch struct {
				ID string `json:"id"`
			} `json:"buildBatch"`
		}
		require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))
		batchID := startOut.BuildBatch.ID

		stopRec := doRequest(t, h, "StopBuildBatch", map[string]any{"id": batchID})
		require.Equal(t, http.StatusOK, stopRec.Code)

		var stopOut struct {
			BuildBatch struct {
				BuildBatchStatus string `json:"buildBatchStatus"`
			} `json:"buildBatch"`
		}
		require.NoError(t, json.NewDecoder(stopRec.Body).Decode(&stopOut))
		assert.Equal(t, "STOPPED", stopOut.BuildBatch.BuildBatchStatus)
	})

	t.Run("stop_build_batch_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "StopBuildBatch", map[string]any{"id": "ghost-proj:ghost-batch"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("retry_build_batch", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "batch-retry-proj")

		startRec := doRequest(t, h, "StartBuildBatch", map[string]any{"projectName": "batch-retry-proj"})
		require.Equal(t, http.StatusOK, startRec.Code)

		var startOut struct {
			BuildBatch struct {
				ID string `json:"id"`
			} `json:"buildBatch"`
		}
		require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))
		batchID := startOut.BuildBatch.ID

		retryRec := doRequest(t, h, "RetryBuildBatch", map[string]any{"id": batchID})
		require.Equal(t, http.StatusOK, retryRec.Code)

		var retryOut struct {
			BuildBatch struct {
				ID               string `json:"id"`
				BuildBatchStatus string `json:"buildBatchStatus"`
				ProjectName      string `json:"projectName"`
			} `json:"buildBatch"`
		}
		require.NoError(t, json.NewDecoder(retryRec.Body).Decode(&retryOut))
		assert.NotEqual(t, batchID, retryOut.BuildBatch.ID)
		assert.Equal(t, "IN_PROGRESS", retryOut.BuildBatch.BuildBatchStatus)
		assert.Equal(t, "batch-retry-proj", retryOut.BuildBatch.ProjectName)
	})

	t.Run("retry_build_batch_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "RetryBuildBatch", map[string]any{"id": "ghost-proj:ghost"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("list_build_batches_for_project", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "batch-list-proj")

		// Start 2 batches.
		doRequest(t, h, "StartBuildBatch", map[string]any{"projectName": "batch-list-proj"})
		doRequest(t, h, "StartBuildBatch", map[string]any{"projectName": "batch-list-proj"})

		listRec := doRequest(t, h, "ListBuildBatchesForProject", map[string]any{
			"projectName": "batch-list-proj",
		})
		require.Equal(t, http.StatusOK, listRec.Code)

		var out struct {
			IDs []string `json:"ids"`
		}
		require.NoError(t, json.NewDecoder(listRec.Body).Decode(&out))
		assert.Len(t, out.IDs, 2)
	})

	t.Run("list_build_batches_for_project_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "ListBuildBatchesForProject", map[string]any{
			"projectName": "ghost-proj",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestHandler_DeleteBuildBatch_RemovesBatch verifies DeleteBuildBatch removes the batch.
func TestHandler_DeleteBuildBatch_RemovesBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantDelete int
		missing    bool
	}{
		{
			name:       "delete_removes_batch",
			wantDelete: http.StatusOK,
		},
		{
			name:       "delete_missing_returns_404",
			wantDelete: http.StatusBadRequest,
			missing:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestProject(t, h, "batch-proj-"+tt.name)

			var batchID string
			if !tt.missing {
				startRec := doRequest(t, h, "StartBuildBatch", map[string]any{"projectName": "batch-proj-" + tt.name})
				require.Equal(t, http.StatusOK, startRec.Code)

				var startOut struct {
					BuildBatch struct {
						ID string `json:"id"`
					} `json:"buildBatch"`
				}
				require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))
				batchID = startOut.BuildBatch.ID
			} else {
				batchID = "ghost-proj:ghost-batch-id"
			}

			deleteRec := doRequest(t, h, "DeleteBuildBatch", map[string]any{"id": batchID})
			assert.Equal(t, tt.wantDelete, deleteRec.Code)

			if !tt.missing {
				listRec := doRequest(
					t,
					h,
					"ListBuildBatchesForProject",
					map[string]any{"projectName": "batch-proj-" + tt.name},
				)
				require.Equal(t, http.StatusOK, listRec.Code)

				var listOut struct {
					IDs []string `json:"ids"`
				}
				require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
				assert.NotContains(t, listOut.IDs, batchID, "batch should be removed after delete")
			}
		})
	}
}

// TestHandler_StartBuildBatch_SetsInProgress verifies StartBuildBatch sets IN_PROGRESS status.
func TestHandler_StartBuildBatch_SetsInProgress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		wantStatus       string
		wantStartTimeSet bool
	}{
		{
			name:             "batch_starts_in_progress",
			wantStatus:       "IN_PROGRESS",
			wantStartTimeSet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestProject(t, h, "batch-status-proj")

			rec := doRequest(t, h, "StartBuildBatch", map[string]any{"projectName": "batch-status-proj"})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				BuildBatch struct {
					BuildBatchStatus string  `json:"buildBatchStatus"`
					StartTime        float64 `json:"startTime"`
				} `json:"buildBatch"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Equal(t, tt.wantStatus, out.BuildBatch.BuildBatchStatus)
			if tt.wantStartTimeSet {
				assert.Greater(t, out.BuildBatch.StartTime, float64(0))
			}
		})
	}
}

// TestHandler_StopBuildBatch_SetsEndTime tests StopBuildBatch sets end time.
func TestHandler_StopBuildBatch_SetsEndTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantStatus     string
		wantEndTimeSet bool
	}{
		{
			name:           "stop_batch_sets_end_time",
			wantStatus:     "STOPPED",
			wantEndTimeSet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestProject(t, h, "stop-batch-proj")

			startRec := doRequest(t, h, "StartBuildBatch", map[string]any{"projectName": "stop-batch-proj"})
			require.Equal(t, http.StatusOK, startRec.Code)

			var startOut struct {
				BuildBatch struct {
					ID string `json:"id"`
				} `json:"buildBatch"`
			}
			require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))

			stopRec := doRequest(t, h, "StopBuildBatch", map[string]any{"id": startOut.BuildBatch.ID})
			require.Equal(t, http.StatusOK, stopRec.Code)

			var out struct {
				BuildBatch struct {
					BuildBatchStatus string  `json:"buildBatchStatus"`
					EndTime          float64 `json:"endTime"`
				} `json:"buildBatch"`
			}
			require.NoError(t, json.NewDecoder(stopRec.Body).Decode(&out))
			assert.Equal(t, tt.wantStatus, out.BuildBatch.BuildBatchStatus)
			if tt.wantEndTimeSet {
				assert.Greater(t, out.BuildBatch.EndTime, float64(0))
			}
		})
	}
}

// TestHandler_StartBuildBatch_ArnSet verifies StartBuildBatch returns a non-empty Arn,
// matching real AWS behavior.
func TestHandler_StartBuildBatch_ArnSet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	makeProject(t, h, "batch-arn-proj")

	rec := doRequest(t, h, "StartBuildBatch", map[string]any{"projectName": "batch-arn-proj"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		BuildBatch map[string]any `json:"buildBatch"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	batchArn, _ := out.BuildBatch["arn"].(string)
	assert.NotEmpty(t, batchArn, "StartBuildBatch must return a non-empty arn")
	assert.Contains(t, batchArn, "arn:aws:codebuild", "arn must be a valid ARN format")
}

// TestHandler_RetryBuildBatch_ArnSet verifies RetryBuildBatch sets a non-empty Arn.
func TestHandler_RetryBuildBatch_ArnSet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	makeProject(t, h, "retry-batch-proj")

	startRec := doRequest(t, h, "StartBuildBatch", map[string]any{"projectName": "retry-batch-proj"})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startOut struct {
		BuildBatch map[string]any `json:"buildBatch"`
	}
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	batchID, _ := startOut.BuildBatch["id"].(string)
	require.NotEmpty(t, batchID)

	retryRec := doRequest(t, h, "RetryBuildBatch", map[string]any{"id": batchID})
	require.Equal(t, http.StatusOK, retryRec.Code)

	var retryOut struct {
		BuildBatch map[string]any `json:"buildBatch"`
	}
	require.NoError(t, json.Unmarshal(retryRec.Body.Bytes(), &retryOut))
	arn, _ := retryOut.BuildBatch["arn"].(string)
	assert.NotEmpty(t, arn, "RetryBuildBatch must return a non-empty arn")
}
