package glue_test

// ops_batch2_audit_test.go — batch-2 ops AWS-accuracy audit for services/glue (go-3n5v).
// Covers four gaps discovered in batch-2 ops:
//
//  1. DeleteUsageProfile silently ignored missing profiles — AWS raises
//     EntityNotFoundException. Fix: add existence check before delete.
//
//  2. CancelDataQualityRuleRecommendationRun silently ignored missing run IDs
//     AND the handler discarded the backend error — AWS raises EntityNotFoundException.
//     Fix: return error from backend; propagate in handler.
//
//  3. StopColumnStatisticsTaskRun silently ignored unknown run IDs — AWS raises
//     EntityNotFoundException. Fix: add existence check before stop.
//
//  4. StopMaterializedViewRefreshTaskRun silently ignored unknown task run IDs —
//     AWS raises EntityNotFoundException. Fix: add existence check before stop.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatch2Audit_DeleteUsageProfile_NotFound verifies that DeleteUsageProfile
// raises EntityNotFoundException when the profile does not exist.
func TestBatch2Audit_DeleteUsageProfile_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		profName  string
		wantError string
		wantCode  int
		create    bool
	}{
		{
			name:      "delete_missing_profile_returns_entity_not_found",
			profName:  "ghost-profile",
			create:    false,
			wantCode:  http.StatusBadRequest,
			wantError: "EntityNotFoundException",
		},
		{
			name:     "delete_existing_profile_succeeds",
			profName: "real-profile",
			create:   true,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.create {
				rec := doGlueRequest(t, h, "CreateUsageProfile", map[string]any{"Name": tt.profName})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doGlueRequest(t, h, "DeleteUsageProfile", map[string]any{"Name": tt.profName})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

// TestBatch2Audit_CancelDQRecommendationRun_NotFound verifies that
// CancelDataQualityRuleRecommendationRun raises EntityNotFoundException for
// an unknown run ID instead of silently succeeding.
func TestBatch2Audit_CancelDQRecommendationRun_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		runID     string
		wantError string
		wantCode  int
		create    bool
	}{
		{
			name:      "cancel_missing_run_returns_entity_not_found",
			runID:     "nonexistent-run-id",
			create:    false,
			wantCode:  http.StatusBadRequest,
			wantError: "EntityNotFoundException",
		},
		{
			name:     "cancel_existing_run_succeeds",
			create:   true,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			runID := tt.runID
			if tt.create {
				rec := doGlueRequest(t, h, "StartDataQualityRuleRecommendationRun", map[string]any{
					"DataSource": map[string]any{
						"GlueTable": map[string]any{"DatabaseName": "db", "TableName": "tbl"},
					},
					"Role": "arn:aws:iam::123456789012:role/GlueRole",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					RunID string `json:"RunId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.NotEmpty(t, out.RunID, "expected RunId in response")
				runID = out.RunID
			}

			rec := doGlueRequest(t, h, "CancelDataQualityRuleRecommendationRun", map[string]any{
				"RunId": runID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

// TestBatch2Audit_StopColumnStatisticsTaskRun_NotFound verifies that
// StopColumnStatisticsTaskRun raises EntityNotFoundException for an unknown run ID.
func TestBatch2Audit_StopColumnStatisticsTaskRun_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		runID     string
		wantError string
		wantCode  int
		create    bool
	}{
		{
			name:      "stop_missing_run_returns_entity_not_found",
			runID:     "no-such-run",
			create:    false,
			wantCode:  http.StatusBadRequest,
			wantError: "EntityNotFoundException",
		},
		{
			name:     "stop_existing_run_succeeds",
			create:   true,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			runID := tt.runID
			if tt.create {
				rec := doGlueRequest(t, h, "StartColumnStatisticsTaskRun", map[string]any{
					"DatabaseName": "mydb",
					"TableName":    "mytable",
					"RoleArn":      "arn:aws:iam::123456789012:role/GlueRole",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					RunID string `json:"ColumnStatisticsTaskRunId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.NotEmpty(t, out.RunID, "expected ColumnStatisticsTaskRunId in response")
				runID = out.RunID
			}

			rec := doGlueRequest(t, h, "StopColumnStatisticsTaskRun", map[string]any{
				"ColumnStatisticsTaskRunId": runID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

// TestBatch2Audit_StopMaterializedViewRefreshTaskRun_NotFound verifies that
// StopMaterializedViewRefreshTaskRun raises EntityNotFoundException for an unknown task run ID.
func TestBatch2Audit_StopMaterializedViewRefreshTaskRun_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		runID     string
		wantError string
		wantCode  int
		create    bool
	}{
		{
			name:      "stop_missing_run_returns_entity_not_found",
			runID:     "no-such-mvr",
			create:    false,
			wantCode:  http.StatusBadRequest,
			wantError: "EntityNotFoundException",
		},
		{
			name:     "stop_existing_run_succeeds",
			create:   true,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			runID := tt.runID
			if tt.create {
				rec := doGlueRequest(t, h, "StartMaterializedViewRefreshTaskRun", map[string]any{
					"DatabaseName": "mydb",
					"TableName":    "myview",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					RunID string `json:"RunId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.NotEmpty(t, out.RunID, "expected RunId in response")
				runID = out.RunID
			}

			rec := doGlueRequest(t, h, "StopMaterializedViewRefreshTaskRun", map[string]any{
				"RunId": runID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}
