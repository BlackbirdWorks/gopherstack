package codebuild_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

func TestHandler_BatchGetSandboxes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		queryIDs      func(ids []string) []string
		name          string
		seedSandboxes int
		wantFound     int
		wantNotFound  int
	}{
		{
			name:          "returns_seeded_sandbox",
			seedSandboxes: 1,
			queryIDs:      func(ids []string) []string { return ids },
			wantFound:     1,
			wantNotFound:  0,
		},
		{
			name:          "missing_id_in_not_found",
			seedSandboxes: 0,
			queryIDs:      func(_ []string) []string { return []string{"ghost-sandbox"} },
			wantFound:     0,
			wantNotFound:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seededIDs := make([]string, 0, tt.seedSandboxes)

			for range tt.seedSandboxes {
				id := "sandbox-abc"
				h.Backend.AddSandboxInternal(&codebuild.Sandbox{
					ID:     id,
					Arn:    "arn:aws:codebuild:us-east-1:000000000000:sandbox/sandbox-abc",
					Status: "RUNNING",
				})
				seededIDs = append(seededIDs, id)
			}

			queryIDs := tt.queryIDs(seededIDs)
			rec := doRequest(t, h, "BatchGetSandboxes", map[string]any{"ids": queryIDs})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

			sandboxes, _ := out["sandboxes"].([]any)
			assert.Len(t, sandboxes, tt.wantFound)

			notFound, _ := out["sandboxesNotFound"].([]any)
			assert.Len(t, notFound, tt.wantNotFound)
		})
	}
}

// TestCodeBuild_Sandbox covers StopSandbox, ListSandboxesForProject.
func TestCodeBuild_Sandbox(t *testing.T) {
	t.Parallel()

	t.Run("stop_sandbox", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "sandbox-proj")

		startRec := doRequest(t, h, "StartSandbox", map[string]any{"projectName": "sandbox-proj"})
		require.Equal(t, http.StatusOK, startRec.Code)

		var startOut struct {
			Sandbox struct {
				ID string `json:"id"`
			} `json:"sandbox"`
		}
		require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))
		sandboxID := startOut.Sandbox.ID

		stopRec := doRequest(t, h, "StopSandbox", map[string]any{"id": sandboxID})
		require.Equal(t, http.StatusOK, stopRec.Code)

		var stopOut struct {
			Sandbox struct {
				Status string `json:"status"`
			} `json:"sandbox"`
		}
		require.NoError(t, json.NewDecoder(stopRec.Body).Decode(&stopOut))
		assert.Equal(t, "STOPPED", stopOut.Sandbox.Status)
	})

	t.Run("stop_sandbox_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "StopSandbox", map[string]any{"id": "ghost-sandbox"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("list_sandboxes_for_project", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "sandbox-list-proj")

		doRequest(t, h, "StartSandbox", map[string]any{"projectName": "sandbox-list-proj"})
		doRequest(t, h, "StartSandbox", map[string]any{"projectName": "sandbox-list-proj"})

		listRec := doRequest(t, h, "ListSandboxesForProject", map[string]any{
			"projectName": "sandbox-list-proj",
		})
		require.Equal(t, http.StatusOK, listRec.Code)

		var out struct {
			IDs []string `json:"ids"`
		}
		require.NoError(t, json.NewDecoder(listRec.Body).Decode(&out))
		assert.Len(t, out.IDs, 2)
	})

	t.Run("list_sandboxes_for_project_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "ListSandboxesForProject", map[string]any{
			"projectName": "ghost-proj",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestHandler_StartSandbox_Schema tests expanded sandbox schema.
func TestHandler_StartSandbox_Schema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		wantArnSet bool
	}{
		{
			name:       "sandbox_has_arn_and_start_time",
			wantStatus: "READY",
			wantArnSet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestProject(t, h, "sandbox-schema-proj")

			rec := doRequest(t, h, "StartSandbox", map[string]any{"projectName": "sandbox-schema-proj"})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Sandbox struct {
					ID        string  `json:"id"`
					Arn       string  `json:"arn"`
					Status    string  `json:"status"`
					StartTime float64 `json:"startTime"`
				} `json:"sandbox"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Equal(t, tt.wantStatus, out.Sandbox.Status)
			if tt.wantArnSet {
				assert.NotEmpty(t, out.Sandbox.Arn)
				assert.Contains(t, out.Sandbox.Arn, "sandbox/")
			}
			assert.Greater(t, out.Sandbox.StartTime, float64(0))
		})
	}
}

// TestHandler_StopSandbox_SetsEndTime tests sandbox stop sets end time.
func TestHandler_StopSandbox_SetsEndTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantStatus     string
		wantEndTimeSet bool
	}{
		{
			name:           "stop_sets_end_time",
			wantStatus:     "STOPPED",
			wantEndTimeSet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestProject(t, h, "stop-sandbox-proj")

			startRec := doRequest(t, h, "StartSandbox", map[string]any{"projectName": "stop-sandbox-proj"})
			require.Equal(t, http.StatusOK, startRec.Code)

			var startOut struct {
				Sandbox struct {
					ID string `json:"id"`
				} `json:"sandbox"`
			}
			require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))

			stopRec := doRequest(t, h, "StopSandbox", map[string]any{"id": startOut.Sandbox.ID})
			require.Equal(t, http.StatusOK, stopRec.Code)

			var out struct {
				Sandbox struct {
					Status  string  `json:"status"`
					EndTime float64 `json:"endTime"`
				} `json:"sandbox"`
			}
			require.NoError(t, json.NewDecoder(stopRec.Body).Decode(&out))
			assert.Equal(t, tt.wantStatus, out.Sandbox.Status)
			if tt.wantEndTimeSet {
				assert.Greater(t, out.Sandbox.EndTime, float64(0))
			}
		})
	}
}
