package codebuild_test

import (
	"encoding/json"
	"net/http"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

func TestHandler_BatchGetCommandExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         any
		name         string
		wantStatus   int
		wantFound    int
		wantNotFound int
	}{
		{
			name: "returns_seeded_execution",
			body: map[string]any{
				"sandboxId":           "sandbox-001",
				"commandExecutionIds": []string{"exec-001"},
			},
			wantStatus:   http.StatusOK,
			wantFound:    1,
			wantNotFound: 0,
		},
		{
			name: "missing_sandbox_id",
			body: map[string]any{
				"commandExecutionIds": []string{"exec-001"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "wrong_sandbox_id_yields_not_found",
			body: map[string]any{
				"sandboxId":           "other-sandbox",
				"commandExecutionIds": []string{"exec-001"},
			},
			wantStatus:   http.StatusOK,
			wantFound:    0,
			wantNotFound: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Seed a command execution on sandbox-001.
			h.Backend.AddCommandExecutionInternal(&codebuild.CommandExecution{
				ID:        "exec-001",
				SandboxID: "sandbox-001",
				Command:   "echo hello",
				Status:    "SUCCEEDED",
			})

			rec := doRequest(t, h, "BatchGetCommandExecutions", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

				executions, _ := out["commandExecutions"].([]any)
				assert.Len(t, executions, tt.wantFound)

				notFound, _ := out["commandExecutionsNotFound"].([]any)
				assert.Len(t, notFound, tt.wantNotFound)
			}
		})
	}
}

// TestHandler_StartCommandExecution_TypeStored tests that command type is stored.
func TestHandler_StartCommandExecution_TypeStored(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		execType string
		command  string
		wantType string
	}{
		{
			name:     "shell_type_stored",
			execType: "SHELL",
			command:  "echo hello",
			wantType: "SHELL",
		},
		{
			name:     "empty_type_stored",
			execType: "",
			command:  "ls",
			wantType: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestProject(t, h, "cmd-exec-proj")

			sbRec := doRequest(t, h, "StartSandbox", map[string]any{"projectName": "cmd-exec-proj"})
			require.Equal(t, http.StatusOK, sbRec.Code)

			var sbOut struct {
				Sandbox struct {
					ID string `json:"id"`
				} `json:"sandbox"`
			}
			require.NoError(t, json.NewDecoder(sbRec.Body).Decode(&sbOut))

			execRec := doRequest(t, h, "StartCommandExecution", map[string]any{
				"sandboxId": sbOut.Sandbox.ID,
				"command":   tt.command,
				"type":      tt.execType,
			})
			require.Equal(t, http.StatusOK, execRec.Code)

			var out struct {
				CommandExecution struct {
					Type      string  `json:"type"`
					Command   string  `json:"command"`
					Status    string  `json:"status"`
					StartTime float64 `json:"startTime"`
					EndTime   float64 `json:"endTime"`
				} `json:"commandExecution"`
			}
			require.NoError(t, json.NewDecoder(execRec.Body).Decode(&out))
			assert.Equal(t, tt.wantType, out.CommandExecution.Type)
			assert.Equal(t, tt.command, out.CommandExecution.Command)
			assert.Equal(t, "SUCCEEDED", out.CommandExecution.Status)
			assert.Greater(t, out.CommandExecution.StartTime, float64(0))
			assert.Greater(t, out.CommandExecution.EndTime, float64(0))
		})
	}
}

// TestCodeBuild_CommandExecutionsForSandbox covers ListCommandExecutionsForSandbox.
func TestCodeBuild_CommandExecutionsForSandbox(t *testing.T) {
	t.Parallel()

	t.Run("list_command_executions_for_sandbox", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "ce-proj")

		startRec := doRequest(t, h, "StartSandbox", map[string]any{"projectName": "ce-proj"})
		require.Equal(t, http.StatusOK, startRec.Code)

		var startOut struct {
			Sandbox struct {
				ID string `json:"id"`
			} `json:"sandbox"`
		}
		require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))
		sandboxID := startOut.Sandbox.ID

		doRequest(t, h, "StartCommandExecution", map[string]any{
			"sandboxId": sandboxID,
			"command":   "echo hello",
			"type":      "COMMAND",
		})
		doRequest(t, h, "StartCommandExecution", map[string]any{
			"sandboxId": sandboxID,
			"command":   "echo world",
			"type":      "COMMAND",
		})

		listRec := doRequest(t, h, "ListCommandExecutionsForSandbox", map[string]any{
			"sandboxId": sandboxID,
		})
		require.Equal(t, http.StatusOK, listRec.Code)

		var out struct {
			CommandExecutions []map[string]any `json:"commandExecutions"`
		}
		require.NoError(t, json.NewDecoder(listRec.Body).Decode(&out))
		assert.Len(t, out.CommandExecutions, 2)
	})

	t.Run("list_command_executions_for_sandbox_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "ListCommandExecutionsForSandbox", map[string]any{
			"sandboxId": "ghost-sandbox",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	// maxResults/sortOrder/nextToken are real ListCommandExecutionsForSandboxInput
	// fields (aws-sdk-go-v2 api_op_ListCommandExecutionsForSandbox.go) that
	// this op previously ignored entirely -- every call returned every
	// execution, unpaginated, in ascending-ID order regardless of what the
	// client requested.
	t.Run("pagination_and_sort_order", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "ce-page-proj")

		startRec := doRequest(t, h, "StartSandbox", map[string]any{"projectName": "ce-page-proj"})
		require.Equal(t, http.StatusOK, startRec.Code)

		var startOut struct {
			Sandbox struct {
				ID string `json:"id"`
			} `json:"sandbox"`
		}
		require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))
		sandboxID := startOut.Sandbox.ID

		ids := make([]string, 0, 3)

		for range 3 {
			rec := doRequest(t, h, "StartCommandExecution", map[string]any{
				"sandboxId": sandboxID,
				"command":   "echo hello",
				"type":      "COMMAND",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				CommandExecution struct {
					ID string `json:"id"`
				} `json:"commandExecution"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			ids = append(ids, out.CommandExecution.ID)
		}

		sortedAsc := append([]string(nil), ids...)
		sort.Strings(sortedAsc)

		firstPage := doRequest(t, h, "ListCommandExecutionsForSandbox", map[string]any{
			"sandboxId":  sandboxID,
			"maxResults": 2,
			"sortOrder":  "DESCENDING",
		})
		require.Equal(t, http.StatusOK, firstPage.Code)

		var firstOut struct {
			NextToken         string           `json:"nextToken"`
			CommandExecutions []map[string]any `json:"commandExecutions"`
		}
		require.NoError(t, json.NewDecoder(firstPage.Body).Decode(&firstOut))
		require.Len(t, firstOut.CommandExecutions, 2)
		assert.Equal(
			t,
			sortedAsc[2],
			firstOut.CommandExecutions[0]["id"],
			"DESCENDING must put the lexicographically largest ID first",
		)
		assert.Equal(t, sortedAsc[1], firstOut.CommandExecutions[1]["id"])
		require.NotEmpty(t, firstOut.NextToken, "a third execution remains, so a nextToken must be returned")

		secondPage := doRequest(t, h, "ListCommandExecutionsForSandbox", map[string]any{
			"sandboxId":  sandboxID,
			"maxResults": 2,
			"sortOrder":  "DESCENDING",
			"nextToken":  firstOut.NextToken,
		})
		require.Equal(t, http.StatusOK, secondPage.Code)

		var secondOut struct {
			CommandExecutions []map[string]any `json:"commandExecutions"`
		}
		require.NoError(t, json.NewDecoder(secondPage.Body).Decode(&secondOut))
		require.Len(t, secondOut.CommandExecutions, 1)
		assert.Equal(t, sortedAsc[0], secondOut.CommandExecutions[0]["id"], "the remainder must be the smallest ID")
	})
}
