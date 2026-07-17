package codebuild_test

import (
	"encoding/json"
	"net/http"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_StartBuild(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		projectName string
		wantStatus  int
		createFirst bool
	}{
		{
			name:        "success",
			projectName: "build-project",
			createFirst: true,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "project_not_found",
			projectName: "nonexistent-project",
			createFirst: false,
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.createFirst {
				doRequest(t, h, "CreateProject", map[string]any{
					"name":      tt.projectName,
					"source":    map[string]any{"type": "NO_SOURCE"},
					"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
					"environment": map[string]any{
						"type":        "LINUX_CONTAINER",
						"image":       "aws/codebuild/standard:5.0",
						"computeType": "BUILD_GENERAL1_SMALL",
					},
				})
			}

			rec := doRequest(t, h, "StartBuild", map[string]any{
				"projectName": tt.projectName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out struct {
					Build struct {
						ID          string `json:"id"`
						BuildStatus string `json:"buildStatus"`
					} `json:"build"`
				}
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				assert.NotEmpty(t, out.Build.ID)
				assert.Equal(t, "IN_PROGRESS", out.Build.BuildStatus)
			}
		})
	}
}

func TestHandler_StartBuild_TimestampsAreNumbers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	env := map[string]any{
		"type":        "LINUX_CONTAINER",
		"image":       "aws/codebuild/standard:5.0",
		"computeType": "BUILD_GENERAL1_SMALL",
	}
	createRec := doRequest(t, h, "CreateProject", map[string]any{
		"name":        "ts-build-project",
		"source":      map[string]any{"type": "NO_SOURCE"},
		"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
		"environment": env,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	startRec := doRequest(t, h, "StartBuild", map[string]any{"projectName": "ts-build-project"})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startOut map[string]any
	require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))

	build, ok := startOut["build"].(map[string]any)
	require.True(t, ok, "response should contain 'build' object")

	_, startTimeIsNumber := build["startTime"].(float64)
	assert.True(t, startTimeIsNumber, "startTime should be a JSON number (Unix epoch), not a string")

	buildID, _ := build["id"].(string)
	require.NotEmpty(t, buildID)

	stopRec := doRequest(t, h, "StopBuild", map[string]any{"id": buildID})
	require.Equal(t, http.StatusOK, stopRec.Code)

	var stopOut map[string]any
	require.NoError(t, json.NewDecoder(stopRec.Body).Decode(&stopOut))

	stoppedBuild, ok := stopOut["build"].(map[string]any)
	require.True(t, ok, "stop response should contain 'build' object")

	_, endTimeIsNumber := stoppedBuild["endTime"].(float64)
	assert.True(t, endTimeIsNumber, "endTime should be a JSON number (Unix epoch), not a string")
}

func TestHandler_BatchGetBuilds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantNotFound   []string
		wantBuildCount int
		queryExisting  bool
	}{
		{
			name:           "returns_builds",
			queryExisting:  true,
			wantBuildCount: 1,
			wantNotFound:   []string{},
		},
		{
			name:           "not_found_in_buildsNotFound",
			queryExisting:  false,
			wantBuildCount: 0,
			wantNotFound:   []string{"nonexistent:build123"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateProject", map[string]any{
				"name":      "batch-build-project",
				"source":    map[string]any{"type": "NO_SOURCE"},
				"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
				"environment": map[string]any{
					"type":        "LINUX_CONTAINER",
					"image":       "aws/codebuild/standard:5.0",
					"computeType": "BUILD_GENERAL1_SMALL",
				},
			})

			var buildID string
			if tt.queryExisting {
				startRec := doRequest(t, h, "StartBuild", map[string]any{
					"projectName": "batch-build-project",
				})
				require.Equal(t, http.StatusOK, startRec.Code)

				var startOut struct {
					Build struct {
						ID string `json:"id"`
					} `json:"build"`
				}
				require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))
				buildID = startOut.Build.ID
			} else {
				buildID = "nonexistent:build123"
			}

			rec := doRequest(t, h, "BatchGetBuilds", map[string]any{
				"ids": []string{buildID},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Builds         []any    `json:"builds"`
				BuildsNotFound []string `json:"buildsNotFound"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Len(t, out.Builds, tt.wantBuildCount)
			assert.Equal(t, tt.wantNotFound, out.BuildsNotFound)
		})
	}
}

func TestHandler_StopBuild(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		useReal    bool
	}{
		{
			name:       "success",
			useReal:    true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			useReal:    false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var buildID string

			if tt.useReal {
				doRequest(t, h, "CreateProject", map[string]any{
					"name":      "stop-build-project",
					"source":    map[string]any{"type": "NO_SOURCE"},
					"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
					"environment": map[string]any{
						"type":        "LINUX_CONTAINER",
						"image":       "aws/codebuild/standard:5.0",
						"computeType": "BUILD_GENERAL1_SMALL",
					},
				})

				startRec := doRequest(t, h, "StartBuild", map[string]any{
					"projectName": "stop-build-project",
				})
				require.Equal(t, http.StatusOK, startRec.Code)

				var startOut struct {
					Build struct {
						ID string `json:"id"`
					} `json:"build"`
				}
				require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))
				buildID = startOut.Build.ID
			} else {
				buildID = "nonexistent:build999"
			}

			rec := doRequest(t, h, "StopBuild", map[string]any{"id": buildID})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out struct {
					Build struct {
						BuildStatus string `json:"buildStatus"`
					} `json:"build"`
				}
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				assert.Equal(t, "STOPPED", out.Build.BuildStatus)
			}
		})
	}
}

// TestHandler_StopBuild_SetsStoppedNotSucceeded verifies StopBuild sets status to STOPPED,
// not SUCCEEDED.
func TestHandler_StopBuild_SetsStoppedNotSucceeded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		wantBuildStatus   string
		wantCurrentPhase  string
		wantBuildComplete bool
	}{
		{
			name:              "stop_sets_STOPPED_not_SUCCEEDED",
			wantBuildStatus:   "STOPPED",
			wantCurrentPhase:  "COMPLETED",
			wantBuildComplete: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestProject(t, h, "stop-proj")
			startRec := doRequest(t, h, "StartBuild", map[string]any{"projectName": "stop-proj"})
			require.Equal(t, http.StatusOK, startRec.Code)

			var startOut struct {
				Build struct {
					ID string `json:"id"`
				} `json:"build"`
			}
			require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))

			stopRec := doRequest(t, h, "StopBuild", map[string]any{"id": startOut.Build.ID})
			require.Equal(t, http.StatusOK, stopRec.Code)

			var out struct {
				Build struct {
					BuildStatus   string  `json:"buildStatus"`
					CurrentPhase  string  `json:"currentPhase"`
					BuildComplete bool    `json:"buildComplete"`
					EndTime       float64 `json:"endTime"`
				} `json:"build"`
			}
			require.NoError(t, json.NewDecoder(stopRec.Body).Decode(&out))
			assert.Equal(t, tt.wantBuildStatus, out.Build.BuildStatus)
			assert.Equal(t, tt.wantCurrentPhase, out.Build.CurrentPhase)
			assert.Equal(t, tt.wantBuildComplete, out.Build.BuildComplete)
			assert.Greater(t, out.Build.EndTime, float64(0), "endTime should be set")
		})
	}
}

func TestHandler_ListBuildsForProject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		projectName string
		buildCount  int
		wantStatus  int
	}{
		{
			name:        "returns_ids",
			projectName: "list-builds-project",
			buildCount:  2,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "project_not_found",
			projectName: "nonexistent",
			buildCount:  0,
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.buildCount > 0 {
				doRequest(t, h, "CreateProject", map[string]any{
					"name":      tt.projectName,
					"source":    map[string]any{"type": "NO_SOURCE"},
					"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
					"environment": map[string]any{
						"type":        "LINUX_CONTAINER",
						"image":       "aws/codebuild/standard:5.0",
						"computeType": "BUILD_GENERAL1_SMALL",
					},
				})

				for range tt.buildCount {
					doRequest(t, h, "StartBuild", map[string]any{
						"projectName": tt.projectName,
					})
				}
			}

			rec := doRequest(t, h, "ListBuildsForProject", map[string]any{
				"projectName": tt.projectName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out struct {
					IDs []string `json:"ids"`
				}
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				assert.Len(t, out.IDs, tt.buildCount)
			}
		})
	}
}

func TestHandler_ListBuildsForProject_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		projectName string
		wantStatus  int
	}{
		{
			name:        "nonexistent project",
			projectName: "nonexistent-project",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "empty project name",
			projectName: "",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "ListBuildsForProject", map[string]any{
				"projectName": tt.projectName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_BatchDeleteBuilds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		buildIDs    []string
		wantDeleted int
		wantHTTP    int
	}{
		{
			name:        "delete_existing_build",
			wantDeleted: 1,
			wantHTTP:    http.StatusOK,
		},
		{
			name:        "delete_missing_build",
			buildIDs:    []string{"nonexistent:abc123"},
			wantDeleted: 0,
			wantHTTP:    http.StatusOK,
		},
		{
			name:        "delete_empty_list",
			buildIDs:    []string{},
			wantDeleted: 0,
			wantHTTP:    http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create a project and start a build.
			doRequest(t, h, "CreateProject", map[string]any{
				"name":      "my-project",
				"source":    map[string]string{"type": "NO_SOURCE"},
				"artifacts": map[string]string{"type": "NO_ARTIFACTS"},
				"environment": map[string]string{
					"type":        "LINUX_CONTAINER",
					"image":       "aws/codebuild/standard:5.0",
					"computeType": "BUILD_GENERAL1_SMALL",
				},
				"serviceRole": "arn:aws:iam::000000000000:role/codebuild",
			})

			var buildID string
			if tt.buildIDs == nil {
				// Start a build and capture the ID.
				startRec := doRequest(t, h, "StartBuild", map[string]any{
					"projectName": "my-project",
				})
				var startResp map[string]any
				require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
				build := startResp["build"].(map[string]any)
				buildID = build["id"].(string)
				tt.buildIDs = []string{buildID}
			}

			rec := doRequest(t, h, "BatchDeleteBuilds", map[string]any{
				"ids": tt.buildIDs,
			})
			assert.Equal(t, tt.wantHTTP, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			deleted := resp["buildsDeleted"].([]any)
			assert.Len(t, deleted, tt.wantDeleted)
		})
	}
}

// TestHandler_BatchDeleteBuilds_NotDeletedList verifies BatchDeleteBuilds includes
// buildsNotDeleted for IDs that did not exist, matching real AWS behavior.
func TestHandler_BatchDeleteBuilds_NotDeletedList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "BatchDeleteBuilds", map[string]any{
		"ids": []string{"nonexistent-proj:abc123", "also-missing:xyz456"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		BuildsDeleted    []string         `json:"buildsDeleted"`
		BuildsNotDeleted []map[string]any `json:"buildsNotDeleted"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out.BuildsDeleted, "no builds deleted when IDs do not exist")
	assert.Len(t, out.BuildsNotDeleted, 2, "both missing IDs must appear in buildsNotDeleted")

	if len(out.BuildsNotDeleted) >= 1 {
		assert.NotEmpty(t, out.BuildsNotDeleted[0]["id"], "buildsNotDeleted item must have id field")
		assert.NotEmpty(t, out.BuildsNotDeleted[0]["statusCode"], "buildsNotDeleted item must have statusCode field")
	}
}

func TestHandler_ListBuilds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create project and start 2 builds.
	doRequest(t, h, "CreateProject", map[string]any{
		"name":      "proj",
		"source":    map[string]string{"type": "NO_SOURCE"},
		"artifacts": map[string]string{"type": "NO_ARTIFACTS"},
		"environment": map[string]string{
			"type":        "LINUX_CONTAINER",
			"image":       "aws/codebuild/standard:5.0",
			"computeType": "BUILD_GENERAL1_SMALL",
		},
		"serviceRole": "arn:aws:iam::000000000000:role/codebuild",
	})
	doRequest(t, h, "StartBuild", map[string]any{"projectName": "proj"})
	doRequest(t, h, "StartBuild", map[string]any{"projectName": "proj"})

	rec := doRequest(t, h, "ListBuilds", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ids := resp["ids"].([]any)
	assert.Len(t, ids, 2)
}

func TestHandler_SortedBuilds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	env := map[string]any{
		"type":        "LINUX_CONTAINER",
		"image":       "aws/codebuild/standard:5.0",
		"computeType": "BUILD_GENERAL1_SMALL",
	}
	createRec := doRequest(t, h, "CreateProject", map[string]any{
		"name":        "sort-build-proj",
		"source":      map[string]any{"type": "NO_SOURCE"},
		"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
		"environment": env,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	// Start 3 builds.
	for range 3 {
		rec := doRequest(t, h, "StartBuild", map[string]any{"projectName": "sort-build-proj"})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// ListBuilds should return sorted IDs.
	listRec := doRequest(t, h, "ListBuilds", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&out))

	ids, _ := out["ids"].([]any)
	require.Len(t, ids, 3)

	idStrs := make([]string, len(ids))
	for i, id := range ids {
		idStrs[i] = id.(string)
	}

	assert.True(t, sort.StringsAreSorted(idStrs), "ListBuilds should return sorted IDs")
}

func TestHandler_RetryBuild(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		id       string
		wantHTTP int
	}{
		{
			name:     "retry_existing",
			wantHTTP: http.StatusOK,
		},
		{
			name:     "retry_missing",
			id:       "nonexistent:abc",
			wantHTTP: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateProject", map[string]any{
				"name":      "proj",
				"source":    map[string]string{"type": "NO_SOURCE"},
				"artifacts": map[string]string{"type": "NO_ARTIFACTS"},
				"environment": map[string]string{
					"type":        "LINUX_CONTAINER",
					"image":       "aws/codebuild/standard:5.0",
					"computeType": "BUILD_GENERAL1_SMALL",
				},
				"serviceRole": "arn:aws:iam::000000000000:role/codebuild",
			})

			id := tt.id
			if id == "" {
				startRec := doRequest(t, h, "StartBuild", map[string]any{"projectName": "proj"})
				var startResp map[string]any
				require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
				build := startResp["build"].(map[string]any)
				id = build["id"].(string)
			}

			rec := doRequest(t, h, "RetryBuild", map[string]any{"id": id})
			assert.Equal(t, tt.wantHTTP, rec.Code)
		})
	}
}
