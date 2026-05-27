package codebuild_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

func newTestHandler(t *testing.T) *codebuild.Handler {
	t.Helper()

	return codebuild.NewHandler(codebuild.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRequest(t *testing.T, h *codebuild.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "CodeBuild_20161006."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "CodeBuild", h.Name())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "codebuild", h.ChaosServiceName())
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateProject")
	assert.Contains(t, ops, "BatchGetProjects")
	assert.Contains(t, ops, "UpdateProject")
	assert.Contains(t, ops, "DeleteProject")
	assert.Contains(t, ops, "ListProjects")
	assert.Contains(t, ops, "StartBuild")
	assert.Contains(t, ops, "BatchGetBuilds")
	assert.Contains(t, ops, "StopBuild")
	assert.Contains(t, ops, "ListBuildsForProject")
	assert.Contains(t, ops, "ListTagsForResource")
	assert.Contains(t, ops, "TagResource")
	assert.Contains(t, ops, "UntagResource")
	assert.Contains(t, ops, "BatchGetBuildBatches")
	assert.Contains(t, ops, "BatchGetCommandExecutions")
	assert.Contains(t, ops, "BatchGetFleets")
	assert.Contains(t, ops, "BatchGetReportGroups")
	assert.Contains(t, ops, "BatchGetReports")
	assert.Contains(t, ops, "BatchGetSandboxes")
	assert.Contains(t, ops, "CreateFleet")
	assert.Contains(t, ops, "CreateReportGroup")
	assert.Contains(t, ops, "CreateWebhook")
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "matching target",
			target: "CodeBuild_20161006.CreateProject",
			want:   true,
		},
		{
			name:   "non-matching target",
			target: "AWSInsightsIndexService.CreateCostCategoryDefinition",
			want:   false,
		},
		{
			name:   "empty target",
			target: "",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestHandler_CreateProject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"name":        "my-project",
				"description": "test project",
				"source":      map[string]any{"type": "GITHUB", "location": "https://github.com/example/repo"},
				"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
				"environment": map[string]any{
					"type":        "LINUX_CONTAINER",
					"image":       "aws/codebuild/standard:5.0",
					"computeType": "BUILD_GENERAL1_SMALL",
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "duplicate_fails",
			body: map[string]any{
				"name":      "dup-project",
				"source":    map[string]any{"type": "NO_SOURCE"},
				"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
				"environment": map[string]any{
					"type":        "LINUX_CONTAINER",
					"image":       "aws/codebuild/standard:5.0",
					"computeType": "BUILD_GENERAL1_SMALL",
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_name",
			body:       map[string]any{"description": "no name"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate_fails" {
				rec := doRequest(t, h, "CreateProject", tt.body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "CreateProject", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateProject_TimestampsAreNumbers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateProject", map[string]any{
		"name":      "ts-project",
		"source":    map[string]any{"type": "NO_SOURCE"},
		"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
		"environment": map[string]any{
			"type":        "LINUX_CONTAINER",
			"image":       "aws/codebuild/standard:5.0",
			"computeType": "BUILD_GENERAL1_SMALL",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	project, ok := out["project"].(map[string]any)
	require.True(t, ok, "response should contain 'project' object")

	_, createdIsNumber := project["created"].(float64)
	assert.True(t, createdIsNumber, "created should be a JSON number (Unix epoch), not a string")

	_, lastModifiedIsNumber := project["lastModified"].(float64)
	assert.True(t, lastModifiedIsNumber, "lastModified should be a JSON number (Unix epoch), not a string")
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

func TestHandler_BatchGetProjects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		projectName      string
		queryNames       []string
		wantNotFound     []string
		wantProjectCount int
	}{
		{
			name:             "returns_project",
			projectName:      "existing-project",
			queryNames:       []string{"existing-project"},
			wantProjectCount: 1,
			wantNotFound:     []string{},
		},
		{
			name:             "not_found_in_projectsNotFound",
			projectName:      "some-project",
			queryNames:       []string{"nonexistent"},
			wantProjectCount: 0,
			wantNotFound:     []string{"nonexistent"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

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

			rec := doRequest(t, h, "BatchGetProjects", map[string]any{
				"names": tt.queryNames,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Projects         []any    `json:"projects"`
				ProjectsNotFound []string `json:"projectsNotFound"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Len(t, out.Projects, tt.wantProjectCount)
			assert.Equal(t, tt.wantNotFound, out.ProjectsNotFound)
		})
	}
}

func TestHandler_UpdateProject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		exists     bool
	}{
		{
			name:       "success",
			exists:     true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			exists:     false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			projectName := "update-project-" + tt.name

			if tt.exists {
				doRequest(t, h, "CreateProject", map[string]any{
					"name":      projectName,
					"source":    map[string]any{"type": "NO_SOURCE"},
					"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
					"environment": map[string]any{
						"type":        "LINUX_CONTAINER",
						"image":       "aws/codebuild/standard:5.0",
						"computeType": "BUILD_GENERAL1_SMALL",
					},
				})
			}

			rec := doRequest(t, h, "UpdateProject", map[string]any{
				"name":        projectName,
				"description": "updated description",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteProject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		exists     bool
	}{
		{
			name:       "success",
			exists:     true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			exists:     false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			projectName := "delete-project-" + tt.name

			if tt.exists {
				doRequest(t, h, "CreateProject", map[string]any{
					"name":      projectName,
					"source":    map[string]any{"type": "NO_SOURCE"},
					"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
					"environment": map[string]any{
						"type":        "LINUX_CONTAINER",
						"image":       "aws/codebuild/standard:5.0",
						"computeType": "BUILD_GENERAL1_SMALL",
					},
				})
			}

			rec := doRequest(t, h, "DeleteProject", map[string]any{"name": projectName})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListProjects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		projectNames []string
	}{
		{
			name:         "returns_names",
			projectNames: []string{"proj-a", "proj-b"},
		},
		{
			name:         "empty",
			projectNames: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, pn := range tt.projectNames {
				doRequest(t, h, "CreateProject", map[string]any{
					"name":      pn,
					"source":    map[string]any{"type": "NO_SOURCE"},
					"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
					"environment": map[string]any{
						"type":        "LINUX_CONTAINER",
						"image":       "aws/codebuild/standard:5.0",
						"computeType": "BUILD_GENERAL1_SMALL",
					},
				})
			}

			rec := doRequest(t, h, "ListProjects", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Projects []string `json:"projects"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

			for _, pn := range tt.projectNames {
				assert.Contains(t, out.Projects, pn)
			}
		})
	}
}

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

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		exists     bool
	}{
		{
			name:       "success",
			exists:     true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			exists:     false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			arn := "arn:aws:codebuild:us-east-1:000000000000:project/tag-test-project"

			if tt.exists {
				doRequest(t, h, "CreateProject", map[string]any{
					"name":      "tag-test-project",
					"source":    map[string]any{"type": "NO_SOURCE"},
					"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
					"environment": map[string]any{
						"type":        "LINUX_CONTAINER",
						"image":       "aws/codebuild/standard:5.0",
						"computeType": "BUILD_GENERAL1_SMALL",
					},
					"tags": map[string]string{"env": "test"},
				})
			} else {
				arn = "arn:aws:codebuild:us-east-1:000000000000:project/nonexistent"
			}

			rec := doRequest(t, h, "ListTagsForResource", map[string]any{
				"resourceArn": arn,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateProject", map[string]any{
				"name":      "tag-resource-project",
				"source":    map[string]any{"type": "NO_SOURCE"},
				"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
				"environment": map[string]any{
					"type":        "LINUX_CONTAINER",
					"image":       "aws/codebuild/standard:5.0",
					"computeType": "BUILD_GENERAL1_SMALL",
				},
			})

			createRec := doRequest(t, h, "BatchGetProjects", map[string]any{
				"names": []string{"tag-resource-project"},
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var batchOut struct {
				Projects []struct {
					Arn string `json:"arn"`
				} `json:"projects"`
			}
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&batchOut))
			require.NotEmpty(t, batchOut.Projects)
			projectARN := batchOut.Projects[0].Arn

			rec := doRequest(t, h, "TagResource", map[string]any{
				"resourceArn": projectARN,
				"tags":        map[string]string{"team": "backend"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
				"resourceArn": projectARN,
			})
			require.Equal(t, http.StatusOK, listRec.Code)

			var listOut struct {
				Tags map[string]string `json:"tags"`
			}
			require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
			assert.Equal(t, "backend", listOut.Tags["team"])
		})
	}
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "success",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateProject", map[string]any{
				"name":      "untag-resource-project",
				"source":    map[string]any{"type": "NO_SOURCE"},
				"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
				"environment": map[string]any{
					"type":        "LINUX_CONTAINER",
					"image":       "aws/codebuild/standard:5.0",
					"computeType": "BUILD_GENERAL1_SMALL",
				},
				"tags": map[string]string{"remove-me": "yes"},
			})

			batchRec := doRequest(t, h, "BatchGetProjects", map[string]any{
				"names": []string{"untag-resource-project"},
			})
			require.Equal(t, http.StatusOK, batchRec.Code)

			var batchOut struct {
				Projects []struct {
					Arn string `json:"arn"`
				} `json:"projects"`
			}
			require.NoError(t, json.NewDecoder(batchRec.Body).Decode(&batchOut))
			require.NotEmpty(t, batchOut.Projects)
			projectARN := batchOut.Projects[0].Arn

			rec := doRequest(t, h, "UntagResource", map[string]any{
				"resourceArn": projectARN,
				"tagKeys":     []string{"remove-me"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
				"resourceArn": projectARN,
			})
			require.Equal(t, http.StatusOK, listRec.Code)

			var listOut struct {
				Tags map[string]string `json:"tags"`
			}
			require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
			_, hasKey := listOut.Tags["remove-me"]
			assert.False(t, hasKey)
		})
	}
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "CodeBuild_20161006.UnknownOp")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantOps []string
		wantLen int
	}{
		{
			name:    "returns all supported operations",
			wantLen: 62,
			wantOps: []string{
				"CreateProject",
				"BatchGetProjects",
				"StartBuild",
				"StopBuild",
				"CreateFleet",
				"CreateReportGroup",
				"CreateWebhook",
				"BatchGetBuildBatches",
				"BatchGetCommandExecutions",
				"BatchGetSandboxes",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			ops := h.ChaosOperations()
			assert.Len(t, ops, tt.wantLen)
			for _, op := range tt.wantOps {
				assert.Contains(t, ops, op)
			}
		})
	}
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantRegions []string
	}{
		{
			name:        "returns configured region",
			wantRegions: []string{"us-east-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			regions := h.ChaosRegions()
			assert.Equal(t, tt.wantRegions, regions)
		})
	}
}

func TestHandler_Region(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantRegion string
	}{
		{
			name:       "returns backend region",
			wantRegion: "us-east-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			assert.Equal(t, tt.wantRegion, h.Backend.Region())
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		wantOp string
	}{
		{
			name:   "create project",
			target: "CodeBuild_20161006.CreateProject",
			wantOp: "CreateProject",
		},
		{
			name:   "batch get projects",
			target: "CodeBuild_20161006.BatchGetProjects",
			wantOp: "BatchGetProjects",
		},
		{
			name:   "empty target",
			target: "",
			wantOp: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			op := h.ExtractOperation(c)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		path         string
		wantResource string
	}{
		{
			name:         "always empty regardless of path",
			path:         "/",
			wantResource: "",
		},
		{
			name:         "empty for specific project path",
			path:         "/projects/my-project",
			wantResource: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			resource := h.ExtractResource(c)
			assert.Equal(t, tt.wantResource, resource)
		})
	}
}

func TestHandler_TagResource_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceArn string
		wantStatus  int
	}{
		{
			name:        "nonexistent project",
			resourceArn: "arn:aws:codebuild:us-east-1:000000000000:project/nonexistent",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "invalid arn format",
			resourceArn: "not-a-valid-arn",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "TagResource", map[string]any{
				"resourceArn": tt.resourceArn,
				"tags":        []map[string]string{{"key": "k", "value": "v"}},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UntagResource_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceArn string
		wantStatus  int
	}{
		{
			name:        "nonexistent project",
			resourceArn: "arn:aws:codebuild:us-east-1:000000000000:project/nonexistent",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "invalid arn format",
			resourceArn: "not-a-valid-arn",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "UntagResource", map[string]any{
				"resourceArn": tt.resourceArn,
				"tagKeys":     []string{"key"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
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

func TestProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantName string
	}{
		{
			name:     "init and name",
			wantName: "CodeBuild",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &codebuild.Provider{}
			assert.Equal(t, tt.wantName, p.Name())

			ctx := &service.AppContext{}
			svc, err := p.Init(ctx)
			require.NoError(t, err)
			assert.NotNil(t, svc)
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

func TestCodeBuild_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	b := codebuild.NewInMemoryBackend("000000000000", "us-east-1")

	// Create a project and start a build.
	src := codebuild.ProjectSource{Type: "NO_SOURCE"}
	arts := codebuild.ProjectArtifacts{Type: "NO_ARTIFACTS"}
	env := codebuild.ProjectEnvironment{
		Type:        "LINUX_CONTAINER",
		Image:       "aws/codebuild/standard:5.0",
		ComputeType: "BUILD_GENERAL1_SMALL",
	}
	_, err := b.CreateProject(codebuild.ProjectConfig{
		Name:        "snap-proj",
		ServiceRole: "arn:aws:iam::000000000000:role/codebuild",
		Source:      &src,
		Artifacts:   &arts,
		Environment: &env,
	})
	require.NoError(t, err)

	build, err := b.StartBuild("snap-proj")
	require.NoError(t, err)
	require.NotEmpty(t, build.ID)

	// Snapshot and restore.
	h := codebuild.NewHandler(b)
	snap := h.Snapshot()
	require.NotEmpty(t, snap)

	b2 := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := codebuild.NewHandler(b2)
	require.NoError(t, h2.Restore(snap))

	// Project is restored.
	projs := b2.ListProjects()
	require.Len(t, projs, 1)
	assert.Equal(t, "snap-proj", projs[0])

	// Build is restored.
	builds, notFound := b2.BatchGetBuilds([]string{build.ID})
	assert.Empty(t, notFound)
	require.Len(t, builds, 1)
	assert.Equal(t, "snap-proj", builds[0].ProjectName)

	// ListBuildsForProject uses the project index.
	ids, err := b2.ListBuildsForProject("snap-proj")
	require.NoError(t, err)
	require.Len(t, ids, 1)
	assert.Equal(t, build.ID, ids[0])
}

func TestHandler_CreateFleet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"name":            "my-fleet",
				"baseCapacity":    2,
				"computeType":     "BUILD_GENERAL1_SMALL",
				"environmentType": "LINUX_CONTAINER",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_name",
			body:       map[string]any{"baseCapacity": 2},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate_fails",
			body: map[string]any{
				"name":         "dup-fleet",
				"baseCapacity": 1,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate_fails" {
				rec := doRequest(t, h, "CreateFleet", tt.body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "CreateFleet", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_BatchGetFleets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setupFleets  []string
		queryNames   []string
		wantFound    int
		wantNotFound int
		wantStatus   int
	}{
		{
			name:         "returns_fleet",
			setupFleets:  []string{"fleet-a"},
			queryNames:   []string{"fleet-a"},
			wantFound:    1,
			wantNotFound: 0,
			wantStatus:   http.StatusOK,
		},
		{
			name:         "not_found_in_fleetsNotFound",
			setupFleets:  []string{},
			queryNames:   []string{"ghost-fleet"},
			wantFound:    0,
			wantNotFound: 1,
			wantStatus:   http.StatusOK,
		},
		{
			name:         "mixed_found_and_not_found",
			setupFleets:  []string{"fleet-x"},
			queryNames:   []string{"fleet-x", "ghost"},
			wantFound:    1,
			wantNotFound: 1,
			wantStatus:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, fn := range tt.setupFleets {
				rec := doRequest(t, h, "CreateFleet", map[string]any{
					"name":         fn,
					"baseCapacity": 1,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "BatchGetFleets", map[string]any{"names": tt.queryNames})
			require.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

			fleets, _ := out["fleets"].([]any)
			assert.Len(t, fleets, tt.wantFound)

			notFound, _ := out["fleetsNotFound"].([]any)
			assert.Len(t, notFound, tt.wantNotFound)
		})
	}
}

func TestHandler_CreateReportGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"name":         "my-report-group",
				"type":         "TEST",
				"exportConfig": map[string]any{"exportConfigType": "S3"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_name",
			body:       map[string]any{"type": "TEST"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_type",
			body:       map[string]any{"name": "no-type-rg"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate_fails",
			body: map[string]any{
				"name": "dup-rg",
				"type": "CODE_COVERAGE",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate_fails" {
				rec := doRequest(t, h, "CreateReportGroup", tt.body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "CreateReportGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_BatchGetReportGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setupGroups  []string
		wantFound    int
		wantNotFound int
	}{
		{
			name:         "returns_report_group_by_arn",
			setupGroups:  []string{"rg-one"},
			wantFound:    1,
			wantNotFound: 0,
		},
		{
			name:         "not_found",
			setupGroups:  []string{},
			wantFound:    0,
			wantNotFound: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			arns := make([]string, 0)
			for _, gn := range tt.setupGroups {
				rec := doRequest(t, h, "CreateReportGroup", map[string]any{
					"name": gn,
					"type": "TEST",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				rg, _ := out["reportGroup"].(map[string]any)
				arns = append(arns, rg["arn"].(string))
			}

			if len(tt.setupGroups) == 0 {
				arns = []string{"arn:aws:codebuild:us-east-1:000000000000:report-group/ghost"}
			}

			rec := doRequest(t, h, "BatchGetReportGroups", map[string]any{"reportGroupArns": arns})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

			groups, _ := out["reportGroups"].([]any)
			assert.Len(t, groups, tt.wantFound)

			notFound, _ := out["reportGroupsNotFound"].([]any)
			assert.Len(t, notFound, tt.wantNotFound)
		})
	}
}

func TestHandler_BatchGetReports(t *testing.T) {
	t.Parallel()

	tests := []struct {
		queryArns    func(arns []string) []string
		name         string
		seedReports  int
		wantFound    int
		wantNotFound int
	}{
		{
			name:         "returns_seeded_report",
			seedReports:  1,
			queryArns:    func(arns []string) []string { return arns },
			wantFound:    1,
			wantNotFound: 0,
		},
		{
			name:        "missing_arn_in_not_found",
			seedReports: 0,
			queryArns: func(_ []string) []string {
				return []string{"arn:aws:codebuild:us-east-1:000000000000:report/ghost:abc"}
			},
			wantFound:    0,
			wantNotFound: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seededArns := make([]string, 0, tt.seedReports)

			for range tt.seedReports {
				reportArn := "arn:aws:codebuild:us-east-1:000000000000:report/my-group:abc123"
				h.Backend.AddReportInternal(&codebuild.Report{
					Arn:    reportArn,
					Status: "SUCCEEDED",
					Type:   "TEST",
				})
				seededArns = append(seededArns, reportArn)
			}

			queryArns := tt.queryArns(seededArns)
			rec := doRequest(t, h, "BatchGetReports", map[string]any{"reportArns": queryArns})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

			reports, _ := out["reports"].([]any)
			assert.Len(t, reports, tt.wantFound)

			notFound, _ := out["reportsNotFound"].([]any)
			assert.Len(t, notFound, tt.wantNotFound)
		})
	}
}

func TestHandler_BatchGetBuildBatches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		queryIDs     func(ids []string) []string
		name         string
		seedBatches  int
		wantFound    int
		wantNotFound int
	}{
		{
			name:         "returns_seeded_batch",
			seedBatches:  1,
			queryIDs:     func(ids []string) []string { return ids },
			wantFound:    1,
			wantNotFound: 0,
		},
		{
			name:         "missing_id_in_not_found",
			seedBatches:  0,
			queryIDs:     func(_ []string) []string { return []string{"my-project:ghost-batch"} },
			wantFound:    0,
			wantNotFound: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seededIDs := make([]string, 0, tt.seedBatches)

			for range tt.seedBatches {
				id := "my-project:batch-001"
				h.Backend.AddBuildBatchInternal(&codebuild.BuildBatch{
					ID:               id,
					Arn:              "arn:aws:codebuild:us-east-1:000000000000:build-batch/my-project:batch-001",
					ProjectName:      "my-project",
					BuildBatchStatus: "SUCCEEDED",
				})
				seededIDs = append(seededIDs, id)
			}

			queryIDs := tt.queryIDs(seededIDs)
			rec := doRequest(t, h, "BatchGetBuildBatches", map[string]any{"ids": queryIDs})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

			batches, _ := out["buildBatches"].([]any)
			assert.Len(t, batches, tt.wantFound)

			notFound, _ := out["buildBatchesNotFound"].([]any)
			assert.Len(t, notFound, tt.wantNotFound)
		})
	}
}

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

func TestHandler_CreateWebhook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"projectName":  "webhook-proj",
				"branchFilter": "main",
				"buildType":    "BUILD",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_project_name",
			body:       map[string]any{"branchFilter": "main"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "project_not_found",
			body: map[string]any{
				"projectName": "nonexistent-proj",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate_fails",
			body: map[string]any{
				"projectName": "dup-webhook-proj",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create the target project for success and duplicate cases.
			if tt.name == "success" || tt.name == "duplicate_fails" {
				proj := map[string]any{
					"name":      tt.body.(map[string]any)["projectName"],
					"source":    map[string]any{"type": "NO_SOURCE"},
					"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
					"environment": map[string]any{
						"type":        "LINUX_CONTAINER",
						"image":       "aws/codebuild/standard:5.0",
						"computeType": "BUILD_GENERAL1_SMALL",
					},
				}
				createRec := doRequest(t, h, "CreateProject", proj)
				require.Equal(t, http.StatusOK, createRec.Code)
			}

			if tt.name == "duplicate_fails" {
				// First webhook creation succeeds.
				rec := doRequest(t, h, "CreateWebhook", tt.body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "CreateWebhook", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateWebhook_ResponseFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create the project first.
	createRec := doRequest(t, h, "CreateProject", map[string]any{
		"name":      "wh-proj",
		"source":    map[string]any{"type": "NO_SOURCE"},
		"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
		"environment": map[string]any{
			"type":        "LINUX_CONTAINER",
			"image":       "aws/codebuild/standard:5.0",
			"computeType": "BUILD_GENERAL1_SMALL",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	rec := doRequest(t, h, "CreateWebhook", map[string]any{
		"projectName":  "wh-proj",
		"branchFilter": "main",
		"buildType":    "BUILD",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	webhook, ok := out["webhook"].(map[string]any)
	require.True(t, ok, "response should contain 'webhook' object")
	assert.Equal(t, "wh-proj", webhook["projectName"])
	assert.Equal(t, "main", webhook["branchFilter"])
	assert.Equal(t, "BUILD", webhook["buildType"])
	assert.NotEmpty(t, webhook["url"])
}

func TestHandler_NewOperations_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := codebuild.NewInMemoryBackend("000000000000", "us-east-1")

	// Seed a fleet.
	_, err := b.CreateFleet("persist-fleet", 2, "BUILD_GENERAL1_SMALL", "LINUX_CONTAINER", nil)
	require.NoError(t, err)

	// Seed a report group.
	_, err = b.CreateReportGroup("persist-rg", "TEST", codebuild.ReportExportConfig{}, nil)
	require.NoError(t, err)

	// Seed a report.
	b.AddReportInternal(&codebuild.Report{
		Arn:    "arn:aws:codebuild:us-east-1:000000000000:report/persist-rg:r1",
		Status: "SUCCEEDED",
	})

	// Seed a build batch.
	b.AddBuildBatchInternal(&codebuild.BuildBatch{
		ID:               "persist-proj:bb1",
		Arn:              "arn:aws:codebuild:us-east-1:000000000000:build-batch/persist-proj:bb1",
		ProjectName:      "persist-proj",
		BuildBatchStatus: "SUCCEEDED",
	})

	// Seed a command execution.
	b.AddCommandExecutionInternal(&codebuild.CommandExecution{
		ID:        "ce1",
		SandboxID: "sb1",
		Status:    "SUCCEEDED",
	})

	// Seed a sandbox.
	b.AddSandboxInternal(&codebuild.Sandbox{
		ID:     "sb1",
		Arn:    "arn:aws:codebuild:us-east-1:000000000000:sandbox/sb1",
		Status: "RUNNING",
	})

	// Snapshot and restore.
	snap := b.Snapshot()
	require.NotEmpty(t, snap)

	b2 := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	// Verify fleet is restored.
	fleets, notFound := b2.BatchGetFleets([]string{"persist-fleet"})
	assert.Empty(t, notFound)
	require.Len(t, fleets, 1)
	assert.Equal(t, "persist-fleet", fleets[0].Name)

	// Verify report group is restored.
	rgs, notFound := b2.BatchGetReportGroups(
		[]string{"arn:aws:codebuild:us-east-1:000000000000:report-group/persist-rg"},
	)
	assert.Empty(t, notFound)
	require.Len(t, rgs, 1)
	assert.Equal(t, "persist-rg", rgs[0].Name)

	// Verify report is restored.
	reports, notFound := b2.BatchGetReports([]string{"arn:aws:codebuild:us-east-1:000000000000:report/persist-rg:r1"})
	assert.Empty(t, notFound)
	require.Len(t, reports, 1)

	// Verify build batch is restored.
	batches, notFound := b2.BatchGetBuildBatches([]string{"persist-proj:bb1"})
	assert.Empty(t, notFound)
	require.Len(t, batches, 1)

	// Verify command execution is restored.
	ces, notFound := b2.BatchGetCommandExecutions("sb1", []string{"ce1"})
	assert.Empty(t, notFound)
	require.Len(t, ces, 1)

	// Verify sandbox is restored.
	sandboxes, notFound := b2.BatchGetSandboxes([]string{"sb1"})
	assert.Empty(t, notFound)
	require.Len(t, sandboxes, 1)
}

func TestProvider_NilCtx(t *testing.T) {
	t.Parallel()

	p := &codebuild.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, codebuild.ErrNilAppContext)
}

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	b := codebuild.NewInMemoryBackend("000000000000", "us-east-1")

	// Seed some data.
	src2 := codebuild.ProjectSource{Type: "NO_SOURCE"}
	arts2 := codebuild.ProjectArtifacts{Type: "NO_ARTIFACTS"}
	env2 := codebuild.ProjectEnvironment{
		Type:        "LINUX_CONTAINER",
		Image:       "aws/codebuild/standard:5.0",
		ComputeType: "BUILD_GENERAL1_SMALL",
	}
	_, err := b.CreateProject(codebuild.ProjectConfig{
		Name:        "reset-proj",
		Source:      &src2,
		Artifacts:   &arts2,
		Environment: &env2,
	})
	require.NoError(t, err)

	_, err = b.CreateFleet("reset-fleet", 1, "BUILD_GENERAL1_SMALL", "LINUX_CONTAINER", nil)
	require.NoError(t, err)

	_, err = b.CreateReportGroup("reset-rg", "TEST", codebuild.ReportExportConfig{}, nil)
	require.NoError(t, err)

	// Reset.
	b.Reset()

	// All should be gone.
	assert.Empty(t, b.ListProjects())
	fleets, _ := b.BatchGetFleets([]string{"reset-fleet"})
	assert.Empty(t, fleets)
	rgs, _ := b.BatchGetReportGroups([]string{"arn:aws:codebuild:us-east-1:000000000000:report-group/reset-rg"})
	assert.Empty(t, rgs)
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, "CreateProject", map[string]any{
		"name":      "hr-proj",
		"source":    map[string]any{"type": "NO_SOURCE"},
		"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
		"environment": map[string]any{
			"type":        "LINUX_CONTAINER",
			"image":       "aws/codebuild/standard:5.0",
			"computeType": "BUILD_GENERAL1_SMALL",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	// Verify project exists.
	listRec := doRequest(t, h, "ListProjects", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	var listOut map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.Len(t, listOut["projects"], 1)

	// Reset.
	h.Reset()

	// Verify project is gone.
	listRec2 := doRequest(t, h, "ListProjects", nil)
	require.Equal(t, http.StatusOK, listRec2.Code)
	var listOut2 map[string]any
	require.NoError(t, json.NewDecoder(listRec2.Body).Decode(&listOut2))
	assert.Empty(t, listOut2["projects"])
}

func TestHandler_SortedOutput(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	env := map[string]any{
		"type":        "LINUX_CONTAINER",
		"image":       "aws/codebuild/standard:5.0",
		"computeType": "BUILD_GENERAL1_SMALL",
	}

	// Create projects in reverse alphabetical order.
	for _, name := range []string{"zebra-proj", "alpha-proj", "middle-proj"} {
		rec := doRequest(t, h, "CreateProject", map[string]any{
			"name":        name,
			"source":      map[string]any{"type": "NO_SOURCE"},
			"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
			"environment": env,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListProjects", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	projects, _ := out["projects"].([]any)
	require.Len(t, projects, 3)
	assert.Equal(t, "alpha-proj", projects[0])
	assert.Equal(t, "middle-proj", projects[1])
	assert.Equal(t, "zebra-proj", projects[2])
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

func TestHandler_TagsOnFleet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create fleet with tags.
	rec := doRequest(t, h, "CreateFleet", map[string]any{
		"name":         "tagged-fleet",
		"baseCapacity": 1,
		"tags":         map[string]any{"env": "test", "team": "platform"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
	fleet, _ := createOut["fleet"].(map[string]any)
	fleetARN := fleet["arn"].(string)

	// ListTagsForResource should include fleet tags.
	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": fleetARN})
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagsOut map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&tagsOut))
	tags, _ := tagsOut["tags"].(map[string]any)
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "platform", tags["team"])

	// TagResource should add new tags.
	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"resourceArn": fleetARN,
		"tags":        map[string]any{"newkey": "newval"},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	// UntagResource should remove tags.
	untagRec := doRequest(t, h, "UntagResource", map[string]any{
		"resourceArn": fleetARN,
		"tagKeys":     []string{"env"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code)

	// Verify final tags.
	finalRec := doRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": fleetARN})
	require.Equal(t, http.StatusOK, finalRec.Code)
	var finalOut map[string]any
	require.NoError(t, json.NewDecoder(finalRec.Body).Decode(&finalOut))
	finalTags, _ := finalOut["tags"].(map[string]any)
	assert.NotContains(t, finalTags, "env")
	assert.Equal(t, "platform", finalTags["team"])
	assert.Equal(t, "newval", finalTags["newkey"])
}

func TestHandler_TagsOnReportGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create report group.
	rec := doRequest(t, h, "CreateReportGroup", map[string]any{
		"name": "tagged-rg",
		"type": "TEST",
		"tags": map[string]any{"owner": "teamA"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
	rg, _ := createOut["reportGroup"].(map[string]any)
	rgARN := rg["arn"].(string)

	// TagResource on report group.
	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"resourceArn": rgARN,
		"tags":        map[string]any{"extra": "val"},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	// ListTagsForResource should return all tags.
	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": rgARN})
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagsOut map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&tagsOut))
	tags, _ := tagsOut["tags"].(map[string]any)
	assert.Equal(t, "teamA", tags["owner"])
	assert.Equal(t, "val", tags["extra"])
}

func TestHandler_TagsDeepCopy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create fleet.
	createRec := doRequest(t, h, "CreateFleet", map[string]any{
		"name":         "deepcopy-fleet",
		"baseCapacity": 1,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
	fleet, _ := createOut["fleet"].(map[string]any)
	fleetARN := fleet["arn"].(string)

	// Tag with a map we'll mutate after the call.
	inputTags := map[string]string{"key": "original"}
	err := h.Backend.TagResource(fleetARN, inputTags)
	require.NoError(t, err)

	// Mutate the original tags map.
	inputTags["key"] = "mutated"

	// Fleet should still have "original", not "mutated".
	storedTags, err := h.Backend.ListTagsForResource(fleetARN)
	require.NoError(t, err)
	assert.Equal(t, "original", storedTags["key"])
}

func TestHandler_ErrValidation_InHandleError(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Try creating a fleet without a name — hits errInvalidRequest (wrapped ErrValidation is not used yet).
	rec := doRequest(t, h, "CreateFleet", map[string]any{"baseCapacity": 2})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
