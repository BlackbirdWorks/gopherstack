package codebuild_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
				assert.Equal(t, "SUCCEEDED", out.Build.BuildStatus)
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
