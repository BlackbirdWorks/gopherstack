package codebuild_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestHandler_Project_SourceVersion verifies the top-level Project.sourceVersion
// field (distinct from secondarySourceVersions) round-trips through
// CreateProject/UpdateProject, matching real AWS's Project shape where
// sourceVersion pins the default version of the primary source when no
// build-level override is supplied.
func TestHandler_Project_SourceVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateProject", map[string]any{
		"name":          "src-version-proj",
		"source":        map[string]any{"type": "GITHUB", "location": "https://github.com/example/repo"},
		"artifacts":     map[string]any{"type": "NO_ARTIFACTS"},
		"sourceVersion": "refs/heads/main",
		"environment": map[string]any{
			"type":        "LINUX_CONTAINER",
			"image":       "aws/codebuild/standard:5.0",
			"computeType": "BUILD_GENERAL1_SMALL",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut struct {
		Project struct {
			SourceVersion string `json:"sourceVersion"`
		} `json:"project"`
	}
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
	assert.Equal(t, "refs/heads/main", createOut.Project.SourceVersion)

	updateRec := doRequest(t, h, "UpdateProject", map[string]any{
		"name":          "src-version-proj",
		"sourceVersion": "pr/42",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateOut struct {
		Project struct {
			SourceVersion string `json:"sourceVersion"`
		} `json:"project"`
	}
	require.NoError(t, json.NewDecoder(updateRec.Body).Decode(&updateOut))
	assert.Equal(t, "pr/42", updateOut.Project.SourceVersion, "UpdateProject must overwrite sourceVersion")
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
		// DeleteProject declares no ResourceNotFoundException in its real error
		// set (botocore codebuild/2016-10-06/service-2.json
		// operations.DeleteProject.errors: only InvalidInputException), so it
		// is idempotent.
		{
			name:       "not_found_is_idempotent",
			exists:     false,
			wantStatus: http.StatusOK,
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

// TestHandler_UpdateProjectVisibility_PublicAlias verifies UpdateProjectVisibility returns
// publicProjectAlias when visibility is PUBLIC_READ, matching real AWS behavior.
func TestHandler_UpdateProjectVisibility_PublicAlias(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, "CreateProject", map[string]any{
		"name":        "vis-alias-proj",
		"serviceRole": "arn:aws:iam::000000000000:role/cb",
		"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
		"environment": map[string]any{
			"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL",
		},
		"source": map[string]any{"type": "NO_SOURCE"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut struct {
		Project map[string]any `json:"project"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	projectArn, _ := createOut.Project["arn"].(string)
	require.NotEmpty(t, projectArn)

	rec := doRequest(t, h, "UpdateProjectVisibility", map[string]any{
		"projectArn":        projectArn,
		"projectVisibility": "PUBLIC_READ",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ProjectArn         string `json:"projectArn"`
		ProjectVisibility  string `json:"projectVisibility"`
		PublicProjectAlias string `json:"publicProjectAlias"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "PUBLIC_READ", out.ProjectVisibility)
	assert.NotEmpty(t, out.PublicProjectAlias,
		"UpdateProjectVisibility with PUBLIC_READ must return publicProjectAlias")
}

// TestCodeBuild_Project covers UpdateProjectVisibility, InvalidateProjectCache.
func TestCodeBuild_Project(t *testing.T) {
	t.Parallel()

	t.Run("update_project_visibility", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "vis-proj")
		arn := projectARN(t, h, "vis-proj")

		rec := doRequest(t, h, "UpdateProjectVisibility", map[string]any{
			"projectArn":        arn,
			"projectVisibility": "PUBLIC_READ",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			ProjectArn        string `json:"projectArn"`
			ProjectVisibility string `json:"projectVisibility"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		assert.Equal(t, arn, out.ProjectArn)
		assert.Equal(t, "PUBLIC_READ", out.ProjectVisibility)
	})

	t.Run("update_project_visibility_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "UpdateProjectVisibility", map[string]any{
			"projectArn":        "arn:aws:codebuild:us-east-1:000000000000:project/ghost",
			"projectVisibility": "PRIVATE",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("update_project_visibility_missing_arn", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "UpdateProjectVisibility", map[string]any{
			"projectVisibility": "PRIVATE",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("invalidate_project_cache", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "cache-proj")

		rec := doRequest(t, h, "InvalidateProjectCache", map[string]any{"projectName": "cache-proj"})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("invalidate_project_cache_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "InvalidateProjectCache", map[string]any{"projectName": "ghost-proj"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestCodeBuild_ListSharedProjects verifies ListSharedProjects returns an empty list.
func TestCodeBuild_ListSharedProjects(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListSharedProjects", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Projects []string `json:"projects"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Empty(t, out.Projects)
}
