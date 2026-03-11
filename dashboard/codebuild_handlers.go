package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	codebuildbackend "github.com/blackbirdworks/gopherstack/services/codebuild"
)

// codebuildProjectView is the view model for a single CodeBuild project.
type codebuildProjectView struct {
	Name        string
	ARN         string
	Description string
	ServiceRole string
}

// codebuildIndexData is the template data for the CodeBuild dashboard index page.
type codebuildIndexData struct {
	PageData

	Projects []codebuildProjectView
}

// codebuildIndex renders the CodeBuild dashboard index page.
func (h *DashboardHandler) codebuildIndex(c *echo.Context) error {
	w := c.Response()

	snippet := &SnippetData{
		ID:    "codebuild-operations",
		Title: "Using AWS CodeBuild",
		Cli: `aws codebuild batch-get-projects \
    --names my-project \
    --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for CodeBuild
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithEndpointResolverWithOptions(
        aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:8000"}, nil
        }),
    ),
)
if err != nil {
    log.Fatal(err)
}
client := codebuild.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for CodeBuild
import boto3

client = boto3.client('codebuild', endpoint_url='http://localhost:8000')`,
	}

	if h.CodeBuildOps == nil {
		h.renderTemplate(w, "codebuild/index.html", codebuildIndexData{
			PageData: PageData{
				Title:     "CodeBuild",
				ActiveTab: "codebuild",
				Snippet:   snippet,
			},
			Projects: []codebuildProjectView{},
		})

		return nil
	}

	names := h.CodeBuildOps.Backend.ListProjects()
	projectViews := make([]codebuildProjectView, 0, len(names))

	if len(names) > 0 {
		projects, _ := h.CodeBuildOps.Backend.BatchGetProjects(names)
		for _, p := range projects {
			projectViews = append(projectViews, codebuildProjectView{
				Name:        p.Name,
				ARN:         p.Arn,
				Description: p.Description,
				ServiceRole: p.ServiceRole,
			})
		}
	}

	h.renderTemplate(w, "codebuild/index.html", codebuildIndexData{
		PageData: PageData{
			Title:     "CodeBuild",
			ActiveTab: "codebuild",
			Snippet:   snippet,
		},
		Projects: projectViews,
	})

	return nil
}

// codebuildCreateProject handles POST /dashboard/codebuild/projects/create.
func (h *DashboardHandler) codebuildCreateProject(c *echo.Context) error {
	if h.CodeBuildOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	description := c.Request().FormValue("description")

	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.CodeBuildOps.Backend.CreateProject(
		name,
		description,
		codebuildbackend.ProjectSource{Type: "NO_SOURCE"},
		codebuildbackend.ProjectArtifacts{Type: "NO_ARTIFACTS"},
		codebuildbackend.ProjectEnvironment{
			Type:        "LINUX_CONTAINER",
			Image:       "aws/codebuild/standard:1.0",
			ComputeType: "BUILD_GENERAL1_SMALL",
		},
		"",
		nil,
	)
	if err != nil {
		h.Logger.Error("failed to create codebuild project", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/codebuild")
}

// codebuildDeleteProject handles POST /dashboard/codebuild/projects/delete.
func (h *DashboardHandler) codebuildDeleteProject(c *echo.Context) error {
	if h.CodeBuildOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.CodeBuildOps.Backend.DeleteProject(name); err != nil {
		h.Logger.Error("failed to delete codebuild project", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/codebuild")
}

// setupCodeBuildRoutes registers routes for the CodeBuild dashboard.
func (h *DashboardHandler) setupCodeBuildRoutes() {
	h.SubRouter.GET("/dashboard/codebuild", h.codebuildIndex)
	h.SubRouter.POST("/dashboard/codebuild/projects/create", h.codebuildCreateProject)
	h.SubRouter.POST("/dashboard/codebuild/projects/delete", h.codebuildDeleteProject)
}
