package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	codepipelinebackend "github.com/blackbirdworks/gopherstack/services/codepipeline"
)

// codepipelineView is the view model for a single CodePipeline pipeline.
type codepipelineView struct {
	Name    string
	ARN     string
	Version int
}

// codepipelineIndexData is the template data for the CodePipeline dashboard index page.
type codepipelineIndexData struct {
	PageData

	Pipelines []codepipelineView
}

// codepipelineSnippet returns the shared SnippetData for the CodePipeline dashboard pages.
func codepipelineSnippet() *SnippetData {
	return &SnippetData{
		ID:    "codepipeline-operations",
		Title: "Using AWS CodePipeline",
		Cli:   `aws codepipeline list-pipelines --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for CodePipeline
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
client := codepipeline.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for CodePipeline
import boto3

client = boto3.client('codepipeline', endpoint_url='http://localhost:8000')`,
	}
}

// codepipelineIndex renders the CodePipeline dashboard index page.
func (h *DashboardHandler) codepipelineIndex(c *echo.Context) error {
	w := c.Response()

	if h.CodePipelineOps == nil {
		h.renderTemplate(w, "codepipeline/index.html", codepipelineIndexData{
			PageData: PageData{
				Title:     "CodePipeline",
				ActiveTab: "codepipeline",
				Snippet:   codepipelineSnippet(),
			},
			Pipelines: []codepipelineView{},
		})

		return nil
	}

	summaries := h.CodePipelineOps.Backend.ListPipelines()
	views := make([]codepipelineView, 0, len(summaries))

	for _, s := range summaries {
		p, err := h.CodePipelineOps.Backend.GetPipeline(s.Name)
		if err != nil {
			h.Logger.Error("failed to get codepipeline pipeline details", "name", s.Name, "error", err)

			continue
		}

		views = append(views, codepipelineView{
			Name:    p.Declaration.Name,
			ARN:     p.Metadata.PipelineArn,
			Version: p.Declaration.Version,
		})
	}

	h.renderTemplate(w, "codepipeline/index.html", codepipelineIndexData{
		PageData: PageData{
			Title:     "CodePipeline",
			ActiveTab: "codepipeline",
			Snippet:   codepipelineSnippet(),
		},
		Pipelines: views,
	})

	return nil
}

// codepipelineCreatePipeline handles POST /dashboard/codepipeline/pipeline/create.
func (h *DashboardHandler) codepipelineCreatePipeline(c *echo.Context) error {
	if h.CodePipelineOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	roleARN := c.Request().FormValue("roleArn")
	if roleARN == "" {
		roleARN = "arn:aws:iam::000000000000:role/pipeline-role"
	}

	decl := codepipelinebackend.PipelineDeclaration{
		Name:    name,
		RoleArn: roleARN,
		ArtifactStore: codepipelinebackend.ArtifactStore{
			Type:     "S3",
			Location: "artifact-bucket",
		},
		Stages: []codepipelinebackend.Stage{
			{
				Name: "Source",
				Actions: []codepipelinebackend.Action{
					{
						Name: "SourceAction",
						ActionTypeID: codepipelinebackend.ActionTypeID{
							Category: "Source",
							Owner:    "AWS",
							Provider: "CodeCommit",
							Version:  "1",
						},
					},
				},
			},
		},
	}

	_, err := h.CodePipelineOps.Backend.CreatePipeline(decl, nil)
	if err != nil {
		h.Logger.Error("failed to create codepipeline pipeline", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/codepipeline")
}

// codepipelineDeletePipeline handles POST /dashboard/codepipeline/pipeline/delete.
func (h *DashboardHandler) codepipelineDeletePipeline(c *echo.Context) error {
	if h.CodePipelineOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.CodePipelineOps.Backend.DeletePipeline(name); err != nil {
		h.Logger.Error("failed to delete codepipeline pipeline", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/codepipeline")
}

// setupCodePipelineRoutes registers routes for the CodePipeline dashboard.
func (h *DashboardHandler) setupCodePipelineRoutes() {
	h.SubRouter.GET("/dashboard/codepipeline", h.codepipelineIndex)
	h.SubRouter.POST("/dashboard/codepipeline/pipeline/create", h.codepipelineCreatePipeline)
	h.SubRouter.POST("/dashboard/codepipeline/pipeline/delete", h.codepipelineDeletePipeline)
}
