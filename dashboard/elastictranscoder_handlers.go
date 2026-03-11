package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// elasticTranscoderPipelineView is the view model for a single pipeline.
type elasticTranscoderPipelineView struct {
	ID     string
	Name   string
	Status string
}

// elasticTranscoderIndexData is the template data for the Elastic Transcoder index page.
type elasticTranscoderIndexData struct {
	PageData

	Pipelines []elasticTranscoderPipelineView
}

// elasticTranscoderSnippet returns the shared SnippetData for the Elastic Transcoder dashboard pages.
func elasticTranscoderSnippet() *SnippetData {
	return &SnippetData{
		ID:    "elastictranscoder-operations",
		Title: "Using Elastic Transcoder",
		Cli:   `aws elastictranscoder list-pipelines --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Elastic Transcoder
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
client := elastictranscoder.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Elastic Transcoder
import boto3

client = boto3.client('elastictranscoder', endpoint_url='http://localhost:8000')`,
	}
}

// elasticTranscoderIndex renders the Elastic Transcoder dashboard index.
func (h *DashboardHandler) elasticTranscoderIndex(c *echo.Context) error {
	w := c.Response()

	if h.ElasticTranscoderOps == nil {
		h.renderTemplate(w, "elastictranscoder/index.html", elasticTranscoderIndexData{
			PageData: PageData{
				Title:     "Elastic Transcoder Pipelines",
				ActiveTab: "elastictranscoder",
				Snippet:   elasticTranscoderSnippet(),
			},
			Pipelines: []elasticTranscoderPipelineView{},
		})

		return nil
	}

	pipelines := h.ElasticTranscoderOps.Backend.ListPipelines()
	views := make([]elasticTranscoderPipelineView, 0, len(pipelines))

	for _, p := range pipelines {
		views = append(views, elasticTranscoderPipelineView{
			ID:     p.ID,
			Name:   p.Name,
			Status: p.Status,
		})
	}

	h.renderTemplate(w, "elastictranscoder/index.html", elasticTranscoderIndexData{
		PageData: PageData{
			Title:     "Elastic Transcoder Pipelines",
			ActiveTab: "elastictranscoder",
			Snippet:   elasticTranscoderSnippet(),
		},
		Pipelines: views,
	})

	return nil
}

// elasticTranscoderCreatePipeline handles POST /dashboard/elastictranscoder/pipelines/create.
func (h *DashboardHandler) elasticTranscoderCreatePipeline(c *echo.Context) error {
	if h.ElasticTranscoderOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	inputBucket := c.Request().FormValue("inputBucket")
	role := c.Request().FormValue("role")

	if name == "" || inputBucket == "" || role == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.ElasticTranscoderOps.Backend.CreatePipeline(name, inputBucket, "", role)
	if err != nil {
		h.Logger.Error("failed to create elastic transcoder pipeline", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/elastictranscoder")
}

// elasticTranscoderDeletePipeline handles POST /dashboard/elastictranscoder/pipelines/delete.
func (h *DashboardHandler) elasticTranscoderDeletePipeline(c *echo.Context) error {
	if h.ElasticTranscoderOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	id := c.Request().FormValue("id")
	if id == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.ElasticTranscoderOps.Backend.DeletePipeline(id); err != nil {
		h.Logger.Error("failed to delete elastic transcoder pipeline", "id", id, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/elastictranscoder")
}

// setupElasticTranscoderRoutes registers routes for the Elastic Transcoder dashboard.
func (h *DashboardHandler) setupElasticTranscoderRoutes() {
	h.SubRouter.GET("/dashboard/elastictranscoder", h.elasticTranscoderIndex)
	h.SubRouter.POST("/dashboard/elastictranscoder/pipelines/create", h.elasticTranscoderCreatePipeline)
	h.SubRouter.POST("/dashboard/elastictranscoder/pipelines/delete", h.elasticTranscoderDeletePipeline)
}
