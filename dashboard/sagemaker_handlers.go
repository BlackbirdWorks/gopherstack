package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	sagemakerbackend "github.com/blackbirdworks/gopherstack/services/sagemaker"
)

// sagemakerModelView is the view model for a single SageMaker model row.
type sagemakerModelView struct {
	ModelName string
	ModelARN  string
	Image     string
}

// sagemakerIndexData is the template data for the SageMaker dashboard page.
type sagemakerIndexData struct {
	PageData

	Models []sagemakerModelView
}

// sagemakerSnippet returns the shared SnippetData for the SageMaker dashboard.
func sagemakerSnippet() *SnippetData {
	return &SnippetData{
		ID:    "sagemaker-operations",
		Title: "Using SageMaker",
		Cli:   `aws sagemaker list-models --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Amazon SageMaker
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
client := sagemaker.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Amazon SageMaker
import boto3

client = boto3.client('sagemaker', endpoint_url='http://localhost:8000')`,
	}
}

// setupSageMakerRoutes registers all SageMaker dashboard routes.
func (h *DashboardHandler) setupSageMakerRoutes() {
	h.SubRouter.GET("/dashboard/sagemaker", h.sagemakerIndex)
	h.SubRouter.POST("/dashboard/sagemaker/create", h.sagemakerCreate)
	h.SubRouter.POST("/dashboard/sagemaker/delete", h.sagemakerDelete)
}

// sagemakerIndex renders the main SageMaker dashboard page.
func (h *DashboardHandler) sagemakerIndex(c *echo.Context) error {
	w := c.Response()

	if h.SageMakerOps == nil {
		h.renderTemplate(w, "sagemaker/index.html", sagemakerIndexData{
			PageData: PageData{
				Title:     "SageMaker",
				ActiveTab: "sagemaker",
				Snippet:   sagemakerSnippet(),
			},
			Models: []sagemakerModelView{},
		})

		return nil
	}

	list, _ := h.SageMakerOps.Backend.ListModels("")
	views := make([]sagemakerModelView, 0, len(list))

	for _, m := range list {
		image := ""

		if m.PrimaryContainer != nil {
			image = m.PrimaryContainer.Image
		}

		views = append(views, sagemakerModelView{
			ModelName: m.ModelName,
			ModelARN:  m.ModelARN,
			Image:     image,
		})
	}

	h.renderTemplate(w, "sagemaker/index.html", sagemakerIndexData{
		PageData: PageData{
			Title:     "SageMaker",
			ActiveTab: "sagemaker",
			Snippet:   sagemakerSnippet(),
		},
		Models: views,
	})

	return nil
}

// sagemakerCreate handles POST /dashboard/sagemaker/create.
func (h *DashboardHandler) sagemakerCreate(c *echo.Context) error {
	if h.SageMakerOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	image := c.Request().FormValue("image")
	ctx := c.Request().Context()

	var container *sagemakerbackend.ContainerDefinition

	if image != "" {
		container = &sagemakerbackend.ContainerDefinition{Image: image}
	}

	if _, err := h.SageMakerOps.Backend.CreateModel(name, "", container, nil, nil); err != nil {
		h.Logger.ErrorContext(ctx, "sagemaker: failed to create model", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/sagemaker")
}

// sagemakerDelete handles POST /dashboard/sagemaker/delete.
func (h *DashboardHandler) sagemakerDelete(c *echo.Context) error {
	if h.SageMakerOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	ctx := c.Request().Context()

	if err := h.SageMakerOps.Backend.DeleteModel(name); err != nil {
		h.Logger.ErrorContext(ctx, "sagemaker: failed to delete model", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/sagemaker")
}
