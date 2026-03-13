package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// pipesView is the view model for a single EventBridge Pipe row.
type pipesView struct {
	Name         string
	ARN          string
	CurrentState string
	Source       string
	Target       string
}

// pipesIndexData is the template data for the Pipes dashboard page.
type pipesIndexData struct {
	PageData

	Pipes []pipesView
}

// pipesSnippet returns the shared SnippetData for the Pipes dashboard.
func pipesSnippet() *SnippetData {
	return &SnippetData{
		ID:    "pipes-operations",
		Title: "Using Pipes",
		Cli:   `aws pipes list-pipes --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for EventBridge Pipes
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
client := pipes.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for EventBridge Pipes
import boto3

client = boto3.client('pipes', endpoint_url='http://localhost:8000')`,
	}
}

// setupPipesRoutes registers all Pipes dashboard routes.
func (h *DashboardHandler) setupPipesRoutes() {
	h.SubRouter.GET("/dashboard/pipes", h.pipesIndex)
	h.SubRouter.POST("/dashboard/pipes/create", h.pipesCreate)
	h.SubRouter.POST("/dashboard/pipes/delete", h.pipesDelete)
}

// pipesIndex renders the main Pipes dashboard page.
func (h *DashboardHandler) pipesIndex(c *echo.Context) error {
	w := c.Response()

	if h.PipesOps == nil {
		h.renderTemplate(w, "pipes/index.html", pipesIndexData{
			PageData: PageData{
				Title:     "Pipes",
				ActiveTab: "pipes",
				Snippet:   pipesSnippet(),
			},
			Pipes: []pipesView{},
		})

		return nil
	}

	list := h.PipesOps.Backend.ListPipes()
	views := make([]pipesView, 0, len(list))

	for _, p := range list {
		views = append(views, pipesView{
			Name:         p.Name,
			ARN:          p.ARN,
			CurrentState: p.CurrentState,
			Source:       p.Source,
			Target:       p.Target,
		})
	}

	h.renderTemplate(w, "pipes/index.html", pipesIndexData{
		PageData: PageData{
			Title:     "Pipes",
			ActiveTab: "pipes",
			Snippet:   pipesSnippet(),
		},
		Pipes: views,
	})

	return nil
}

// pipesCreate handles POST /dashboard/pipes/create.
func (h *DashboardHandler) pipesCreate(c *echo.Context) error {
	if h.PipesOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	source := c.Request().FormValue("source")
	target := c.Request().FormValue("target")
	roleARN := c.Request().FormValue("role_arn")

	if name == "" || source == "" || target == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	ctx := c.Request().Context()

	if _, err := h.PipesOps.Backend.CreatePipe(name, roleARN, source, target, "", "RUNNING", nil); err != nil {
		h.Logger.ErrorContext(ctx, "pipes: failed to create pipe", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/pipes")
}

// pipesDelete handles POST /dashboard/pipes/delete.
func (h *DashboardHandler) pipesDelete(c *echo.Context) error {
	if h.PipesOps == nil {
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

	if err := h.PipesOps.Backend.DeletePipe(name); err != nil {
		h.Logger.ErrorContext(ctx, "pipes: failed to delete pipe", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/pipes")
}
