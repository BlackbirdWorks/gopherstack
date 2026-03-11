package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// codedeployApplicationView is the view model for a single CodeDeploy application.
type codedeployApplicationView struct {
	Name            string
	ApplicationID   string
	ComputePlatform string
}

// codedeployIndexData is the template data for the CodeDeploy dashboard index page.
type codedeployIndexData struct {
	PageData

	Applications []codedeployApplicationView
}

// codedeploySnippet returns the shared SnippetData for the CodeDeploy dashboard pages.
func codedeploySnippet() *SnippetData {
	return &SnippetData{
		ID:    "codedeploy-operations",
		Title: "Using CodeDeploy",
		Cli:   `aws codedeploy list-applications --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for CodeDeploy
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
client := codedeploy.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for CodeDeploy
import boto3

client = boto3.client('codedeploy', endpoint_url='http://localhost:8000')`,
	}
}

// codedeployIndex renders the CodeDeploy dashboard index.
func (h *DashboardHandler) codedeployIndex(c *echo.Context) error {
	w := c.Response()

	if h.CodeDeployOps == nil {
		h.renderTemplate(w, "codedeploy/index.html", codedeployIndexData{
			PageData: PageData{
				Title:     "CodeDeploy Applications",
				ActiveTab: "codedeploy",
				Snippet:   codedeploySnippet(),
			},
			Applications: []codedeployApplicationView{},
		})

		return nil
	}

	apps := h.CodeDeployOps.Backend.ListApplicationDetails()
	views := make([]codedeployApplicationView, 0, len(apps))

	for _, app := range apps {
		views = append(views, codedeployApplicationView{
			Name:            app.ApplicationName,
			ApplicationID:   app.ApplicationID,
			ComputePlatform: app.ComputePlatform,
		})
	}

	h.renderTemplate(w, "codedeploy/index.html", codedeployIndexData{
		PageData: PageData{
			Title:     "CodeDeploy Applications",
			ActiveTab: "codedeploy",
			Snippet:   codedeploySnippet(),
		},
		Applications: views,
	})

	return nil
}

// codedeployCreateApplication handles POST /dashboard/codedeploy/application/create.
func (h *DashboardHandler) codedeployCreateApplication(c *echo.Context) error {
	if h.CodeDeployOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	computePlatform := c.Request().FormValue("computePlatform")

	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if computePlatform == "" {
		computePlatform = "Server"
	}

	_, err := h.CodeDeployOps.Backend.CreateApplication(name, computePlatform, nil)
	if err != nil {
		h.Logger.Error("failed to create application", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/codedeploy")
}

// codedeployDeleteApplication handles POST /dashboard/codedeploy/application/delete.
func (h *DashboardHandler) codedeployDeleteApplication(c *echo.Context) error {
	if h.CodeDeployOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.CodeDeployOps.Backend.DeleteApplication(name); err != nil {
		h.Logger.Error("failed to delete application", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/codedeploy")
}

// setupCodeDeployRoutes registers routes for the CodeDeploy dashboard.
func (h *DashboardHandler) setupCodeDeployRoutes() {
	h.SubRouter.GET("/dashboard/codedeploy", h.codedeployIndex)
	h.SubRouter.POST("/dashboard/codedeploy/application/create", h.codedeployCreateApplication)
	h.SubRouter.POST("/dashboard/codedeploy/application/delete", h.codedeployDeleteApplication)
}
