package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// emrServerlessApplicationView is the view model for a single EMR Serverless application.
type emrServerlessApplicationView struct {
	ApplicationID string
	Name          string
	Type          string
	ReleaseLabel  string
	State         string
	ARN           string
}

// emrServerlessIndexData is the template data for the EMR Serverless dashboard index page.
type emrServerlessIndexData struct {
	PageData

	Applications []emrServerlessApplicationView
}

// emrServerlessIndex renders the EMR Serverless dashboard index page.
func (h *DashboardHandler) emrServerlessIndex(c *echo.Context) error {
	w := c.Response()

	snippet := &SnippetData{
		ID:    "emrserverless-operations",
		Title: "Using EMR Serverless",
		Cli: `aws emr-serverless list-applications \
    --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for EMR Serverless
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
client := emrserverless.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for EMR Serverless
import boto3

client = boto3.client('emr-serverless', endpoint_url='http://localhost:8000')`,
	}

	if h.EmrServerlessOps == nil {
		h.renderTemplate(w, "emrserverless/index.html", emrServerlessIndexData{
			PageData: PageData{
				Title:     "EMR Serverless",
				ActiveTab: "emrserverless",
				Snippet:   snippet,
			},
			Applications: []emrServerlessApplicationView{},
		})

		return nil
	}

	apps := h.EmrServerlessOps.Backend.ListApplications()
	appViews := make([]emrServerlessApplicationView, 0, len(apps))

	for _, app := range apps {
		appViews = append(appViews, emrServerlessApplicationView{
			ApplicationID: app.ApplicationID,
			Name:          app.Name,
			Type:          app.Type,
			ReleaseLabel:  app.ReleaseLabel,
			State:         app.State,
			ARN:           app.Arn,
		})
	}

	h.renderTemplate(w, "emrserverless/index.html", emrServerlessIndexData{
		PageData: PageData{
			Title:     "EMR Serverless",
			ActiveTab: "emrserverless",
			Snippet:   snippet,
		},
		Applications: appViews,
	})

	return nil
}

// emrServerlessCreateApplication handles POST /dashboard/emrserverless/applications/create.
func (h *DashboardHandler) emrServerlessCreateApplication(c *echo.Context) error {
	if h.EmrServerlessOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	appType := c.Request().FormValue("type")
	releaseLabel := c.Request().FormValue("releaseLabel")

	if name == "" || appType == "" || releaseLabel == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.EmrServerlessOps.Backend.CreateApplication(name, appType, releaseLabel, nil)
	if err != nil {
		h.Logger.Error("failed to create emr serverless application", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/emrserverless")
}

// emrServerlessDeleteApplication handles POST /dashboard/emrserverless/applications/delete.
func (h *DashboardHandler) emrServerlessDeleteApplication(c *echo.Context) error {
	if h.EmrServerlessOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	id := c.Request().FormValue("id")
	if id == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.EmrServerlessOps.Backend.DeleteApplication(id); err != nil {
		h.Logger.Error("failed to delete emr serverless application", "id", id, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/emrserverless")
}

// setupEmrServerlessRoutes registers routes for the EMR Serverless dashboard.
func (h *DashboardHandler) setupEmrServerlessRoutes() {
	h.SubRouter.GET("/dashboard/emrserverless", h.emrServerlessIndex)
	h.SubRouter.POST("/dashboard/emrserverless/applications/create", h.emrServerlessCreateApplication)
	h.SubRouter.POST("/dashboard/emrserverless/applications/delete", h.emrServerlessDeleteApplication)
}
