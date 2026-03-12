package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// kinesisanalyticsAppView is the view model for a single Kinesis Analytics application row.
type kinesisanalyticsAppView struct {
	ApplicationName   string
	ApplicationARN    string
	ApplicationStatus string
}

// kinesisanalyticsIndexData is the template data for the Kinesis Analytics dashboard page.
type kinesisanalyticsIndexData struct {
	PageData

	Applications []kinesisanalyticsAppView
}

// kinesisanalyticsSnippet returns the shared SnippetData for the Kinesis Analytics dashboard.
func kinesisanalyticsSnippet() *SnippetData {
	return &SnippetData{
		ID:    "kinesisanalytics-operations",
		Title: "Using Kinesis Analytics",
		Cli:   `aws kinesisanalytics list-applications --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Kinesis Analytics
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
client := kinesisanalytics.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Kinesis Analytics
import boto3

client = boto3.client('kinesisanalytics', endpoint_url='http://localhost:8000')`,
	}
}

// setupKinesisAnalyticsRoutes registers all Kinesis Analytics dashboard routes.
func (h *DashboardHandler) setupKinesisAnalyticsRoutes() {
	h.SubRouter.GET("/dashboard/kinesisanalytics", h.kinesisanalyticsIndex)
	h.SubRouter.POST("/dashboard/kinesisanalytics/application/create", h.kinesisanalyticsCreateApplication)
	h.SubRouter.POST("/dashboard/kinesisanalytics/application/delete", h.kinesisanalyticsDeleteApplication)
}

// kinesisanalyticsIndex renders the main Kinesis Analytics dashboard page.
func (h *DashboardHandler) kinesisanalyticsIndex(c *echo.Context) error {
	w := c.Response()

	if h.KinesisAnalyticsOps == nil {
		h.renderTemplate(w, "kinesisanalytics/index.html", kinesisanalyticsIndexData{
			PageData: PageData{
				Title:     "Kinesis Analytics",
				ActiveTab: "kinesisanalytics",
				Snippet:   kinesisanalyticsSnippet(),
			},
			Applications: []kinesisanalyticsAppView{},
		})

		return nil
	}

	rawApps, _ := h.KinesisAnalyticsOps.Backend.ListApplications("", 0)
	views := make([]kinesisanalyticsAppView, 0, len(rawApps))

	for _, app := range rawApps {
		views = append(views, kinesisanalyticsAppView{
			ApplicationName:   app.ApplicationName,
			ApplicationARN:    app.ApplicationARN,
			ApplicationStatus: app.ApplicationStatus,
		})
	}

	h.renderTemplate(w, "kinesisanalytics/index.html", kinesisanalyticsIndexData{
		PageData: PageData{
			Title:     "Kinesis Analytics Applications",
			ActiveTab: "kinesisanalytics",
			Snippet:   kinesisanalyticsSnippet(),
		},
		Applications: views,
	})

	return nil
}

// kinesisanalyticsCreateApplication handles POST /dashboard/kinesisanalytics/application/create.
func (h *DashboardHandler) kinesisanalyticsCreateApplication(c *echo.Context) error {
	if h.KinesisAnalyticsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, _ = h.KinesisAnalyticsOps.Backend.CreateApplication(
		h.GlobalConfig.Region,
		h.GlobalConfig.AccountID,
		name,
		c.Request().FormValue("description"),
		"",
		nil,
	)

	return c.Redirect(http.StatusSeeOther, "/dashboard/kinesisanalytics")
}

// kinesisanalyticsDeleteApplication handles POST /dashboard/kinesisanalytics/application/delete.
func (h *DashboardHandler) kinesisanalyticsDeleteApplication(c *echo.Context) error {
	if h.KinesisAnalyticsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_ = h.KinesisAnalyticsOps.Backend.DeleteApplication(name, nil)

	return c.Redirect(http.StatusSeeOther, "/dashboard/kinesisanalytics")
}
