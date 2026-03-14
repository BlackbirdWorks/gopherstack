package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// kinesisanalyticsv2AppView is the view model for a single Kinesis Data Analytics v2 application.
type kinesisanalyticsv2AppView struct {
	ApplicationARN     string
	ApplicationName    string
	ApplicationStatus  string
	RuntimeEnvironment string
	ApplicationMode    string
}

// kinesisanalyticsv2IndexData is the template data for the Kinesis Data Analytics v2 dashboard page.
type kinesisanalyticsv2IndexData struct {
	PageData

	Applications []kinesisanalyticsv2AppView
}

// setupKinesisAnalyticsV2Routes registers all Kinesis Data Analytics v2 dashboard routes.
func (h *DashboardHandler) setupKinesisAnalyticsV2Routes() {
	h.SubRouter.GET("/dashboard/kinesisanalyticsv2", h.kinesisanalyticsv2Index)
	h.SubRouter.POST("/dashboard/kinesisanalyticsv2/applications/create", h.kinesisanalyticsv2CreateApplication)
	h.SubRouter.POST("/dashboard/kinesisanalyticsv2/applications/delete", h.kinesisanalyticsv2DeleteApplication)
}

// kinesisanalyticsv2Index renders the Kinesis Data Analytics v2 dashboard index page.
func (h *DashboardHandler) kinesisanalyticsv2Index(c *echo.Context) error {
	w := c.Response()

	snippet := &SnippetData{
		ID:    "kinesisanalyticsv2-operations",
		Title: "Using Amazon Kinesis Data Analytics v2",
		Cli: `aws kinesisanalyticsv2 list-applications \
    --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Kinesis Data Analytics v2
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
client := kinesisanalyticsv2.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Kinesis Data Analytics v2
import boto3

client = boto3.client('kinesisanalyticsv2', endpoint_url='http://localhost:8000')`,
	}

	if h.KinesisAnalyticsV2Ops == nil {
		h.renderTemplate(w, "kinesisanalyticsv2/index.html", kinesisanalyticsv2IndexData{
			PageData: PageData{
				Title:     "Kinesis Data Analytics v2",
				ActiveTab: "kinesisanalyticsv2",
				Snippet:   snippet,
			},
			Applications: []kinesisanalyticsv2AppView{},
		})

		return nil
	}

	apps, _ := h.KinesisAnalyticsV2Ops.Backend.ListApplications("")
	appViews := make([]kinesisanalyticsv2AppView, 0, len(apps))

	for _, app := range apps {
		appViews = append(appViews, kinesisanalyticsv2AppView{
			ApplicationARN:     app.ApplicationARN,
			ApplicationName:    app.ApplicationName,
			ApplicationStatus:  app.ApplicationStatus,
			RuntimeEnvironment: app.RuntimeEnvironment,
			ApplicationMode:    app.ApplicationMode,
		})
	}

	h.renderTemplate(w, "kinesisanalyticsv2/index.html", kinesisanalyticsv2IndexData{
		PageData: PageData{
			Title:     "Kinesis Data Analytics v2",
			ActiveTab: "kinesisanalyticsv2",
			Snippet:   snippet,
		},
		Applications: appViews,
	})

	return nil
}

// kinesisanalyticsv2CreateApplication handles POST /dashboard/kinesisanalyticsv2/applications/create.
func (h *DashboardHandler) kinesisanalyticsv2CreateApplication(c *echo.Context) error {
	if h.KinesisAnalyticsV2Ops == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	runtime := c.Request().FormValue("runtime_environment")
	serviceRole := c.Request().FormValue("service_execution_role")

	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if runtime == "" {
		runtime = "FLINK-1_18"
	}

	_, err := h.KinesisAnalyticsV2Ops.Backend.CreateApplication(name, runtime, serviceRole, "", "", nil)
	if err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/kinesisanalyticsv2")
}

// kinesisanalyticsv2DeleteApplication handles POST /dashboard/kinesisanalyticsv2/applications/delete.
func (h *DashboardHandler) kinesisanalyticsv2DeleteApplication(c *echo.Context) error {
	if h.KinesisAnalyticsV2Ops == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	appName := c.Request().FormValue("application_name")
	if appName == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.KinesisAnalyticsV2Ops.Backend.DeleteApplication(appName); err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/kinesisanalyticsv2")
}
