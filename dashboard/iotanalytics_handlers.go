package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// iotanalyticsChannelView is the view model for a single IoT Analytics channel row.
type iotanalyticsChannelView struct {
	Name         string
	ARN          string
	Status       string
	CreationTime string
}

// iotanalyticsIndexData is the template data for the IoT Analytics dashboard page.
type iotanalyticsIndexData struct {
	PageData

	Channels []iotanalyticsChannelView
}

// iotanalyticsSnippet returns the shared SnippetData for the IoT Analytics dashboard.
func iotanalyticsSnippet() *SnippetData {
	return &SnippetData{
		ID:    "iotanalytics-operations",
		Title: "Using IoT Analytics",
		Cli:   `aws iotanalytics list-channels --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for IoT Analytics
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
client := iotanalytics.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for IoT Analytics
import boto3

client = boto3.client('iotanalytics', endpoint_url='http://localhost:8000')`,
	}
}

// setupIoTAnalyticsRoutes registers all IoT Analytics dashboard routes.
func (h *DashboardHandler) setupIoTAnalyticsRoutes() {
	h.SubRouter.GET("/dashboard/iotanalytics", h.iotanalyticsIndex)
	h.SubRouter.POST("/dashboard/iotanalytics/channel/create", h.iotanalyticsCreateChannel)
	h.SubRouter.POST("/dashboard/iotanalytics/channel/delete", h.iotanalyticsDeleteChannel)
}

// iotanalyticsIndex renders the main IoT Analytics dashboard page.
func (h *DashboardHandler) iotanalyticsIndex(c *echo.Context) error {
	w := c.Response()

	if h.IoTAnalyticsOps == nil {
		h.renderTemplate(w, "iotanalytics/index.html", iotanalyticsIndexData{
			PageData: PageData{Title: "IoT Analytics", ActiveTab: "iotanalytics", Snippet: iotanalyticsSnippet()},
			Channels: []iotanalyticsChannelView{},
		})

		return nil
	}

	rawChannels := h.IoTAnalyticsOps.Backend.ListChannels()
	views := make([]iotanalyticsChannelView, 0, len(rawChannels))

	for _, ch := range rawChannels {
		views = append(views, iotanalyticsChannelView{
			Name:         ch.Name,
			ARN:          ch.ARN,
			Status:       ch.Status,
			CreationTime: ch.CreationTime,
		})
	}

	h.renderTemplate(w, "iotanalytics/index.html", iotanalyticsIndexData{
		PageData: PageData{Title: "IoT Analytics Channels", ActiveTab: "iotanalytics", Snippet: iotanalyticsSnippet()},
		Channels: views,
	})

	return nil
}

// iotanalyticsCreateChannel handles POST /dashboard/iotanalytics/channel/create.
func (h *DashboardHandler) iotanalyticsCreateChannel(c *echo.Context) error {
	if h.IoTAnalyticsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.IoTAnalyticsOps.Backend.CreateChannel(name, nil)
	if err != nil {
		h.Logger.Error("failed to create iotanalytics channel", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/iotanalytics")
}

// iotanalyticsDeleteChannel handles POST /dashboard/iotanalytics/channel/delete.
func (h *DashboardHandler) iotanalyticsDeleteChannel(c *echo.Context) error {
	if h.IoTAnalyticsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.IoTAnalyticsOps.Backend.DeleteChannel(name); err != nil {
		h.Logger.Error("failed to delete iotanalytics channel", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/iotanalytics")
}
