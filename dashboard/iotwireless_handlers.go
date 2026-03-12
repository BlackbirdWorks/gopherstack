package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// iotwirelessServiceProfileView is the view model for a single IoT Wireless service profile row.
type iotwirelessServiceProfileView struct {
	Name string
	ID   string
	ARN  string
}

// iotwirelessIndexData is the template data for the IoT Wireless dashboard page.
type iotwirelessIndexData struct {
	PageData

	ServiceProfiles []iotwirelessServiceProfileView
}

// iotwirelessSnippet returns the shared SnippetData for the IoT Wireless dashboard.
func iotwirelessSnippet() *SnippetData {
	return &SnippetData{
		ID:    "iotwireless-operations",
		Title: "Using IoT Wireless",
		Cli:   `aws iotwireless list-service-profiles --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for IoT Wireless
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
client := iotwireless.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for IoT Wireless
import boto3

client = boto3.client('iotwireless', endpoint_url='http://localhost:8000')`,
	}
}

// setupIoTWirelessRoutes registers all IoT Wireless dashboard routes.
func (h *DashboardHandler) setupIoTWirelessRoutes() {
	h.SubRouter.GET("/dashboard/iotwireless", h.iotwirelessIndex)
	h.SubRouter.POST("/dashboard/iotwireless/service-profile/create", h.iotwirelessCreateServiceProfile)
	h.SubRouter.POST("/dashboard/iotwireless/service-profile/delete", h.iotwirelessDeleteServiceProfile)
}

// iotwirelessIndex renders the main IoT Wireless dashboard page.
func (h *DashboardHandler) iotwirelessIndex(c *echo.Context) error {
	w := c.Response()

	if h.IoTWirelessOps == nil {
		h.renderTemplate(w, "iotwireless/index.html", iotwirelessIndexData{
			PageData:        PageData{Title: "IoT Wireless", ActiveTab: "iotwireless", Snippet: iotwirelessSnippet()},
			ServiceProfiles: []iotwirelessServiceProfileView{},
		})

		return nil
	}

	rawProfiles := h.IoTWirelessOps.Backend.ListServiceProfiles(h.GlobalConfig.AccountID, h.GlobalConfig.Region)
	views := make([]iotwirelessServiceProfileView, 0, len(rawProfiles))

	for _, sp := range rawProfiles {
		views = append(views, iotwirelessServiceProfileView{
			Name: sp.Name,
			ID:   sp.ID,
			ARN:  sp.ARN,
		})
	}

	h.renderTemplate(w, "iotwireless/index.html", iotwirelessIndexData{
		PageData: PageData{
			Title:     "IoT Wireless Service Profiles",
			ActiveTab: "iotwireless",
			Snippet:   iotwirelessSnippet(),
		},
		ServiceProfiles: views,
	})

	return nil
}

// iotwirelessCreateServiceProfile handles POST /dashboard/iotwireless/service-profile/create.
func (h *DashboardHandler) iotwirelessCreateServiceProfile(c *echo.Context) error {
	if h.IoTWirelessOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.IoTWirelessOps.Backend.CreateServiceProfile(h.GlobalConfig.AccountID, h.GlobalConfig.Region, name, nil)
	if err != nil {
		h.Logger.Error("failed to create iot wireless service profile", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/iotwireless")
}

// iotwirelessDeleteServiceProfile handles POST /dashboard/iotwireless/service-profile/delete.
func (h *DashboardHandler) iotwirelessDeleteServiceProfile(c *echo.Context) error {
	if h.IoTWirelessOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	id := c.Request().FormValue("id")
	if id == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.IoTWirelessOps.Backend.DeleteServiceProfile(
		h.GlobalConfig.AccountID,
		h.GlobalConfig.Region,
		id,
	); err != nil {
		h.Logger.Error("failed to delete iot wireless service profile", "id", id, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/iotwireless")
}
