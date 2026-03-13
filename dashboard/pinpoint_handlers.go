package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// pinpointAppView is the view model for a single Pinpoint app row.
type pinpointAppView struct {
	ARN  string
	ID   string
	Name string
}

// pinpointIndexData is the template data for the Pinpoint dashboard page.
type pinpointIndexData struct {
	PageData

	Apps []pinpointAppView
}

// pinpointSnippet returns the shared SnippetData for the Pinpoint dashboard.
func pinpointSnippet() *SnippetData {
	return &SnippetData{
		ID:    "pinpoint-operations",
		Title: "Using Pinpoint",
		Cli:   `aws pinpoint get-apps --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Pinpoint
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
client := pinpoint.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Pinpoint
import boto3

client = boto3.client('pinpoint', endpoint_url='http://localhost:8000')`,
	}
}

// setupPinpointRoutes registers all Pinpoint dashboard routes.
func (h *DashboardHandler) setupPinpointRoutes() {
	h.SubRouter.GET("/dashboard/pinpoint", h.pinpointIndex)
	h.SubRouter.POST("/dashboard/pinpoint/app/create", h.pinpointCreateApp)
	h.SubRouter.POST("/dashboard/pinpoint/app/delete", h.pinpointDeleteApp)
}

// pinpointIndex renders the main Pinpoint dashboard page.
func (h *DashboardHandler) pinpointIndex(c *echo.Context) error {
	w := c.Response()

	if h.PinpointOps == nil {
		h.renderTemplate(w, "pinpoint/index.html", pinpointIndexData{
			PageData: PageData{
				Title:     "Pinpoint",
				ActiveTab: "pinpoint",
				Snippet:   pinpointSnippet(),
			},
			Apps: []pinpointAppView{},
		})

		return nil
	}

	ctx := c.Request().Context()

	apps, err := h.PinpointOps.Backend.GetApps()
	if err != nil {
		h.Logger.ErrorContext(ctx, "pinpoint: failed to list apps", "error", err)

		apps = nil
	}

	views := make([]pinpointAppView, 0, len(apps))

	for _, app := range apps {
		views = append(views, pinpointAppView{
			ID:   app.ID,
			Name: app.Name,
			ARN:  app.ARN,
		})
	}

	h.renderTemplate(w, "pinpoint/index.html", pinpointIndexData{
		PageData: PageData{
			Title:     "Pinpoint Apps",
			ActiveTab: "pinpoint",
			Snippet:   pinpointSnippet(),
		},
		Apps: views,
	})

	return nil
}

// pinpointCreateApp handles POST /dashboard/pinpoint/app/create.
func (h *DashboardHandler) pinpointCreateApp(c *echo.Context) error {
	if h.PinpointOps == nil {
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

	if _, err := h.PinpointOps.Backend.CreateApp(
		h.GlobalConfig.Region,
		h.GlobalConfig.AccountID,
		name,
		nil,
	); err != nil {
		h.Logger.ErrorContext(ctx, "pinpoint: failed to create app", "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/pinpoint")
}

// pinpointDeleteApp handles POST /dashboard/pinpoint/app/delete.
func (h *DashboardHandler) pinpointDeleteApp(c *echo.Context) error {
	if h.PinpointOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	appID := c.Request().FormValue("id")
	if appID == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	ctx := c.Request().Context()

	if _, err := h.PinpointOps.Backend.DeleteApp(appID); err != nil {
		h.Logger.ErrorContext(ctx, "pinpoint: failed to delete app", "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/pinpoint")
}
