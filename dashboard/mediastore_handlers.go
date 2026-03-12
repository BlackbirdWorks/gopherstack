package dashboard

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v5"
)

// mediastoreContainerView is the view model for a single container row.
type mediastoreContainerView struct {
	Name                 string
	ARN                  string
	Status               string
	Endpoint             string
	AccessLoggingEnabled bool
}

// mediastoreIndexData is the template data for the MediaStore dashboard page.
type mediastoreIndexData struct {
	PageData

	Containers []mediastoreContainerView
}

// mediastoreSnippet returns the shared SnippetData for the MediaStore dashboard.
func mediastoreSnippet() *SnippetData {
	return &SnippetData{
		ID:    "mediastore-operations",
		Title: "Using MediaStore",
		Cli:   `aws mediastore list-containers --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for MediaStore
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
client := mediastore.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for MediaStore
import boto3

client = boto3.client('mediastore', endpoint_url='http://localhost:8000')`,
	}
}

// setupMediaStoreRoutes registers all MediaStore dashboard routes.
func (h *DashboardHandler) setupMediaStoreRoutes() {
	h.SubRouter.GET("/dashboard/mediastore", h.mediastoreIndex)
	h.SubRouter.POST("/dashboard/mediastore/container/create", h.mediastoreCreateContainer)
	h.SubRouter.POST("/dashboard/mediastore/container/delete", h.mediastoreDeleteContainer)
}

// mediastoreIndex renders the main MediaStore dashboard page.
func (h *DashboardHandler) mediastoreIndex(c *echo.Context) error {
	w := c.Response()

	if h.MediaStoreOps == nil {
		h.renderTemplate(w, "mediastore/index.html", mediastoreIndexData{
			PageData: PageData{
				Title:     "MediaStore",
				ActiveTab: "mediastore",
				Snippet:   mediastoreSnippet(),
			},
			Containers: []mediastoreContainerView{},
		})

		return nil
	}

	rawContainers, _ := h.MediaStoreOps.Backend.ListContainers()
	views := make([]mediastoreContainerView, 0, len(rawContainers))

	for _, ct := range rawContainers {
		views = append(views, mediastoreContainerView{
			Name:                 ct.Name,
			ARN:                  ct.ARN,
			Status:               ct.Status,
			Endpoint:             ct.Endpoint,
			AccessLoggingEnabled: ct.AccessLoggingEnabled,
		})
	}

	h.renderTemplate(w, "mediastore/index.html", mediastoreIndexData{
		PageData: PageData{
			Title:     "MediaStore Containers",
			ActiveTab: "mediastore",
			Snippet:   mediastoreSnippet(),
		},
		Containers: views,
	})

	return nil
}

// mediastoreCreateContainer handles POST /dashboard/mediastore/container/create.
func (h *DashboardHandler) mediastoreCreateContainer(c *echo.Context) error {
	if h.MediaStoreOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.MediaStoreOps.Backend.CreateContainer(
		h.GlobalConfig.Region,
		h.GlobalConfig.AccountID,
		name,
		nil,
	); err != nil {
		h.Logger.ErrorContext(context.Background(), "mediastore: failed to create container", "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/mediastore")
}

// mediastoreDeleteContainer handles POST /dashboard/mediastore/container/delete.
func (h *DashboardHandler) mediastoreDeleteContainer(c *echo.Context) error {
	if h.MediaStoreOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.MediaStoreOps.Backend.DeleteContainer(name); err != nil {
		h.Logger.ErrorContext(context.Background(), "mediastore: failed to delete container", "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/mediastore")
}
