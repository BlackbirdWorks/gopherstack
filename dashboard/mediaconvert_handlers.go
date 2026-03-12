package dashboard

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v5"
)

// mediaconvertQueueView is the view model for a single MediaConvert queue row.
type mediaconvertQueueView struct {
	Name        string
	ARN         string
	Status      string
	PricingPlan string
	Type        string
}

// mediaconvertIndexData is the template data for the MediaConvert dashboard page.
type mediaconvertIndexData struct {
	PageData

	Queues []mediaconvertQueueView
}

// mediaconvertSnippet returns the shared SnippetData for the MediaConvert dashboard.
func mediaconvertSnippet() *SnippetData {
	return &SnippetData{
		ID:    "mediaconvert-operations",
		Title: "Using MediaConvert",
		Cli:   `aws mediaconvert list-queues --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for MediaConvert
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
)
if err != nil {
    log.Fatal(err)
}
client := mediaconvert.NewFromConfig(cfg, func(o *mediaconvert.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})`,
		Python: `# Initialize boto3 client for MediaConvert
import boto3

client = boto3.client('mediaconvert', endpoint_url='http://localhost:8000')`,
	}
}

// setupMediaConvertRoutes registers all MediaConvert dashboard routes.
func (h *DashboardHandler) setupMediaConvertRoutes() {
	h.SubRouter.GET("/dashboard/mediaconvert", h.mediaconvertIndex)
	h.SubRouter.POST("/dashboard/mediaconvert/queues/create", h.mediaconvertCreateQueue)
	h.SubRouter.POST("/dashboard/mediaconvert/queues/delete", h.mediaconvertDeleteQueue)
}

// mediaconvertIndex renders the main MediaConvert dashboard page.
func (h *DashboardHandler) mediaconvertIndex(c *echo.Context) error {
	w := c.Response()

	if h.MediaConvertOps == nil {
		h.renderTemplate(w, "mediaconvert/index.html", mediaconvertIndexData{
			PageData: PageData{
				Title:     "MediaConvert Queues",
				ActiveTab: "mediaconvert",
				Snippet:   mediaconvertSnippet(),
			},
			Queues: []mediaconvertQueueView{},
		})

		return nil
	}

	queues := h.MediaConvertOps.Backend.ListQueues()
	views := make([]mediaconvertQueueView, 0, len(queues))

	for _, q := range queues {
		views = append(views, mediaconvertQueueView{
			Name:        q.Name,
			ARN:         q.Arn,
			Status:      q.Status,
			PricingPlan: q.PricingPlan,
			Type:        q.Type,
		})
	}

	h.renderTemplate(w, "mediaconvert/index.html", mediaconvertIndexData{
		PageData: PageData{
			Title:     "MediaConvert Queues",
			ActiveTab: "mediaconvert",
			Snippet:   mediaconvertSnippet(),
		},
		Queues: views,
	})

	return nil
}

// mediaconvertCreateQueue handles POST /dashboard/mediaconvert/queues/create.
func (h *DashboardHandler) mediaconvertCreateQueue(c *echo.Context) error {
	if h.MediaConvertOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	description := c.Request().FormValue("description")

	if _, err := h.MediaConvertOps.Backend.CreateQueue(name, description, "", ""); err != nil {
		h.Logger.ErrorContext(context.Background(), "mediaconvert: failed to create queue", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/mediaconvert")
}

// mediaconvertDeleteQueue handles POST /dashboard/mediaconvert/queues/delete.
func (h *DashboardHandler) mediaconvertDeleteQueue(c *echo.Context) error {
	if h.MediaConvertOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.MediaConvertOps.Backend.DeleteQueue(name); err != nil {
		h.Logger.ErrorContext(context.Background(), "mediaconvert: failed to delete queue", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/mediaconvert")
}
