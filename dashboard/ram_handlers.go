package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// ramShareView is the view model for a single RAM resource share row.
type ramShareView struct {
	Name                    string
	ARN                     string
	Status                  string
	AllowExternalPrincipals bool
}

// ramIndexData is the template data for the RAM dashboard page.
type ramIndexData struct {
	PageData

	ResourceShares []ramShareView
}

// ramSnippet returns the shared SnippetData for the RAM dashboard.
func ramSnippet() *SnippetData {
	return &SnippetData{
		ID:    "ram-operations",
		Title: "Using RAM",
		Cli:   `aws ram get-resource-shares --resource-owner SELF --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for AWS RAM
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
client := ram.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for AWS RAM
import boto3

client = boto3.client('ram', endpoint_url='http://localhost:8000')`,
	}
}

// setupRAMRoutes registers all RAM dashboard routes.
func (h *DashboardHandler) setupRAMRoutes() {
	h.SubRouter.GET("/dashboard/ram", h.ramIndex)
	h.SubRouter.POST("/dashboard/ram/create", h.ramCreate)
	h.SubRouter.POST("/dashboard/ram/delete", h.ramDelete)
}

// ramIndex renders the main RAM dashboard page.
func (h *DashboardHandler) ramIndex(c *echo.Context) error {
	w := c.Response()

	if h.RAMOps == nil {
		h.renderTemplate(w, "ram/index.html", ramIndexData{
			PageData: PageData{
				Title:     "RAM",
				ActiveTab: "ram",
				Snippet:   ramSnippet(),
			},
			ResourceShares: []ramShareView{},
		})

		return nil
	}

	list := h.RAMOps.Backend.ListResourceShares("SELF")
	views := make([]ramShareView, 0, len(list))

	for _, rs := range list {
		views = append(views, ramShareView{
			Name:                    rs.Name,
			ARN:                     rs.ARN,
			Status:                  rs.Status,
			AllowExternalPrincipals: rs.AllowExternalPrincipals,
		})
	}

	h.renderTemplate(w, "ram/index.html", ramIndexData{
		PageData: PageData{
			Title:     "RAM",
			ActiveTab: "ram",
			Snippet:   ramSnippet(),
		},
		ResourceShares: views,
	})

	return nil
}

// ramCreate handles POST /dashboard/ram/create.
func (h *DashboardHandler) ramCreate(c *echo.Context) error {
	if h.RAMOps == nil {
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

	if _, err := h.RAMOps.Backend.CreateResourceShare(name, true, nil, nil, nil); err != nil {
		h.Logger.ErrorContext(ctx, "ram: failed to create resource share", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/ram")
}

// ramDelete handles POST /dashboard/ram/delete.
func (h *DashboardHandler) ramDelete(c *echo.Context) error {
	if h.RAMOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	shareARN := c.Request().FormValue("arn")
	if shareARN == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	ctx := c.Request().Context()

	if err := h.RAMOps.Backend.DeleteResourceShare(shareARN); err != nil {
		h.Logger.ErrorContext(ctx, "ram: failed to delete resource share", "arn", shareARN, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/ram")
}
