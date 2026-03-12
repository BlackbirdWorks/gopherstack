package dashboard

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v5"
)

// lakeformationResourceView is the view model for a single Lake Formation registered resource row.
type lakeformationResourceView struct {
	ResourceArn string
	RoleArn     string
}

// lakeformationIndexData is the template data for the Lake Formation dashboard page.
type lakeformationIndexData struct {
	PageData

	Resources []lakeformationResourceView
}

// lakeformationSnippet returns the shared SnippetData for the Lake Formation dashboard.
func lakeformationSnippet() *SnippetData {
	return &SnippetData{
		ID:    "lakeformation-operations",
		Title: "Using Lake Formation",
		Cli:   `aws lakeformation list-resources --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Lake Formation
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
client := lakeformation.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Lake Formation
import boto3

client = boto3.client('lakeformation', endpoint_url='http://localhost:8000')`,
	}
}

// setupLakeFormationRoutes registers all Lake Formation dashboard routes.
func (h *DashboardHandler) setupLakeFormationRoutes() {
	h.SubRouter.GET("/dashboard/lakeformation", h.lakeformationIndex)
	h.SubRouter.POST("/dashboard/lakeformation/resource/register", h.lakeformationRegisterResource)
	h.SubRouter.POST("/dashboard/lakeformation/resource/deregister", h.lakeformationDeregisterResource)
}

// lakeformationIndex renders the main Lake Formation dashboard page.
func (h *DashboardHandler) lakeformationIndex(c *echo.Context) error {
	w := c.Response()

	if h.LakeFormationOps == nil {
		h.renderTemplate(w, "lakeformation/index.html", lakeformationIndexData{
			PageData: PageData{
				Title:     "Lake Formation",
				ActiveTab: "lakeformation",
				Snippet:   lakeformationSnippet(),
			},
			Resources: []lakeformationResourceView{},
		})

		return nil
	}

	rawResources, _ := h.LakeFormationOps.Backend.ListResources(0, "")
	views := make([]lakeformationResourceView, 0, len(rawResources))

	for _, r := range rawResources {
		views = append(views, lakeformationResourceView{
			ResourceArn: r.ResourceArn,
			RoleArn:     r.RoleArn,
		})
	}

	h.renderTemplate(w, "lakeformation/index.html", lakeformationIndexData{
		PageData: PageData{
			Title:     "Lake Formation Resources",
			ActiveTab: "lakeformation",
			Snippet:   lakeformationSnippet(),
		},
		Resources: views,
	})

	return nil
}

// lakeformationRegisterResource handles POST /dashboard/lakeformation/resource/register.
func (h *DashboardHandler) lakeformationRegisterResource(c *echo.Context) error {
	if h.LakeFormationOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	resourceArn := c.Request().FormValue("resource_arn")
	roleArn := c.Request().FormValue("role_arn")

	if resourceArn == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.LakeFormationOps.Backend.RegisterResource(resourceArn, roleArn); err != nil {
		h.Logger.ErrorContext(context.Background(), "lakeformation: failed to register resource", "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/lakeformation")
}

// lakeformationDeregisterResource handles POST /dashboard/lakeformation/resource/deregister.
func (h *DashboardHandler) lakeformationDeregisterResource(c *echo.Context) error {
	if h.LakeFormationOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	resourceArn := c.Request().FormValue("resource_arn")
	if resourceArn == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.LakeFormationOps.Backend.DeregisterResource(resourceArn); err != nil {
		h.Logger.ErrorContext(context.Background(), "lakeformation: failed to deregister resource", "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/lakeformation")
}
