package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// serverlessrepoApplicationView is the view model for a single SAR application row.
type serverlessrepoApplicationView struct {
	ApplicationID string
	Name          string
	Description   string
	Author        string
}

// serverlessrepoIndexData is the template data for the Serverless Application Repository dashboard page.
type serverlessrepoIndexData struct {
	PageData

	Applications []serverlessrepoApplicationView
}

// serverlessrepoSnippet returns the shared SnippetData for the Serverless Application Repository dashboard.
func serverlessrepoSnippet() *SnippetData {
	return &SnippetData{
		ID:    "serverlessrepo-operations",
		Title: "Using Serverless Application Repository",
		Cli:   `aws serverlessrepo list-applications --endpoint-url http://localhost:8000`,
		Go: `// The AWS SDK v2 does not provide a standalone serverlessrepo client package.
// Use net/http directly with AWS SigV4 signing, or use the AWS CLI.
// Example using net/http:
req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
    "http://localhost:8000/applications", nil)
// Add AWS SigV4 Authorization header here.
resp, _ := http.DefaultClient.Do(req)`,
		Python: `# Initialize boto3 client for Serverless Application Repository
import boto3

client = boto3.client('serverlessrepo', endpoint_url='http://localhost:8000')`,
	}
}

// setupServerlessRepoRoutes registers all Serverless Application Repository dashboard routes.
func (h *DashboardHandler) setupServerlessRepoRoutes() {
	h.SubRouter.GET("/dashboard/serverlessrepo", h.serverlessrepoIndex)
	h.SubRouter.POST("/dashboard/serverlessrepo/create", h.serverlessrepoCreate)
	h.SubRouter.POST("/dashboard/serverlessrepo/delete", h.serverlessrepoDelete)
}

// serverlessrepoIndex renders the main Serverless Application Repository dashboard page.
func (h *DashboardHandler) serverlessrepoIndex(c *echo.Context) error {
	w := c.Response()

	if h.ServerlessRepoOps == nil {
		h.renderTemplate(w, "serverlessrepo/index.html", serverlessrepoIndexData{
			PageData: PageData{
				Title:     "Serverless Application Repository",
				ActiveTab: "serverlessrepo",
				Snippet:   serverlessrepoSnippet(),
			},
			Applications: []serverlessrepoApplicationView{},
		})

		return nil
	}

	list := h.ServerlessRepoOps.Backend.ListApplications()
	views := make([]serverlessrepoApplicationView, 0, len(list))

	for _, a := range list {
		views = append(views, serverlessrepoApplicationView{
			ApplicationID: a.ApplicationID,
			Name:          a.Name,
			Description:   a.Description,
			Author:        a.Author,
		})
	}

	h.renderTemplate(w, "serverlessrepo/index.html", serverlessrepoIndexData{
		PageData: PageData{
			Title:     "Serverless Application Repository",
			ActiveTab: "serverlessrepo",
			Snippet:   serverlessrepoSnippet(),
		},
		Applications: views,
	})

	return nil
}

// serverlessrepoCreate handles POST /dashboard/serverlessrepo/create.
func (h *DashboardHandler) serverlessrepoCreate(c *echo.Context) error {
	if h.ServerlessRepoOps == nil {
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
	author := c.Request().FormValue("author")
	ctx := c.Request().Context()

	if _, err := h.ServerlessRepoOps.Backend.CreateApplication(name, description, author, "", "", nil); err != nil {
		h.Logger.ErrorContext(ctx, "serverlessrepo: failed to create application", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/serverlessrepo")
}

// serverlessrepoDelete handles POST /dashboard/serverlessrepo/delete.
func (h *DashboardHandler) serverlessrepoDelete(c *echo.Context) error {
	if h.ServerlessRepoOps == nil {
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

	if err := h.ServerlessRepoOps.Backend.DeleteApplication(name); err != nil {
		h.Logger.ErrorContext(ctx, "serverlessrepo: failed to delete application", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/serverlessrepo")
}
