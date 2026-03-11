package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// codecommitRepositoryView is the view model for a single CodeCommit repository.
type codecommitRepositoryView struct {
	Name        string
	ARN         string
	Description string
}

// codecommitIndexData is the template data for the CodeCommit index page.
type codecommitIndexData struct {
	PageData

	Repositories []codecommitRepositoryView
}

// codecommitSnippet returns the shared SnippetData for the CodeCommit dashboard pages.
func codecommitSnippet() *SnippetData {
	return &SnippetData{
		ID:    "codecommit-operations",
		Title: "Using CodeCommit",
		Cli:   `aws codecommit list-repositories --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for CodeCommit
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
client := codecommit.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for CodeCommit
import boto3

client = boto3.client('codecommit', endpoint_url='http://localhost:8000')`,
	}
}

// codecommitIndex renders the CodeCommit dashboard index.
func (h *DashboardHandler) codecommitIndex(c *echo.Context) error {
	w := c.Response()

	if h.CodeCommitOps == nil {
		h.renderTemplate(w, "codecommit/index.html", codecommitIndexData{
			PageData: PageData{
				Title:     "CodeCommit Repositories",
				ActiveTab: "codecommit",
				Snippet:   codecommitSnippet(),
			},
			Repositories: []codecommitRepositoryView{},
		})

		return nil
	}

	repos := h.CodeCommitOps.Backend.ListRepositories()
	views := make([]codecommitRepositoryView, 0, len(repos))

	for _, r := range repos {
		views = append(views, codecommitRepositoryView{
			Name:        r.RepositoryName,
			ARN:         r.ARN,
			Description: r.Description,
		})
	}

	h.renderTemplate(w, "codecommit/index.html", codecommitIndexData{
		PageData: PageData{
			Title:     "CodeCommit Repositories",
			ActiveTab: "codecommit",
			Snippet:   codecommitSnippet(),
		},
		Repositories: views,
	})

	return nil
}

// codecommitCreateRepository handles POST /dashboard/codecommit/repository/create.
func (h *DashboardHandler) codecommitCreateRepository(c *echo.Context) error {
	if h.CodeCommitOps == nil {
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

	_, err := h.CodeCommitOps.Backend.CreateRepository(name, description, nil)
	if err != nil {
		h.Logger.Error("failed to create codecommit repository", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/codecommit")
}

// codecommitDeleteRepository handles POST /dashboard/codecommit/repository/delete.
func (h *DashboardHandler) codecommitDeleteRepository(c *echo.Context) error {
	if h.CodeCommitOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.CodeCommitOps.Backend.DeleteRepository(name); err != nil {
		h.Logger.Error("failed to delete codecommit repository", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/codecommit")
}

// setupCodeCommitRoutes registers routes for the CodeCommit dashboard.
func (h *DashboardHandler) setupCodeCommitRoutes() {
	h.SubRouter.GET("/dashboard/codecommit", h.codecommitIndex)
	h.SubRouter.POST("/dashboard/codecommit/repository/create", h.codecommitCreateRepository)
	h.SubRouter.POST("/dashboard/codecommit/repository/delete", h.codecommitDeleteRepository)
}
