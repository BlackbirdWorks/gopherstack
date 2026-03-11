package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// codeartifactDomainView is the view model for a single CodeArtifact domain.
type codeartifactDomainView struct {
	Name   string
	ARN    string
	Status string
}

// codeartifactIndexData is the template data for the CodeArtifact index page.
type codeartifactIndexData struct {
	PageData

	Domains []codeartifactDomainView
}

// codeartifactSnippet returns the shared SnippetData for the CodeArtifact dashboard pages.
func codeartifactSnippet() *SnippetData {
	return &SnippetData{
		ID:    "codeartifact-operations",
		Title: "Using CodeArtifact",
		Cli:   `aws codeartifact list-domains --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for CodeArtifact
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
client := codeartifact.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for CodeArtifact
import boto3

client = boto3.client('codeartifact', endpoint_url='http://localhost:8000')`,
	}
}

// codeartifactIndex renders the CodeArtifact dashboard index.
func (h *DashboardHandler) codeartifactIndex(c *echo.Context) error {
	w := c.Response()

	if h.CodeArtifactOps == nil {
		h.renderTemplate(w, "codeartifact/index.html", codeartifactIndexData{
			PageData: PageData{
				Title:     "CodeArtifact Domains",
				ActiveTab: "codeartifact",
				Snippet:   codeartifactSnippet(),
			},
			Domains: []codeartifactDomainView{},
		})

		return nil
	}

	domains := h.CodeArtifactOps.Backend.ListDomains()
	views := make([]codeartifactDomainView, 0, len(domains))

	for _, d := range domains {
		views = append(views, codeartifactDomainView{
			Name:   d.Name,
			ARN:    d.ARN,
			Status: d.Status,
		})
	}

	h.renderTemplate(w, "codeartifact/index.html", codeartifactIndexData{
		PageData: PageData{Title: "CodeArtifact Domains", ActiveTab: "codeartifact", Snippet: codeartifactSnippet()},
		Domains:  views,
	})

	return nil
}

// codeartifactCreateDomain handles POST /dashboard/codeartifact/domain/create.
func (h *DashboardHandler) codeartifactCreateDomain(c *echo.Context) error {
	if h.CodeArtifactOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.CodeArtifactOps.Backend.CreateDomain(name, "", nil)
	if err != nil {
		h.Logger.Error("failed to create codeartifact domain", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/codeartifact")
}

// codeartifactDeleteDomain handles POST /dashboard/codeartifact/domain/delete.
func (h *DashboardHandler) codeartifactDeleteDomain(c *echo.Context) error {
	if h.CodeArtifactOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.CodeArtifactOps.Backend.DeleteDomain(name); err != nil {
		h.Logger.Error("failed to delete codeartifact domain", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/codeartifact")
}
