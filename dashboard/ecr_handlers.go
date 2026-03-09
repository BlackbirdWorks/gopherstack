package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// ecrIndexData is the template data for the ECR dashboard page.
type ecrIndexData struct {
	PageData

	Repositories []ecrRepositoryView
}

// ecrRepositoryView is the view model for a single ECR repository.
type ecrRepositoryView struct {
	Name       string
	ARN        string
	URI        string
	RegistryID string
	CreatedAt  string
}

// ecrIndex renders the ECR dashboard page.
func (h *DashboardHandler) ecrIndex(c *echo.Context) error {
	w := c.Response()

	if h.ECROps == nil {
		h.renderTemplate(w, "ecr/index.html", ecrIndexData{
			PageData: PageData{
				Title:     "ECR",
				ActiveTab: "ecr",
				Snippet:   ecrSnippetData(),
			},
			Repositories: []ecrRepositoryView{},
		})

		return nil
	}

	// The ECR service handler lazily sets the backend endpoint on the first API
	// request. The dashboard bypasses that handler, so we replicate the lazy
	// initialization here to ensure repository URIs reflect the local address.
	if h.ECROps.Backend.ProxyEndpoint() == "" {
		if host := c.Request().Host; host != "" {
			h.ECROps.Backend.SetEndpoint(host)
		}
	}

	repos, err := h.ECROps.Backend.DescribeRepositories(nil)
	if err != nil {
		h.Logger.Error("failed to list ECR repositories", "error", err)
		repos = nil
	}

	views := make([]ecrRepositoryView, 0, len(repos))

	for _, r := range repos {
		views = append(views, ecrRepositoryView{
			Name:       r.RepositoryName,
			ARN:        r.RepositoryARN,
			URI:        r.RepositoryURI,
			RegistryID: r.RegistryID,
			CreatedAt:  r.CreatedAt.Format("2006-01-02 15:04:05"),
		})
	}

	h.renderTemplate(w, "ecr/index.html", ecrIndexData{
		PageData: PageData{
			Title:     "ECR",
			ActiveTab: "ecr",
			Snippet:   ecrSnippetData(),
		},
		Repositories: views,
	})

	return nil
}

// ecrCreateRepository handles POST /dashboard/ecr/repository/create.
func (h *DashboardHandler) ecrCreateRepository(c *echo.Context) error {
	if h.ECROps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.ECROps.Backend.CreateRepository(name); err != nil {
		h.Logger.Error("failed to create ECR repository", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/ecr")
}

// ecrDeleteRepository handles POST /dashboard/ecr/repository/delete.
func (h *DashboardHandler) ecrDeleteRepository(c *echo.Context) error {
	if h.ECROps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.ECROps.Backend.DeleteRepository(name); err != nil {
		h.Logger.Error("failed to delete ECR repository", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/ecr")
}

func ecrSnippetData() *SnippetData {
	return &SnippetData{
		ID:    "ecr-operations",
		Title: "Using ECR",
		Cli:   `aws ecr describe-repositories --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for ECR
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
client := ecr.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for ECR
import boto3

client = boto3.client('ecr', endpoint_url='http://localhost:8000')`,
	}
}
