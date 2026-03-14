package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	elasticsearchbackend "github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

// elasticsearchDomainView is the view model for a single Elasticsearch domain.
type elasticsearchDomainView struct {
	Name                 string
	ElasticsearchVersion string
	Endpoint             string
	Status               string
}

// elasticsearchIndexData is the template data for the Elasticsearch index page.
type elasticsearchIndexData struct {
	PageData

	Domains []elasticsearchDomainView
}

// elasticsearchIndex renders the list of all Elasticsearch domains.
//
//nolint:dupl // intentionally similar to opensearchIndex with different service fields
func (h *DashboardHandler) elasticsearchIndex(c *echo.Context) error {
	w := c.Response()

	if h.ElasticsearchOps == nil {
		h.renderTemplate(w, "elasticsearch/index.html", elasticsearchIndexData{
			PageData: PageData{Title: "Elasticsearch", ActiveTab: "elasticsearch",
				Snippet: &SnippetData{
					ID:    "elasticsearch-operations",
					Title: "Using Elasticsearch",
					Cli:   `aws es help --endpoint-url http://localhost:8000`,
					Go: `// Initialize AWS SDK v2 for Using Elasticsearch
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
client := elasticsearchservice.NewFromConfig(cfg)`,
					Python: `# Initialize boto3 client for Using Elasticsearch
import boto3

client = boto3.client('es', endpoint_url='http://localhost:8000')`,
				}},
			Domains: []elasticsearchDomainView{},
		})

		return nil
	}

	names := h.ElasticsearchOps.Backend.ListDomainNames()
	views := make([]elasticsearchDomainView, 0, len(names))

	for _, name := range names {
		d, err := h.ElasticsearchOps.Backend.DescribeDomain(name)
		if err != nil {
			continue
		}

		views = append(views, elasticsearchDomainView{
			Name:                 d.Name,
			ElasticsearchVersion: d.ElasticsearchVersion,
			Endpoint:             d.Endpoint,
			Status:               d.Status,
		})
	}

	h.renderTemplate(w, "elasticsearch/index.html", elasticsearchIndexData{
		PageData: PageData{Title: "Elasticsearch", ActiveTab: "elasticsearch",
			Snippet: &SnippetData{
				ID:    "elasticsearch-operations",
				Title: "Using Elasticsearch",
				Cli:   `aws es help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Elasticsearch
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
client := elasticsearchservice.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Elasticsearch
import boto3

client = boto3.client('es', endpoint_url='http://localhost:8000')`,
			}},
		Domains: views,
	})

	return nil
}

// elasticsearchCreateDomain handles POST /dashboard/elasticsearch/create.
func (h *DashboardHandler) elasticsearchCreateDomain(c *echo.Context) error {
	if h.ElasticsearchOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("domain_name")
	esVersion := c.Request().FormValue("elasticsearch_version")

	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.ElasticsearchOps.Backend.CreateDomain(
		name,
		esVersion,
		elasticsearchbackend.ClusterConfig{},
		elasticsearchbackend.EBSOptions{},
	); err != nil {
		h.Logger.Error("failed to create Elasticsearch domain", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/elasticsearch")
}

// elasticsearchDeleteDomain handles POST /dashboard/elasticsearch/delete.
func (h *DashboardHandler) elasticsearchDeleteDomain(c *echo.Context) error {
	if h.ElasticsearchOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.ElasticsearchOps.Backend.DeleteDomain(name); err != nil {
		h.Logger.Error("failed to delete Elasticsearch domain", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/elasticsearch")
}
