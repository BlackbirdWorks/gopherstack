package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	opensearchbackend "github.com/blackbirdworks/gopherstack/services/opensearch"
)

// opensearchDomainView is the view model for a single OpenSearch domain.
type opensearchDomainView struct {
	Name          string
	EngineVersion string
	Endpoint      string
	Status        string
}

// opensearchIndexData is the template data for the OpenSearch index page.
type opensearchIndexData struct {
	PageData

	Domains []opensearchDomainView
}

// opensearchDomainDetailData is the template data for the OpenSearch domain detail page.
type opensearchDomainDetailData struct {
	PageData

	DomainName    string
	EngineVersion string
	Endpoint      string
	Status        string
	InstanceType  string
	InstanceCount int
}

// opensearchIndex renders the list of all OpenSearch domains.
//
//nolint:dupl // intentionally similar to elasticsearchIndex with different service fields
func (h *DashboardHandler) opensearchIndex(c *echo.Context) error {
	w := c.Response()

	if h.OpenSearchOps == nil {
		h.renderTemplate(w, "opensearch/index.html", opensearchIndexData{
			PageData: PageData{Title: "OpenSearch", ActiveTab: "opensearch",
				Snippet: &SnippetData{
					ID:    "opensearch-operations",
					Title: "Using Opensearch",
					Cli:   `aws opensearch help --endpoint-url http://localhost:8000`,
					Go: `// Initialize AWS SDK v2 for Using Opensearch
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
client := opensearch.NewFromConfig(cfg)`,
					Python: `# Initialize boto3 client for Using Opensearch
import boto3

client = boto3.client('opensearch', endpoint_url='http://localhost:8000')`,
				}},
			Domains: []opensearchDomainView{},
		})

		return nil
	}

	names := h.OpenSearchOps.Backend.ListDomainNames()
	views := make([]opensearchDomainView, 0, len(names))

	for _, name := range names {
		d, err := h.OpenSearchOps.Backend.DescribeDomain(name)
		if err != nil {
			continue
		}

		views = append(views, opensearchDomainView{
			Name:          d.Name,
			EngineVersion: d.EngineVersion,
			Endpoint:      d.Endpoint,
			Status:        d.Status,
		})
	}

	h.renderTemplate(w, "opensearch/index.html", opensearchIndexData{
		PageData: PageData{Title: "OpenSearch", ActiveTab: "opensearch",
			Snippet: &SnippetData{
				ID:    "opensearch-operations",
				Title: "Using Opensearch",
				Cli:   `aws opensearch help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Opensearch
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
client := opensearch.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Opensearch
import boto3

client = boto3.client('opensearch', endpoint_url='http://localhost:8000')`,
			}},
		Domains: views,
	})

	return nil
}

// opensearchDomainDetail renders the detail page for a single OpenSearch domain.
func (h *DashboardHandler) opensearchDomainDetail(c *echo.Context) error {
	w := c.Response()

	if h.OpenSearchOps == nil {
		return c.NoContent(http.StatusNotFound)
	}

	name := c.QueryParam("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	d, err := h.OpenSearchOps.Backend.DescribeDomain(name)
	if err != nil {
		return c.NoContent(http.StatusNotFound)
	}

	h.renderTemplate(w, "opensearch/domain_detail.html", opensearchDomainDetailData{
		PageData: PageData{Title: d.Name, ActiveTab: "opensearch",
			Snippet: &SnippetData{
				ID:    "opensearch-operations",
				Title: "Using Opensearch",
				Cli:   `aws opensearch help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Opensearch
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
client := opensearch.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Opensearch
import boto3

client = boto3.client('opensearch', endpoint_url='http://localhost:8000')`,
			}},
		DomainName:    d.Name,
		EngineVersion: d.EngineVersion,
		Endpoint:      d.Endpoint,
		Status:        d.Status,
		InstanceType:  d.ClusterConfig.InstanceType,
		InstanceCount: d.ClusterConfig.InstanceCount,
	})

	return nil
}

// opensearchCreateDomain handles POST /dashboard/opensearch/create.
func (h *DashboardHandler) opensearchCreateDomain(c *echo.Context) error {
	if h.OpenSearchOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("domain_name")
	engineVersion := c.Request().FormValue("engine_version")

	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.OpenSearchOps.Backend.CreateDomain(
		name,
		engineVersion,
		opensearchbackend.ClusterConfig{},
	); err != nil {
		h.Logger.Error("failed to create OpenSearch domain", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/opensearch")
}

// opensearchDeleteDomain handles POST /dashboard/opensearch/delete.
func (h *DashboardHandler) opensearchDeleteDomain(c *echo.Context) error {
	if h.OpenSearchOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.OpenSearchOps.Backend.DeleteDomain(name); err != nil {
		h.Logger.Error("failed to delete OpenSearch domain", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/opensearch")
}
