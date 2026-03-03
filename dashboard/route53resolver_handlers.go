package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// route53resolverEndpointView is the view model for a single Route53 Resolver endpoint.
type route53resolverEndpointView struct {
	ID        string
	Name      string
	Direction string
	Status    string
}

// route53resolverIndexData is the template data for the Route53Resolver index page.
type route53resolverIndexData struct {
	PageData

	Endpoints []route53resolverEndpointView
}

// route53resolverIndex renders the Route53Resolver dashboard index.
func (h *DashboardHandler) route53resolverIndex(c *echo.Context) error {
	w := c.Response()

	if h.Route53ResolverOps == nil {
		h.renderTemplate(w, "route53resolver/index.html", route53resolverIndexData{
			PageData: PageData{Title: "Route53 Resolver Endpoints", ActiveTab: "route53resolver",
				Snippet: &SnippetData{
					ID:    "route53resolver-operations",
					Title: "Using Route53resolver",
					Cli:   `aws route53resolver help --endpoint-url http://localhost:8000`,
					Go: `// Initialize AWS SDK v2 for Using Route53resolver
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
client := route53resolver.NewFromConfig(cfg)`,
					Python: `# Initialize boto3 client for Using Route53resolver
import boto3

client = boto3.client('route53resolver', endpoint_url='http://localhost:8000')`,
				}},
			Endpoints: []route53resolverEndpointView{},
		})

		return nil
	}

	endpoints := h.Route53ResolverOps.Backend.ListResolverEndpoints()
	views := make([]route53resolverEndpointView, 0, len(endpoints))

	for _, ep := range endpoints {
		views = append(views, route53resolverEndpointView{
			ID:        ep.ID,
			Name:      ep.Name,
			Direction: ep.Direction,
			Status:    ep.Status,
		})
	}

	h.renderTemplate(w, "route53resolver/index.html", route53resolverIndexData{
		PageData: PageData{Title: "Route53 Resolver Endpoints", ActiveTab: "route53resolver",
			Snippet: &SnippetData{
				ID:    "route53resolver-operations",
				Title: "Using Route53resolver",
				Cli:   `aws route53resolver help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Route53resolver
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
client := route53resolver.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Route53resolver
import boto3

client = boto3.client('route53resolver', endpoint_url='http://localhost:8000')`,
			}},
		Endpoints: views,
	})

	return nil
}

// route53resolverCreateEndpoint handles POST /dashboard/route53resolver/create.
func (h *DashboardHandler) route53resolverCreateEndpoint(c *echo.Context) error {
	if h.Route53ResolverOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	direction := c.Request().FormValue("direction")

	if name == "" || direction == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.Route53ResolverOps.Backend.CreateResolverEndpoint(name, direction, "", nil); err != nil {
		h.Logger.Error("failed to create resolver endpoint", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/route53resolver")
}

// route53resolverDeleteEndpoint handles POST /dashboard/route53resolver/delete.
func (h *DashboardHandler) route53resolverDeleteEndpoint(c *echo.Context) error {
	if h.Route53ResolverOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	id := c.Request().FormValue("id")
	if id == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.Route53ResolverOps.Backend.DeleteResolverEndpoint(id); err != nil {
		h.Logger.Error("failed to delete resolver endpoint", "id", id, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/route53resolver")
}
