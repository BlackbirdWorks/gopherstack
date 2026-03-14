package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// servicediscoveryNamespaceView is the view model for a single namespace row.
type servicediscoveryNamespaceView struct {
	ID   string
	Name string
	Type string
	ARN  string
}

// servicediscoveryServiceView is the view model for a single Cloud Map service row.
type servicediscoveryServiceView struct {
	ID          string
	Name        string
	NamespaceID string
	ARN         string
}

// servicediscoveryIndexData is the template data for the Cloud Map dashboard page.
type servicediscoveryIndexData struct {
	PageData

	Namespaces []servicediscoveryNamespaceView
	Services   []servicediscoveryServiceView
}

// servicediscoverySnippet returns the shared SnippetData for the Cloud Map dashboard.
func servicediscoverySnippet() *SnippetData {
	return &SnippetData{
		ID:    "servicediscovery-operations",
		Title: "Using AWS Cloud Map",
		Cli: "aws servicediscovery create-http-namespace" +
			" --name my-namespace" +
			" --endpoint-url http://localhost:8000",
		Go: `// Initialize AWS SDK v2 for Service Discovery
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
client := servicediscovery.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Service Discovery
import boto3

client = boto3.client('servicediscovery', endpoint_url='http://localhost:8000')`,
	}
}

// setupServiceDiscoveryRoutes registers all Cloud Map dashboard routes.
func (h *DashboardHandler) setupServiceDiscoveryRoutes() {
	h.SubRouter.GET("/dashboard/servicediscovery", h.servicediscoveryIndex)
	h.SubRouter.POST("/dashboard/servicediscovery/namespace", h.servicediscoveryCreateNamespace)
}

// servicediscoveryIndex renders the main Cloud Map dashboard page.
func (h *DashboardHandler) servicediscoveryIndex(c *echo.Context) error {
	w := c.Response()

	if h.ServiceDiscoveryOps == nil {
		h.renderTemplate(w, "servicediscovery/index.html", servicediscoveryIndexData{
			PageData: PageData{
				Title:     "Service Discovery",
				ActiveTab: "servicediscovery",
				Snippet:   servicediscoverySnippet(),
			},
			Namespaces: []servicediscoveryNamespaceView{},
			Services:   []servicediscoveryServiceView{},
		})

		return nil
	}

	namespaces := h.ServiceDiscoveryOps.Backend.ListNamespaces()
	nsViews := make([]servicediscoveryNamespaceView, 0, len(namespaces))

	for _, ns := range namespaces {
		nsViews = append(nsViews, servicediscoveryNamespaceView{
			ID:   ns.ID,
			Name: ns.Name,
			Type: ns.Type,
			ARN:  ns.ARN,
		})
	}

	services := h.ServiceDiscoveryOps.Backend.ListServices("")
	svcViews := make([]servicediscoveryServiceView, 0, len(services))

	for _, svc := range services {
		svcViews = append(svcViews, servicediscoveryServiceView{
			ID:          svc.ID,
			Name:        svc.Name,
			NamespaceID: svc.NamespaceID,
			ARN:         svc.ARN,
		})
	}

	h.renderTemplate(w, "servicediscovery/index.html", servicediscoveryIndexData{
		PageData: PageData{
			Title:     "Service Discovery",
			ActiveTab: "servicediscovery",
			Snippet:   servicediscoverySnippet(),
		},
		Namespaces: nsViews,
		Services:   svcViews,
	})

	return nil
}

// servicediscoveryCreateNamespace handles POST /dashboard/servicediscovery/namespace.
func (h *DashboardHandler) servicediscoveryCreateNamespace(c *echo.Context) error {
	if h.ServiceDiscoveryOps == nil {
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

	if _, err := h.ServiceDiscoveryOps.Backend.CreateHTTPNamespace(name, "", nil); err != nil {
		h.Logger.ErrorContext(ctx, "servicediscovery: failed to create namespace", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/servicediscovery")
}
