package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// codeconnectionsConnectionView is the view model for a single CodeConnections connection.
type codeconnectionsConnectionView struct {
	Name         string
	ARN          string
	ProviderType string
	Status       string
}

// codeconnectionsIndexData is the template data for the CodeConnections dashboard index page.
type codeconnectionsIndexData struct {
	PageData

	Connections []codeconnectionsConnectionView
}

// codeconnectionsIndex renders the CodeConnections dashboard index page.
func (h *DashboardHandler) codeconnectionsIndex(c *echo.Context) error {
	w := c.Response()

	snippet := &SnippetData{
		ID:    "codeconnections-operations",
		Title: "Using AWS CodeConnections",
		Cli: `aws codeconnections list-connections \
    --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for CodeConnections
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
client := codeconnections.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for CodeConnections
import boto3

client = boto3.client('codeconnections', endpoint_url='http://localhost:8000')`,
	}

	if h.CodeConnectionsOps == nil {
		h.renderTemplate(w, "codeconnections/index.html", codeconnectionsIndexData{
			PageData: PageData{
				Title:     "CodeConnections",
				ActiveTab: "codeconnections",
				Snippet:   snippet,
			},
			Connections: []codeconnectionsConnectionView{},
		})

		return nil
	}

	conns := h.CodeConnectionsOps.Backend.ListConnections("")
	connViews := make([]codeconnectionsConnectionView, 0, len(conns))

	for _, conn := range conns {
		connViews = append(connViews, codeconnectionsConnectionView{
			Name:         conn.ConnectionName,
			ARN:          conn.ConnectionArn,
			ProviderType: conn.ProviderType,
			Status:       conn.Status,
		})
	}

	h.renderTemplate(w, "codeconnections/index.html", codeconnectionsIndexData{
		PageData: PageData{
			Title:     "CodeConnections",
			ActiveTab: "codeconnections",
			Snippet:   snippet,
		},
		Connections: connViews,
	})

	return nil
}

// codeconnectionsCreateConnection handles POST /dashboard/codeconnections/create.
func (h *DashboardHandler) codeconnectionsCreateConnection(c *echo.Context) error {
	if h.CodeConnectionsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	providerType := c.Request().FormValue("providerType")

	if name == "" || providerType == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.CodeConnectionsOps.Backend.CreateConnection(name, providerType, nil)
	if err != nil {
		h.Logger.Error("failed to create connection", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/codeconnections")
}

// codeconnectionsDeleteConnection handles POST /dashboard/codeconnections/delete.
func (h *DashboardHandler) codeconnectionsDeleteConnection(c *echo.Context) error {
	if h.CodeConnectionsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	connectionArn := c.Request().FormValue("arn")
	if connectionArn == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.CodeConnectionsOps.Backend.DeleteConnection(connectionArn); err != nil {
		h.Logger.Error("failed to delete connection", "arn", connectionArn, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/codeconnections")
}

// setupCodeConnectionsRoutes registers routes for the CodeConnections dashboard.
func (h *DashboardHandler) setupCodeConnectionsRoutes() {
	h.SubRouter.GET("/dashboard/codeconnections", h.codeconnectionsIndex)
	h.SubRouter.POST("/dashboard/codeconnections/create", h.codeconnectionsCreateConnection)
	h.SubRouter.POST("/dashboard/codeconnections/delete", h.codeconnectionsDeleteConnection)
}
