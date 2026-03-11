package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// codestarConnectionView is the view model for a single CodeStar connection.
type codestarConnectionView struct {
	Name         string
	ARN          string
	Status       string
	ProviderType string
}

// codestarHostView is the view model for a single CodeStar host.
type codestarHostView struct {
	Name             string
	ARN              string
	ProviderType     string
	ProviderEndpoint string
	Status           string
}

// codestarConnectionsIndexData is the template data for the CodeStar Connections dashboard index page.
type codestarConnectionsIndexData struct {
	PageData

	Connections []codestarConnectionView
	Hosts       []codestarHostView
}

// codestarConnectionsIndex renders the CodeStar Connections dashboard index page.
func (h *DashboardHandler) codestarConnectionsIndex(c *echo.Context) error {
	w := c.Response()

	snippet := &SnippetData{
		ID:    "codestarconnections-operations",
		Title: "Using AWS CodeStar Connections",
		Cli: `aws codestar-connections list-connections \
    --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for CodeStar Connections
cfg, err := config.LoadDefaultConfig(context.TODO())
client := codestarconnections.NewFromConfig(cfg, func(o *codestarconnections.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})`,
		Python: `# Initialize boto3 client for CodeStar Connections
import boto3
client = boto3.client('codestar-connections', endpoint_url='http://localhost:8000')`,
	}

	if h.CodeStarConnectionsOps == nil {
		h.renderTemplate(w, "codestarconnections/index.html", codestarConnectionsIndexData{
			PageData: PageData{
				Title:     "CodeStar Connections",
				ActiveTab: "codestarconnections",
				Snippet:   snippet,
			},
			Connections: []codestarConnectionView{
				{
					Name:         "demo-github",
					ARN:          "arn:aws:codestar-connections:us-east-1:000000000000:connection/demo",
					Status:       "AVAILABLE",
					ProviderType: "GitHub",
				},
			},
			Hosts: []codestarHostView{
				{
					Name:             "demo-host",
					ARN:              "arn:aws:codestar-connections:us-east-1:000000000000:host/demo-host/abc12345",
					ProviderType:     "GitHubEnterpriseServer",
					ProviderEndpoint: "https://github.example.com",
					Status:           "AVAILABLE",
				},
			},
		})

		return nil
	}

	connections := h.CodeStarConnectionsOps.Backend.ListConnections("", "")
	connViews := make([]codestarConnectionView, 0, len(connections))

	for _, c := range connections {
		connViews = append(connViews, codestarConnectionView{
			Name:         c.ConnectionName,
			ARN:          c.ConnectionArn,
			Status:       c.ConnectionStatus,
			ProviderType: c.ProviderType,
		})
	}

	hosts := h.CodeStarConnectionsOps.Backend.ListHosts()
	hostViews := make([]codestarHostView, 0, len(hosts))

	for _, host := range hosts {
		hostViews = append(hostViews, codestarHostView{
			Name:             host.Name,
			ARN:              host.HostArn,
			ProviderType:     host.ProviderType,
			ProviderEndpoint: host.ProviderEndpoint,
			Status:           host.Status,
		})
	}

	h.renderTemplate(w, "codestarconnections/index.html", codestarConnectionsIndexData{
		PageData: PageData{
			Title:     "CodeStar Connections",
			ActiveTab: "codestarconnections",
			Snippet:   snippet,
		},
		Connections: connViews,
		Hosts:       hostViews,
	})

	return nil
}

// codestarConnectionsCreateConnection handles POST /dashboard/codestarconnections/connections/create.
func (h *DashboardHandler) codestarConnectionsCreateConnection(c *echo.Context) error {
	if h.CodeStarConnectionsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	providerType := c.Request().FormValue("provider_type")

	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.CodeStarConnectionsOps.Backend.CreateConnection(name, providerType, "", nil)
	if err != nil {
		h.Logger.Error("failed to create codestar connection", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/codestarconnections")
}

// codestarConnectionsDeleteConnection handles POST /dashboard/codestarconnections/connections/delete.
func (h *DashboardHandler) codestarConnectionsDeleteConnection(c *echo.Context) error {
	if h.CodeStarConnectionsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	arn := c.Request().FormValue("arn")
	if arn == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.CodeStarConnectionsOps.Backend.DeleteConnection(arn); err != nil {
		h.Logger.Error("failed to delete codestar connection", "arn", arn, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/codestarconnections")
}

// codestarConnectionsCreateHost handles POST /dashboard/codestarconnections/hosts/create.
func (h *DashboardHandler) codestarConnectionsCreateHost(c *echo.Context) error {
	if h.CodeStarConnectionsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	providerType := c.Request().FormValue("provider_type")
	endpoint := c.Request().FormValue("endpoint")

	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.CodeStarConnectionsOps.Backend.CreateHost(name, providerType, endpoint, nil)
	if err != nil {
		h.Logger.Error("failed to create codestar host", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/codestarconnections")
}

// codestarConnectionsDeleteHost handles POST /dashboard/codestarconnections/hosts/delete.
func (h *DashboardHandler) codestarConnectionsDeleteHost(c *echo.Context) error {
	if h.CodeStarConnectionsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	arn := c.Request().FormValue("arn")
	if arn == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.CodeStarConnectionsOps.Backend.DeleteHost(arn); err != nil {
		h.Logger.Error("failed to delete codestar host", "arn", arn, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/codestarconnections")
}

// setupCodeStarConnectionsRoutes registers routes for the CodeStar Connections dashboard.
func (h *DashboardHandler) setupCodeStarConnectionsRoutes() {
	h.SubRouter.GET("/dashboard/codestarconnections", h.codestarConnectionsIndex)
	h.SubRouter.POST("/dashboard/codestarconnections/connections/create", h.codestarConnectionsCreateConnection)
	h.SubRouter.POST("/dashboard/codestarconnections/connections/delete", h.codestarConnectionsDeleteConnection)
	h.SubRouter.POST("/dashboard/codestarconnections/hosts/create", h.codestarConnectionsCreateHost)
	h.SubRouter.POST("/dashboard/codestarconnections/hosts/delete", h.codestarConnectionsDeleteHost)
}
