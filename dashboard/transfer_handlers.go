package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// transferServerView is the view model for a single Transfer server.
type transferServerView struct {
	ServerID  string
	State     string
	Protocols []string
}

// transferIndexData is the template data for the Transfer index page.
type transferIndexData struct {
	PageData

	Servers []transferServerView
}

func transferSnippet() *SnippetData {
	return &SnippetData{
		ID:    "transfer-operations",
		Title: "Using Transfer",
		Cli: `# Create a Transfer Family server
aws transfer create-server \
  --protocols SFTP \
  --endpoint-url http://localhost:8000

# List servers
aws transfer list-servers \
  --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Transfer
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithBaseEndpoint("http://localhost:8000"),
)
if err != nil {
    log.Fatal(err)
}
client := transfer.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Transfer
import boto3

client = boto3.client('transfer', endpoint_url='http://localhost:8000')
response = client.create_server(Protocols=['SFTP'])`,
	}
}

// setupTransferRoutes registers the Transfer dashboard routes.
func (h *DashboardHandler) setupTransferRoutes() {
	h.SubRouter.GET("/dashboard/transfer", h.transferIndex)
	h.SubRouter.POST("/dashboard/transfer/create-server", h.transferCreateServer)
	h.SubRouter.POST("/dashboard/transfer/delete-server", h.transferDeleteServer)
}

// transferIndex renders the Transfer dashboard index.
func (h *DashboardHandler) transferIndex(c *echo.Context) error {
	w := c.Response()

	pageData := PageData{Title: "Transfer Servers", ActiveTab: "transfer", Snippet: transferSnippet()}

	if h.TransferOps == nil {
		h.renderTemplate(w, "transfer/index.html", transferIndexData{
			PageData: pageData,
			Servers:  []transferServerView{},
		})

		return nil
	}

	servers := h.TransferOps.Backend.ListServers()
	views := make([]transferServerView, 0, len(servers))

	for _, s := range servers {
		views = append(views, transferServerView{
			ServerID:  s.ServerID,
			State:     s.State,
			Protocols: s.Protocols,
		})
	}

	h.renderTemplate(w, "transfer/index.html", transferIndexData{
		PageData: pageData,
		Servers:  views,
	})

	return nil
}

// transferCreateServer handles POST /dashboard/transfer/create-server.
func (h *DashboardHandler) transferCreateServer(c *echo.Context) error {
	if h.TransferOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	protocol := c.Request().FormValue("protocol")
	if protocol == "" {
		protocol = "SFTP"
	}

	if _, err := h.TransferOps.Backend.CreateServer([]string{protocol}, nil); err != nil {
		h.Logger.Error("failed to create transfer server", "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/transfer")
}

// transferDeleteServer handles POST /dashboard/transfer/delete-server.
func (h *DashboardHandler) transferDeleteServer(c *echo.Context) error {
	if h.TransferOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	serverID := c.Request().FormValue("server_id")
	if serverID == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.TransferOps.Backend.DeleteServer(serverID); err != nil {
		h.Logger.Error("failed to delete transfer server", "server_id", serverID, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/transfer")
}
