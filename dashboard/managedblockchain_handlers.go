package dashboard

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v5"
)

// managedblockchainNetworkView is the view model for a single network row.
type managedblockchainNetworkView struct {
	NetworkName string
	NetworkID   string
	Framework   string
	Status      string
	MemberCount int
}

// managedblockchainIndexData is the template data for the Managed Blockchain dashboard page.
type managedblockchainIndexData struct {
	PageData

	Networks []managedblockchainNetworkView
}

// managedblockchainSnippet returns the shared SnippetData for the Managed Blockchain dashboard.
func managedblockchainSnippet() *SnippetData {
	return &SnippetData{
		ID:    "managedblockchain-operations",
		Title: "Using Managed Blockchain",
		Cli:   `aws managedblockchain list-networks --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Managed Blockchain
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
client := managedblockchain.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Managed Blockchain
import boto3

client = boto3.client('managedblockchain', endpoint_url='http://localhost:8000')`,
	}
}

// setupManagedBlockchainRoutes registers all Managed Blockchain dashboard routes.
func (h *DashboardHandler) setupManagedBlockchainRoutes() {
	h.SubRouter.GET("/dashboard/managedblockchain", h.managedblockchainIndex)
	h.SubRouter.POST("/dashboard/managedblockchain/network/create", h.managedblockchainCreateNetwork)
}

// managedblockchainIndex renders the main Managed Blockchain dashboard page.
func (h *DashboardHandler) managedblockchainIndex(c *echo.Context) error {
	w := c.Response()

	if h.ManagedBlockchainOps == nil {
		h.renderTemplate(w, "managedblockchain/index.html", managedblockchainIndexData{
			PageData: PageData{
				Title:     "Managed Blockchain",
				ActiveTab: "managedblockchain",
				Snippet:   managedblockchainSnippet(),
			},
			Networks: []managedblockchainNetworkView{},
		})

		return nil
	}

	rawNetworks, _ := h.ManagedBlockchainOps.Backend.ListNetworks()
	views := make([]managedblockchainNetworkView, 0, len(rawNetworks))

	for _, n := range rawNetworks {
		members, _ := h.ManagedBlockchainOps.Backend.ListMembers(n.ID)
		views = append(views, managedblockchainNetworkView{
			NetworkName: n.Name,
			NetworkID:   n.ID,
			Framework:   n.Framework,
			Status:      n.Status,
			MemberCount: len(members),
		})
	}

	h.renderTemplate(w, "managedblockchain/index.html", managedblockchainIndexData{
		PageData: PageData{
			Title:     "Managed Blockchain Networks",
			ActiveTab: "managedblockchain",
			Snippet:   managedblockchainSnippet(),
		},
		Networks: views,
	})

	return nil
}

// managedblockchainCreateNetwork handles POST /dashboard/managedblockchain/network/create.
func (h *DashboardHandler) managedblockchainCreateNetwork(c *echo.Context) error {
	if h.ManagedBlockchainOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	memberName := c.Request().FormValue("member_name")

	if name == "" || memberName == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, _, err := h.ManagedBlockchainOps.Backend.CreateNetwork(
		h.GlobalConfig.Region,
		h.GlobalConfig.AccountID,
		name,
		"",
		"",
		"",
		memberName,
		"",
		nil,
	); err != nil {
		h.Logger.ErrorContext(context.Background(), "managedblockchain: failed to create network", "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/managedblockchain")
}
