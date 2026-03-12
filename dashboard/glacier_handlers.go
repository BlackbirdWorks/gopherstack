package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// glacierVaultView is the view model for a single Glacier vault row.
type glacierVaultView struct {
	VaultName        string
	VaultARN         string
	CreationDate     string
	NumberOfArchives int64
	SizeInBytes      int64
}

// glacierIndexData is the template data for the Glacier dashboard page.
type glacierIndexData struct {
	PageData

	Vaults []glacierVaultView
}

// glacierSnippet returns the shared SnippetData for the Glacier dashboard.
func glacierSnippet() *SnippetData {
	return &SnippetData{
		ID:    "glacier-operations",
		Title: "Using Glacier",
		Cli:   `aws glacier help --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Glacier
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
client := glacier.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Glacier
import boto3

client = boto3.client('glacier', endpoint_url='http://localhost:8000')`,
	}
}

// setupGlacierRoutes registers all Glacier dashboard routes.
func (h *DashboardHandler) setupGlacierRoutes() {
	h.SubRouter.GET("/dashboard/glacier", h.glacierIndex)
	h.SubRouter.POST("/dashboard/glacier/vault/create", h.glacierCreateVault)
	h.SubRouter.POST("/dashboard/glacier/vault/delete", h.glacierDeleteVault)
}

// glacierIndex renders the main Glacier dashboard page.
func (h *DashboardHandler) glacierIndex(c *echo.Context) error {
	w := c.Response()

	if h.GlacierOps == nil {
		h.renderTemplate(w, "glacier/index.html", glacierIndexData{
			PageData: PageData{Title: "Glacier", ActiveTab: "glacier", Snippet: glacierSnippet()},
			Vaults:   []glacierVaultView{},
		})

		return nil
	}

	rawVaults := h.GlacierOps.Backend.ListVaults(h.GlobalConfig.AccountID, h.GlobalConfig.Region)
	views := make([]glacierVaultView, 0, len(rawVaults))

	for _, v := range rawVaults {
		views = append(views, glacierVaultView{
			VaultName:        v.VaultName,
			VaultARN:         v.VaultARN,
			CreationDate:     v.CreationDate,
			NumberOfArchives: v.NumberOfArchives,
			SizeInBytes:      v.SizeInBytes,
		})
	}

	h.renderTemplate(w, "glacier/index.html", glacierIndexData{
		PageData: PageData{Title: "Glacier Vaults", ActiveTab: "glacier", Snippet: glacierSnippet()},
		Vaults:   views,
	})

	return nil
}

// glacierCreateVault handles POST /dashboard/glacier/vault/create.
func (h *DashboardHandler) glacierCreateVault(c *echo.Context) error {
	if h.GlacierOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.GlacierOps.Backend.CreateVault(h.GlobalConfig.AccountID, h.GlobalConfig.Region, name)
	if err != nil {
		h.Logger.Error("failed to create glacier vault", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/glacier")
}

// glacierDeleteVault handles POST /dashboard/glacier/vault/delete.
func (h *DashboardHandler) glacierDeleteVault(c *echo.Context) error {
	if h.GlacierOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.GlacierOps.Backend.DeleteVault(h.GlobalConfig.AccountID, h.GlobalConfig.Region, name); err != nil {
		h.Logger.Error("failed to delete glacier vault", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/glacier")
}
