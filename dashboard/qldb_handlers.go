package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// qldbLedgerView is the view model for a single QLDB ledger row.
type qldbLedgerView struct {
	Name              string
	ARN               string
	State             string
	PermissionsMode   string
	DeletionProtected bool
}

// qldbIndexData is the template data for the QLDB dashboard page.
type qldbIndexData struct {
	PageData

	Ledgers []qldbLedgerView
}

// qldbSnippet returns the shared SnippetData for the QLDB dashboard.
func qldbSnippet() *SnippetData {
	return &SnippetData{
		ID:    "qldb-operations",
		Title: "Using QLDB",
		Cli:   `aws qldb list-ledgers --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for QLDB
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
client := qldb.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for QLDB
import boto3

client = boto3.client('qldb', endpoint_url='http://localhost:8000')`,
	}
}

// setupQLDBRoutes registers all QLDB dashboard routes.
func (h *DashboardHandler) setupQLDBRoutes() {
	h.SubRouter.GET("/dashboard/qldb", h.qldbIndex)
	h.SubRouter.POST("/dashboard/qldb/create", h.qldbCreate)
	h.SubRouter.POST("/dashboard/qldb/delete", h.qldbDelete)
}

// qldbIndex renders the main QLDB dashboard page.
func (h *DashboardHandler) qldbIndex(c *echo.Context) error {
	w := c.Response()

	if h.QLDBOps == nil {
		h.renderTemplate(w, "qldb/index.html", qldbIndexData{
			PageData: PageData{
				Title:     "QLDB",
				ActiveTab: "qldb",
				Snippet:   qldbSnippet(),
			},
			Ledgers: []qldbLedgerView{},
		})

		return nil
	}

	list := h.QLDBOps.Backend.ListLedgers()
	views := make([]qldbLedgerView, 0, len(list))

	for _, l := range list {
		views = append(views, qldbLedgerView{
			Name:              l.Name,
			ARN:               l.ARN,
			State:             l.State,
			PermissionsMode:   l.PermissionsMode,
			DeletionProtected: l.DeletionProtected,
		})
	}

	h.renderTemplate(w, "qldb/index.html", qldbIndexData{
		PageData: PageData{
			Title:     "QLDB",
			ActiveTab: "qldb",
			Snippet:   qldbSnippet(),
		},
		Ledgers: views,
	})

	return nil
}

// qldbCreate handles POST /dashboard/qldb/create.
func (h *DashboardHandler) qldbCreate(c *echo.Context) error {
	if h.QLDBOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	permissionsMode := c.Request().FormValue("permissions_mode")

	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if permissionsMode == "" {
		permissionsMode = "ALLOW_ALL"
	}

	ctx := c.Request().Context()

	if _, err := h.QLDBOps.Backend.CreateLedger(name, permissionsMode, false, nil); err != nil {
		h.Logger.ErrorContext(ctx, "qldb: failed to create ledger", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/qldb")
}

// qldbDelete handles POST /dashboard/qldb/delete.
func (h *DashboardHandler) qldbDelete(c *echo.Context) error {
	if h.QLDBOps == nil {
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

	if err := h.QLDBOps.Backend.DeleteLedger(name); err != nil {
		h.Logger.ErrorContext(ctx, "qldb: failed to delete ledger", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/qldb")
}
