package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// redshiftDataStatementView is the view model for a single Redshift Data statement row.
type redshiftDataStatementView struct {
	ID          string
	QueryString string
	Status      string
	Database    string
}

// redshiftDataIndexData is the template data for the Redshift Data dashboard page.
type redshiftDataIndexData struct {
	PageData

	Statements []redshiftDataStatementView
}

// redshiftDataSnippet returns the shared SnippetData for the Redshift Data dashboard.
func redshiftDataSnippet() *SnippetData {
	return &SnippetData{
		ID:    "redshiftdata-operations",
		Title: "Using Redshift Data",
		Cli: `aws redshift-data execute-statement --cluster-identifier my-cluster` +
			` --database dev --sql "SELECT 1" --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Redshift Data API
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
client := redshiftdata.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Redshift Data API
import boto3

client = boto3.client('redshift-data', endpoint_url='http://localhost:8000')`,
	}
}

// setupRedshiftDataRoutes registers all Redshift Data dashboard routes.
func (h *DashboardHandler) setupRedshiftDataRoutes() {
	h.SubRouter.GET("/dashboard/redshiftdata", h.redshiftDataIndex)
	h.SubRouter.POST("/dashboard/redshiftdata/execute", h.redshiftDataExecute)
	h.SubRouter.POST("/dashboard/redshiftdata/cancel", h.redshiftDataCancel)
}

// redshiftDataIndex renders the main Redshift Data dashboard page.
func (h *DashboardHandler) redshiftDataIndex(c *echo.Context) error {
	w := c.Response()

	if h.RedshiftDataOps == nil {
		h.renderTemplate(w, "redshiftdata/index.html", redshiftDataIndexData{
			PageData: PageData{
				Title:     "Redshift Data",
				ActiveTab: "redshiftdata",
				Snippet:   redshiftDataSnippet(),
			},
			Statements: []redshiftDataStatementView{},
		})

		return nil
	}

	list := h.RedshiftDataOps.Backend.ListStatements("", "")
	views := make([]redshiftDataStatementView, 0, len(list))

	for _, stmt := range list {
		views = append(views, redshiftDataStatementView{
			ID:          stmt.ID,
			QueryString: stmt.QueryString,
			Status:      stmt.Status,
			Database:    stmt.Database,
		})
	}

	h.renderTemplate(w, "redshiftdata/index.html", redshiftDataIndexData{
		PageData: PageData{
			Title:     "Redshift Data",
			ActiveTab: "redshiftdata",
			Snippet:   redshiftDataSnippet(),
		},
		Statements: views,
	})

	return nil
}

// redshiftDataExecute handles POST /dashboard/redshiftdata/execute.
func (h *DashboardHandler) redshiftDataExecute(c *echo.Context) error {
	if h.RedshiftDataOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	sql := c.Request().FormValue("sql")
	if sql == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	clusterID := c.Request().FormValue("cluster_identifier")
	database := c.Request().FormValue("database")

	ctx := c.Request().Context()

	if _, err := h.RedshiftDataOps.Backend.ExecuteStatement(sql, clusterID, "", database, "", "", ""); err != nil {
		h.Logger.ErrorContext(ctx, "redshiftdata: failed to execute statement", "sql", sql, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/redshiftdata")
}

// redshiftDataCancel handles POST /dashboard/redshiftdata/cancel.
func (h *DashboardHandler) redshiftDataCancel(c *echo.Context) error {
	if h.RedshiftDataOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	id := c.Request().FormValue("id")
	if id == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	ctx := c.Request().Context()

	if err := h.RedshiftDataOps.Backend.CancelStatement(id); err != nil {
		h.Logger.ErrorContext(ctx, "redshiftdata: failed to cancel statement", "id", id, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/redshiftdata")
}
