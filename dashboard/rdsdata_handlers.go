package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// rdsdataStatementView is the view model for a single RDS Data executed statement row.
type rdsdataStatementView struct {
	SQL           string
	ResourceARN   string
	TransactionID string
}

// rdsdataIndexData is the template data for the RDS Data dashboard page.
type rdsdataIndexData struct {
	PageData

	Statements []rdsdataStatementView
}

// rdsdataSnippet returns the shared SnippetData for the RDS Data dashboard.
func rdsdataSnippet() *SnippetData {
	return &SnippetData{
		ID:    "rdsdata-operations",
		Title: "Using RDS Data",
		Cli: "aws rds-data execute-statement" +
			" --resource-arn arn:aws:rds:us-east-1:000000000000:cluster:my-cluster" +
			" --secret-arn arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret" +
			" --sql \"SELECT 1\" --endpoint-url http://localhost:8000",
		Go: `// Initialize AWS SDK v2 for RDS Data
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
client := rdsdata.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for RDS Data
import boto3

client = boto3.client('rds-data', endpoint_url='http://localhost:8000')`,
	}
}

// setupRDSDataRoutes registers all RDS Data dashboard routes.
func (h *DashboardHandler) setupRDSDataRoutes() {
	h.SubRouter.GET("/dashboard/rdsdata", h.rdsdataIndex)
	h.SubRouter.POST("/dashboard/rdsdata/execute", h.rdsdataExecute)
}

// rdsdataIndex renders the main RDS Data dashboard page.
func (h *DashboardHandler) rdsdataIndex(c *echo.Context) error {
	w := c.Response()

	if h.RDSDataOps == nil {
		h.renderTemplate(w, "rdsdata/index.html", rdsdataIndexData{
			PageData: PageData{
				Title:     "RDS Data",
				ActiveTab: "rdsdata",
				Snippet:   rdsdataSnippet(),
			},
			Statements: []rdsdataStatementView{},
		})

		return nil
	}

	list := h.RDSDataOps.Backend.ListExecutedStatements()
	views := make([]rdsdataStatementView, 0, len(list))

	for _, s := range list {
		views = append(views, rdsdataStatementView{
			SQL:           s.SQL,
			ResourceARN:   s.ResourceARN,
			TransactionID: s.TransactionID,
		})
	}

	h.renderTemplate(w, "rdsdata/index.html", rdsdataIndexData{
		PageData: PageData{
			Title:     "RDS Data",
			ActiveTab: "rdsdata",
			Snippet:   rdsdataSnippet(),
		},
		Statements: views,
	})

	return nil
}

// rdsdataExecute handles POST /dashboard/rdsdata/execute.
func (h *DashboardHandler) rdsdataExecute(c *echo.Context) error {
	if h.RDSDataOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	resourceARN := c.Request().FormValue("resource_arn")
	sql := c.Request().FormValue("sql")

	if resourceARN == "" || sql == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	ctx := c.Request().Context()

	if _, _, _, err := h.RDSDataOps.Backend.ExecuteStatement(resourceARN, sql, ""); err != nil {
		h.Logger.ErrorContext(ctx, "rdsdata: failed to execute statement", "sql", sql, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/rdsdata")
}
