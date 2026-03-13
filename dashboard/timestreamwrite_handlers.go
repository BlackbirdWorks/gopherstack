package dashboard

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

// timestreamwriteDatabaseView is the view model for a Timestream Write database.
type timestreamwriteDatabaseView struct {
	Name       string
	TableCount string
}

// timestreamwriteIndexData is the template data for the Timestream Write index page.
type timestreamwriteIndexData struct {
	PageData

	Databases []timestreamwriteDatabaseView
}

func timestreamwriteSnippet() *SnippetData {
	return &SnippetData{
		ID:    "timestreamwrite-operations",
		Title: "Using Timestream Write",
		Cli: `aws timestream-write create-database \
  --database-name my-db \
  --endpoint-url http://localhost:8000

aws timestream-write create-table \
  --database-name my-db \
  --table-name my-table \
  --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Timestream Write
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithBaseEndpoint("http://localhost:8000"),
)
if err != nil {
    log.Fatal(err)
}
client := timestreamwrite.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Timestream Write
import boto3

client = boto3.client(
    'timestream-write',
    endpoint_url='http://localhost:8000',
)
client.create_database(DatabaseName='my-db')`,
	}
}

// setupTimestreamWriteRoutes registers the Timestream Write dashboard routes.
func (h *DashboardHandler) setupTimestreamWriteRoutes() {
	h.SubRouter.GET("/dashboard/timestreamwrite", h.timestreamwriteIndex)
	h.SubRouter.POST("/dashboard/timestreamwrite/create-database", h.timestreamwriteCreateDatabase)
	h.SubRouter.POST("/dashboard/timestreamwrite/delete-database", h.timestreamwriteDeleteDatabase)
}

// timestreamwriteIndex renders the Timestream Write dashboard index.
func (h *DashboardHandler) timestreamwriteIndex(c *echo.Context) error {
	w := c.Response()

	pageData := PageData{
		Title:     "Timestream Write",
		ActiveTab: "timestreamwrite",
		Snippet:   timestreamwriteSnippet(),
	}

	if h.TimestreamWriteOps == nil {
		h.renderTemplate(w, "timestreamwrite/index.html", timestreamwriteIndexData{
			PageData:  pageData,
			Databases: []timestreamwriteDatabaseView{},
		})

		return nil
	}

	dbs := h.TimestreamWriteOps.Backend.ListDatabases()
	views := make([]timestreamwriteDatabaseView, 0, len(dbs))

	for _, db := range dbs {
		views = append(views, timestreamwriteDatabaseView{
			Name:       db.DatabaseName,
			TableCount: strconv.Itoa(db.TableCount),
		})
	}

	h.renderTemplate(w, "timestreamwrite/index.html", timestreamwriteIndexData{
		PageData:  pageData,
		Databases: views,
	})

	return nil
}

// timestreamwriteCreateDatabase handles POST /dashboard/timestreamwrite/create-database.
func (h *DashboardHandler) timestreamwriteCreateDatabase(c *echo.Context) error {
	if h.TimestreamWriteOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.TimestreamWriteOps.Backend.CreateDatabase(name); err != nil {
		h.Logger.Error("failed to create timestream database", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/timestreamwrite")
}

// timestreamwriteDeleteDatabase handles POST /dashboard/timestreamwrite/delete-database.
func (h *DashboardHandler) timestreamwriteDeleteDatabase(c *echo.Context) error {
	if h.TimestreamWriteOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.TimestreamWriteOps.Backend.DeleteDatabase(name); err != nil {
		h.Logger.Error("failed to delete timestream database", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/timestreamwrite")
}
