package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// dmsInstanceView is the view model for a single DMS replication instance.
type dmsInstanceView struct {
	Identifier string
	Class      string
	Status     string
	ARN        string
}

// dmsIndexData is the template data for the DMS index page.
type dmsIndexData struct {
	PageData

	Instances []dmsInstanceView
}

// dmsSnippet returns the shared SnippetData for the DMS dashboard pages.
func dmsSnippet() *SnippetData {
	return &SnippetData{
		ID:    "dms-operations",
		Title: "Using DMS",
		Cli:   `aws dms help --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for DMS
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
client := databasemigrationservice.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for DMS
import boto3

client = boto3.client('dms', endpoint_url='http://localhost:8000')`,
	}
}

// dmsIndex renders the DMS dashboard index.
func (h *DashboardHandler) dmsIndex(c *echo.Context) error {
	w := c.Response()

	if h.DMSOps == nil {
		h.renderTemplate(w, "dms/index.html", dmsIndexData{
			PageData:  PageData{Title: "Database Migration Service", ActiveTab: "dms", Snippet: dmsSnippet()},
			Instances: []dmsInstanceView{},
		})

		return nil
	}

	instances, _ := h.DMSOps.Backend.DescribeReplicationInstances("")
	views := make([]dmsInstanceView, 0, len(instances))

	for _, ri := range instances {
		views = append(views, dmsInstanceView{
			Identifier: ri.ReplicationInstanceIdentifier,
			Class:      ri.ReplicationInstanceClass,
			Status:     ri.ReplicationInstanceStatus,
			ARN:        ri.ReplicationInstanceArn,
		})
	}

	h.renderTemplate(w, "dms/index.html", dmsIndexData{
		PageData:  PageData{Title: "Database Migration Service", ActiveTab: "dms", Snippet: dmsSnippet()},
		Instances: views,
	})

	return nil
}

// dmsCreateInstance handles POST /dashboard/dms/instance/create.
func (h *DashboardHandler) dmsCreateInstance(c *echo.Context) error {
	if h.DMSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	identifier := c.Request().FormValue("identifier")
	class := c.Request().FormValue("class")

	if identifier == "" || class == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.DMSOps.Backend.CreateReplicationInstance(
		identifier, class, "", "", 0, false, true, false, nil,
	)
	if err != nil {
		h.Logger.Error("failed to create DMS replication instance", "identifier", identifier, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/dms")
}

// dmsDeleteInstance handles POST /dashboard/dms/instance/delete.
func (h *DashboardHandler) dmsDeleteInstance(c *echo.Context) error {
	if h.DMSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	identifier := c.Request().FormValue("identifier")
	if identifier == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.DMSOps.Backend.DeleteReplicationInstance(identifier); err != nil {
		h.Logger.Error("failed to delete DMS replication instance", "identifier", identifier, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/dms")
}

// setupDMSRoutes registers routes for the DMS dashboard.
func (h *DashboardHandler) setupDMSRoutes() {
	h.SubRouter.GET("/dashboard/dms", h.dmsIndex)
	h.SubRouter.POST("/dashboard/dms/instance/create", h.dmsCreateInstance)
	h.SubRouter.POST("/dashboard/dms/instance/delete", h.dmsDeleteInstance)
}
