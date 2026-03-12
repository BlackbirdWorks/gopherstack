package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	memorydbbackend "github.com/blackbirdworks/gopherstack/services/memorydb"
)

// memorydbClusterView is the view model for a single cluster row.
type memorydbClusterView struct {
	Name          string
	ARN           string
	Status        string
	NodeType      string
	EngineVersion string
	ACLName       string
}

// memorydbIndexData is the template data for the MemoryDB dashboard page.
type memorydbIndexData struct {
	PageData

	Clusters []memorydbClusterView
}

// memorydbSnippet returns the shared SnippetData for the MemoryDB dashboard.
func memorydbSnippet() *SnippetData {
	return &SnippetData{
		ID:    "memorydb-operations",
		Title: "Using MemoryDB",
		Cli:   `aws memorydb describe-clusters --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for MemoryDB
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
client := memorydb.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for MemoryDB
import boto3

client = boto3.client('memorydb', endpoint_url='http://localhost:8000')`,
	}
}

// setupMemoryDBRoutes registers all MemoryDB dashboard routes.
func (h *DashboardHandler) setupMemoryDBRoutes() {
	h.SubRouter.GET("/dashboard/memorydb", h.memorydbIndex)
	h.SubRouter.POST("/dashboard/memorydb/cluster/create", h.memorydbCreateCluster)
	h.SubRouter.POST("/dashboard/memorydb/cluster/delete", h.memorydbDeleteCluster)
}

// memorydbIndex renders the main MemoryDB dashboard page.
func (h *DashboardHandler) memorydbIndex(c *echo.Context) error {
	w := c.Response()

	if h.MemoryDBOps == nil {
		h.renderTemplate(w, "memorydb/index.html", memorydbIndexData{
			PageData: PageData{
				Title:     "MemoryDB",
				ActiveTab: "memorydb",
				Snippet:   memorydbSnippet(),
			},
			Clusters: []memorydbClusterView{},
		})

		return nil
	}

	ctx := c.Request().Context()

	rawClusters, err := h.MemoryDBOps.Backend.DescribeClusters("")
	if err != nil {
		h.Logger.ErrorContext(ctx, "memorydb: failed to list clusters", "error", err)

		rawClusters = nil
	}

	views := make([]memorydbClusterView, 0, len(rawClusters))

	for _, cl := range rawClusters {
		views = append(views, memorydbClusterView{
			Name:          cl.Name,
			ARN:           cl.ARN,
			Status:        cl.Status,
			NodeType:      cl.NodeType,
			EngineVersion: cl.EngineVersion,
			ACLName:       cl.ACLName,
		})
	}

	h.renderTemplate(w, "memorydb/index.html", memorydbIndexData{
		PageData: PageData{
			Title:     "MemoryDB Clusters",
			ActiveTab: "memorydb",
			Snippet:   memorydbSnippet(),
		},
		Clusters: views,
	})

	return nil
}

// memorydbCreateCluster handles POST /dashboard/memorydb/cluster/create.
func (h *DashboardHandler) memorydbCreateCluster(c *echo.Context) error {
	if h.MemoryDBOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	nodeType := c.Request().FormValue("node_type")
	if nodeType == "" {
		nodeType = "db.r6g.large"
	}

	ctx := c.Request().Context()

	req := &memorydbbackend.ExportedCreateClusterRequest{
		ClusterName: name,
		NodeType:    nodeType,
		ACLName:     "open-access",
	}

	if _, err := h.MemoryDBOps.Backend.CreateCluster(
		h.GlobalConfig.Region,
		h.GlobalConfig.AccountID,
		req,
	); err != nil {
		h.Logger.ErrorContext(ctx, "memorydb: failed to create cluster", "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/memorydb")
}

// memorydbDeleteCluster handles POST /dashboard/memorydb/cluster/delete.
func (h *DashboardHandler) memorydbDeleteCluster(c *echo.Context) error {
	if h.MemoryDBOps == nil {
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

	if _, err := h.MemoryDBOps.Backend.DeleteCluster(name); err != nil {
		h.Logger.ErrorContext(ctx, "memorydb: failed to delete cluster", "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/memorydb")
}
