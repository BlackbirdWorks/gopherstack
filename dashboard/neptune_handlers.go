package dashboard

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v5"
)

// neptuneClusterView is the view model for a single Neptune cluster.
type neptuneClusterView struct {
	DBClusterIdentifier         string
	Engine                      string
	Status                      string
	DBClusterParameterGroupName string
	Endpoint                    string
	Port                        int
}

// neptuneInstanceView is the view model for a single Neptune instance.
type neptuneInstanceView struct {
	DBInstanceIdentifier string
	DBClusterIdentifier  string
	DBInstanceClass      string
	Engine               string
	DBInstanceStatus     string
	Endpoint             string
	Port                 int
}

// neptuneSubnetGroupView is the view model for a single Neptune subnet group.
type neptuneSubnetGroupView struct {
	DBSubnetGroupName        string
	DBSubnetGroupDescription string
	VpcID                    string
	Status                   string
}

// neptuneIndexData is the template data for the Neptune index page.
type neptuneIndexData struct {
	PageData

	Clusters     []neptuneClusterView
	Instances    []neptuneInstanceView
	SubnetGroups []neptuneSubnetGroupView
}

// neptuneSnippet returns the SnippetData for the Neptune dashboard page.
func neptuneSnippet() *SnippetData {
	return &SnippetData{
		ID:    "neptune-operations",
		Title: "Using Neptune",
		Cli:   `aws neptune help --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Neptune
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
client := neptune.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Neptune
import boto3

client = boto3.client('neptune', endpoint_url='http://localhost:8000')`,
	}
}

// setupNeptuneRoutes registers all Neptune dashboard routes.
func (h *DashboardHandler) setupNeptuneRoutes() {
	h.SubRouter.GET("/dashboard/neptune", h.neptuneIndex)
	h.SubRouter.POST("/dashboard/neptune/create", h.neptuneCreateCluster)
	h.SubRouter.POST("/dashboard/neptune/delete", h.neptuneDeleteCluster)
}

// neptuneIndex renders the Neptune dashboard page.
func (h *DashboardHandler) neptuneIndex(c *echo.Context) error {
	w := c.Response()

	if h.NeptuneOps == nil {
		h.renderTemplate(w, "neptune/index.html", neptuneIndexData{
			PageData:     PageData{Title: "Neptune Clusters", ActiveTab: "neptune", Snippet: neptuneSnippet()},
			Clusters:     []neptuneClusterView{},
			Instances:    []neptuneInstanceView{},
			SubnetGroups: []neptuneSubnetGroupView{},
		})

		return nil
	}

	clusters, err := h.NeptuneOps.Backend.DescribeDBClusters("")
	if err != nil {
		h.Logger.ErrorContext(context.Background(), "neptune: failed to describe clusters", "error", err)
		h.renderTemplate(c.Response(), "neptune/index.html", neptuneIndexData{
			PageData:     PageData{Title: "Neptune Clusters", ActiveTab: "neptune", Snippet: neptuneSnippet()},
			Clusters:     []neptuneClusterView{},
			Instances:    []neptuneInstanceView{},
			SubnetGroups: []neptuneSubnetGroupView{},
		})

		return nil
	}

	instances, err := h.NeptuneOps.Backend.DescribeDBInstances("")
	if err != nil {
		h.Logger.ErrorContext(context.Background(), "neptune: failed to describe instances", "error", err)
		h.renderTemplate(c.Response(), "neptune/index.html", neptuneIndexData{
			PageData:     PageData{Title: "Neptune Clusters", ActiveTab: "neptune", Snippet: neptuneSnippet()},
			Clusters:     []neptuneClusterView{},
			Instances:    []neptuneInstanceView{},
			SubnetGroups: []neptuneSubnetGroupView{},
		})

		return nil
	}

	subnetGroups, err := h.NeptuneOps.Backend.DescribeDBSubnetGroups("")
	if err != nil {
		h.Logger.ErrorContext(context.Background(), "neptune: failed to describe subnet groups", "error", err)
		h.renderTemplate(c.Response(), "neptune/index.html", neptuneIndexData{
			PageData:     PageData{Title: "Neptune Clusters", ActiveTab: "neptune", Snippet: neptuneSnippet()},
			Clusters:     []neptuneClusterView{},
			Instances:    []neptuneInstanceView{},
			SubnetGroups: []neptuneSubnetGroupView{},
		})

		return nil
	}

	clusterViews := make([]neptuneClusterView, 0, len(clusters))
	for _, cl := range clusters {
		clusterViews = append(clusterViews, neptuneClusterView{
			DBClusterIdentifier:         cl.DBClusterIdentifier,
			Engine:                      cl.Engine,
			Status:                      cl.Status,
			DBClusterParameterGroupName: cl.DBClusterParameterGroupName,
			Endpoint:                    cl.Endpoint,
			Port:                        cl.Port,
		})
	}

	instViews := make([]neptuneInstanceView, 0, len(instances))
	for _, inst := range instances {
		instViews = append(instViews, neptuneInstanceView{
			DBInstanceIdentifier: inst.DBInstanceIdentifier,
			DBClusterIdentifier:  inst.DBClusterIdentifier,
			DBInstanceClass:      inst.DBInstanceClass,
			Engine:               inst.Engine,
			DBInstanceStatus:     inst.DBInstanceStatus,
			Endpoint:             inst.Endpoint,
			Port:                 inst.Port,
		})
	}

	sgViews := make([]neptuneSubnetGroupView, 0, len(subnetGroups))
	for _, sg := range subnetGroups {
		sgViews = append(sgViews, neptuneSubnetGroupView{
			DBSubnetGroupName:        sg.DBSubnetGroupName,
			DBSubnetGroupDescription: sg.DBSubnetGroupDescription,
			VpcID:                    sg.VpcID,
			Status:                   sg.Status,
		})
	}

	h.renderTemplate(w, "neptune/index.html", neptuneIndexData{
		PageData:     PageData{Title: "Neptune Clusters", ActiveTab: "neptune", Snippet: neptuneSnippet()},
		Clusters:     clusterViews,
		Instances:    instViews,
		SubnetGroups: sgViews,
	})

	return nil
}

// neptuneCreateCluster handles POST /dashboard/neptune/create.
func (h *DashboardHandler) neptuneCreateCluster(c *echo.Context) error {
	if h.NeptuneOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	id := c.Request().FormValue("cluster_id")

	if id == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.NeptuneOps.Backend.CreateDBCluster(id, "", 0); err != nil {
		h.Logger.Error("failed to create Neptune cluster", "id", id, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/neptune")
}

// neptuneDeleteCluster handles POST /dashboard/neptune/delete.
func (h *DashboardHandler) neptuneDeleteCluster(c *echo.Context) error {
	if h.NeptuneOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	id := c.Request().FormValue("cluster_id")
	if id == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.NeptuneOps.Backend.DeleteDBCluster(id); err != nil {
		h.Logger.Error("failed to delete Neptune cluster", "id", id, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/neptune")
}
