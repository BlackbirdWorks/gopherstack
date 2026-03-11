package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// docdbClusterView is the view model for a single DocDB cluster.
type docdbClusterView struct {
	DBClusterIdentifier         string
	Engine                      string
	Status                      string
	MasterUsername              string
	DatabaseName                string
	DBClusterParameterGroupName string
	Endpoint                    string
	Port                        int
}

// docdbInstanceView is the view model for a single DocDB instance.
type docdbInstanceView struct {
	DBInstanceIdentifier string
	DBClusterIdentifier  string
	DBInstanceClass      string
	Engine               string
	DBInstanceStatus     string
	Endpoint             string
	Port                 int
}

// docdbSubnetGroupView is the view model for a single DocDB subnet group.
type docdbSubnetGroupView struct {
	DBSubnetGroupName        string
	DBSubnetGroupDescription string
	VpcID                    string
	Status                   string
}

// docdbIndexData is the template data for the DocDB index page.
type docdbIndexData struct {
	PageData

	Clusters     []docdbClusterView
	Instances    []docdbInstanceView
	SubnetGroups []docdbSubnetGroupView
}

// docdbSnippet returns the SnippetData for the DocDB dashboard page.
func docdbSnippet() *SnippetData {
	return &SnippetData{
		ID:    "docdb-operations",
		Title: "Using DocDB",
		Cli:   `aws docdb help --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for DocDB
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
client := docdb.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for DocDB
import boto3

client = boto3.client('docdb', endpoint_url='http://localhost:8000')`,
	}
}

// docdbIndex renders the DocDB dashboard page.
func (h *DashboardHandler) docdbIndex(c *echo.Context) error {
	w := c.Response()

	if h.DocDBOps == nil {
		h.renderTemplate(w, "docdb/index.html", docdbIndexData{
			PageData:     PageData{Title: "DocDB Clusters", ActiveTab: "docdb", Snippet: docdbSnippet()},
			Clusters:     []docdbClusterView{},
			Instances:    []docdbInstanceView{},
			SubnetGroups: []docdbSubnetGroupView{},
		})

		return nil
	}

	clusters, err := h.DocDBOps.Backend.DescribeDBClusters("")
	if err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	instances, err := h.DocDBOps.Backend.DescribeDBInstances("")
	if err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	subnetGroups, err := h.DocDBOps.Backend.DescribeDBSubnetGroups("")
	if err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	clusterViews := make([]docdbClusterView, 0, len(clusters))
	for _, c := range clusters {
		clusterViews = append(clusterViews, docdbClusterView{
			DBClusterIdentifier:         c.DBClusterIdentifier,
			Engine:                      c.Engine,
			Status:                      c.Status,
			MasterUsername:              c.MasterUsername,
			DatabaseName:                c.DatabaseName,
			DBClusterParameterGroupName: c.DBClusterParameterGroupName,
			Endpoint:                    c.Endpoint,
			Port:                        c.Port,
		})
	}

	instViews := make([]docdbInstanceView, 0, len(instances))
	for _, inst := range instances {
		instViews = append(instViews, docdbInstanceView{
			DBInstanceIdentifier: inst.DBInstanceIdentifier,
			DBClusterIdentifier:  inst.DBClusterIdentifier,
			DBInstanceClass:      inst.DBInstanceClass,
			Engine:               inst.Engine,
			DBInstanceStatus:     inst.DBInstanceStatus,
			Endpoint:             inst.Endpoint,
			Port:                 inst.Port,
		})
	}

	sgViews := make([]docdbSubnetGroupView, 0, len(subnetGroups))
	for _, sg := range subnetGroups {
		sgViews = append(sgViews, docdbSubnetGroupView{
			DBSubnetGroupName:        sg.DBSubnetGroupName,
			DBSubnetGroupDescription: sg.DBSubnetGroupDescription,
			VpcID:                    sg.VpcID,
			Status:                   sg.Status,
		})
	}

	h.renderTemplate(w, "docdb/index.html", docdbIndexData{
		PageData:     PageData{Title: "DocDB Clusters", ActiveTab: "docdb", Snippet: docdbSnippet()},
		Clusters:     clusterViews,
		Instances:    instViews,
		SubnetGroups: sgViews,
	})

	return nil
}

// docdbCreateCluster handles POST /dashboard/docdb/create.
func (h *DashboardHandler) docdbCreateCluster(c *echo.Context) error {
	if h.DocDBOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	id := c.Request().FormValue("cluster_id")
	engine := c.Request().FormValue("engine")
	masterUser := c.Request().FormValue("master_username")

	if id == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.DocDBOps.Backend.CreateDBCluster(id, engine, masterUser, "", "", 0); err != nil {
		h.Logger.Error("failed to create DocDB cluster", "id", id, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/docdb")
}

// docdbDeleteCluster handles POST /dashboard/docdb/delete.
func (h *DashboardHandler) docdbDeleteCluster(c *echo.Context) error {
	if h.DocDBOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	id := c.Request().FormValue("cluster_id")
	if id == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.DocDBOps.Backend.DeleteDBCluster(id); err != nil {
		h.Logger.Error("failed to delete DocDB cluster", "id", id, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/docdb")
}
