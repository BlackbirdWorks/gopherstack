package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// rdsInstanceView is the view model for a single RDS DB instance.
type rdsInstanceView struct {
	DBInstanceIdentifier string
	Engine               string
	DBInstanceStatus     string
	Endpoint             string
	DBInstanceClass      string
	MasterUsername       string
	DBName               string
	Port                 int
	AllocatedStorage     int
}

// rdsSnapshotView is the view model for a single RDS snapshot.
type rdsSnapshotView struct {
	DBSnapshotIdentifier string
	DBInstanceIdentifier string
	Engine               string
	Status               string
}

// rdsSubnetGroupView is the view model for a single RDS subnet group.
type rdsSubnetGroupView struct {
	DBSubnetGroupName        string
	DBSubnetGroupDescription string
	VpcID                    string
	Status                   string
}

// rdsIndexData is the template data for the RDS index page.
type rdsIndexData struct {
	PageData

	Instances    []rdsInstanceView
	Snapshots    []rdsSnapshotView
	SubnetGroups []rdsSubnetGroupView
}

// rdsInstanceDetailData is the template data for the RDS instance detail page.
type rdsInstanceDetailData struct {
	PageData

	Instance rdsInstanceView
}

// rdsIndex renders the list of all RDS instances, snapshots, and subnet groups.
func (h *DashboardHandler) rdsIndex(c *echo.Context) error {
	w := c.Response()

	if h.RDSOps == nil {
		h.renderTemplate(w, "rds/index.html", rdsIndexData{
			PageData:     PageData{Title: "RDS Instances", ActiveTab: "rds",
		Snippet: &SnippetData{
			ID:    "rds-operations",
			Title: "Using Rds",
			Cli:   "aws rds help --endpoint-url http://localhost:8000",
			Go: "/* Write AWS SDK v2 Code for Rds */",
			Python: "# Write boto3 code for Rds\nimport boto3\nclient = boto3.client('rds', endpoint_url='http://localhost:8000')",
		},},
			Instances:    []rdsInstanceView{},
			Snapshots:    []rdsSnapshotView{},
			SubnetGroups: []rdsSubnetGroupView{},
		})

		return nil
	}

	instances, err := h.RDSOps.Backend.DescribeDBInstances("")
	if err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	snapshots, err := h.RDSOps.Backend.DescribeDBSnapshots("")
	if err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	subnetGroups, err := h.RDSOps.Backend.DescribeDBSubnetGroups("")
	if err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	instViews := make([]rdsInstanceView, 0, len(instances))
	for _, inst := range instances {
		instViews = append(instViews, rdsInstanceView{
			DBInstanceIdentifier: inst.DBInstanceIdentifier,
			Engine:               inst.Engine,
			DBInstanceStatus:     inst.DBInstanceStatus,
			Endpoint:             inst.Endpoint,
			Port:                 inst.Port,
			DBInstanceClass:      inst.DBInstanceClass,
			MasterUsername:       inst.MasterUsername,
			DBName:               inst.DBName,
			AllocatedStorage:     inst.AllocatedStorage,
		})
	}

	snapViews := make([]rdsSnapshotView, 0, len(snapshots))
	for _, snap := range snapshots {
		snapViews = append(snapViews, rdsSnapshotView{
			DBSnapshotIdentifier: snap.DBSnapshotIdentifier,
			DBInstanceIdentifier: snap.DBInstanceIdentifier,
			Engine:               snap.Engine,
			Status:               snap.Status,
		})
	}

	sgViews := make([]rdsSubnetGroupView, 0, len(subnetGroups))
	for _, sg := range subnetGroups {
		sgViews = append(sgViews, rdsSubnetGroupView{
			DBSubnetGroupName:        sg.DBSubnetGroupName,
			DBSubnetGroupDescription: sg.DBSubnetGroupDescription,
			VpcID:                    sg.VpcID,
			Status:                   sg.Status,
		})
	}

	h.renderTemplate(w, "rds/index.html", rdsIndexData{
		PageData:     PageData{Title: "RDS Instances", ActiveTab: "rds",
		Snippet: &SnippetData{
			ID:    "rds-operations",
			Title: "Using Rds",
			Cli:   "aws rds help --endpoint-url http://localhost:8000",
			Go: "/* Write AWS SDK v2 Code for Rds */",
			Python: `# Write boto3 code for Rds
import boto3
client = boto3.client('rds', endpoint_url='http://localhost:8000')`,
		},},
		Instances:    instViews,
		Snapshots:    snapViews,
		SubnetGroups: sgViews,
	})

	return nil
}

// rdsInstanceDetail renders the detail page for a single RDS instance.
func (h *DashboardHandler) rdsInstanceDetail(c *echo.Context) error {
	w := c.Response()

	if h.RDSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	id := c.QueryParam("id")
	if id == "" {
		return c.Redirect(http.StatusFound, "/dashboard/rds")
	}

	instances, err := h.RDSOps.Backend.DescribeDBInstances(id)
	if err != nil || len(instances) == 0 {
		return c.NoContent(http.StatusNotFound)
	}

	inst := instances[0]
	h.renderTemplate(w, "rds/instance_detail.html", rdsInstanceDetailData{
		PageData: PageData{Title: "RDS Instance: " + id, ActiveTab: "rds",
		Snippet: &SnippetData{
			ID:    "rds-operations",
			Title: "Using Rds",
			Cli:   "aws rds help --endpoint-url http://localhost:8000",
			Go: "/* Write AWS SDK v2 Code for Rds */",
			Python: `# Write boto3 code for Rds
import boto3
client = boto3.client('rds', endpoint_url='http://localhost:8000')`,
		},},
		Instance: rdsInstanceView{
			DBInstanceIdentifier: inst.DBInstanceIdentifier,
			Engine:               inst.Engine,
			DBInstanceStatus:     inst.DBInstanceStatus,
			Endpoint:             inst.Endpoint,
			Port:                 inst.Port,
			DBInstanceClass:      inst.DBInstanceClass,
			MasterUsername:       inst.MasterUsername,
			DBName:               inst.DBName,
			AllocatedStorage:     inst.AllocatedStorage,
		},
	})

	return nil
}

// rdsCreateInstance handles POST /dashboard/rds/create.
func (h *DashboardHandler) rdsCreateInstance(c *echo.Context) error {
	if h.RDSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	id := c.Request().FormValue("db_instance_id")
	engine := c.Request().FormValue("engine")
	instanceClass := c.Request().FormValue("instance_class")
	dbName := c.Request().FormValue("db_name")
	masterUser := c.Request().FormValue("master_username")

	if id == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.RDSOps.Backend.CreateDBInstance(id, engine, instanceClass, dbName, masterUser, 0); err != nil {
		h.Logger.Error("failed to create RDS instance", "id", id, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/rds")
}

// rdsDeleteInstance handles POST /dashboard/rds/delete.
func (h *DashboardHandler) rdsDeleteInstance(c *echo.Context) error {
	if h.RDSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	id := c.Request().FormValue("db_instance_id")
	if id == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.RDSOps.Backend.DeleteDBInstance(id); err != nil {
		h.Logger.Error("failed to delete RDS instance", "id", id, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/rds")
}
