package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// redshiftClusterView is the view model for a single Redshift cluster.
type redshiftClusterView struct {
	ClusterIdentifier string
	NodeType          string
	Endpoint          string
	Status            string
	DBName            string
}

// redshiftIndexData is the template data for the Redshift index page.
type redshiftIndexData struct {
	PageData

	Clusters []redshiftClusterView
}

// redshiftIndex renders the list of all Redshift clusters.
func (h *DashboardHandler) redshiftIndex(c *echo.Context) error {
	w := c.Response()

	if h.RedshiftOps == nil {
		h.renderTemplate(w, "redshift/index.html", redshiftIndexData{
			PageData: PageData{Title: "Redshift Clusters", ActiveTab: "redshift",
				Snippet: &SnippetData{
					ID:    "redshift-operations",
					Title: "Using Redshift",
					Cli:   `aws redshift help --endpoint-url http://localhost:8000`,
					Go: `// Initialize AWS SDK v2 for Using Redshift
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
client := redshift.NewFromConfig(cfg)`,
					Python: `# Initialize boto3 client for Using Redshift
import boto3

client = boto3.client('redshift', endpoint_url='http://localhost:8000')`,
				}},
			Clusters: []redshiftClusterView{},
		})

		return nil
	}

	clusters, err := h.RedshiftOps.Backend.DescribeClusters("")
	if err != nil {
		return c.NoContent(http.StatusInternalServerError)
	}

	views := make([]redshiftClusterView, 0, len(clusters))

	for _, cluster := range clusters {
		views = append(views, redshiftClusterView{
			ClusterIdentifier: cluster.ClusterIdentifier,
			NodeType:          cluster.NodeType,
			Endpoint:          cluster.Endpoint,
			Status:            cluster.Status,
			DBName:            cluster.DBName,
		})
	}

	h.renderTemplate(w, "redshift/index.html", redshiftIndexData{
		PageData: PageData{Title: "Redshift Clusters", ActiveTab: "redshift",
			Snippet: &SnippetData{
				ID:    "redshift-operations",
				Title: "Using Redshift",
				Cli:   `aws redshift help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Redshift
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
client := redshift.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Redshift
import boto3

client = boto3.client('redshift', endpoint_url='http://localhost:8000')`,
			}},
		Clusters: views,
	})

	return nil
}

// redshiftCreateCluster handles POST /dashboard/redshift/create.
func (h *DashboardHandler) redshiftCreateCluster(c *echo.Context) error {
	if h.RedshiftOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	clusterID := c.Request().FormValue("cluster_id")
	nodeType := c.Request().FormValue("node_type")
	dbName := c.Request().FormValue("db_name")
	masterUsername := c.Request().FormValue("master_username")

	if clusterID == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.RedshiftOps.Backend.CreateCluster(clusterID, nodeType, dbName, masterUsername); err != nil {
		h.Logger.Error("failed to create Redshift cluster", "id", clusterID, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/redshift")
}

// redshiftDeleteCluster handles POST /dashboard/redshift/delete.
func (h *DashboardHandler) redshiftDeleteCluster(c *echo.Context) error {
	if h.RedshiftOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	clusterID := c.Request().FormValue("cluster_id")
	if clusterID == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.RedshiftOps.Backend.DeleteCluster(clusterID); err != nil {
		h.Logger.Error("failed to delete Redshift cluster", "id", clusterID, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/redshift")
}
