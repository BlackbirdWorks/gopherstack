package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	elasticachebackend "github.com/blackbirdworks/gopherstack/elasticache"
)

// elastiCacheClusterView is the view model for a single cluster in the index listing.
type elastiCacheClusterView struct {
	ClusterID string
	Engine    string
	Status    string
	Endpoint  string
	NodeType  string
	ARN       string
	Port      int
}

// elastiCacheIndexData is the template data for the ElastiCache index page.
type elastiCacheIndexData struct {
	PageData

	Clusters []elastiCacheClusterView
}

// elastiCacheClusterDetailData is the template data for the ElastiCache cluster detail page.
type elastiCacheClusterDetailData struct {
	PageData

	ClusterID     string
	Engine        string
	EngineVersion string
	Status        string
	Endpoint      string
	NodeType      string
	ARN           string
	Port          int
	NumCacheNodes int
}

// elastiCacheIndex renders the list of all ElastiCache clusters.
func (h *DashboardHandler) elastiCacheIndex(c *echo.Context) error {
	w := c.Response()

	if h.ElastiCacheOps == nil {
		h.renderTemplate(w, "elasticache/index.html", elastiCacheIndexData{
			PageData: PageData{Title: "ElastiCache Clusters", ActiveTab: "elasticache",
				Snippet: &SnippetData{
					ID:    "elasticache-operations",
					Title: "Using Elasticache",
					Cli:   "aws elasticache help --endpoint-url http://localhost:8000",
					Go:    "/* Write AWS SDK v2 Code for Elasticache */",
					Python: "# Write boto3 code for ElastiCache\nimport boto3\n" +
						"client = boto3.client('elasticache', endpoint_url='http://localhost:8000')",
				}},
			Clusters: []elastiCacheClusterView{},
		})

		return nil
	}

	bk, ok := h.ElastiCacheOps.Backend.(*elasticachebackend.InMemoryBackend)
	if !ok {
		h.renderTemplate(w, "elasticache/index.html", elastiCacheIndexData{
			PageData: PageData{Title: "ElastiCache Clusters", ActiveTab: "elasticache",
				Snippet: &SnippetData{
					ID:    "elasticache-operations",
					Title: "Using Elasticache",
					Cli:   "aws elasticache help --endpoint-url http://localhost:8000",
					Go:    "/* Write AWS SDK v2 Code for Elasticache */",
					Python: `# Write boto3 code for Elasticache
import boto3
client = boto3.client('elasticache', endpoint_url='http://localhost:8000')`,
				}},
			Clusters: []elastiCacheClusterView{},
		})

		return nil
	}

	all := bk.ListAll()
	views := make([]elastiCacheClusterView, 0, len(all))

	for _, cl := range all {
		views = append(views, elastiCacheClusterView{
			ClusterID: cl.ClusterID,
			Engine:    cl.Engine,
			Status:    cl.Status,
			Endpoint:  cl.Endpoint,
			Port:      cl.Port,
			NodeType:  cl.NodeType,
			ARN:       cl.ARN,
		})
	}

	h.renderTemplate(w, "elasticache/index.html", elastiCacheIndexData{
		PageData: PageData{Title: "ElastiCache Clusters", ActiveTab: "elasticache",
			Snippet: &SnippetData{
				ID:    "elasticache-operations",
				Title: "Using Elasticache",
				Cli:   "aws elasticache help --endpoint-url http://localhost:8000",
				Go:    "/* Write AWS SDK v2 Code for Elasticache */",
				Python: `# Write boto3 code for Elasticache
import boto3
client = boto3.client('elasticache', endpoint_url='http://localhost:8000')`,
			}},
		Clusters: views,
	})

	return nil
}

// elastiCacheClusterDetail renders the detail page for a single ElastiCache cluster.
func (h *DashboardHandler) elastiCacheClusterDetail(c *echo.Context) error {
	w := c.Response()

	if h.ElastiCacheOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	id := c.Request().URL.Query().Get("id")

	clusters, err := h.ElastiCacheOps.Backend.DescribeClusters(id)
	if err != nil || len(clusters) == 0 {
		return c.NoContent(http.StatusNotFound)
	}

	cl := clusters[0]

	h.renderTemplate(w, "elasticache/cluster_detail.html", elastiCacheClusterDetailData{
		PageData: PageData{Title: "Cluster: " + cl.ClusterID, ActiveTab: "elasticache",
			Snippet: &SnippetData{
				ID:    "elasticache-operations",
				Title: "Using Elasticache",
				Cli:   "aws elasticache help --endpoint-url http://localhost:8000",
				Go:    "/* Write AWS SDK v2 Code for Elasticache */",
				Python: `# Write boto3 code for Elasticache
import boto3
client = boto3.client('elasticache', endpoint_url='http://localhost:8000')`,
			}},
		ClusterID:     cl.ClusterID,
		Engine:        cl.Engine,
		EngineVersion: cl.EngineVersion,
		Status:        cl.Status,
		Endpoint:      cl.Endpoint,
		Port:          cl.Port,
		NodeType:      cl.NodeType,
		NumCacheNodes: cl.NumCacheNodes,
		ARN:           cl.ARN,
	})

	return nil
}

// elastiCacheCreateCluster handles a POST to create a new cache cluster.
func (h *DashboardHandler) elastiCacheCreateCluster(c *echo.Context) error {
	if h.ElastiCacheOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.String(http.StatusBadRequest, "cannot parse form")
	}

	id := c.Request().Form.Get("cluster_id")
	engine := c.Request().Form.Get("engine")
	nodeType := c.Request().Form.Get("node_type")

	if engine == "" {
		engine = "redis"
	}
	if nodeType == "" {
		nodeType = "cache.t3.micro"
	}

	if _, err := h.ElastiCacheOps.Backend.CreateCluster(id, engine, nodeType, 0); err != nil {
		return c.String(http.StatusBadRequest, err.Error())
	}

	return c.Redirect(http.StatusFound, "/dashboard/elasticache")
}

// elastiCacheDeleteCluster handles a DELETE to remove a cache cluster.
func (h *DashboardHandler) elastiCacheDeleteCluster(c *echo.Context) error {
	if h.ElastiCacheOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	id := c.Request().URL.Query().Get("id")
	if err := h.ElastiCacheOps.Backend.DeleteCluster(id); err != nil {
		return c.String(http.StatusBadRequest, err.Error())
	}

	return c.Redirect(http.StatusFound, "/dashboard/elasticache")
}
