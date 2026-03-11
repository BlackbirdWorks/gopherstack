package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// eksClusterView is the view model for a single EKS cluster.
type eksClusterView struct {
	Name    string
	ARN     string
	Version string
	Status  string
}

// eksIndexData is the template data for the EKS index page.
type eksIndexData struct {
	PageData

	Clusters []eksClusterView
}

// eksSnippet returns the shared SnippetData for the EKS dashboard pages.
func eksSnippet() *SnippetData {
	return &SnippetData{
		ID:    "eks-operations",
		Title: "Using EKS",
		Cli:   `aws eks help --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for EKS
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
)
if err != nil {
    log.Fatal(err)
}
client := eks.NewFromConfig(cfg, func(o *eks.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})`,
		Python: `# Initialize boto3 client for EKS
import boto3

client = boto3.client('eks', endpoint_url='http://localhost:8000')`,
	}
}

// eksIndex renders the EKS dashboard index.
func (h *DashboardHandler) eksIndex(c *echo.Context) error {
	w := c.Response()

	if h.EKSOps == nil {
		h.renderTemplate(w, "eks/index.html", eksIndexData{
			PageData: PageData{Title: "EKS Clusters", ActiveTab: "eks", Snippet: eksSnippet()},
			Clusters: []eksClusterView{},
		})

		return nil
	}

	clusters := h.EKSOps.Backend.ListAllClusters()
	views := make([]eksClusterView, 0, len(clusters))

	for _, c := range clusters {
		views = append(views, eksClusterView{
			Name:    c.Name,
			ARN:     c.ARN,
			Version: c.Version,
			Status:  c.Status,
		})
	}

	h.renderTemplate(w, "eks/index.html", eksIndexData{
		PageData: PageData{Title: "EKS Clusters", ActiveTab: "eks", Snippet: eksSnippet()},
		Clusters: views,
	})

	return nil
}

// eksCreateCluster handles POST /dashboard/eks/cluster/create.
func (h *DashboardHandler) eksCreateCluster(c *echo.Context) error {
	if h.EKSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	version := c.Request().FormValue("version")

	_, err := h.EKSOps.Backend.CreateCluster(name, version, "", nil)
	if err != nil {
		h.Logger.Error("failed to create eks cluster", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/eks")
}

// eksDeleteCluster handles POST /dashboard/eks/cluster/delete.
func (h *DashboardHandler) eksDeleteCluster(c *echo.Context) error {
	if h.EKSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.EKSOps.Backend.DeleteCluster(name); err != nil {
		h.Logger.Error("failed to delete eks cluster", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/eks")
}

// setupEKSRoutes registers the EKS dashboard routes.
func (h *DashboardHandler) setupEKSRoutes() {
	h.SubRouter.GET("/dashboard/eks", h.eksIndex)
	h.SubRouter.POST("/dashboard/eks/cluster/create", h.eksCreateCluster)
	h.SubRouter.POST("/dashboard/eks/cluster/delete", h.eksDeleteCluster)
}
