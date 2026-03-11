package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	emrbackend "github.com/blackbirdworks/gopherstack/services/emr"
)

// emrClusterView is the view model for a single EMR cluster.
type emrClusterView struct {
	ID           string
	Name         string
	ReleaseLabel string
	State        string
	ARN          string
}

// emrIndexData is the template data for the EMR dashboard index page.
type emrIndexData struct {
	PageData

	Clusters []emrClusterView
}

// emrIndex renders the EMR dashboard index page.
func (h *DashboardHandler) emrIndex(c *echo.Context) error {
	w := c.Response()

	snippet := &SnippetData{
		ID:    "emr-operations",
		Title: "Using Amazon EMR",
		Cli: `aws emr list-clusters \
    --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for EMR
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
client := emr.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for EMR
import boto3

client = boto3.client('emr', endpoint_url='http://localhost:8000')`,
	}

	if h.EMROps == nil {
		h.renderTemplate(w, "emr/index.html", emrIndexData{
			PageData: PageData{
				Title:     "EMR",
				ActiveTab: "emr",
				Snippet:   snippet,
			},
			Clusters: []emrClusterView{},
		})

		return nil
	}

	clusters := h.EMROps.Backend.ListClusters()
	clusterViews := make([]emrClusterView, 0, len(clusters))

	for _, c := range clusters {
		clusterViews = append(clusterViews, emrClusterView{
			ID:           c.ID,
			Name:         c.Name,
			ReleaseLabel: c.ReleaseLabel,
			State:        c.Status.State,
			ARN:          c.ClusterArn,
		})
	}

	h.renderTemplate(w, "emr/index.html", emrIndexData{
		PageData: PageData{
			Title:     "EMR",
			ActiveTab: "emr",
			Snippet:   snippet,
		},
		Clusters: clusterViews,
	})

	return nil
}

// emrCreateCluster handles POST /dashboard/emr/clusters/create.
func (h *DashboardHandler) emrCreateCluster(c *echo.Context) error {
	if h.EMROps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	releaseLabel := c.Request().FormValue("release_label")
	if releaseLabel == "" {
		releaseLabel = "emr-6.0.0"
	}

	_, err := h.EMROps.Backend.RunJobFlow(name, releaseLabel, []emrbackend.Tag{})
	if err != nil {
		h.Logger.Error("failed to create EMR cluster", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/emr")
}

// emrDeleteCluster handles POST /dashboard/emr/clusters/delete.
func (h *DashboardHandler) emrDeleteCluster(c *echo.Context) error {
	if h.EMROps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	id := c.Request().FormValue("id")
	if id == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.EMROps.Backend.TerminateJobFlows([]string{id}); err != nil {
		h.Logger.Error("failed to delete EMR cluster", "id", id, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/emr")
}

// setupEMRRoutes registers routes for the EMR dashboard.
func (h *DashboardHandler) setupEMRRoutes() {
	h.SubRouter.GET("/dashboard/emr", h.emrIndex)
	h.SubRouter.POST("/dashboard/emr/clusters/create", h.emrCreateCluster)
	h.SubRouter.POST("/dashboard/emr/clusters/delete", h.emrDeleteCluster)
}
