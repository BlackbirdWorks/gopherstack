package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
)

// ecsIndexData is the template data for the ECS dashboard page.
type ecsIndexData struct {
	PageData

	Clusters        []ecsClusterView
	TaskDefinitions []ecsTaskDefinitionView
}

// ecsClusterView is the view model for a single ECS cluster.
type ecsClusterView struct {
	Name                string
	ARN                 string
	Status              string
	CreatedAt           string
	ActiveServicesCount int
	RunningTasksCount   int
	PendingTasksCount   int
}

// ecsTaskDefinitionView is the view model for a single ECS task definition.
type ecsTaskDefinitionView struct {
	Family   string
	ARN      string
	Status   string
	Revision int
}

// ecsIndex renders the ECS dashboard page.
func (h *DashboardHandler) ecsIndex(c *echo.Context) error {
	w := c.Response()

	if h.ECSOps == nil {
		h.renderTemplate(w, "ecs/index.html", ecsIndexData{
			PageData: PageData{
				Title:     "ECS",
				ActiveTab: "ecs",
				Snippet:   ecsSnippetData(),
			},
			Clusters:        []ecsClusterView{},
			TaskDefinitions: []ecsTaskDefinitionView{},
		})

		return nil
	}

	clusters, err := h.ECSOps.Backend.DescribeClusters(nil)
	if err != nil {
		h.Logger.Error("failed to list ECS clusters", "error", err)
		clusters = nil
	}

	clusterViews := make([]ecsClusterView, 0, len(clusters))

	for _, c := range clusters {
		clusterViews = append(clusterViews, ecsClusterView{
			Name:                c.ClusterName,
			ARN:                 c.ClusterArn,
			Status:              c.Status,
			ActiveServicesCount: c.ActiveServicesCount,
			RunningTasksCount:   c.RunningTasksCount,
			PendingTasksCount:   c.PendingTasksCount,
			CreatedAt:           c.CreatedAt.Format("2006-01-02 15:04:05"),
		})
	}

	tdArns, err := h.ECSOps.Backend.ListTaskDefinitions("")
	if err != nil {
		h.Logger.Error("failed to list ECS task definitions", "error", err)
		tdArns = nil
	}

	tdViews := make([]ecsTaskDefinitionView, 0, len(tdArns))

	for _, arn := range tdArns {
		td, tdErr := h.ECSOps.Backend.DescribeTaskDefinition(arn)
		if tdErr != nil {
			h.Logger.Error("failed to describe task definition", "arn", arn, "error", tdErr)

			continue
		}

		tdViews = append(tdViews, ecsTaskDefinitionView{
			Family:   td.Family,
			ARN:      td.TaskDefinitionArn,
			Revision: td.Revision,
			Status:   td.Status,
		})
	}

	h.renderTemplate(w, "ecs/index.html", ecsIndexData{
		PageData: PageData{
			Title:     "ECS",
			ActiveTab: "ecs",
			Snippet:   ecsSnippetData(),
		},
		Clusters:        clusterViews,
		TaskDefinitions: tdViews,
	})

	return nil
}

// ecsCreateCluster handles POST /dashboard/ecs/cluster/create.
func (h *DashboardHandler) ecsCreateCluster(c *echo.Context) error {
	if h.ECSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.ECSOps.Backend.CreateCluster(ecsbackend.CreateClusterInput{ClusterName: name}); err != nil {
		h.Logger.Error("failed to create ECS cluster", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/ecs")
}

// ecsDeleteCluster handles POST /dashboard/ecs/cluster/delete.
func (h *DashboardHandler) ecsDeleteCluster(c *echo.Context) error {
	if h.ECSOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.ECSOps.Backend.DeleteCluster(name); err != nil {
		h.Logger.Error("failed to delete ECS cluster", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/ecs")
}

func ecsSnippetData() *SnippetData {
	return &SnippetData{
		ID:    "ecs-operations",
		Title: "Using ECS",
		Cli:   `aws ecs list-clusters --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for ECS
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
client := ecs.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for ECS
import boto3

client = boto3.client('ecs', endpoint_url='http://localhost:8000')`,
	}
}
