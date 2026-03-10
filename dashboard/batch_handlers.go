package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	batchbackend "github.com/blackbirdworks/gopherstack/services/batch"
)

const defaultJobQueuePriority = 10

type batchComputeEnvironmentView struct {
	Name   string
	ARN    string
	Type   string
	State  string
	Status string
}

// batchJobQueueView is the view model for a single Batch job queue.
type batchJobQueueView struct {
	Name     string
	ARN      string
	State    string
	Status   string
	Priority int32
}

// batchJobDefinitionView is the view model for a single Batch job definition.
type batchJobDefinitionView struct {
	Name     string
	ARN      string
	Type     string
	Status   string
	Revision int32
}

// batchIndexData is the template data for the Batch dashboard index page.
type batchIndexData struct {
	PageData

	ComputeEnvironments []batchComputeEnvironmentView
	JobQueues           []batchJobQueueView
	JobDefinitions      []batchJobDefinitionView
}

// batchIndex renders the Batch dashboard index page.
func (h *DashboardHandler) batchIndex(c *echo.Context) error {
	w := c.Response()

	snippet := &SnippetData{
		ID:    "batch-operations",
		Title: "Using AWS Batch",
		Cli: `aws batch describe-compute-environments \
    --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Batch
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
client := batch.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Batch
import boto3

client = boto3.client('batch', endpoint_url='http://localhost:8000')`,
	}

	if h.BatchOps == nil {
		h.renderTemplate(w, "batch/index.html", batchIndexData{
			PageData: PageData{
				Title:     "Batch",
				ActiveTab: "batch",
				Snippet:   snippet,
			},
			ComputeEnvironments: []batchComputeEnvironmentView{},
			JobQueues:           []batchJobQueueView{},
			JobDefinitions:      []batchJobDefinitionView{},
		})

		return nil
	}

	ces := h.BatchOps.Backend.DescribeComputeEnvironments(nil)
	ceViews := make([]batchComputeEnvironmentView, 0, len(ces))

	for _, ce := range ces {
		ceViews = append(ceViews, batchComputeEnvironmentView{
			Name:   ce.ComputeEnvironmentName,
			ARN:    ce.ComputeEnvironmentArn,
			Type:   ce.Type,
			State:  ce.State,
			Status: ce.Status,
		})
	}

	jqs := h.BatchOps.Backend.DescribeJobQueues(nil)
	jqViews := make([]batchJobQueueView, 0, len(jqs))

	for _, jq := range jqs {
		jqViews = append(jqViews, batchJobQueueView{
			Name:     jq.JobQueueName,
			ARN:      jq.JobQueueArn,
			State:    jq.State,
			Status:   jq.Status,
			Priority: jq.Priority,
		})
	}

	jds := h.BatchOps.Backend.DescribeJobDefinitions(nil)
	jdViews := make([]batchJobDefinitionView, 0, len(jds))

	for _, jd := range jds {
		jdViews = append(jdViews, batchJobDefinitionView{
			Name:     jd.JobDefinitionName,
			ARN:      jd.JobDefinitionArn,
			Type:     jd.Type,
			Status:   jd.Status,
			Revision: jd.Revision,
		})
	}

	h.renderTemplate(w, "batch/index.html", batchIndexData{
		PageData: PageData{
			Title:     "Batch",
			ActiveTab: "batch",
			Snippet:   snippet,
		},
		ComputeEnvironments: ceViews,
		JobQueues:           jqViews,
		JobDefinitions:      jdViews,
	})

	return nil
}

// batchCreateComputeEnvironment handles POST /dashboard/batch/compute-environments/create.
func (h *DashboardHandler) batchCreateComputeEnvironment(c *echo.Context) error {
	if h.BatchOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	ceType := c.Request().FormValue("type")

	if name == "" || ceType == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.BatchOps.Backend.CreateComputeEnvironment(name, ceType, "ENABLED", nil)
	if err != nil {
		h.Logger.Error("failed to create compute environment", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/batch")
}

// batchDeleteComputeEnvironment handles POST /dashboard/batch/compute-environments/delete.
func (h *DashboardHandler) batchDeleteComputeEnvironment(c *echo.Context) error {
	if h.BatchOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.BatchOps.Backend.DeleteComputeEnvironment(name); err != nil {
		h.Logger.Error("failed to delete compute environment", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/batch")
}

// batchCreateJobQueue handles POST /dashboard/batch/job-queues/create.
func (h *DashboardHandler) batchCreateJobQueue(c *echo.Context) error {
	if h.BatchOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.BatchOps.Backend.CreateJobQueue(
		name,
		defaultJobQueuePriority,
		"ENABLED",
		[]batchbackend.ComputeEnvironmentOrder{},
		nil,
	)
	if err != nil {
		h.Logger.Error("failed to create job queue", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/batch")
}

// batchDeleteJobQueue handles POST /dashboard/batch/job-queues/delete.
func (h *DashboardHandler) batchDeleteJobQueue(c *echo.Context) error {
	if h.BatchOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.BatchOps.Backend.DeleteJobQueue(name); err != nil {
		h.Logger.Error("failed to delete job queue", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/batch")
}

// setupBatchRoutes registers routes for the Batch dashboard.
func (h *DashboardHandler) setupBatchRoutes() {
	h.SubRouter.GET("/dashboard/batch", h.batchIndex)
	h.SubRouter.POST("/dashboard/batch/compute-environments/create", h.batchCreateComputeEnvironment)
	h.SubRouter.POST("/dashboard/batch/compute-environments/delete", h.batchDeleteComputeEnvironment)
	h.SubRouter.POST("/dashboard/batch/job-queues/create", h.batchCreateJobQueue)
	h.SubRouter.POST("/dashboard/batch/job-queues/delete", h.batchDeleteJobQueue)
}
