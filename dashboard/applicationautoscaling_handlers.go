package dashboard

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

// applicationautoscalingView is the view model for a single Application Auto Scaling scalable target.
type applicationautoscalingView struct {
	ServiceNamespace  string
	ResourceID        string
	ScalableDimension string
	ARN               string
	MinCapacity       int32
	MaxCapacity       int32
}

// applicationautoscalingIndexData is the template data for the Application Auto Scaling index page.
type applicationautoscalingIndexData struct {
	PageData

	Targets []applicationautoscalingView
}

// applicationautoscalingIndex renders the Application Auto Scaling dashboard index,
// listing all registered scalable targets.
func (h *DashboardHandler) applicationautoscalingIndex(c *echo.Context) error {
	w := c.Response()

	if h.ApplicationAutoscalingOps == nil {
		h.renderTemplate(w, "applicationautoscaling/index.html", applicationautoscalingIndexData{
			PageData: PageData{Title: "Application Auto Scaling", ActiveTab: "applicationautoscaling",
				Snippet: &SnippetData{
					ID:    "applicationautoscaling-operations",
					Title: "Using Application Auto Scaling",
					Cli: `aws application-autoscaling describe-scalable-targets \
    --service-namespace ecs --endpoint-url http://localhost:8000`,
					Go: `// Initialize AWS SDK v2 for Application Auto Scaling
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
client := applicationautoscaling.NewFromConfig(cfg)`,
					Python: `# Initialize boto3 client for Application Auto Scaling
import boto3

client = boto3.client('application-autoscaling', endpoint_url='http://localhost:8000')`,
				}},
			Targets: []applicationautoscalingView{},
		})

		return nil
	}

	targets := h.ApplicationAutoscalingOps.Backend.DescribeScalableTargets("")
	views := make([]applicationautoscalingView, 0, len(targets))

	for _, t := range targets {
		views = append(views, applicationautoscalingView{
			ServiceNamespace:  t.ServiceNamespace,
			ResourceID:        t.ResourceID,
			ScalableDimension: t.ScalableDimension,
			ARN:               t.ARN,
			MinCapacity:       t.MinCapacity,
			MaxCapacity:       t.MaxCapacity,
		})
	}

	h.renderTemplate(w, "applicationautoscaling/index.html", applicationautoscalingIndexData{
		PageData: PageData{Title: "Application Auto Scaling", ActiveTab: "applicationautoscaling",
			Snippet: &SnippetData{
				ID:    "applicationautoscaling-operations",
				Title: "Using Application Auto Scaling",
				Cli: `aws application-autoscaling describe-scalable-targets \
    --service-namespace ecs --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Application Auto Scaling
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
client := applicationautoscaling.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Application Auto Scaling
import boto3

client = boto3.client('application-autoscaling', endpoint_url='http://localhost:8000')`,
			}},
		Targets: views,
	})

	return nil
}

// applicationautoscalingCreate handles POST /dashboard/applicationautoscaling/create.
func (h *DashboardHandler) applicationautoscalingCreate(c *echo.Context) error {
	if h.ApplicationAutoscalingOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	namespace := c.Request().FormValue("namespace")
	resourceID := c.Request().FormValue("resource_id")
	scalableDimension := c.Request().FormValue("scalable_dimension")

	if namespace == "" || resourceID == "" || scalableDimension == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	minVal, err := strconv.ParseInt(c.Request().FormValue("min_capacity"), 10, 32)
	if err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	maxVal, err := strconv.ParseInt(c.Request().FormValue("max_capacity"), 10, 32)
	if err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err = h.ApplicationAutoscalingOps.Backend.RegisterScalableTarget(
		namespace, resourceID, scalableDimension, int32(minVal), int32(maxVal),
	)
	if err != nil {
		h.Logger.Error("failed to register scalable target", "resource_id", resourceID, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/applicationautoscaling")
}

// applicationautoscalingDelete handles POST /dashboard/applicationautoscaling/delete.
func (h *DashboardHandler) applicationautoscalingDelete(c *echo.Context) error {
	if h.ApplicationAutoscalingOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	namespace := c.Request().FormValue("namespace")
	resourceID := c.Request().FormValue("resource_id")
	scalableDimension := c.Request().FormValue("scalable_dimension")

	if namespace == "" || resourceID == "" || scalableDimension == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.ApplicationAutoscalingOps.Backend.DeregisterScalableTarget(
		namespace,
		resourceID,
		scalableDimension,
	); err != nil {
		h.Logger.Error("failed to deregister scalable target", "resource_id", resourceID, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/applicationautoscaling")
}
