package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// cloudFormationIndexData is the template data for the CloudFormation stacks list page.
type cloudFormationIndexData struct {
	PageData

	Stacks []*cfnbackend.Stack
}

// cloudFormationStackDetailData is the template data for the CloudFormation stack detail page.
type cloudFormationStackDetailData struct {
	PageData

	Stack  *cfnbackend.Stack
	Events []cfnbackend.StackEvent
}

func (h *DashboardHandler) cloudFormationIndex(c *echo.Context) error {
	if h.CloudFormationOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}
	stacks := h.CloudFormationOps.Backend.ListAll()
	data := cloudFormationIndexData{
		PageData: PageData{Title: "CloudFormation", ActiveTab: "cloudformation",
			Snippet: &SnippetData{
				ID:    "cloudformation-operations",
				Title: "Using Cloudformation",
				Cli:   `aws cloudformation help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Cloudformation
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
client := cloudformation.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Cloudformation
import boto3

client = boto3.client('cloudformation', endpoint_url='http://localhost:8000')`,
			}},
		Stacks: stacks,
	}
	h.renderTemplate(c.Response(), "cloudformation/index.html", data)

	return nil
}

func (h *DashboardHandler) cloudFormationStackDetail(c *echo.Context) error {
	if h.CloudFormationOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}
	stackName := c.Request().URL.Query().Get("name")
	stack, err := h.CloudFormationOps.Backend.DescribeStack(stackName)
	if err != nil {
		return c.NoContent(http.StatusNotFound)
	}
	events, _ := h.CloudFormationOps.Backend.DescribeStackEvents(stackName)
	data := cloudFormationStackDetailData{
		PageData: PageData{Title: "Stack: " + stack.StackName, ActiveTab: "cloudformation",
			Snippet: &SnippetData{
				ID:    "cloudformation-operations",
				Title: "Using Cloudformation",
				Cli:   `aws cloudformation help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Cloudformation
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
client := cloudformation.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Cloudformation
import boto3

client = boto3.client('cloudformation', endpoint_url='http://localhost:8000')`,
			}},
		Stack:  stack,
		Events: events,
	}
	h.renderTemplate(c.Response(), "cloudformation/stack_detail.html", data)

	return nil
}
