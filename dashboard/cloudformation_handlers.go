package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	cfnbackend "github.com/blackbirdworks/gopherstack/cloudformation"
)

func (h *DashboardHandler) cloudFormationIndex(c *echo.Context) error {
	if h.CloudFormationOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}
	stacks := h.CloudFormationOps.Backend.ListAll()
	data := struct {
		PageData

		Stacks []*cfnbackend.Stack
	}{
		PageData: PageData{Title: "CloudFormation", ActiveTab: "cloudformation",
		Snippet: &SnippetData{
			ID:    "cloudformation-operations",
			Title: "Using Cloudformation",
			Cli:   "aws cloudformation help --endpoint-url http://localhost:8000",
			Go: "/* Write AWS SDK v2 Code for Cloudformation */",
			Python: "# Write boto3 code for Cloudformation\nimport boto3\nclient = boto3.client('cloudformation', endpoint_url='http://localhost:8000')",
		},},
		Stacks:   stacks,
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
	data := struct {
		PageData

		Stack  *cfnbackend.Stack
		Events []cfnbackend.StackEvent
	}{
		PageData: PageData{Title: "Stack: " + stack.StackName, ActiveTab: "cloudformation",
		Snippet: &SnippetData{
			ID:    "cloudformation-operations",
			Title: "Using Cloudformation",
			Cli:   "aws cloudformation help --endpoint-url http://localhost:8000",
			Go: "/* Write AWS SDK v2 Code for Cloudformation */",
			Python: `# Write boto3 code for Cloudformation
import boto3
client = boto3.client('cloudformation', endpoint_url='http://localhost:8000')`,
		},},
		Stack:    stack,
		Events:   events,
	}
	h.renderTemplate(c.Response(), "cloudformation/stack_detail.html", data)

	return nil
}
