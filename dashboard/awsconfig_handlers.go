package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// awsconfigRecorderView is the view model for a single AWS Config recorder.
type awsconfigRecorderView struct {
	Name    string
	RoleARN string
	Status  string
}

// awsconfigIndexData is the template data for the AWS Config index page.
type awsconfigIndexData struct {
	PageData

	Recorders []awsconfigRecorderView
}

// awsconfigIndex renders the AWS Config dashboard index.
func (h *DashboardHandler) awsconfigIndex(c *echo.Context) error {
	w := c.Response()

	if h.AWSConfigOps == nil {
		h.renderTemplate(w, "awsconfig/index.html", awsconfigIndexData{
			PageData:  PageData{Title: "AWS Config", ActiveTab: "awsconfig",
		Snippet: &SnippetData{
			ID:    "awsconfig-operations",
			Title: "Using Awsconfig",
			Cli:   "aws awsconfig help --endpoint-url http://localhost:8000",
			Go: "/* Write AWS SDK v2 Code for Awsconfig */",
			Python: "# Write boto3 code for Awsconfig\nimport boto3\nclient = boto3.client('awsconfig', endpoint_url='http://localhost:8000')",
		},},
			Recorders: []awsconfigRecorderView{},
		})

		return nil
	}

	recorders := h.AWSConfigOps.Backend.DescribeConfigurationRecorders()
	views := make([]awsconfigRecorderView, 0, len(recorders))

	for _, r := range recorders {
		views = append(views, awsconfigRecorderView{
			Name:    r.Name,
			RoleARN: r.RoleARN,
			Status:  r.Status,
		})
	}

	h.renderTemplate(w, "awsconfig/index.html", awsconfigIndexData{
		PageData:  PageData{Title: "AWS Config", ActiveTab: "awsconfig",
		Snippet: &SnippetData{
			ID:    "awsconfig-operations",
			Title: "Using Awsconfig",
			Cli:   "aws awsconfig help --endpoint-url http://localhost:8000",
			Go: "/* Write AWS SDK v2 Code for Awsconfig */",
			Python: `# Write boto3 code for Awsconfig
import boto3
client = boto3.client('awsconfig', endpoint_url='http://localhost:8000')`,
		},},
		Recorders: views,
	})

	return nil
}

// awsconfigPutRecorder handles POST /dashboard/awsconfig/recorder.
func (h *DashboardHandler) awsconfigPutRecorder(c *echo.Context) error {
	if h.AWSConfigOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	roleARN := c.Request().FormValue("role_arn")

	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.AWSConfigOps.Backend.PutConfigurationRecorder(name, roleARN); err != nil {
		h.Logger.Error("failed to put AWS Config recorder", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/awsconfig")
}
