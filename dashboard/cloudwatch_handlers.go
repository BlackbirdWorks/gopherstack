package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	cwbackend "github.com/blackbirdworks/gopherstack/cloudwatch"
)

func (h *DashboardHandler) cloudWatchIndex(c *echo.Context) error {
	if h.CloudWatchOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	metrics, _ := h.CloudWatchOps.Backend.ListMetrics("", "")
	alarms, _ := h.CloudWatchOps.Backend.DescribeAlarms(nil, "")
	data := struct {
		PageData

		Metrics []cwbackend.Metric
		Alarms  []cwbackend.MetricAlarm
	}{
		PageData: PageData{Title: "CloudWatch", ActiveTab: "cloudwatch",
			Snippet: &SnippetData{
				ID:    "cloudwatch-operations",
				Title: "Using Cloudwatch",
				Cli:   "aws cloudwatch help --endpoint-url http://localhost:8000",
				Go:    "/* Write AWS SDK v2 Code for Cloudwatch */",
				Python: "# Write boto3 code for CloudWatch\nimport boto3\n" +
					"client = boto3.client('cloudwatch', endpoint_url='http://localhost:8000')",
			}},
		Metrics: metrics,
		Alarms:  alarms,
	}

	h.renderTemplate(c.Response(), "cloudwatch/index.html", data)

	return nil
}
