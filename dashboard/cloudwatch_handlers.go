package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	cwbackend "github.com/blackbirdworks/gopherstack/cloudwatch"
)

// cloudWatchIndexData is the template data for the CloudWatch overview page.
type cloudWatchIndexData struct {
	PageData

	Metrics []cwbackend.Metric
	Alarms  []cwbackend.MetricAlarm
}

func (h *DashboardHandler) cloudWatchIndex(c *echo.Context) error {
	if h.CloudWatchOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	metrics, _ := h.CloudWatchOps.Backend.ListMetrics("", "")
	alarms, _ := h.CloudWatchOps.Backend.DescribeAlarms(nil, "")
	data := cloudWatchIndexData{
		PageData: PageData{Title: "CloudWatch", ActiveTab: "cloudwatch",
			Snippet: &SnippetData{
				ID:    "cloudwatch-operations",
				Title: "Using Cloudwatch",
				Cli:   `aws cloudwatch help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Cloudwatch
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
client := cloudwatch.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Cloudwatch
import boto3

client = boto3.client('cloudwatch', endpoint_url='http://localhost:8000')`,
			}},
		Metrics: metrics,
		Alarms:  alarms,
	}

	h.renderTemplate(c.Response(), "cloudwatch/index.html", data)

	return nil
}
