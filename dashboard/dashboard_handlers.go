package dashboard

import (
	"github.com/labstack/echo/v5"
)

// dashboardIndex renders the main global overview page.
//
//nolint:funlen // long due to service icon SVG constants
func (h *DashboardHandler) dashboardIndex(c *echo.Context) error {
	w := c.Response()

	type ServiceStatus struct {
		Name string
		Icon string
		Link string
	}

	data := struct {
		PageData

		Services []ServiceStatus
	}{
		PageData: PageData{
			Title:     "Overview",
			ActiveTab: "dashboard",

			Snippet: &SnippetData{
				ID:    "dashboard-operations",
				Title: "Using Overview",
				Cli:   "aws dashboard help --endpoint-url http://localhost:8000",
				Go:    "/* Write AWS SDK v2 Code for Overview */",
				Python: "# Write boto3 code for Overview\nimport boto3\n" +
					"client = boto3.client('dashboard', endpoint_url='http://localhost:8000')",
			},
		},
	}

	// List of services to display on the dashboard landing page
	if h.DDBOps != nil || h.ddbProvider != nil {
		data.Services = append(
			data.Services,
			ServiceStatus{
				Name: "DynamoDB",
				Link: "/dashboard/dynamodb",
				Icon: `<img src="/dashboard/static/icons/dynamodb.svg" class="w-6 h-6 flex-shrink-0 rounded-md shadow-sm" alt="DynamoDB" />`,
			},
		)
	}
	// Add S3
	if h.S3Ops != nil || h.s3Provider != nil {
		data.Services = append(
			data.Services,
			ServiceStatus{
				Name: "S3",
				Link: "/dashboard/s3",
				Icon: `<img src="/dashboard/static/icons/s3.svg" class="w-6 h-6 flex-shrink-0 rounded-md shadow-sm" alt="S3" />`,
			},
		)
	}
	if h.IAMOps != nil {
		data.Services = append(
			data.Services,
			ServiceStatus{
				Name: "IAM",
				Link: "/dashboard/iam",
				Icon: `<img src="/dashboard/static/icons/iam.svg" class="w-6 h-6 flex-shrink-0 rounded-md shadow-sm" alt="IAM" />`,
			},
		)
	}
	if h.SNSOps != nil {
		data.Services = append(
			data.Services,
			ServiceStatus{
				Name: "SNS",
				Link: "/dashboard/sns",
				Icon: `<img src="/dashboard/static/icons/sns.svg" class="w-6 h-6 flex-shrink-0 rounded-md shadow-sm" alt="SNS" />`,
			},
		)
	}
	if h.SQSOps != nil {
		data.Services = append(
			data.Services,
			ServiceStatus{
				Name: "SQS",
				Link: "/dashboard/sqs",
				Icon: `<img src="/dashboard/static/icons/sqs.svg" class="w-6 h-6 flex-shrink-0 rounded-md shadow-sm" alt="SQS" />`,
			},
		)
	}
	if h.LambdaOps != nil {
		data.Services = append(
			data.Services,
			ServiceStatus{
				Name: "Lambda",
				Link: "/dashboard/lambda",
				Icon: `<img src="/dashboard/static/icons/lambda.svg" class="w-6 h-6 flex-shrink-0 rounded-md shadow-sm" alt="Lambda" />`,
			},
		)
	}
	if h.ElastiCacheOps != nil {
		data.Services = append(
			data.Services,
			ServiceStatus{
				Name: "ElastiCache",
				Link: "/dashboard/elasticache",
				Icon: `<img src="/dashboard/static/icons/elasticache.svg" class="w-6 h-6 flex-shrink-0 rounded-md shadow-sm" alt="ElastiCache" />`,
			},
		)
	}
	if h.STSOps != nil {
		data.Services = append(
			data.Services,
			ServiceStatus{
				Name: "STS",
				Link: "/dashboard/sts",
				Icon: `<img src="/dashboard/static/icons/sts.svg" class="w-6 h-6 flex-shrink-0 rounded-md shadow-sm" alt="STS" />`,
			},
		)
	}
	if h.SWFOps != nil {
		data.Services = append(
			data.Services,
			ServiceStatus{
				Name: "SWF Domains",
				Link: "/dashboard/swf",
				Icon: `<img src="/dashboard/static/icons/swf.svg" class="w-6 h-6 flex-shrink-0 rounded-md shadow-sm" alt="SWF" />`,
			},
		)
	}
	if h.ResourceGroupsOps != nil {
		data.Services = append(
			data.Services,
			ServiceStatus{
				Name: "Resource Groups",
				Link: "/dashboard/resourcegroups",
				Icon: `<img src="/dashboard/static/icons/resourcegroups.svg" class="w-6 h-6 flex-shrink-0 rounded-md shadow-sm" alt="Resource Groups" />`,
			},
		)
	}

	h.renderTemplate(w, "dashboard_index.html", data)

	return nil
}
