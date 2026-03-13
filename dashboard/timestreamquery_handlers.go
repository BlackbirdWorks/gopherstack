package dashboard

import (
	"net/http"
	"sort"

	"github.com/labstack/echo/v5"
)

// timestreamqueryScheduledQueryView is the view model for a single scheduled query row.
type timestreamqueryScheduledQueryView struct {
	Arn                string
	Name               string
	State              string
	QueryString        string
	ScheduleExpression string
}

// timestreamqueryIndexData is the template data for the Timestream Query dashboard page.
type timestreamqueryIndexData struct {
	PageData

	ScheduledQueries []timestreamqueryScheduledQueryView
}

// timestreamquerySnippet returns the shared SnippetData for the Timestream Query dashboard.
func timestreamquerySnippet() *SnippetData {
	return &SnippetData{
		ID:    "timestreamquery-operations",
		Title: "Using Timestream Query",
		Cli:   `aws timestream-query list-scheduled-queries --endpoint-url http://localhost:8000`,
		Go: `import (
    "context"
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/timestreamquery"
)

cfg, _ := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
)
client := timestreamquery.NewFromConfig(cfg, func(o *timestreamquery.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
    o.EndpointDiscovery.EnableEndpointDiscovery = aws.EndpointDiscoveryDisabled
})
out, _ := client.ListScheduledQueries(context.TODO(), &timestreamquery.ListScheduledQueriesInput{})`,
		Python: `import boto3

client = boto3.client('timestream-query', endpoint_url='http://localhost:8000',
    region_name='us-east-1')
response = client.list_scheduled_queries()`,
	}
}

// setupTimestreamQueryRoutes registers all Timestream Query dashboard routes.
func (h *DashboardHandler) setupTimestreamQueryRoutes() {
	h.SubRouter.GET("/dashboard/timestreamquery", h.timestreamqueryIndex)
	h.SubRouter.POST("/dashboard/timestreamquery/create", h.timestreamqueryCreate)
	h.SubRouter.POST("/dashboard/timestreamquery/delete", h.timestreamqueryDelete)
}

// timestreamqueryIndex renders the main Timestream Query dashboard page.
func (h *DashboardHandler) timestreamqueryIndex(c *echo.Context) error {
	w := c.Response()

	if h.TimestreamQueryOps == nil {
		h.renderTemplate(w, "timestreamquery/index.html", timestreamqueryIndexData{
			PageData: PageData{
				Title:     "Timestream Query",
				ActiveTab: "timestreamquery",
				Snippet:   timestreamquerySnippet(),
			},
		})

		return nil
	}

	queries := h.TimestreamQueryOps.Backend.ListScheduledQueriesFull()
	views := make([]timestreamqueryScheduledQueryView, 0, len(queries))

	for _, sq := range queries {
		views = append(views, timestreamqueryScheduledQueryView{
			Arn:                sq.Arn,
			Name:               sq.Name,
			State:              sq.State,
			QueryString:        sq.QueryString,
			ScheduleExpression: sq.ScheduleExpression,
		})
	}

	sort.Slice(views, func(i, j int) bool {
		return views[i].Name < views[j].Name
	})

	h.renderTemplate(w, "timestreamquery/index.html", timestreamqueryIndexData{
		PageData: PageData{
			Title:     "Timestream Query",
			ActiveTab: "timestreamquery",
			Snippet:   timestreamquerySnippet(),
		},
		ScheduledQueries: views,
	})

	return nil
}

// timestreamqueryCreate creates a scheduled query from the dashboard form.
func (h *DashboardHandler) timestreamqueryCreate(c *echo.Context) error {
	if h.TimestreamQueryOps == nil {
		return c.String(http.StatusServiceUnavailable, "Timestream Query service not configured")
	}

	name := c.FormValue("name")
	queryString := c.FormValue("query_string")
	scheduleExpression := c.FormValue("schedule_expression")

	if name == "" || queryString == "" || scheduleExpression == "" {
		return c.Redirect(http.StatusSeeOther, "/dashboard/timestreamquery")
	}

	_, _ = h.TimestreamQueryOps.Backend.CreateScheduledQuery(
		name, queryString, scheduleExpression,
		"arn:aws:iam::000000000000:role/demo-role",
		"", "", "", "",
		nil,
	)

	return c.Redirect(http.StatusSeeOther, "/dashboard/timestreamquery")
}

// timestreamqueryDelete deletes a scheduled query from the dashboard.
func (h *DashboardHandler) timestreamqueryDelete(c *echo.Context) error {
	if h.TimestreamQueryOps == nil {
		return c.String(http.StatusServiceUnavailable, "Timestream Query service not configured")
	}

	arn := c.FormValue("arn")
	if arn != "" {
		_ = h.TimestreamQueryOps.Backend.DeleteScheduledQuery(arn)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/timestreamquery")
}
