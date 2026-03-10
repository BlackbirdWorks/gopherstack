package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	athenabackend "github.com/blackbirdworks/gopherstack/services/athena"
)

// athenaIndexData is the template data for the Athena list page.
type athenaIndexData struct {
	PageData

	WorkGroups []athenabackend.WorkGroupSummary
}

// athenaDetailData is the template data for the Athena workgroup detail page.
type athenaDetailData struct {
	PageData

	WorkGroup    *athenabackend.WorkGroup
	NamedQueries []athenabackend.NamedQuery
}

func (h *DashboardHandler) athenaSnippet() *SnippetData {
	return &SnippetData{
		ID:    "athena-operations",
		Title: "Using Athena",
		Cli: `# Create a workgroup
aws athena create-work-group \
  --name my-workgroup \
  --description "Test workgroup" \
  --endpoint-url http://localhost:8000

# List workgroups
aws athena list-work-groups \
  --endpoint-url http://localhost:8000

# Start a query execution
aws athena start-query-execution \
  --query-string "SELECT 1" \
  --work-group my-workgroup \
  --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Athena
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
)
if err != nil {
    log.Fatal(err)
}
client := athena.NewFromConfig(cfg, func(o *athena.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})

// Create a workgroup
_, err = client.CreateWorkGroup(context.TODO(), &athena.CreateWorkGroupInput{
    Name: aws.String("my-workgroup"),
})`,
		Python: `# Initialize boto3 client for Athena
import boto3

client = boto3.client('athena', endpoint_url='http://localhost:8000')

# Create a workgroup
client.create_work_group(Name='my-workgroup')

# Start a query execution
response = client.start_query_execution(
    QueryString='SELECT 1',
    WorkGroup='my-workgroup',
)
execution_id = response['QueryExecutionId']`,
	}
}

func (h *DashboardHandler) athenaIndex(c *echo.Context) error {
	if h.AthenaOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	workGroups, _ := h.AthenaOps.Backend.ListWorkGroups()
	data := athenaIndexData{
		PageData: PageData{
			Title:     "Athena",
			ActiveTab: "athena",
			Snippet:   h.athenaSnippet(),
		},
		WorkGroups: workGroups,
	}

	h.renderTemplate(c.Response(), "athena/index.html", data)

	return nil
}

func (h *DashboardHandler) athenaDetail(c *echo.Context) error {
	if h.AthenaOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	name := c.Request().URL.Query().Get("name")
	if name == "" {
		return c.String(http.StatusBadRequest, "Missing name")
	}

	wg, err := h.AthenaOps.Backend.GetWorkGroup(name)
	if err != nil {
		return c.String(http.StatusNotFound, "WorkGroup not found")
	}

	ids, _ := h.AthenaOps.Backend.ListNamedQueries(name)
	var namedQueries []athenabackend.NamedQuery
	if len(ids) > 0 {
		namedQueries, _ = h.AthenaOps.Backend.BatchGetNamedQuery(ids)
	}

	data := athenaDetailData{
		PageData: PageData{
			Title:     "Athena — " + wg.Name,
			ActiveTab: "athena",
			Snippet:   h.athenaSnippet(),
		},
		WorkGroup:    wg,
		NamedQueries: namedQueries,
	}

	h.renderTemplate(c.Response(), "athena/detail.html", data)

	return nil
}
