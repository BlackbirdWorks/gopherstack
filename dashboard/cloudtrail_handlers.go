package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// cloudtrailTrailView is the view model for a single CloudTrail trail.
type cloudtrailTrailView struct {
	Name      string
	ARN       string
	IsLogging bool
}

// cloudtrailIndexData is the template data for the CloudTrail index page.
type cloudtrailIndexData struct {
	PageData

	Trails []cloudtrailTrailView
}

// cloudtrailSnippet returns the shared SnippetData for the CloudTrail dashboard pages.
func cloudtrailSnippet() *SnippetData {
	return &SnippetData{
		ID:    "cloudtrail-operations",
		Title: "Using CloudTrail",
		Cli:   `aws cloudtrail help --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for CloudTrail
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
client := cloudtrail.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for CloudTrail
import boto3

client = boto3.client('cloudtrail', endpoint_url='http://localhost:8000')`,
	}
}

// cloudtrailIndex renders the CloudTrail dashboard index.
func (h *DashboardHandler) cloudtrailIndex(c *echo.Context) error {
	w := c.Response()

	if h.CloudTrailOps == nil {
		h.renderTemplate(w, "cloudtrail/index.html", cloudtrailIndexData{
			PageData: PageData{Title: "CloudTrail Trails", ActiveTab: "cloudtrail", Snippet: cloudtrailSnippet()},
			Trails:   []cloudtrailTrailView{},
		})

		return nil
	}

	trails := h.CloudTrailOps.Backend.ListTrails()
	views := make([]cloudtrailTrailView, 0, len(trails))

	for _, t := range trails {
		views = append(views, cloudtrailTrailView{
			Name:      t.Name,
			ARN:       t.TrailARN,
			IsLogging: t.IsLogging,
		})
	}

	h.renderTemplate(w, "cloudtrail/index.html", cloudtrailIndexData{
		PageData: PageData{Title: "CloudTrail Trails", ActiveTab: "cloudtrail", Snippet: cloudtrailSnippet()},
		Trails:   views,
	})

	return nil
}

// cloudtrailCreateTrail handles POST /dashboard/cloudtrail/trail/create.
func (h *DashboardHandler) cloudtrailCreateTrail(c *echo.Context) error {
	if h.CloudTrailOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	s3Bucket := c.Request().FormValue("s3bucket")

	if name == "" || s3Bucket == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.CloudTrailOps.Backend.CreateTrail(name, s3Bucket, "", "", "", "", "", false, false, false, nil)
	if err != nil {
		h.Logger.Error("failed to create trail", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/cloudtrail")
}

// cloudtrailDeleteTrail handles POST /dashboard/cloudtrail/trail/delete.
func (h *DashboardHandler) cloudtrailDeleteTrail(c *echo.Context) error {
	if h.CloudTrailOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.CloudTrailOps.Backend.DeleteTrail(name); err != nil {
		h.Logger.Error("failed to delete trail", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/cloudtrail")
}
