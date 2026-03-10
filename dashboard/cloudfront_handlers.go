package dashboard

import (
	"bytes"
	"encoding/xml"
	"net/http"

	"github.com/labstack/echo/v5"
)

// cloudfrontDistributionView is the view model for a CloudFront distribution.
type cloudfrontDistributionView struct {
	ID         string
	DomainName string
	Status     string
	Comment    string
	Enabled    bool
}

// cloudfrontIndexData is the template data for the CloudFront index page.
type cloudfrontIndexData struct {
	PageData

	Distributions []cloudfrontDistributionView
}

// cloudfrontSnippet returns the shared SnippetData for the CloudFront dashboard pages.
func cloudfrontSnippet() *SnippetData {
	return &SnippetData{
		ID:    "cloudfront-operations",
		Title: "Using CloudFront",
		Cli:   `aws cloudfront list-distributions --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for CloudFront
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
client := cloudfront.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for CloudFront
import boto3

client = boto3.client('cloudfront', endpoint_url='http://localhost:8000')`,
	}
}

// cloudfrontIndex renders the CloudFront dashboard index.
func (h *DashboardHandler) cloudfrontIndex(c *echo.Context) error {
	w := c.Response()

	if h.CloudFrontOps == nil {
		h.renderTemplate(w, "cloudfront/index.html", cloudfrontIndexData{
			PageData: PageData{
				Title:     "CloudFront Distributions",
				ActiveTab: "cloudfront",
				Snippet:   cloudfrontSnippet(),
			},
			Distributions: []cloudfrontDistributionView{},
		})

		return nil
	}

	dists := h.CloudFrontOps.Backend.ListDistributions()
	views := make([]cloudfrontDistributionView, 0, len(dists))

	for _, d := range dists {
		views = append(views, cloudfrontDistributionView{
			ID:         d.ID,
			DomainName: d.DomainName,
			Status:     d.Status,
			Comment:    d.Comment,
			Enabled:    d.Enabled,
		})
	}

	h.renderTemplate(w, "cloudfront/index.html", cloudfrontIndexData{
		PageData: PageData{
			Title:     "CloudFront Distributions",
			ActiveTab: "cloudfront",
			Snippet:   cloudfrontSnippet(),
		},
		Distributions: views,
	})

	return nil
}

// cloudfrontCreateDistribution handles POST /dashboard/cloudfront/distribution/create.
func (h *DashboardHandler) cloudfrontCreateDistribution(c *echo.Context) error {
	if h.CloudFrontOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	comment := c.Request().FormValue("comment")

	var escapedComment bytes.Buffer
	if escErr := xml.EscapeText(&escapedComment, []byte(comment)); escErr != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	rawConfig := []byte(`<DistributionConfig>` +
		`<CallerReference>dashboard-create</CallerReference>` +
		`<Comment>` + escapedComment.String() + `</Comment>` +
		`<Enabled>true</Enabled>` +
		`<Origins><Quantity>0</Quantity></Origins>` +
		`<DefaultCacheBehavior><ViewerProtocolPolicy>redirect-to-https</ViewerProtocolPolicy></DefaultCacheBehavior>` +
		`</DistributionConfig>`)

	_, err := h.CloudFrontOps.Backend.CreateDistribution("dashboard-create", comment, true, rawConfig)
	if err != nil {
		h.Logger.Error("failed to create cloudfront distribution", "comment", comment, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/cloudfront")
}

// cloudfrontDeleteDistribution handles POST /dashboard/cloudfront/distribution/delete.
func (h *DashboardHandler) cloudfrontDeleteDistribution(c *echo.Context) error {
	if h.CloudFrontOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	id := c.Request().FormValue("id")
	if id == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.CloudFrontOps.Backend.DeleteDistribution(id); err != nil {
		h.Logger.Error("failed to delete cloudfront distribution", "id", id, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/cloudfront")
}
