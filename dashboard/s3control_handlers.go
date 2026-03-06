package dashboard

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/services/s3control"
	"github.com/labstack/echo/v5"
)

// s3controlConfigView is the view model for a public access block config.
type s3controlConfigView struct {
	AccountID             string
	BlockPublicAcls       bool
	IgnorePublicAcls      bool
	BlockPublicPolicy     bool
	RestrictPublicBuckets bool
}

// s3controlIndexData is the template data for the S3 Control index page.
type s3controlIndexData struct {
	PageData

	Configs []s3controlConfigView
}

// s3controlIndex renders the S3 Control dashboard index.
func (h *DashboardHandler) s3controlIndex(c *echo.Context) error {
	w := c.Response()

	if h.S3ControlOps == nil {
		h.renderTemplate(w, "s3control/index.html", s3controlIndexData{
			PageData: PageData{Title: "S3 Control", ActiveTab: "s3control",
				Snippet: &SnippetData{
					ID:    "s3control-operations",
					Title: "Using S3control",
					Cli:   `aws s3control help --endpoint-url http://localhost:8000`,
					Go: `// Initialize AWS SDK v2 for Using S3control
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
client := s3control.NewFromConfig(cfg)`,
					Python: `# Initialize boto3 client for Using S3control
import boto3

client = boto3.client('s3control', endpoint_url='http://localhost:8000')`,
				}},
			Configs: []s3controlConfigView{},
		})

		return nil
	}

	all := h.S3ControlOps.Backend.ListAll()
	views := make([]s3controlConfigView, 0, len(all))

	for _, cfg := range all {
		views = append(views, s3controlConfigView{
			AccountID:             cfg.AccountID,
			BlockPublicAcls:       cfg.BlockPublicAcls,
			IgnorePublicAcls:      cfg.IgnorePublicAcls,
			BlockPublicPolicy:     cfg.BlockPublicPolicy,
			RestrictPublicBuckets: cfg.RestrictPublicBuckets,
		})
	}

	h.renderTemplate(w, "s3control/index.html", s3controlIndexData{
		PageData: PageData{Title: "S3 Control", ActiveTab: "s3control",
			Snippet: &SnippetData{
				ID:    "s3control-operations",
				Title: "Using S3control",
				Cli:   `aws s3control help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using S3control
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
client := s3control.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using S3control
import boto3

client = boto3.client('s3control', endpoint_url='http://localhost:8000')`,
			}},
		Configs: views,
	})

	return nil
}

// s3controlPutConfig handles POST /dashboard/s3control/config.
func (h *DashboardHandler) s3controlPutConfig(c *echo.Context) error {
	if h.S3ControlOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	accountID := c.Request().FormValue("account_id")
	if accountID == "" {
		accountID = h.GlobalConfig.AccountID
	}

	h.S3ControlOps.Backend.PutPublicAccessBlock(s3control.PublicAccessBlock{
		AccountID:             accountID,
		BlockPublicAcls:       c.Request().FormValue("block_public_acls") == "on",
		IgnorePublicAcls:      c.Request().FormValue("ignore_public_acls") == "on",
		BlockPublicPolicy:     c.Request().FormValue("block_public_policy") == "on",
		RestrictPublicBuckets: c.Request().FormValue("restrict_public_buckets") == "on",
	})

	return c.Redirect(http.StatusFound, "/dashboard/s3control")
}
