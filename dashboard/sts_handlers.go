package dashboard

import (
	"github.com/labstack/echo/v5"

	stsbackend "github.com/blackbirdworks/gopherstack/sts"
)

// stsIndex renders the STS caller-identity overview page.
func (h *DashboardHandler) stsIndex(c *echo.Context) error {
	w := c.Response()

	data := struct {
		PageData

		Account string
		Arn     string
		UserID  string
	}{
		PageData: PageData{
			Title:     "STS Security Token Service",
			ActiveTab: "sts",
			Snippet: &SnippetData{
				ID:    "sts-operations",
				Title: "Using Sts",
				Cli:   `aws sts help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Sts
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
client := sts.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Sts
import boto3

client = boto3.client('sts', endpoint_url='http://localhost:8000')`,
			},
		},
		Account: stsbackend.MockAccountID,
		Arn:     stsbackend.MockUserArn,
		UserID:  stsbackend.MockUserID,
	}

	h.renderTemplate(w, "sts/index.html", data)

	return nil
}
