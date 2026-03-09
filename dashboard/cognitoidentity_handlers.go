package dashboard

import (
	"time"

	"github.com/labstack/echo/v5"
)

// cognitoIdentityPoolView is the view model for a single Cognito Identity Pool.
type cognitoIdentityPoolView struct {
	PoolID               string
	Name                 string
	CreatedAt            string
	AllowUnauthenticated bool
}

// cognitoIdentityIndexData is the template data for the Cognito Identity index page.
type cognitoIdentityIndexData struct {
	PageData

	Pools []cognitoIdentityPoolView
}

func (h *DashboardHandler) cognitoIdentitySnippet() *SnippetData {
	return &SnippetData{
		ID:    "cognitoidentity-operations",
		Title: "Using Cognito Identity",
		Cli:   `aws cognito-identity list-identity-pools --max-results 10 --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Cognito Identity
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
)
if err != nil {
    log.Fatal(err)
}
client := cognitoidentity.NewFromConfig(cfg, func(o *cognitoidentity.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})`,
		Python: `# Initialize boto3 client for Cognito Identity
import boto3

client = boto3.client(
    'cognito-identity',
    endpoint_url='http://localhost:8000',
    region_name='us-east-1',
)`,
	}
}

// cognitoIdentityIndex handles GET /dashboard/cognitoidentity.
func (h *DashboardHandler) cognitoIdentityIndex(c *echo.Context) error {
	w := c.Response()

	if h.CognitoIdentityOps == nil {
		h.renderTemplate(w, "cognitoidentity/index.html", cognitoIdentityIndexData{
			PageData: PageData{
				Title:     "Cognito Identity Pools",
				ActiveTab: "cognitoidentity",
				Snippet:   h.cognitoIdentitySnippet(),
			},
			Pools: []cognitoIdentityPoolView{},
		})

		return nil
	}

	pools := h.CognitoIdentityOps.Backend.ListIdentityPools(0)
	views := make([]cognitoIdentityPoolView, 0, len(pools))

	for _, pool := range pools {
		views = append(views, cognitoIdentityPoolView{
			PoolID:               pool.IdentityPoolID,
			Name:                 pool.IdentityPoolName,
			CreatedAt:            pool.CreatedAt.Format(time.RFC3339),
			AllowUnauthenticated: pool.AllowUnauthenticatedIdentities,
		})
	}

	h.renderTemplate(w, "cognitoidentity/index.html", cognitoIdentityIndexData{
		PageData: PageData{
			Title:     "Cognito Identity Pools",
			ActiveTab: "cognitoidentity",
			Snippet:   h.cognitoIdentitySnippet(),
		},
		Pools: views,
	})

	return nil
}
