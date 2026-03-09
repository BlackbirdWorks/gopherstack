package dashboard

import (
	pkgslogger "github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/labstack/echo/v5"
)

// appSyncAPIView is the view model for a single AppSync GraphQL API.
type appSyncAPIView struct {
	APIID              string
	ARN                string
	Name               string
	AuthenticationType string
	GraphQLURL         string
}

// appSyncIndexData is the template data for the AppSync index page.
type appSyncIndexData struct {
	PageData

	APIs []appSyncAPIView
}

func (h *DashboardHandler) appSyncSnippet() *SnippetData {
	return &SnippetData{
		ID:    "appsync-operations",
		Title: "Using AppSync",
		Cli:   `aws appsync list-graphql-apis --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for AppSync
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
)
if err != nil {
    log.Fatal(err)
}
client := appsync.NewFromConfig(cfg, func(o *appsync.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})`,
		Python: `# Initialize boto3 client for AppSync
import boto3

client = boto3.client(
    'appsync',
    endpoint_url='http://localhost:8000',
    region_name='us-east-1',
)`,
	}
}

// appSyncIndex handles GET /dashboard/appsync.
func (h *DashboardHandler) appSyncIndex(c *echo.Context) error {
	w := c.Response()
	ctx := c.Request().Context()

	if h.AppSyncOps == nil {
		h.renderTemplate(w, "appsync/index.html", appSyncIndexData{
			PageData: PageData{
				Title:     "AppSync GraphQL APIs",
				ActiveTab: "appsync",
				Snippet:   h.appSyncSnippet(),
			},
			APIs: []appSyncAPIView{},
		})

		return nil
	}

	allAPIs, err := h.AppSyncOps.Backend.ListGraphqlAPIs()
	if err != nil {
		pkgslogger.Load(ctx).ErrorContext(ctx, "failed to list AppSync APIs", "error", err)

		allAPIs = nil
	}

	views := make([]appSyncAPIView, 0, len(allAPIs))

	for _, api := range allAPIs {
		graphqlURL := ""
		if api.URIs != nil {
			graphqlURL = api.URIs["GRAPHQL"]
		}

		views = append(views, appSyncAPIView{
			APIID:              api.APIID,
			ARN:                api.ARN,
			Name:               api.Name,
			AuthenticationType: string(api.AuthenticationType),
			GraphQLURL:         graphqlURL,
		})
	}

	h.renderTemplate(w, "appsync/index.html", appSyncIndexData{
		PageData: PageData{
			Title:     "AppSync GraphQL APIs",
			ActiveTab: "appsync",
			Snippet:   h.appSyncSnippet(),
		},
		APIs: views,
	})

	return nil
}
