package dashboard

import (
	pkgslogger "github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/labstack/echo/v5"
)

// amplifyBranchView is the view model for a single Amplify branch.
type amplifyBranchView struct {
	BranchName string
	Stage      string
}

// amplifyAppView is the view model for a single Amplify app.
type amplifyAppView struct {
	AppID         string
	ARN           string
	Name          string
	Platform      string
	DefaultDomain string
	Branches      []amplifyBranchView
}

// amplifyIndexData is the template data for the Amplify index page.
type amplifyIndexData struct {
	PageData

	Apps []amplifyAppView
}

func (h *DashboardHandler) amplifySnippet() *SnippetData {
	return &SnippetData{
		ID:    "amplify-operations",
		Title: "Using Amplify",
		Cli:   `aws amplify list-apps --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Amplify
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
)
if err != nil {
    log.Fatal(err)
}
client := amplify.NewFromConfig(cfg, func(o *amplify.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})`,
		Python: `# Initialize boto3 client for Amplify
import boto3

client = boto3.client(
    'amplify',
    endpoint_url='http://localhost:8000',
    region_name='us-east-1',
)`,
	}
}

// amplifyIndex handles GET /dashboard/amplify.
func (h *DashboardHandler) amplifyIndex(c *echo.Context) error {
	w := c.Response()
	ctx := c.Request().Context()

	if h.AmplifyOps == nil {
		h.renderTemplate(w, "amplify/index.html", amplifyIndexData{
			PageData: PageData{
				Title:     "Amplify Apps",
				ActiveTab: "amplify",
				Snippet:   h.amplifySnippet(),
			},
			Apps: []amplifyAppView{},
		})

		return nil
	}

	allApps, _, err := h.AmplifyOps.Backend.ListApps("", 0)
	if err != nil {
		pkgslogger.Load(ctx).ErrorContext(ctx, "failed to list Amplify apps", "error", err)

		allApps = nil
	}

	views := make([]amplifyAppView, 0, len(allApps))

	for _, app := range allApps {
		branches, _, bErr := h.AmplifyOps.Backend.ListBranches(app.AppID, "", 0)
		if bErr != nil {
			branches = nil
		}

		branchViews := make([]amplifyBranchView, 0, len(branches))

		for _, b := range branches {
			branchViews = append(branchViews, amplifyBranchView{
				BranchName: b.BranchName,
				Stage:      string(b.Stage),
			})
		}

		views = append(views, amplifyAppView{
			AppID:         app.AppID,
			ARN:           app.ARN,
			Name:          app.Name,
			Platform:      string(app.Platform),
			DefaultDomain: app.DefaultDomain,
			Branches:      branchViews,
		})
	}

	h.renderTemplate(w, "amplify/index.html", amplifyIndexData{
		PageData: PageData{
			Title:     "Amplify Apps",
			ActiveTab: "amplify",
			Snippet:   h.amplifySnippet(),
		},
		Apps: views,
	})

	return nil
}
