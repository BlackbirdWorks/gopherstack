package dashboard

import (
	"github.com/labstack/echo/v5"
)

// qldbSessionView is the view model for a single active QLDB Session.
type qldbSessionView struct {
	Token      string
	LedgerName string
	CreatedAt  string
}

// qldbSessionIndexData is the template data for the QLDB Session dashboard page.
type qldbSessionIndexData struct {
	PageData

	Sessions []qldbSessionView
}

// qldbSessionSnippet returns the shared SnippetData for the QLDB Session dashboard.
func qldbSessionSnippet() *SnippetData {
	return &SnippetData{
		ID:    "qldbsession-operations",
		Title: "Using QLDB Session",
		Cli:   `# QLDB Session does not have a direct CLI command — use the QLDB driver or SDK.`,
		Go: `// Initialize AWS SDK v2 for QLDB Session
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
client := qldbsession.NewFromConfig(cfg)
out, err := client.SendCommand(context.TODO(), &qldbsession.SendCommandInput{
    StartSession: &types.StartSessionRequest{
        LedgerName: aws.String("my-ledger"),
    },
})`,
		Python: `# Initialize boto3 client for QLDB Session
import boto3

client = boto3.client('qldb-session', endpoint_url='http://localhost:8000')
resp = client.send_command(StartSession={'LedgerName': 'my-ledger'})`,
	}
}

// setupQLDBSessionRoutes registers all QLDB Session dashboard routes.
func (h *DashboardHandler) setupQLDBSessionRoutes() {
	h.SubRouter.GET("/dashboard/qldbsession", h.qldbSessionIndex)
}

// qldbSessionIndex renders the main QLDB Session dashboard page.
func (h *DashboardHandler) qldbSessionIndex(c *echo.Context) error {
	w := c.Response()

	if h.QLDBSessionOps == nil {
		h.renderTemplate(w, "qldbsession/index.html", qldbSessionIndexData{
			PageData: PageData{
				Title:     "QLDB Session",
				ActiveTab: "qldbsession",
				Snippet:   qldbSessionSnippet(),
			},
			Sessions: []qldbSessionView{},
		})

		return nil
	}

	list := h.QLDBSessionOps.Backend.ListSessions()
	views := make([]qldbSessionView, 0, len(list))

	for _, s := range list {
		views = append(views, qldbSessionView{
			Token:      s.Token,
			LedgerName: s.LedgerName,
			CreatedAt:  s.CreatedAt.UTC().Format("2006-01-02 15:04:05 UTC"),
		})
	}

	h.renderTemplate(w, "qldbsession/index.html", qldbSessionIndexData{
		PageData: PageData{
			Title:     "QLDB Session",
			ActiveTab: "qldbsession",
			Snippet:   qldbSessionSnippet(),
		},
		Sessions: views,
	})

	return nil
}
