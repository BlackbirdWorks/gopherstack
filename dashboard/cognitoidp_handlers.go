package dashboard

import (
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
)

// cognitoIDPUserPoolView is the view model for a single Cognito IDP user pool.
type cognitoIDPUserPoolView struct {
	PoolID    string
	Name      string
	ARN       string
	CreatedAt string
}

// cognitoIDPIndexData is the template data for the Cognito IDP index page.
type cognitoIDPIndexData struct {
	PageData

	UserPools []cognitoIDPUserPoolView
}

func cognitoIDPSnippetData() *SnippetData {
	return &SnippetData{
		ID:    "cognitoidp-operations",
		Title: "Using Cognito User Pools",
		Cli:   `aws cognito-idp list-user-pools --max-results 10 --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Cognito IDP
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
)
if err != nil {
    log.Fatal(err)
}
client := cognitoidentityprovider.NewFromConfig(cfg, func(o *cognitoidentityprovider.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})`,
		Python: `# Initialize boto3 client for Cognito IDP
import boto3

client = boto3.client(
    'cognito-idp',
    endpoint_url='http://localhost:8000',
    region_name='us-east-1',
)`,
	}
}

// cognitoIDPIndex handles GET /dashboard/cognitoidp.
func (h *DashboardHandler) cognitoIDPIndex(c *echo.Context) error {
	w := c.Response()

	if h.CognitoIDPOps == nil {
		h.renderTemplate(w, "cognitoidp/index.html", cognitoIDPIndexData{
			PageData: PageData{
				Title:     "Cognito User Pools",
				ActiveTab: "cognitoidp",
				Snippet:   cognitoIDPSnippetData(),
			},
			UserPools: []cognitoIDPUserPoolView{},
		})

		return nil
	}

	pools := h.CognitoIDPOps.Backend.ListUserPools()
	views := make([]cognitoIDPUserPoolView, 0, len(pools))

	for _, pool := range pools {
		views = append(views, cognitoIDPUserPoolView{
			PoolID:    pool.ID,
			Name:      pool.Name,
			ARN:       pool.ARN,
			CreatedAt: pool.CreatedAt.Format(time.RFC3339),
		})
	}

	h.renderTemplate(w, "cognitoidp/index.html", cognitoIDPIndexData{
		PageData: PageData{
			Title:     "Cognito User Pools",
			ActiveTab: "cognitoidp",
			Snippet:   cognitoIDPSnippetData(),
		},
		UserPools: views,
	})

	return nil
}

// cognitoIDPCreateUserPool handles POST /dashboard/cognitoidp/user-pool/create.
func (h *DashboardHandler) cognitoIDPCreateUserPool(c *echo.Context) error {
	if h.CognitoIDPOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.CognitoIDPOps.Backend.CreateUserPool(name); err != nil {
		h.Logger.Error("failed to create Cognito user pool", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/cognitoidp")
}

// cognitoIDPDeleteUserPool handles POST /dashboard/cognitoidp/user-pool/delete.
func (h *DashboardHandler) cognitoIDPDeleteUserPool(c *echo.Context) error {
	if h.CognitoIDPOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	poolID := c.Request().FormValue("pool_id")
	if poolID == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.CognitoIDPOps.Backend.DeleteUserPool(poolID); err != nil {
		h.Logger.Error("failed to delete Cognito user pool", "pool_id", poolID, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/cognitoidp")
}
