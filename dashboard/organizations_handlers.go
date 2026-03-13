package dashboard

import (
"github.com/labstack/echo/v5"

organizationsbackend "github.com/blackbirdworks/gopherstack/services/organizations"
)

// organizationsAccountView is the view model for a single account row.
type organizationsAccountView struct {
ID           string
ARN          string
Name         string
Email        string
Status       string
JoinedMethod string
}

// organizationsOUView is the view model for a single OU row.
type organizationsOUView struct {
ID   string
ARN  string
Name string
}

// organizationsIndexData is the template data for the Organizations dashboard page.
type organizationsIndexData struct {
PageData

OrgID      string
OrgARN     string
FeatureSet string
Accounts   []organizationsAccountView
OUs        []organizationsOUView
}

// organizationsSnippet returns the shared SnippetData for the Organizations dashboard.
func organizationsSnippet() *SnippetData {
return &SnippetData{
ID:    "organizations-operations",
Title: "Using Organizations",
Cli:   `aws organizations describe-organization --endpoint-url http://localhost:8000`,
Go: `// Initialize AWS SDK v2 for Organizations
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
client := organizations.NewFromConfig(cfg)`,
Python: `# Initialize boto3 client for Organizations
import boto3

client = boto3.client('organizations', endpoint_url='http://localhost:8000')`,
}
}

// setupOrganizationsRoutes registers all Organizations dashboard routes.
func (h *DashboardHandler) setupOrganizationsRoutes() {
h.SubRouter.GET("/dashboard/organizations", h.organizationsIndex)
}

// organizationsIndex renders the main Organizations dashboard page.
func (h *DashboardHandler) organizationsIndex(c *echo.Context) error {
w := c.Response()

if h.OrganizationsOps == nil {
h.renderTemplate(w, "organizations/index.html", organizationsIndexData{
PageData: PageData{
Title:     "Organizations",
ActiveTab: "organizations",
Snippet:   organizationsSnippet(),
},
})

return nil
}

ctx := c.Request().Context()

data := organizationsIndexData{
PageData: PageData{
Title:     "Organizations",
ActiveTab: "organizations",
Snippet:   organizationsSnippet(),
},
}

org, err := h.OrganizationsOps.Backend.DescribeOrganization()
if err == nil && org != nil {
data.OrgID = org.ID
data.OrgARN = org.ARN
data.FeatureSet = org.FeatureSet
}

accounts, listErr := h.OrganizationsOps.Backend.ListAccounts()
if listErr != nil {
h.Logger.ErrorContext(ctx, "organizations: failed to list accounts", "error", listErr)
}

for _, a := range accounts {
data.Accounts = append(data.Accounts, organizationsAccountView{
ID:           a.ID,
ARN:          a.ARN,
Name:         a.Name,
Email:        a.Email,
Status:       a.Status,
JoinedMethod: a.JoinedMethod,
})
}

if org != nil {
roots, rootErr := h.OrganizationsOps.Backend.ListRoots()
if rootErr == nil && len(roots) > 0 {
ous, ouErr := h.OrganizationsOps.Backend.ListOrganizationalUnitsForParent(roots[0].ID)
if ouErr != nil {
h.Logger.ErrorContext(ctx, "organizations: failed to list OUs", "error", ouErr)
}

for _, ou := range ous {
data.OUs = append(data.OUs, organizationsOUView{
ID:   ou.ID,
ARN:  ou.ARN,
Name: ou.Name,
})
}
}
}

h.renderTemplate(w, "organizations/index.html", data)

return nil
}

// demoOrganizations seeds demo data for visual inspection.
func demoOrganizations(b *organizationsbackend.InMemoryBackend) {
const (
demoFeatureSet    = "ALL"
demoAccountName   = "dev-account"
demoAccountEmail  = "dev@example.com"
demoAccountName2  = "prod-account"
demoAccountEmail2 = "prod@example.com"
demoOUName        = "development"
)

_, _, err := b.CreateOrganization(demoFeatureSet)
if err != nil {
return
}

roots, err := b.ListRoots()
if err != nil || len(roots) == 0 {
return
}

_, _ = b.CreateAccount(demoAccountName, demoAccountEmail, nil)
_, _ = b.CreateAccount(demoAccountName2, demoAccountEmail2, nil)
_, _ = b.CreateOrganizationalUnit(roots[0].ID, demoOUName, nil)
}
