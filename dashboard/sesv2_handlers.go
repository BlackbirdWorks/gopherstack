package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// sesv2IndexData is the template data for the SES v2 dashboard page.
type sesv2IndexData struct {
	PageData

	Identities        []sesv2IdentityView
	ConfigurationSets []sesv2ConfigSetView
}

// sesv2IdentityView is the view model for a single SES v2 email identity.
type sesv2IdentityView struct {
	Name               string
	IdentityType       string
	VerifiedForSending bool
}

// sesv2ConfigSetView is the view model for a single SES v2 configuration set.
type sesv2ConfigSetView struct {
	Name string
}

// sesv2Index renders the SES v2 dashboard page.
func (h *DashboardHandler) sesv2Index(c *echo.Context) error {
	w := c.Response()

	if h.SESv2Ops == nil {
		h.renderTemplate(w, "sesv2/index.html", sesv2IndexData{
			PageData: PageData{
				Title:     "SES v2",
				ActiveTab: "sesv2",
				Snippet:   sesv2SnippetData(),
			},
			Identities:        []sesv2IdentityView{},
			ConfigurationSets: []sesv2ConfigSetView{},
		})

		return nil
	}

	identityPage := h.SESv2Ops.Backend.ListEmailIdentities("", 0)
	identityViews := make([]sesv2IdentityView, 0, len(identityPage.Data))

	for _, ei := range identityPage.Data {
		identityViews = append(identityViews, sesv2IdentityView{
			Name:               ei.Identity,
			IdentityType:       ei.IdentityType,
			VerifiedForSending: ei.VerifiedForSending,
		})
	}

	configSetPage := h.SESv2Ops.Backend.ListConfigurationSets("", 0)
	configSetViews := make([]sesv2ConfigSetView, 0, len(configSetPage.Data))

	for _, cs := range configSetPage.Data {
		configSetViews = append(configSetViews, sesv2ConfigSetView{Name: cs.Name})
	}

	h.renderTemplate(w, "sesv2/index.html", sesv2IndexData{
		PageData: PageData{
			Title:     "SES v2",
			ActiveTab: "sesv2",
			Snippet:   sesv2SnippetData(),
		},
		Identities:        identityViews,
		ConfigurationSets: configSetViews,
	})

	return nil
}

// sesv2CreateIdentity handles POST /dashboard/sesv2/identity/create.
func (h *DashboardHandler) sesv2CreateIdentity(c *echo.Context) error {
	if h.SESv2Ops == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	identity := c.Request().FormValue("identity")
	if identity == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.SESv2Ops.Backend.CreateEmailIdentity(identity); err != nil {
		h.Logger.Error("failed to create SES v2 identity", "identity", identity, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/sesv2")
}

// sesv2DeleteIdentity handles POST /dashboard/sesv2/identity/delete.
func (h *DashboardHandler) sesv2DeleteIdentity(c *echo.Context) error {
	if h.SESv2Ops == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	identity := c.Request().FormValue("identity")
	if identity == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.SESv2Ops.Backend.DeleteEmailIdentity(identity); err != nil {
		h.Logger.Error("failed to delete SES v2 identity", "identity", identity, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/sesv2")
}

// sesv2CreateConfigSet handles POST /dashboard/sesv2/configuration-set/create.
func (h *DashboardHandler) sesv2CreateConfigSet(c *echo.Context) error {
	if h.SESv2Ops == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.SESv2Ops.Backend.CreateConfigurationSet(name); err != nil {
		h.Logger.Error("failed to create SES v2 configuration set", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/sesv2")
}

// sesv2DeleteConfigSet handles POST /dashboard/sesv2/configuration-set/delete.
func (h *DashboardHandler) sesv2DeleteConfigSet(c *echo.Context) error {
	if h.SESv2Ops == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.SESv2Ops.Backend.DeleteConfigurationSet(name); err != nil {
		h.Logger.Error("failed to delete SES v2 configuration set", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/sesv2")
}

func sesv2SnippetData() *SnippetData {
	return &SnippetData{
		ID:    "sesv2-operations",
		Title: "Using SES v2",
		Cli:   `aws sesv2 list-email-identities --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for SES v2
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
client := sesv2.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for SES v2
import boto3

client = boto3.client('sesv2', endpoint_url='http://localhost:8000')`,
	}
}
