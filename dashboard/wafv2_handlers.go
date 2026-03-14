package dashboard

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// wafv2WebACLView is the view model for a single WAFv2 Web ACL row.
type wafv2WebACLView struct {
	ID            string
	Name          string
	Scope         string
	DefaultAction string
	ARN           string
}

// wafv2IndexData is the template data for the WAFv2 dashboard page.
type wafv2IndexData struct {
	PageData

	WebACLs []wafv2WebACLView
}

// wafv2Snippet returns the shared SnippetData for the WAFv2 dashboard.
func wafv2Snippet() *SnippetData {
	return &SnippetData{
		ID:    "wafv2-operations",
		Title: "Using AWS WAFv2",
		Cli:   `aws wafv2 list-web-acls --scope REGIONAL --endpoint-url http://localhost:8000`,
		Go: `// Initialize the WAFv2 client using AWS SDK v2.
import "github.com/aws/aws-sdk-go-v2/service/wafv2"

client := wafv2.NewFromConfig(cfg, func(o *wafv2.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})`,
		Python: `# Initialize boto3 client for WAFv2
import boto3

client = boto3.client('wafv2', endpoint_url='http://localhost:8000')`,
	}
}

// setupWafv2Routes registers all WAFv2 dashboard routes.
func (h *DashboardHandler) setupWafv2Routes() {
	h.SubRouter.GET("/dashboard/wafv2", h.wafv2Index)
	h.SubRouter.POST("/dashboard/wafv2/create", h.wafv2Create)
	h.SubRouter.POST("/dashboard/wafv2/delete", h.wafv2Delete)
}

// wafv2Index renders the main WAFv2 dashboard page.
func (h *DashboardHandler) wafv2Index(c *echo.Context) error {
	w := c.Response()

	if h.Wafv2Ops == nil {
		h.renderTemplate(w, "wafv2/index.html", wafv2IndexData{
			PageData: PageData{
				Title:     "WAFv2",
				ActiveTab: "wafv2",
				Snippet:   wafv2Snippet(),
			},
			WebACLs: []wafv2WebACLView{},
		})

		return nil
	}

	list := h.Wafv2Ops.Backend.ListWebACLs()
	views := make([]wafv2WebACLView, 0, len(list))

	for _, acl := range list {
		arnStr := h.Wafv2Ops.Backend.WebACLARN(acl.Name, acl.ID, acl.Scope)
		views = append(views, wafv2WebACLView{
			ID:            acl.ID,
			Name:          acl.Name,
			Scope:         acl.Scope,
			DefaultAction: acl.DefaultAction,
			ARN:           arnStr,
		})
	}

	h.renderTemplate(w, "wafv2/index.html", wafv2IndexData{
		PageData: PageData{
			Title:     "WAFv2",
			ActiveTab: "wafv2",
			Snippet:   wafv2Snippet(),
		},
		WebACLs: views,
	})

	return nil
}

// wafv2Create handles POST /dashboard/wafv2/create.
func (h *DashboardHandler) wafv2Create(c *echo.Context) error {
	if h.Wafv2Ops == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	scope := c.Request().FormValue("scope")
	defaultAction := c.Request().FormValue("default_action")

	if name == "" || scope == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if defaultAction == "" {
		defaultAction = "ALLOW"
	}

	ctx := c.Request().Context()

	if _, err := h.Wafv2Ops.Backend.CreateWebACL(name, scope, "", defaultAction, nil); err != nil {
		h.Logger.ErrorContext(ctx, "wafv2: failed to create web ACL", "name", name, "error", err)

		if errors.Is(err, awserr.ErrConflict) {
			return c.NoContent(http.StatusConflict)
		}

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/wafv2")
}

// wafv2Delete handles POST /dashboard/wafv2/delete.
func (h *DashboardHandler) wafv2Delete(c *echo.Context) error {
	if h.Wafv2Ops == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	id := c.Request().FormValue("id")
	if id == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	ctx := c.Request().Context()

	if err := h.Wafv2Ops.Backend.DeleteWebACL(id); err != nil {
		h.Logger.ErrorContext(ctx, "wafv2: failed to delete web ACL", "id", id, "error", err)

		if errors.Is(err, awserr.ErrNotFound) {
			return c.NoContent(http.StatusNotFound)
		}

		return c.NoContent(http.StatusInternalServerError)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/wafv2")
}
