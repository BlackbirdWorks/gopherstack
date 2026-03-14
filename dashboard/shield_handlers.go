package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// shieldProtectionView is the view model for a single Shield protection row.
type shieldProtectionView struct {
	ID          string
	Name        string
	ResourceARN string
}

// shieldIndexData is the template data for the Shield dashboard page.
type shieldIndexData struct {
	PageData

	SubscriptionState string
	Protections       []shieldProtectionView
}

// shieldSnippet returns the shared SnippetData for the Shield dashboard.
func shieldSnippet() *SnippetData {
	return &SnippetData{
		ID:    "shield-operations",
		Title: "Using AWS Shield",
		Cli:   `aws shield get-subscription-state --endpoint-url http://localhost:8000`,
		Go: `// Initialize the Shield client using AWS SDK v2.
import "github.com/aws/aws-sdk-go-v2/service/shield"

client := shield.NewFromConfig(cfg, func(o *shield.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})`,
		Python: `# Initialize boto3 client for Shield
import boto3

client = boto3.client('shield', endpoint_url='http://localhost:8000')`,
	}
}

// setupShieldRoutes registers all Shield dashboard routes.
func (h *DashboardHandler) setupShieldRoutes() {
	h.SubRouter.GET("/dashboard/shield", h.shieldIndex)
	h.SubRouter.POST("/dashboard/shield/subscribe", h.shieldSubscribe)
	h.SubRouter.POST("/dashboard/shield/protect", h.shieldProtect)
	h.SubRouter.POST("/dashboard/shield/delete", h.shieldDelete)
}

// shieldIndex renders the main Shield dashboard page.
func (h *DashboardHandler) shieldIndex(c *echo.Context) error {
	w := c.Response()

	if h.ShieldOps == nil {
		h.renderTemplate(w, "shield/index.html", shieldIndexData{
			PageData: PageData{
				Title:     "Shield",
				ActiveTab: "shield",
				Snippet:   shieldSnippet(),
			},
			Protections:       []shieldProtectionView{},
			SubscriptionState: "INACTIVE",
		})

		return nil
	}

	list := h.ShieldOps.Backend.ListProtections()
	views := make([]shieldProtectionView, 0, len(list))

	for _, p := range list {
		views = append(views, shieldProtectionView{
			ID:          p.ID,
			Name:        p.Name,
			ResourceARN: p.ResourceARN,
		})
	}

	state := h.ShieldOps.Backend.GetSubscriptionState()

	h.renderTemplate(w, "shield/index.html", shieldIndexData{
		PageData: PageData{
			Title:     "Shield",
			ActiveTab: "shield",
			Snippet:   shieldSnippet(),
		},
		Protections:       views,
		SubscriptionState: state,
	})

	return nil
}

// shieldSubscribe handles POST /dashboard/shield/subscribe.
func (h *DashboardHandler) shieldSubscribe(c *echo.Context) error {
	if h.ShieldOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	ctx := c.Request().Context()

	if err := h.ShieldOps.Backend.CreateSubscription(); err != nil {
		h.Logger.ErrorContext(ctx, "shield: failed to create subscription", "error", err)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/shield")
}

// shieldProtect handles POST /dashboard/shield/protect.
func (h *DashboardHandler) shieldProtect(c *echo.Context) error {
	if h.ShieldOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	resourceARN := c.Request().FormValue("resource_arn")

	if name == "" || resourceARN == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	ctx := c.Request().Context()

	if _, err := h.ShieldOps.Backend.CreateProtection(name, resourceARN, nil); err != nil {
		h.Logger.ErrorContext(ctx, "shield: failed to create protection", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/shield")
}

// shieldDelete handles POST /dashboard/shield/delete.
func (h *DashboardHandler) shieldDelete(c *echo.Context) error {
	if h.ShieldOps == nil {
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

	if err := h.ShieldOps.Backend.DeleteProtection(id); err != nil {
		h.Logger.ErrorContext(ctx, "shield: failed to delete protection", "id", id, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/shield")
}
