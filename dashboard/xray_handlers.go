package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// xrayGroupView is the view model for an X-Ray group.
type xrayGroupView struct {
	GroupName        string
	FilterExpression string
}

// xrayIndexData is the template data for the X-Ray index page.
type xrayIndexData struct {
	PageData

	Groups []xrayGroupView
}

func xraySnippet() *SnippetData {
	return &SnippetData{
		ID:    "xray-operations",
		Title: "Using X-Ray",
		Cli: `# Create an X-Ray group
aws xray create-group \
  --group-name my-group \
  --filter-expression "service(\"my-service\")" \
  --endpoint-url http://localhost:8000

# List groups
aws xray get-groups \
  --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for X-Ray
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithBaseEndpoint("http://localhost:8000"),
)
if err != nil {
    log.Fatal(err)
}
client := xray.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for X-Ray
import boto3

client = boto3.client('xray', endpoint_url='http://localhost:8000')
response = client.create_group(GroupName='my-group')`,
	}
}

// setupXrayRoutes registers the X-Ray dashboard routes.
func (h *DashboardHandler) setupXrayRoutes() {
	h.SubRouter.GET("/dashboard/xray", h.xrayIndex)
	h.SubRouter.POST("/dashboard/xray/create-group", h.xrayCreateGroup)
	h.SubRouter.POST("/dashboard/xray/delete-group", h.xrayDeleteGroup)
}

// xrayIndex renders the X-Ray dashboard index.
func (h *DashboardHandler) xrayIndex(c *echo.Context) error {
	w := c.Response()

	pageData := PageData{Title: "X-Ray Groups", ActiveTab: "xray", Snippet: xraySnippet()}

	if h.XrayOps == nil {
		h.renderTemplate(w, "xray/index.html", xrayIndexData{
			PageData: pageData,
			Groups:   []xrayGroupView{},
		})

		return nil
	}

	groups := h.XrayOps.Backend.GetGroups()
	views := make([]xrayGroupView, 0, len(groups))

	for _, g := range groups {
		views = append(views, xrayGroupView{
			GroupName:        g.GroupName,
			FilterExpression: g.FilterExpression,
		})
	}

	h.renderTemplate(w, "xray/index.html", xrayIndexData{
		PageData: pageData,
		Groups:   views,
	})

	return nil
}

// xrayCreateGroup handles POST /dashboard/xray/create-group.
func (h *DashboardHandler) xrayCreateGroup(c *echo.Context) error {
	if h.XrayOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	groupName := c.Request().FormValue("group_name")
	if groupName == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	filterExpression := c.Request().FormValue("filter_expression")

	if _, err := h.XrayOps.Backend.CreateGroup(groupName, filterExpression); err != nil {
		h.Logger.Error("failed to create x-ray group", "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/xray")
}

// xrayDeleteGroup handles POST /dashboard/xray/delete-group.
func (h *DashboardHandler) xrayDeleteGroup(c *echo.Context) error {
	if h.XrayOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	groupName := c.Request().FormValue("group_name")
	if groupName == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.XrayOps.Backend.DeleteGroup(groupName); err != nil {
		h.Logger.Error("failed to delete x-ray group", "group_name", groupName, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/xray")
}
