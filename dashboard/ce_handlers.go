package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// ceView is the view model for a single Cost Explorer cost category.
type ceView struct {
	ARN            string
	Name           string
	RuleVersion    string
	EffectiveStart string
}

// ceIndexData is the template data for the Ce dashboard index page.
type ceIndexData struct {
	PageData

	CostCategories []ceView
}

func ceSnippet() *SnippetData {
	return &SnippetData{
		ID:    "ce-operations",
		Title: "Using Cost Explorer",
		Cli: `aws ce list-cost-category-definitions \
    --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Cost Explorer
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
)
if err != nil {
    log.Fatal(err)
}
client := costexplorer.NewFromConfig(cfg, func(o *costexplorer.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})`,
		Python: `# Initialize boto3 client for Cost Explorer
import boto3

client = boto3.client('ce', endpoint_url='http://localhost:8000')`,
	}
}

// ceIndex renders the Ce dashboard index, listing all cost categories.
func (h *DashboardHandler) ceIndex(c *echo.Context) error {
	w := c.Response()

	if h.CeOps == nil {
		h.renderTemplate(w, "ce/index.html", ceIndexData{
			PageData:       PageData{Title: "Cost Explorer", ActiveTab: "ce", Snippet: ceSnippet()},
			CostCategories: []ceView{},
		})

		return nil
	}

	cats := h.CeOps.Backend.ListCostCategoryDefinitions()
	views := make([]ceView, 0, len(cats))

	for _, cat := range cats {
		views = append(views, ceView{
			ARN:            cat.ARN,
			Name:           cat.Name,
			RuleVersion:    cat.RuleVersion,
			EffectiveStart: cat.EffectiveStart,
		})
	}

	h.renderTemplate(w, "ce/index.html", ceIndexData{
		PageData:       PageData{Title: "Cost Explorer", ActiveTab: "ce", Snippet: ceSnippet()},
		CostCategories: views,
	})

	return nil
}

// ceCreate handles POST /dashboard/ce/create.
func (h *DashboardHandler) ceCreate(c *echo.Context) error {
	if h.CeOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.CeOps.Backend.CreateCostCategoryDefinition(
		name,
		"CostCategoryExpression.v1",
		"",
		nil,
		nil,
	)
	if err != nil {
		h.Logger.Error("failed to create cost category", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/ce")
}

// ceDelete handles POST /dashboard/ce/delete.
func (h *DashboardHandler) ceDelete(c *echo.Context) error {
	if h.CeOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	arn := c.Request().FormValue("arn")
	if arn == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.CeOps.Backend.DeleteCostCategoryDefinition(arn); err != nil {
		h.Logger.Error("failed to delete cost category", "arn", arn, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/ce")
}

// setupCeRoutes registers Cost Explorer dashboard routes.
func (h *DashboardHandler) setupCeRoutes() {
	h.SubRouter.GET("/dashboard/ce", h.ceIndex)
	h.SubRouter.POST("/dashboard/ce/create", h.ceCreate)
	h.SubRouter.POST("/dashboard/ce/delete", h.ceDelete)
}
