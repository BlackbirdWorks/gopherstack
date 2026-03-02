package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// resourceGroupView is the view model for a single resource group.
type resourceGroupView struct {
	Name        string
	ARN         string
	Description string
}

// resourcegroupsIndexData is the template data for the Resource Groups index page.
type resourcegroupsIndexData struct {
	PageData

	Groups []resourceGroupView
}

// resourcegroupsIndex renders the Resource Groups dashboard index.
func (h *DashboardHandler) resourcegroupsIndex(c *echo.Context) error {
	w := c.Response()

	if h.ResourceGroupsOps == nil {
		h.renderTemplate(w, "resourcegroups/index.html", resourcegroupsIndexData{
			PageData: PageData{Title: "Resource Groups", ActiveTab: "resourcegroups",
				Snippet: &SnippetData{
					ID:    "resourcegroups-operations",
					Title: "Using Resourcegroups",
					Cli:   "aws resourcegroups help --endpoint-url http://localhost:8000",
					Go:    "/* Write AWS SDK v2 Code for Resourcegroups */",
					Python: "# Write boto3 code for Resource Groups\nimport boto3\n" +
						"client = boto3.client('resource-groups', endpoint_url='http://localhost:8000')",
				}},
			Groups: []resourceGroupView{},
		})

		return nil
	}

	groups := h.ResourceGroupsOps.Backend.ListGroups()
	views := make([]resourceGroupView, 0, len(groups))

	for _, g := range groups {
		views = append(views, resourceGroupView{
			Name:        g.Name,
			ARN:         g.ARN,
			Description: g.Description,
		})
	}

	h.renderTemplate(w, "resourcegroups/index.html", resourcegroupsIndexData{
		PageData: PageData{Title: "Resource Groups", ActiveTab: "resourcegroups",
			Snippet: &SnippetData{
				ID:    "resourcegroups-operations",
				Title: "Using Resourcegroups",
				Cli:   "aws resourcegroups help --endpoint-url http://localhost:8000",
				Go:    "/* Write AWS SDK v2 Code for Resourcegroups */",
				Python: `# Write boto3 code for Resourcegroups
import boto3
client = boto3.client('resourcegroups', endpoint_url='http://localhost:8000')`,
			}},
		Groups: views,
	})

	return nil
}

// resourcegroupsCreate handles POST /dashboard/resourcegroups/create.
func (h *DashboardHandler) resourcegroupsCreate(c *echo.Context) error {
	if h.ResourceGroupsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	description := c.Request().FormValue("description")

	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.ResourceGroupsOps.Backend.CreateGroup(name, description, nil); err != nil {
		h.Logger.Error("failed to create resource group", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/resourcegroups")
}

// resourcegroupsDelete handles POST /dashboard/resourcegroups/delete.
func (h *DashboardHandler) resourcegroupsDelete(c *echo.Context) error {
	if h.ResourceGroupsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.ResourceGroupsOps.Backend.DeleteGroup(name); err != nil {
		h.Logger.Error("failed to delete resource group", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/resourcegroups")
}
