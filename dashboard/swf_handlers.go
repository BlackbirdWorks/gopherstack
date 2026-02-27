package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// swfDomainView is the view model for a single SWF domain.
type swfDomainView struct {
	Name        string
	Description string
	Status      string
}

// swfIndexData is the template data for the SWF index page.
type swfIndexData struct {
	PageData

	Domains []swfDomainView
}

// swfIndex renders the SWF dashboard index.
func (h *DashboardHandler) swfIndex(c *echo.Context) error {
	w := c.Response()

	if h.SWFOps == nil {
		h.renderTemplate(w, "swf/index.html", swfIndexData{
			PageData: PageData{Title: "SWF Domains", ActiveTab: "swf"},
			Domains:  []swfDomainView{},
		})

		return nil
	}

	domains := h.SWFOps.Backend.ListDomains("")
	views := make([]swfDomainView, 0, len(domains))

	for _, d := range domains {
		views = append(views, swfDomainView{
			Name:        d.Name,
			Description: d.Description,
			Status:      d.Status,
		})
	}

	h.renderTemplate(w, "swf/index.html", swfIndexData{
		PageData: PageData{Title: "SWF Domains", ActiveTab: "swf"},
		Domains:  views,
	})

	return nil
}

// swfRegisterDomain handles POST /dashboard/swf/register.
func (h *DashboardHandler) swfRegisterDomain(c *echo.Context) error {
	if h.SWFOps == nil {
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

	if err := h.SWFOps.Backend.RegisterDomain(name, description); err != nil {
		h.Logger.Error("failed to register SWF domain", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/swf")
}
