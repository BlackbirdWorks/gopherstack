package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// s3controlConfigView is the view model for a public access block config.
type s3controlConfigView struct {
	AccountID             string
	BlockPublicAcls       bool
	IgnorePublicAcls      bool
	BlockPublicPolicy     bool
	RestrictPublicBuckets bool
}

// s3controlIndexData is the template data for the S3 Control index page.
type s3controlIndexData struct {
	PageData

	Configs []s3controlConfigView
}

// s3controlIndex renders the S3 Control dashboard index.
func (h *DashboardHandler) s3controlIndex(c *echo.Context) error {
	w := c.Response()

	if h.S3ControlOps == nil {
		h.renderTemplate(w, "s3control/index.html", s3controlIndexData{
			PageData: PageData{Title: "S3 Control", ActiveTab: "s3control"},
			Configs:  []s3controlConfigView{},
		})

		return nil
	}

	h.renderTemplate(w, "s3control/index.html", s3controlIndexData{
		PageData: PageData{Title: "S3 Control", ActiveTab: "s3control"},
		Configs:  []s3controlConfigView{},
	})

	return nil
}

// s3controlPutConfig handles POST /dashboard/s3control/config.
func (h *DashboardHandler) s3controlPutConfig(c *echo.Context) error {
	if h.S3ControlOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	accountID := c.Request().FormValue("account_id")
	if accountID == "" {
		accountID = "default"
	}

	return c.Redirect(http.StatusFound, "/dashboard/s3control")
}
