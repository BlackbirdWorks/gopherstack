package dashboard

import (
	"github.com/labstack/echo/v5"

	stsbackend "github.com/blackbirdworks/gopherstack/sts"
)

// stsIndex renders the STS caller-identity overview page.
func (h *DashboardHandler) stsIndex(c *echo.Context) error {
	w := c.Response()

	data := struct {
		PageData

		Account string
		Arn     string
		UserID  string
	}{
		PageData: PageData{
			Title:     "STS Security Token Service",
			ActiveTab: "sts",
		},
		Account: stsbackend.MockAccountID,
		Arn:     stsbackend.MockUserArn,
		UserID:  stsbackend.MockUserID,
	}

	h.renderTemplate(w, "sts/index.html", data)

	return nil
}
