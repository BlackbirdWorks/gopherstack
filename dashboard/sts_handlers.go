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
		Snippet: &SnippetData{
			ID:    "sts-operations",
			Title: "Using Sts",
			Cli:   "aws sts help --endpoint-url http://localhost:8000",
			Go: "/* Write AWS SDK v2 Code for Sts */",
			Python: "# Write boto3 code for Sts\nimport boto3\nclient = boto3.client('sts', endpoint_url='http://localhost:8000')",
		},
		},
		Account: stsbackend.MockAccountID,
		Arn:     stsbackend.MockUserArn,
		UserID:  stsbackend.MockUserID,
	}

	h.renderTemplate(w, "sts/index.html", data)

	return nil
}
