package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// supportCaseView is the view model for a single support case.
type supportCaseView struct {
	CaseID       string
	Subject      string
	Status       string
	ServiceCode  string
	CategoryCode string
	SeverityCode string
}

// supportIndexData is the template data for the Support index page.
type supportIndexData struct {
	PageData

	Cases []supportCaseView
}

// supportIndex renders the Support dashboard index.
func (h *DashboardHandler) supportIndex(c *echo.Context) error {
	w := c.Response()

	if h.SupportOps == nil {
		h.renderTemplate(w, "support/index.html", supportIndexData{
			PageData: PageData{Title: "Support Cases", ActiveTab: "support",
				Snippet: &SnippetData{
					ID:    "support-operations",
					Title: "Using Support",
					Cli:   "aws support help --endpoint-url http://localhost:8000",
					Go:    "/* Write AWS SDK v2 Code for Support */",
					Python: "# Write boto3 code for Support\nimport boto3\n" +
						"client = boto3.client('support', endpoint_url='http://localhost:8000')",
				}},
			Cases: []supportCaseView{},
		})

		return nil
	}

	cases := h.SupportOps.Backend.DescribeCases(nil)
	views := make([]supportCaseView, 0, len(cases))

	for _, cs := range cases {
		views = append(views, supportCaseView{
			CaseID:       cs.CaseID,
			Subject:      cs.Subject,
			Status:       cs.Status,
			ServiceCode:  cs.ServiceCode,
			CategoryCode: cs.CategoryCode,
			SeverityCode: cs.SeverityCode,
		})
	}

	h.renderTemplate(w, "support/index.html", supportIndexData{
		PageData: PageData{Title: "Support Cases", ActiveTab: "support",
			Snippet: &SnippetData{
				ID:    "support-operations",
				Title: "Using Support",
				Cli:   "aws support help --endpoint-url http://localhost:8000",
				Go:    "/* Write AWS SDK v2 Code for Support */",
				Python: `# Write boto3 code for Support
import boto3
client = boto3.client('support', endpoint_url='http://localhost:8000')`,
			}},
		Cases: views,
	})

	return nil
}

// supportCreateCase handles POST /dashboard/support/create.
func (h *DashboardHandler) supportCreateCase(c *echo.Context) error {
	if h.SupportOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	subject := c.Request().FormValue("subject")
	serviceCode := c.Request().FormValue("serviceCode")
	categoryCode := c.Request().FormValue("categoryCode")
	severityCode := c.Request().FormValue("severityCode")
	body := c.Request().FormValue("body")

	if subject == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.SupportOps.Backend.CreateCase(subject, serviceCode, categoryCode, severityCode, body); err != nil {
		h.Logger.Error("failed to create support case", "subject", subject, "error", err)

		return c.NoContent(http.StatusInternalServerError)
	}

	return c.Redirect(http.StatusFound, "/dashboard/support")
}
