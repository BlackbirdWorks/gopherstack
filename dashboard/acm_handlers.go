package dashboard

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

// acmCertView is the view model for a single ACM certificate.
type acmCertView struct {
	ARN        string
	ARNShort   string
	DomainName string
	Status     string
	Type       string
	CreatedAt  string
}

// acmIndexData is the template data for the ACM index page.
type acmIndexData struct {
	PageData

	Certificates []acmCertView
}

// acmIndex renders the list of all ACM certificates.
func (h *DashboardHandler) acmIndex(c *echo.Context) error {
	w := c.Response()

	if h.ACMOps == nil {
		h.renderTemplate(w, "acm/index.html", acmIndexData{
			PageData:     PageData{Title: "ACM Certificates", ActiveTab: "acm"},
			Certificates: []acmCertView{},
		})

		return nil
	}

	certs := h.ACMOps.Backend.ListCertificates()
	views := make([]acmCertView, 0, len(certs))

	for _, cert := range certs {
		arnShort := cert.ARN
		if parts := strings.Split(cert.ARN, "/"); len(parts) > 1 {
			arnShort = fmt.Sprintf(".../%s", parts[len(parts)-1])
		}

		views = append(views, acmCertView{
			ARN:        cert.ARN,
			ARNShort:   arnShort,
			DomainName: cert.DomainName,
			Status:     cert.Status,
			Type:       cert.Type,
			CreatedAt:  cert.CreatedAt.Format(time.RFC3339),
		})
	}

	h.renderTemplate(w, "acm/index.html", acmIndexData{
		PageData:     PageData{Title: "ACM Certificates", ActiveTab: "acm"},
		Certificates: views,
	})

	return nil
}

// acmRequestCertificate handles POST /dashboard/acm/request.
func (h *DashboardHandler) acmRequestCertificate(c *echo.Context) error {
	if h.ACMOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	domainName := c.Request().FormValue("domain_name")
	if domainName == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if _, err := h.ACMOps.Backend.RequestCertificate(domainName, ""); err != nil {
		h.Logger.Error("failed to request ACM certificate", "domain", domainName, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/acm")
}

// acmDeleteCertificate handles POST /dashboard/acm/delete.
func (h *DashboardHandler) acmDeleteCertificate(c *echo.Context) error {
	if h.ACMOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	arn := c.Request().FormValue("arn")
	if arn == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.ACMOps.Backend.DeleteCertificate(arn); err != nil {
		h.Logger.Error("failed to delete ACM certificate", "arn", arn, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/acm")
}
