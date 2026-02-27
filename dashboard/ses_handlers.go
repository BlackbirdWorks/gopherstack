package dashboard

import (
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

// sesEmailView is the view model for a single email in the inbox list.
type sesEmailView struct {
	Timestamp    time.Time
	MessageID    string
	From         string
	ToStr        string
	Subject      string
	TimestampStr string
}

// sesEmailDetailView is the view model for the email detail page.
type sesEmailDetailView struct {
	PageData

	Timestamp    time.Time
	MessageID    string
	From         string
	Subject      string
	BodyHTML     template.HTML
	BodyText     string
	RawMessage   string
	ToStr        string
	TimestampStr string
	DefaultTab   string
}

// sesIndexData is the template data for the SES inbox page.
type sesIndexData struct {
	PageData

	Emails     []sesEmailView
	Identities []string
}

// sesIndex renders the SES inbox page.
func (h *DashboardHandler) sesIndex(c *echo.Context) error {
	w := c.Response()

	if h.SESOps == nil {
		h.renderTemplate(w, "ses/index.html", sesIndexData{
			PageData:   PageData{Title: "SES Inbox", ActiveTab: "ses"},
			Emails:     []sesEmailView{},
			Identities: []string{},
		})

		return nil
	}

	emails := h.SESOps.Backend.ListEmails()
	emailViews := make([]sesEmailView, 0, len(emails))

	for _, e := range emails {
		emailViews = append(emailViews, sesEmailView{
			MessageID:    e.MessageID,
			From:         e.From,
			ToStr:        strings.Join(e.To, ", "),
			Subject:      e.Subject,
			Timestamp:    e.Timestamp,
			TimestampStr: e.Timestamp.Format(time.RFC3339),
		})
	}

	identities := h.SESOps.Backend.ListIdentities()

	h.renderTemplate(w, "ses/index.html", sesIndexData{
		PageData:   PageData{Title: "SES Inbox", ActiveTab: "ses"},
		Emails:     emailViews,
		Identities: identities,
	})

	return nil
}

// sesEmailDetail renders the detail page for a single email.
func (h *DashboardHandler) sesEmailDetail(c *echo.Context) error {
	w := c.Response()

	if h.SESOps == nil {
		return c.NoContent(http.StatusNotFound)
	}

	msgID := c.QueryParam("id")
	if msgID == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	email, err := h.SESOps.Backend.GetEmailByID(msgID)
	if err != nil {
		return c.NoContent(http.StatusNotFound)
	}

	raw := fmt.Sprintf(
		"From: %s\r\nTo: %s\r\nSubject: %s\r\nDate: %s\r\n\r\n%s",
		email.From,
		strings.Join(email.To, ", "),
		email.Subject,
		email.Timestamp.Format(time.RFC1123Z),
		email.BodyText,
	)

	defaultTab := "raw-body"
	if email.BodyHTML != "" {
		defaultTab = "html-body"
	} else if email.BodyText != "" {
		defaultTab = "text-body"
	}

	h.renderTemplate(w, "ses/email_detail.html", sesEmailDetailView{
		PageData:     PageData{Title: email.Subject, ActiveTab: "ses"},
		MessageID:    email.MessageID,
		From:         email.From,
		ToStr:        strings.Join(email.To, ", "),
		Subject:      email.Subject,
		BodyHTML:     template.HTML(email.BodyHTML), //nolint:gosec // controlled test-only SES content
		BodyText:     email.BodyText,
		RawMessage:   raw,
		Timestamp:    email.Timestamp,
		TimestampStr: email.Timestamp.Format(time.RFC3339),
		DefaultTab:   defaultTab,
	})

	return nil
}

// sesVerifyIdentity handles POST /dashboard/ses/identity/verify.
func (h *DashboardHandler) sesVerifyIdentity(c *echo.Context) error {
	if h.SESOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	identity := c.Request().FormValue("identity")
	if identity == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.SESOps.Backend.VerifyEmailIdentity(identity); err != nil {
		h.Logger.Error("failed to verify SES identity", "identity", identity, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/ses")
}

// sesDeleteIdentity handles POST /dashboard/ses/identity/delete.
func (h *DashboardHandler) sesDeleteIdentity(c *echo.Context) error {
	if h.SESOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	identity := c.Request().FormValue("identity")
	if identity == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.SESOps.Backend.DeleteIdentity(identity); err != nil {
		h.Logger.Error("failed to delete SES identity", "identity", identity, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/ses")
}
