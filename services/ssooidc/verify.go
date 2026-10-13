package ssooidc

import (
	"errors"
	"fmt"
	"html"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v5"
)

// Emulator-only browser endpoints behind verificationUri and authorizationEndpoint.
const (
	DevicePath    = "/_gopherstack/sso/device"
	AuthorizePath = "/_gopherstack/sso/authorize"

	actionApprove = "approve"
	actionDeny    = "deny"
)

func (h *Handler) resolve(userName string) (Subject, error) {
	r := h.resolver.Load()
	if r == nil {
		return Subject{}, ErrNoResolver
	}

	return (*r).ResolveSubject(userName)
}

func writePage(c *echo.Context, status int, title, body string) error {
	page := fmt.Sprintf(
		"<!doctype html><html><head><meta charset=\"utf-8\"><title>%s</title></head><body><h1>%s</h1>%s</body></html>",
		html.EscapeString(title), html.EscapeString(title), body,
	)

	return c.HTML(status, page)
}

func devicePrompt(userCode string) string {
	return fmt.Sprintf(
		`<form method="post" action="%s"><input type="hidden" name="user_code" value="%s">`+
			`<p>Authorize the device showing code <b>%s</b>.</p>`+
			`<p><label>User name (default user if empty) <input name="user"></label></p>`+
			`<button name="action" value="approve">Approve</button> <button name="action" value="deny">Deny</button>`+
			`</form>`,
		DevicePath, html.EscapeString(userCode), html.EscapeString(userCode),
	)
}

func codeEntryForm() string {
	return `<form method="get" action="` + DevicePath + `"><label>Code <input name="user_code"></label> ` +
		`<button>Continue</button></form>`
}

// ServeDeviceVerification serves the device-code verification page. With action=approve or
// action=deny (query or form) it completes the authorization; user selects the signed-in user.
func (h *Handler) ServeDeviceVerification(c *echo.Context) error {
	userCode := c.FormValue("user_code")
	if userCode == "" {
		return writePage(c, http.StatusOK, "Enter your code", codeEntryForm())
	}

	if !h.Backend.DeviceUserCodeValid(userCode) {
		return writePage(c, http.StatusNotFound, "Unknown or expired code",
			"<p>The code <b>"+html.EscapeString(userCode)+"</b> is not a pending authorization.</p>")
	}

	switch c.FormValue("action") {
	case actionApprove:
		return h.approveDevice(c, userCode)
	case actionDeny:
		if err := h.Backend.DenyDevice(userCode); err != nil {
			return writePage(c, http.StatusNotFound, "Unknown or expired code", "")
		}

		return writePage(c, http.StatusOK, "Request denied", "<p>You can close this window.</p>")
	}

	return writePage(c, http.StatusOK, "Authorize device", devicePrompt(userCode))
}

func (h *Handler) approveDevice(c *echo.Context, userCode string) error {
	userName := c.FormValue("user")

	subject, err := h.resolve(userName)
	if err != nil {
		return writeResolveError(c, userName, err)
	}

	if err = h.Backend.ApproveDevice(userCode, subject); err != nil {
		return writePage(c, http.StatusNotFound, "Unknown or expired code", "")
	}

	return writePage(c, http.StatusOK, "Request approved",
		"<p>Signed in as <b>"+html.EscapeString(subject.UserName)+"</b>. You can close this window.</p>")
}

func writeResolveError(c *echo.Context, userName string, err error) error {
	status := http.StatusServiceUnavailable
	if errors.Is(err, ErrUnknownUser) {
		status = http.StatusNotFound
	}

	return writePage(c, status, "Cannot sign in",
		"<p>"+html.EscapeString(err.Error())+" ("+html.EscapeString(userName)+")</p>")
}

// ServeAuthorize implements the authorization-code endpoint: it signs in the requested (or default)
// user and redirects to redirect_uri with a single-use code, requiring a PKCE S256 challenge.
func (h *Handler) ServeAuthorize(c *echo.Context) error {
	redirectURI := c.FormValue("redirect_uri")

	if c.FormValue("response_type") != "code" {
		return writePage(c, http.StatusBadRequest, "Invalid request", "<p>response_type must be code.</p>")
	}

	userName := c.FormValue("user")

	subject, err := h.resolve(userName)
	if err != nil {
		return writeResolveError(c, userName, err)
	}

	code, apiErr := h.Backend.IssueAuthCode(
		c.FormValue("client_id"), redirectURI, c.FormValue("code_challenge"),
		c.FormValue("code_challenge_method"), subject,
	)
	if apiErr != nil {
		return writePage(c, apiErr.Status, apiErr.Code, "<p>"+html.EscapeString(apiErr.Message)+"</p>")
	}

	target, perr := url.Parse(redirectURI)
	if perr != nil {
		return writePage(c, http.StatusBadRequest, "Invalid request", "<p>redirect_uri is malformed.</p>")
	}

	q := target.Query()
	q.Set("code", code)

	if state := c.FormValue("state"); state != "" {
		q.Set("state", state)
	}

	target.RawQuery = q.Encode()

	return c.Redirect(http.StatusFound, target.String())
}
