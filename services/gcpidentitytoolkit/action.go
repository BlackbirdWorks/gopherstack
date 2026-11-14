package gcpidentitytoolkit

import (
	"errors"
	"html/template"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	modeVerifyEmail  = "verifyEmail"
	modeVerifyChange = "verifyAndChangeEmail"
	modeSignIn       = "signIn"
)

type actionPage struct {
	Fields   map[string]string
	Title    string
	Message  string
	Continue string
	Form     bool
}

//nolint:gochecknoglobals // parsed once
var actionTmpl = template.Must(template.New("action").Parse(`<!doctype html>
<html><head><meta charset="utf-8"><title>{{.Title}}</title></head>
<body><h1>{{.Title}}</h1><p>{{.Message}}</p>
{{if .Form}}<form method="get" action="/emulator/action">
{{range $k, $v := .Fields}}<input type="hidden" name="{{$k}}" value="{{$v}}">{{end}}
<label>New password <input type="password" name="newPassword"></label>
<button type="submit">Reset password</button></form>{{end}}
{{if .Continue}}<p><a href="{{.Continue}}">Continue</a></p>{{end}}
</body></html>`))

// findCode locates an out-of-band code in any project and returns its owning project and a copy of the record.
func (b *InMemoryBackend) findCode(code string) (string, oobRec, bool) {
	b.mu.RLock("findCode")
	defer b.mu.RUnlock()

	for name, p := range b.projects {
		if rec, ok := p.Codes[code]; ok {
			return name, *rec, true
		}
	}

	return "", oobRec{}, false
}

// applyAction applies the out-of-band link behind /emulator/action and returns the project's authorized domains.
// A resetPassword link without newPassword only validates the code.
func (b *InMemoryBackend) applyAction(host, mode, code, newPassword string) (string, []string, error) {
	project, rec, ok := b.findCode(code)
	if !ok {
		return "", nil, idtk("INVALID_OOB_CODE")
	}

	b.mu.RLock("applyAction")
	authorized := slices.Clone(b.projects[project].Config.AuthorizedDomains)
	b.mu.RUnlock()

	email, err := b.applyOob(call{project: project, tenant: rec.TenantID, host: host}, rec, mode, code, newPassword)

	return email, authorized, err
}

func (b *InMemoryBackend) applyOob(cl call, rec oobRec, mode, code, newPassword string) (string, error) {
	if oobModes[rec.RequestType] != mode {
		return "", idtk("INVALID_OOB_CODE")
	}

	switch mode {
	case modeVerifyEmail, modeVerifyChange:
		if _, err := b.Update(cl, &authRequest{OobCode: code}); err != nil {
			return "", err
		}

		return rec.Email, nil
	case opResetPassword:
		if _, err := b.ResetPassword(cl, &authRequest{OobCode: code, NewPassword: newPassword}); err != nil {
			return "", err
		}

		return rec.Email, nil
	}

	if _, err := b.ResetPassword(cl, &authRequest{OobCode: code}); err != nil {
		return "", err
	}

	return rec.Email, nil
}

func (h *Handler) emulatorAction(c *echo.Context) error {
	q := c.Request().URL.Query()
	mode, code, newPW := q.Get("mode"), q.Get("oobCode"), q.Get("newPassword")

	email, authorized, err := h.Backend.applyAction(c.Request().Host, mode, code, newPW)
	if err != nil {
		msg := "The action code is invalid."

		if ae, ok := errors.AsType[*APIError](err); ok {
			msg = ae.Message
		}

		return renderAction(c, http.StatusBadRequest, actionPage{Title: "Action failed", Message: msg})
	}

	page := actionPage{Title: "Done", Message: "The action was applied for " + email + "."}

	switch {
	case mode == opResetPassword && newPW == "":
		page = actionPage{
			Title: "Reset password", Message: "Choose a new password for " + email + ".", Form: true,
			Fields: map[string]string{"mode": mode, "oobCode": code, "tenantId": q.Get("tenantId")},
		}
	case mode == modeSignIn:
		page.Message = "Sign-in link for " + email + " is valid; finish by calling accounts:signInWithEmailLink."
	}

	page.Continue = continueLink(q, mode, code, authorized)

	return renderAction(c, http.StatusOK, page)
}

func continueLink(q url.Values, mode, code string, authorized []string) string {
	raw := q.Get("continueUrl")
	if raw == "" || checkContinueURL(authorized, raw) != nil {
		return ""
	}

	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}

	if mode == modeSignIn {
		qq := u.Query()
		qq.Set("mode", mode)
		qq.Set("oobCode", code)
		qq.Set("apiKey", fakeAPIKey)
		u.RawQuery = qq.Encode()
	}

	return strings.TrimSpace(u.String())
}

func renderAction(c *echo.Context, status int, p actionPage) error {
	c.Response().Header().Set("Content-Type", "text/html; charset=utf-8")
	c.Response().Header().Set("Cache-Control", "no-store")
	c.Response().WriteHeader(status)

	return actionTmpl.Execute(c.Response(), p)
}
