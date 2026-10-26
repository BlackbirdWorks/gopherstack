package dashboard

import (
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	fnbackend "github.com/blackbirdworks/gopherstack/services/ocifunctions"
)

const (
	ociFunctionsPrefix = ociProxyPrefix + "/functions"
	ociConfigMask      = "••••••••"
	ociInvokeMaxBody   = 1 << 20
)

var ociSecretKeyRe = regexp.MustCompile( //nolint:gochecknoglobals // compiled once
	`(?i)(secret|passw|pwd|token|credential|private|auth|bearer|signature|api[_-]?key|access[_-]?key|[_-]key$|^key)`)

type ociFunctionsAPI struct {
	b    *fnbackend.InMemoryBackend
	port int
}

type ociInvokeIn struct {
	Body        string `json:"body"`
	ContentType string `json:"contentType"`
	DryRun      bool   `json:"dryRun"`
}

type ociInvokeOut struct {
	Headers map[string]string `json:"headers"`
	Text    string            `json:"text,omitempty"`
	Base64  string            `json:"base64"`
	CallID  string            `json:"callId,omitempty"`
	Status  int               `json:"status"`
	IsText  bool              `json:"isText"`
}

// registerOCIFunctionsRoutes serves the Functions page from the backend in-process. Invocation is synchronous
// and addresses the fixed in-process backend only; configuration values whose key looks like a secret are masked.
func (h *DashboardHandler) registerOCIFunctionsRoutes() {
	api := &ociFunctionsAPI{}
	if h.config.OCIFunctionsOps != nil {
		api.b = h.config.OCIFunctionsOps.Backend
		api.port = h.config.OCIFunctionsOps.Port
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc {
		return ociGuard(api.b != nil, "OCI Functions", next)
	}

	r := h.SubRouter
	p := ociFunctionsPrefix
	r.Add(http.MethodGet, p+"/applications", guard(api.applications))
	r.Add(http.MethodGet, p+"/applications/:app", guard(api.application))
	r.Add(http.MethodGet, p+"/applications/:app/functions", guard(api.functions))
	r.Add(http.MethodGet, p+"/functions/:fn", guard(api.function))
	r.Add(http.MethodPost, p+"/functions/:fn/invoke", guard(api.invoke))
	r.Add(http.MethodGet, p+"/workrequests", guard(api.workRequests))
	r.Add(http.MethodGet, p+"/workrequests/:id/logs", guard(api.workRequestLogs))
}

// maskedConfig renders a wire value as JSON with secret-looking config values replaced.
func maskedConfig(v any) (map[string]any, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	out := map[string]any{}
	if err = json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}

	cfg, ok := out["config"].(map[string]any)
	if !ok {
		return out, nil
	}

	masked := []string{}

	for k := range cfg {
		if ociSecretKeyRe.MatchString(k) {
			cfg[k] = ociConfigMask
			masked = append(masked, k)
		}
	}

	out["maskedConfigKeys"] = masked

	return out, nil
}

func (a *ociFunctionsAPI) endpoint(c *echo.Context) string {
	host := c.Request().Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	if host == "" {
		host = "localhost"
	}

	return "http://" + net.JoinHostPort(host, strconv.Itoa(a.port))
}

func (a *ociFunctionsAPI) applications(c *echo.Context) error {
	items, err := ociAcross(a.b.Compartments(), func(comp string) ([]any, error) {
		return ociCollect(func(token string) (page.Page[any], error) {
			pg, perr := a.b.ListApplications(comp, "", "", "", fnbackend.ListOpts{Page: token, Limit: ociListLimit})

			return anyPage(pg.Data, pg.Next), perr
		})
	})
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"applications": items})
}

func (a *ociFunctionsAPI) application(c *echo.Context) error {
	app, _, err := a.b.GetApplication(c.Param("app"))
	if err != nil {
		return ociFail(c, err)
	}

	out, err := maskedConfig(app)
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, out)
}

func (a *ociFunctionsAPI) functions(c *echo.Context) error {
	endpoint := a.endpoint(c)

	items, err := ociCollect(func(token string) (page.Page[any], error) {
		pg, perr := a.b.ListFunctions(
			c.Param("app"), "", "", "", endpoint, fnbackend.ListOpts{Page: token, Limit: ociListLimit})

		return anyPage(pg.Data, pg.Next), perr
	})
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"functions": items})
}

func (a *ociFunctionsAPI) function(c *echo.Context) error {
	fn, _, err := a.b.GetFunction(c.Param("fn"), a.endpoint(c))
	if err != nil {
		return ociFail(c, err)
	}

	out, err := maskedConfig(fn)
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, out)
}

func (a *ociFunctionsAPI) invoke(c *echo.Context) error {
	var in ociInvokeIn
	if !cloudJSONDecode(c, &in) {
		return nil
	}

	if len(in.Body) > ociInvokeMaxBody {
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "body is too large")
	}

	id := c.Param("fn")
	hdr := http.Header{}

	if in.ContentType != "" {
		hdr.Set("Content-Type", in.ContentType)
	}

	res, err := a.b.Invoke(c.Request().Context(), id, fnbackend.InvokeIn{
		Header: hdr, Body: []byte(in.Body), DryRun: in.DryRun,
		URL: "/20181201/functions/" + url.PathEscape(id) + "/actions/invoke",
	})
	if err != nil {
		return ociFail(c, err)
	}

	out := ociInvokeOut{Status: res.Status, CallID: res.CallID, Headers: map[string]string{}, Base64: base64.StdEncoding.EncodeToString(res.Body)}

	for k, vs := range res.Header {
		out.Headers[k] = strings.Join(vs, ", ")
	}

	if utf8.Valid(res.Body) {
		out.IsText, out.Text = true, string(res.Body)
	}

	c.Response().Header().Set("Cache-Control", "no-store")

	return c.JSON(http.StatusOK, out)
}

func (a *ociFunctionsAPI) workRequests(c *echo.Context) error {
	resource := c.QueryParam("resourceId")

	items, err := ociAcross(a.b.Compartments(), func(comp string) ([]any, error) {
		return ociCollect(func(token string) (page.Page[any], error) {
			pg, perr := a.b.ListWorkRequests(comp, resource, "", fnbackend.ListOpts{Page: token, Limit: ociListLimit})

			return anyPage(pg.Data, pg.Next), perr
		})
	})
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"workRequests": items})
}

func (a *ociFunctionsAPI) workRequestLogs(c *echo.Context) error {
	items, err := ociCollect(func(token string) (page.Page[any], error) {
		pg, perr := a.b.WorkRequestLogs(c.Param("id"), fnbackend.ListOpts{Page: token, Limit: ociListLimit})

		return anyPage(pg.Data, pg.Next), perr
	})
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"logs": items})
}
