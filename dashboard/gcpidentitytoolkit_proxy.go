package dashboard

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"time"

	"github.com/labstack/echo/v5"
)

const (
	gcpIDPDisableKey = "disableUser"
	gcpIDPLocalID    = "localId"
	gcpIDPMaxOffset  = 1 << 30
	gcpIDPPrefix     = "/dashboard/api/v1/gcp/identitytoolkit"
	gcpIDPProjects   = "projects"
	gcpIDPUsers      = usersResource
	gcpIDPOOB        = "oobcodes"
	gcpIDPTenants    = "tenants"
	gcpIDPConfig     = "config"
	gcpIDPAdminToken = "Bearer dashboard"
	gcpIDPTenantKey  = "tenantId"
	gcpIDPUserSegs   = 4
	gcpIDPActionSegs = 5
	gcpIDPProjSegs   = 3
	gcpIDPDefLimit   = 100
	gcpIDPMaxLimit   = 500
	gcpIDPMaxUID     = 128
	gcpIDPMaxClaims  = 1000
)

//nolint:gochecknoglobals // immutable allow-list
var (
	gcpIDPIDRE    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	gcpIDPActions = []string{iamVerbDisable, iamVerbEnable, "revoke", "claims"}
)

// registerGCPIdentityToolkitRoutes serves the Identity Platform page by dispatching in-process to the emulator
// with a fixed admin credential. Only user listing, creation, disable/enable, deletion, custom claims and token
// revocation, stored out-of-band codes, tenants and the email/anonymous sign-in toggles are reachable. Every write
// forwards a body rebuilt from fixed fields, and every response is rebuilt from a field allow-list so password
// hashes, salts, signing keys and API keys never reach the browser.
func (h *DashboardHandler) registerGCPIdentityToolkitRoutes() {
	var inner *echo.Echo

	ops := h.config.GCPIdentityToolkitOps
	if ops != nil {
		inner = echo.New()
		inner.Any("/*", ops.Handler())
	}

	serve := ociGuard(inner != nil, "Identity Platform", func(c *echo.Context) error {
		r := c.Request()

		parts, ok := ociSplitPath(r.URL.EscapedPath()[len(gcpIDPPrefix):])
		if !ok {
			return ociInvalid(c, "unsupported Identity Platform proxy request")
		}

		if len(parts) == 1 && parts[0] == gcpIDPProjects && r.Method == http.MethodGet {
			return c.JSON(http.StatusOK, map[string]any{gcpIDPProjects: ops.Backend.ProjectIDs()})
		}

		px := &gcpIDPRequest{c: c, inner: inner, project: "", tenant: r.URL.Query().Get(gcpIDPTenantKey)}

		return px.route(parts)
	})

	for _, m := range []string{http.MethodGet, http.MethodPost, http.MethodPatch, http.MethodDelete} {
		h.SubRouter.Add(m, gcpIDPPrefix+"/*", serve)
	}
}

type gcpIDPRequest struct {
	c       *echo.Context
	inner   *echo.Echo
	project string
	tenant  string
}

func (p *gcpIDPRequest) reject() error {
	return ociInvalid(p.c, "unsupported Identity Platform proxy request")
}

func (p *gcpIDPRequest) route(parts []string) error {
	if !p.scoped(parts) {
		return p.reject()
	}

	p.project = parts[1]
	method := p.c.Request().Method

	switch len(parts) {
	case gcpIDPProjSegs:
		return p.collection(method, parts[2])
	case gcpIDPUserSegs:
		if method == http.MethodDelete && parts[2] == gcpIDPUsers && validUID(parts[3]) {
			return p.deleteUser(parts[3])
		}
	case gcpIDPActionSegs:
		if method == http.MethodPost && parts[2] == gcpIDPUsers && validUID(parts[3]) &&
			slices.Contains(gcpIDPActions, parts[4]) {
			return p.userAction(parts[3], parts[4])
		}
	}

	return p.reject()
}

func (p *gcpIDPRequest) scoped(parts []string) bool {
	return len(parts) >= gcpIDPProjSegs && parts[0] == gcpIDPProjects && gcpIDPIDRE.MatchString(parts[1]) &&
		(p.tenant == "" || gcpIDPIDRE.MatchString(p.tenant))
}

func validUID(uid string) bool {
	return uid != "" && len(uid) <= gcpIDPMaxUID && uid != "." && uid != ".."
}

func (p *gcpIDPRequest) collection(method, kind string) error {
	switch {
	case method == http.MethodGet && kind == gcpIDPUsers:
		return p.listUsers()
	case method == http.MethodPost && kind == gcpIDPUsers:
		return p.createUser()
	case method == http.MethodGet && kind == gcpIDPOOB:
		return p.listOOB()
	case method == http.MethodGet && kind == gcpIDPTenants:
		return p.listTenants()
	case method == http.MethodGet && kind == gcpIDPConfig:
		return p.getConfig()
	case method == http.MethodPatch && kind == gcpIDPConfig:
		return p.patchConfig()
	}

	return p.reject()
}

// call dispatches one upstream request with the fixed admin credential and no caller-supplied bytes.
func (p *gcpIDPRequest) call(method, path, rawQuery string, body any) (*httptest.ResponseRecorder, error) {
	var payload []byte

	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err //nolint:wrapcheck // surfaced as a 400 by the caller
		}

		payload = b
	}

	r := p.c.Request()
	fwd := r.Clone(r.Context())
	fwd.Method, fwd.URL.Path, fwd.URL.RawPath, fwd.URL.RawQuery = method, path, "", rawQuery
	fwd.RequestURI = ""
	fwd.Body, fwd.ContentLength = io.NopCloser(bytes.NewReader(payload)), int64(len(payload))
	fwd.Header = http.Header{"Authorization": {gcpIDPAdminToken}, "Content-Type": {echo.MIMEApplicationJSON}}

	rec := httptest.NewRecorder()
	p.inner.ServeHTTP(rec, fwd)

	return rec, nil
}

// upstream relays a failed upstream response; it reports true when the response was an error.
func (p *gcpIDPRequest) failed(rec *httptest.ResponseRecorder) (bool, error) {
	if rec.Code < http.StatusBadRequest {
		return false, nil
	}

	p.c.Response().Header().Set("Content-Type", "application/json")

	return true, p.c.JSONBlob(rec.Code, rec.Body.Bytes())
}

// run performs an upstream call and hands a successful body to view, which rebuilds the browser response.
func (p *gcpIDPRequest) run(method, path, rawQuery string, body any, view func([]byte) (any, error)) error {
	rec, err := p.call(method, path, rawQuery, body)
	if err != nil {
		return ociInvalid(p.c, "invalid Identity Platform proxy request body")
	}

	if done, ferr := p.failed(rec); done {
		return ferr
	}

	out, err := view(rec.Body.Bytes())
	if err != nil {
		return cloudJSONFail(p.c, http.StatusBadGateway, "UNAVAILABLE", "unreadable Identity Platform response")
	}

	return p.c.JSON(http.StatusOK, out)
}

func (p *gcpIDPRequest) base() string { return "/v1/projects/" + url.PathEscape(p.project) }

func (p *gcpIDPRequest) listUsers() error {
	limit, ok := boundedInt(p.c.QueryParam("limit"), gcpIDPDefLimit, 1, gcpIDPMaxLimit)
	offset, ok2 := boundedInt(p.c.QueryParam("offset"), 0, 0, gcpIDPMaxOffset)

	if !ok || !ok2 {
		return ociInvalid(p.c, "invalid limit or offset")
	}

	body := map[string]any{
		gcpIDPTenantKey: p.tenant, "limit": strconv.Itoa(limit), "offset": strconv.Itoa(offset),
		"sortBy": "CREATED_AT", "order": "DESC",
	}

	return p.run(http.MethodPost, p.base()+"/accounts:query", "", body, idpUsersView)
}

func (p *gcpIDPRequest) createUser() error {
	var in struct {
		Email         string `json:"email"`
		Password      string `json:"password"`
		DisplayName   string `json:"displayName"`
		EmailVerified bool   `json:"emailVerified"`
	}

	if !ociDecodeBody(p.c, &in) || in.Email == "" || in.Password == "" {
		return ociInvalid(p.c, "email and password are required")
	}

	body := map[string]any{
		"email": in.Email, "password": in.Password, "emailVerified": in.EmailVerified,
		gcpIDPTenantKey: p.tenant,
	}
	if in.DisplayName != "" {
		body["displayName"] = in.DisplayName
	}

	return p.run(http.MethodPost, p.base()+"/accounts", "", body, idpLocalIDView)
}

func (p *gcpIDPRequest) deleteUser(uid string) error {
	body := map[string]any{gcpIDPLocalID: uid, gcpIDPTenantKey: p.tenant}

	return p.run(http.MethodPost, p.base()+"/accounts:delete", "", body, idpEmptyView)
}

func (p *gcpIDPRequest) userAction(uid, action string) error {
	body := map[string]any{gcpIDPLocalID: uid, gcpIDPTenantKey: p.tenant}

	switch action {
	case iamVerbDisable, iamVerbEnable:
		body[gcpIDPDisableKey] = action == iamVerbDisable
	case "revoke":
		body["validSince"] = strconv.FormatInt(time.Now().Unix(), 10)
	default:
		var in struct {
			Claims map[string]any `json:"claims"`
		}

		if !ociDecodeBody(p.c, &in) || in.Claims == nil {
			return ociInvalid(p.c, "claims must be a JSON object")
		}

		raw, err := json.Marshal(in.Claims)
		if err != nil || len(raw) > gcpIDPMaxClaims {
			return ociInvalid(p.c, "claims must be a JSON object under 1000 bytes")
		}

		body["customAttributes"] = string(raw)
	}

	return p.run(http.MethodPost, p.base()+"/accounts:update", "", body, idpLocalIDView)
}

func (p *gcpIDPRequest) listOOB() error {
	q := url.Values{}
	if p.tenant != "" {
		q.Set(gcpIDPTenantKey, p.tenant)
	}

	path := "/emulator/v1/projects/" + url.PathEscape(p.project) + "/oobCodes"

	return p.run(http.MethodGet, path, q.Encode(), nil, idpOOBView)
}

func (p *gcpIDPRequest) listTenants() error {
	q := url.Values{}

	for _, k := range []string{"pageSize", "pageToken"} {
		if v := p.c.QueryParam(k); v != "" {
			q.Set(k, v)
		}
	}

	return p.run(http.MethodGet, "/v2/projects/"+url.PathEscape(p.project)+"/tenants", q.Encode(), nil, idpTenantsView)
}

func (p *gcpIDPRequest) getConfig() error {
	return p.run(http.MethodGet, "/v2/projects/"+url.PathEscape(p.project)+"/config", "", nil, idpConfigView)
}

func (p *gcpIDPRequest) patchConfig() error {
	var in struct {
		Email     *bool `json:"emailEnabled"`
		Anonymous *bool `json:"anonymousEnabled"`
	}

	if !ociDecodeBody(p.c, &in) || (in.Email == nil && in.Anonymous == nil) {
		return ociInvalid(p.c, "emailEnabled or anonymousEnabled is required")
	}

	signIn, mask := map[string]any{}, []string{}

	if in.Email != nil {
		signIn["email"] = map[string]any{"enabled": *in.Email}

		mask = append(mask, "signIn.email.enabled")
	}

	if in.Anonymous != nil {
		signIn["anonymous"] = map[string]any{"enabled": *in.Anonymous}

		mask = append(mask, "signIn.anonymous.enabled")
	}

	q := url.Values{"updateMask": {joinComma(mask)}}

	return p.run(http.MethodPatch, "/v2/projects/"+url.PathEscape(p.project)+"/config", q.Encode(),
		map[string]any{"signIn": signIn}, idpConfigView)
}
