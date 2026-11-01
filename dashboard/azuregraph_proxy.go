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
	"strings"
	"time"
	"unicode/utf8"

	"github.com/labstack/echo/v5"
)

const (
	azureGraphPrefix   = "/dashboard/api/v1/azure/graph"
	graphPxMaxBody     = 1 << 20
	graphPxMaxName     = 120
	graphPxMaxPassword = 256
	graphPxMaxIDs      = 100
	graphPxMaxTop      = 999
	graphPxGraphHost   = "https://graph.microsoft.com/v1.0/directoryObjects/"
	graphPxApplication = "applications"
	graphPxPrincipals  = "serviceprincipals"
	graphPxUsers       = "users"
	graphPxGroups      = "groups"
	graphPxAddPassword = "addpassword"
	graphPxRemovePw    = "removepassword"
	graphPxMembers     = "members"
	graphPxRef         = "$ref"
	graphPxPassword    = "password"
	graphPxDisplayName = "displayName"
	graphPxItem        = 2
	graphPxSub         = 3
)

//nolint:gochecknoglobals // immutable allow-lists and validators
var (
	graphPxRoutes = map[string]bool{
		"GET applications": true, "POST applications": true, "GET applications/*": true,
		"DELETE applications/*": true, "GET applications/*/owners": true,
		"POST applications/*/addpassword": true, "POST applications/*/removepassword": true,
		"GET serviceprincipals": true, "POST serviceprincipals": true, "GET serviceprincipals/*": true,
		"GET serviceprincipals/*/approleassignments": true,
		"GET users": true, "POST users": true, "GET users/*": true,
		"GET groups": true, "POST groups": true, "GET groups/*": true, "GET groups/*/members": true,
		"POST groups/*/members/$ref": true, "DELETE groups/*/members/*/$ref": true,
		"POST directoryobjects/getbyids": true,
	}
	graphPxGUID     = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	graphPxSelectRE = regexp.MustCompile(`^[A-Za-z0-9_,]{1,200}$`)
	graphPxTokenRE  = regexp.MustCompile(`^[A-Za-z0-9_=.\-]{1,1000}$`)
	graphPxNickRE   = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	graphPxUPNRE    = regexp.MustCompile(`^[A-Za-z0-9._'+-]{1,64}@[A-Za-z0-9.-]{1,253}$`)
	graphPxSecrets  = []string{"secrettext", "passwordprofile", graphPxPassword}
)

type graphProxyErr struct {
	Error graphProxyErrBody `json:"error"`
}

type graphProxyErrBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// registerAzureGraphRoutes serves the Entra ID page by dispatching in-process to the Microsoft Graph handler with a
// server-minted token. Paths and methods are allow-listed, every write body is rebuilt from fixed fields, and
// secret-bearing fields are scrubbed from every response except the one addPassword answer that carries a new secret.
func (h *DashboardHandler) registerAzureGraphRoutes() {
	ops := h.config.AzureGraphOps

	var inner *echo.Echo

	if ops != nil {
		inner = echo.New()
		inner.Any("/*", ops.Handler())
	}

	serve := func(c *echo.Context) error {
		if inner == nil {
			return c.JSON(http.StatusServiceUnavailable, graphProxyErr{
				graphProxyErrBody{errServiceUnavailable, "Microsoft Graph is not enabled"},
			})
		}

		r := c.Request()
		rest := strings.TrimPrefix(r.URL.EscapedPath(), azureGraphPrefix)

		segs, ok := graphProxySegments(r.Method, rest)
		if !ok {
			return c.JSON(http.StatusBadRequest, graphProxyErr{
				graphProxyErrBody{"InvalidRequestUri", "unsupported Microsoft Graph proxy request"},
			})
		}

		in, err := io.ReadAll(http.MaxBytesReader(c.Response(), r.Body, graphPxMaxBody))
		if err != nil {
			return graphProxyBadBody(c)
		}

		body, ok := graphProxyBody(r.Method, segs, in)
		if !ok {
			return graphProxyBadBody(c)
		}

		token, err := ops.InProcessToken()
		if err != nil {
			return c.JSON(http.StatusInternalServerError, graphProxyErr{
				graphProxyErrBody{"InternalFailure", "could not authorise the Microsoft Graph call"},
			})
		}

		fwd := graphProxyRequest(r, segs, body, token)
		rec := httptest.NewRecorder()
		inner.ServeHTTP(rec, fwd)

		return graphProxyRelay(c, rec, graphIsAddPassword(r.Method, segs))
	}

	for _, m := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
		h.SubRouter.Add(m, azureGraphPrefix+"/*", serve)
	}
}

func graphProxyBadBody(c *echo.Context) error {
	return c.JSON(http.StatusBadRequest, graphProxyErr{
		graphProxyErrBody{"InvalidRequestContent", "invalid Microsoft Graph proxy request body"},
	})
}

func graphProxyRequest(r *http.Request, segs []string, body []byte, token string) *http.Request {
	fwd := r.Clone(r.Context())
	fwd.URL.Path = "/v1.0/" + strings.Join(segs, "/")
	fwd.URL.RawPath = ""
	fwd.URL.RawQuery = graphProxyQuery(r.URL.Query()).Encode()
	fwd.RequestURI = ""
	fwd.Header = http.Header{echo.HeaderAuthorization: {"Bearer " + token}}
	fwd.Body, fwd.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))

	if len(body) > 0 {
		fwd.Header.Set("Content-Type", "application/json")
	}

	return fwd
}

// graphProxySegments validates the proxied path and returns its decoded segments, keeping the case the caller sent.
func graphProxySegments(method, raw string) ([]string, bool) {
	parts := strings.Split(strings.Trim(raw, "/"), "/")

	for i, p := range parts {
		dec, err := url.PathUnescape(p)
		if err != nil || dec == "" || strings.ContainsAny(dec, "/:?#\\") || dec == "." || dec == ".." {
			return nil, false
		}

		parts[i] = dec
	}

	if !graphProxyAllowed(method, graphLower(parts)) {
		return nil, false
	}

	return parts, true
}

func graphLower(parts []string) []string {
	out := make([]string, len(parts))
	for i, p := range parts {
		out[i] = strings.ToLower(p)
	}

	return out
}

// graphProxyAllowed matches the lower-cased path, with every GUID segment folded to "*", against graphPxRoutes.
func graphProxyAllowed(method string, p []string) bool {
	shape := make([]string, len(p))

	for i, seg := range p {
		shape[i] = seg
		if graphPxGUID.MatchString(seg) {
			shape[i] = "*"
		}
	}

	return graphPxRoutes[method+" "+strings.Join(shape, "/")]
}

func graphIsAddPassword(method string, segs []string) bool {
	return method == http.MethodPost && len(segs) == graphPxSub && strings.EqualFold(segs[2], graphPxAddPassword)
}

func graphProxyQuery(in url.Values) url.Values {
	q := url.Values{}

	if v := in.Get("$select"); graphPxSelectRE.MatchString(v) {
		q.Set("$select", v)
	}

	if v := in.Get("$skiptoken"); graphPxTokenRE.MatchString(v) {
		q.Set("$skiptoken", v)
	}

	if n, err := strconv.Atoi(in.Get("$top")); err == nil && n >= 1 && n <= graphPxMaxTop {
		q.Set("$top", strconv.Itoa(n))
	}

	return q
}

// graphProxyBody rebuilds the body of every write from fixed fields; reads and deletes carry no body.
func graphProxyBody(method string, segs []string, in []byte) ([]byte, bool) {
	if method != http.MethodPost {
		return nil, true
	}

	var out any

	var ok bool

	p := graphLower(segs)

	switch {
	case len(p) == graphPxItem && p[0] == "directoryobjects":
		out, ok = graphIDsBody(in)
	case len(p) == 1:
		out, ok = graphCreateBody(p[0], in)
	case p[2] == graphPxAddPassword:
		out, ok = graphAddPasswordBody(in)
	case p[2] == graphPxRemovePw:
		out, ok = graphKeyIDBody(in)
	default:
		out, ok = graphMemberBody(in)
	}

	if !ok {
		return nil, false
	}

	b, err := json.Marshal(out)

	return b, err == nil
}

func graphDecode(in []byte, into any) bool {
	return json.Unmarshal(in, into) == nil
}

func graphText(v string) (string, bool) {
	v = strings.TrimSpace(v)
	control := strings.ContainsFunc(v, func(r rune) bool { return r < ' ' })
	if v == "" || utf8.RuneCountInString(v) > graphPxMaxName || control {
		return "", false
	}

	return v, true
}

func graphCreateBody(coll string, in []byte) (any, bool) {
	var req struct {
		DisplayName       string `json:"displayName"`
		AppID             string `json:"appId"`
		MailNickname      string `json:"mailNickname"`
		UserPrincipalName string `json:"userPrincipalName"`
		Password          string `json:"password"`
		ForceChange       bool   `json:"forceChangePasswordNextSignIn"`
	}

	if !graphDecode(in, &req) {
		return nil, false
	}

	switch coll {
	case graphPxPrincipals:
		if !graphPxGUID.MatchString(strings.ToLower(req.AppID)) {
			return nil, false
		}

		return map[string]any{"appId": req.AppID}, true
	case graphPxApplication:
		name, ok := graphText(req.DisplayName)

		return map[string]any{graphPxDisplayName: name}, ok
	case graphPxGroups:
		return graphGroupBody(req.DisplayName, req.MailNickname)
	}

	return graphUserBody(req.DisplayName, req.MailNickname, req.UserPrincipalName, req.Password, req.ForceChange)
}

func graphGroupBody(displayName, nick string) (any, bool) {
	name, ok := graphText(displayName)
	if !ok || !graphPxNickRE.MatchString(nick) {
		return nil, false
	}

	return map[string]any{
		graphPxDisplayName: name, "mailNickname": nick, "mailEnabled": false, "securityEnabled": true,
	}, true
}

func graphUserBody(displayName, nick, upn, password string, force bool) (any, bool) {
	name, ok := graphText(displayName)
	if !ok || !graphPxNickRE.MatchString(nick) || !graphPxUPNRE.MatchString(upn) ||
		password == "" || len(password) > graphPxMaxPassword {
		return nil, false
	}

	return map[string]any{
		"accountEnabled": true, graphPxDisplayName: name, "mailNickname": nick, "userPrincipalName": upn,
		"passwordProfile": map[string]any{"password": password, "forceChangePasswordNextSignIn": force},
	}, true
}

func graphAddPasswordBody(in []byte) (any, bool) {
	var req struct {
		DisplayName string `json:"displayName"`
		EndDateTime string `json:"endDateTime"`
	}

	if len(bytes.TrimSpace(in)) > 0 && !graphDecode(in, &req) {
		return nil, false
	}

	cred := map[string]any{}

	if req.DisplayName != "" {
		name, ok := graphText(req.DisplayName)
		if !ok {
			return nil, false
		}

		cred[graphPxDisplayName] = name
	}

	if req.EndDateTime != "" {
		t, err := time.Parse(time.RFC3339, req.EndDateTime)
		if err != nil {
			return nil, false
		}

		cred["endDateTime"] = t.UTC().Format(time.RFC3339)
	}

	return map[string]any{"passwordCredential": cred}, true
}

func graphKeyIDBody(in []byte) (any, bool) {
	var req struct {
		KeyID string `json:"keyId"`
	}

	if !graphDecode(in, &req) || !graphPxGUID.MatchString(strings.ToLower(req.KeyID)) {
		return nil, false
	}

	return map[string]any{"keyId": req.KeyID}, true
}

func graphMemberBody(in []byte) (any, bool) {
	var req struct {
		ID string `json:"id"`
	}

	if !graphDecode(in, &req) || !graphPxGUID.MatchString(strings.ToLower(req.ID)) {
		return nil, false
	}

	return map[string]any{"@odata.id": graphPxGraphHost + req.ID}, true
}

func graphIDsBody(in []byte) (any, bool) {
	var req struct {
		IDs []string `json:"ids"`
	}

	if !graphDecode(in, &req) || len(req.IDs) == 0 || len(req.IDs) > graphPxMaxIDs {
		return nil, false
	}

	for _, id := range req.IDs {
		if !graphPxGUID.MatchString(strings.ToLower(id)) {
			return nil, false
		}
	}

	return map[string]any{
		"ids": req.IDs, "types": []string{"user", "group", "servicePrincipal", "application"},
	}, true
}

// graphProxyRelay copies the Graph answer after removing secret-bearing fields. The addPassword answer is the one
// place a freshly generated secret may reach the browser, and it is marked uncacheable.
func graphProxyRelay(c *echo.Context, rec *httptest.ResponseRecorder, keepSecret bool) error {
	body := rec.Body.Bytes()
	ok := rec.Code < http.StatusMultipleChoices

	if len(body) > 0 && !keepSecret || !ok {
		var doc any
		if json.Unmarshal(body, &doc) == nil {
			graphScrub(doc)

			if out, err := json.Marshal(doc); err == nil {
				body = out
			}
		}
	}

	c.Response().Header().Set("Cache-Control", "no-store")

	if len(body) == 0 {
		return c.NoContent(rec.Code)
	}

	return c.JSONBlob(rec.Code, body)
}

func graphScrub(v any) {
	switch t := v.(type) {
	case []any:
		for _, x := range t {
			graphScrub(x)
		}
	case map[string]any:
		for k, x := range t {
			if slices.Contains(graphPxSecrets, strings.ToLower(k)) {
				delete(t, k)

				continue
			}

			graphScrub(x)
		}
	}
}
