package opensearch

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"io"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	anonymousUserName        = "opendistro_security_anonymous"
	anonymousBackendRole     = "opendistro_security_anonymous_backendrole"
	securityAPIPrefix        = "_plugins/_security/"
	securityAPILegacyPrefix  = "_opendistro/_security/"
	sigV4Algorithm           = "AWS4-HMAC-SHA256"
	assumedRoleResourcePrefx = "assumed-role/"
	securityExceptionType    = "security_exception"
)

type fgacCtxKey struct{}

// fgacRequestAuth is the authorization state a request carries into the bulk handler.
type fgacRequestAuth struct {
	perms effectivePerms
}

func withFGACAuth(r *http.Request, perms effectivePerms) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), fgacCtxKey{}, &fgacRequestAuth{perms: perms}))
}

func fgacAuthOf(r *http.Request) (*fgacRequestAuth, bool) {
	a, ok := r.Context().Value(fgacCtxKey{}).(*fgacRequestAuth)

	return a, ok
}

// securityAPISubpath returns the path below the security plugin prefix.
func securityAPISubpath(p string) (string, bool) {
	p = strings.TrimPrefix(p, "/")
	if sub, ok := strings.CutPrefix(p, securityAPIPrefix); ok {
		return strings.Trim(sub, "/"), true
	}

	if sub, ok := strings.CutPrefix(p, securityAPILegacyPrefix); ok {
		return strings.Trim(sub, "/"), true
	}

	return "", false
}

func isSigV4(r *http.Request) bool {
	return strings.HasPrefix(r.Header.Get("Authorization"), sigV4Algorithm) ||
		r.URL.Query().Get("X-Amz-Algorithm") == sigV4Algorithm
}

// iamPrincipal maps a SigV4 caller onto a principal whose backend roles are its IAM ARNs.
func iamPrincipal(r *http.Request) securityPrincipal {
	meta := awsmeta.Get(r.Context())
	principalARN := ""

	if p := awsmeta.GetPrincipal(r.Context()); p != nil {
		principalARN = p.Arn
	}

	if principalARN == "" {
		principalARN = arn.Build("iam", "", meta.Account, "root")
	}

	roles := []string{principalARN}

	if res, ok := strings.CutPrefix(arnResource(principalARN), assumedRoleResourcePrefx); ok {
		if name, _, found := strings.Cut(res, "/"); found {
			roles = append(roles, arn.Build("iam", "", arnAccount(principalARN), "role/"+name))
		}
	}

	return securityPrincipal{Name: principalARN, BackendRoles: roles, IAM: true}
}

func arnResource(a string) string {
	parts := strings.SplitN(a, ":", 6) //nolint:mnd // ARN has six fields

	if len(parts) < 6 { //nolint:mnd // ARN has six fields
		return ""
	}

	return parts[5]
}

func arnAccount(a string) string {
	parts := strings.SplitN(a, ":", 6) //nolint:mnd // ARN has six fields

	if len(parts) < 6 { //nolint:mnd // ARN has six fields
		return ""
	}

	return parts[4]
}

// authenticateFGAC resolves the caller of a request to a security principal.
func (h *Handler) authenticateFGAC(r *http.Request, dom *Domain) (securityPrincipal, bool) {
	aso := dom.AdvancedSecurityOptions

	switch {
	case isSigV4(r):
		return iamPrincipal(r), true
	case strings.HasPrefix(r.Header.Get("Authorization"), "Basic "):
		user, pass, ok := r.BasicAuth()
		if !ok || !aso.InternalUserDatabaseEnabled {
			return securityPrincipal{}, false
		}

		return h.Backend.AuthenticateInternalUser(dom.Name, user, pass)
	case r.Header.Get("Authorization") == "" && h.Backend.AnonymousAuthActive(aso):
		return securityPrincipal{Name: anonymousUserName, BackendRoles: []string{anonymousBackendRole}}, true
	}

	return securityPrincipal{}, false
}

func writeUnauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Basic realm="OpenSearch Security"`)
	http.Error(w, "Unauthorized", http.StatusUnauthorized)
}

// securityExceptionBody renders an OpenSearch security_exception error.
func securityExceptionBody(reason string) map[string]any {
	return map[string]any{
		jsonKeyError: map[string]any{
			"root_cause": []any{
				map[string]any{jsonKeyPolicyTypeJR: securityExceptionType, jsonKeyReason: reason},
			},
			jsonKeyPolicyTypeJR: securityExceptionType,
			jsonKeyReason:       reason,
		},
		jsonKeyStatusLower: http.StatusForbidden,
	}
}

func noPermissionsReason(action string, p securityPrincipal) string {
	return "no permissions for [" + action + "] and " + p.String()
}

func writeSecurityException(w http.ResponseWriter, action string, p securityPrincipal) {
	writeRawJSON(w, http.StatusForbidden, securityExceptionBody(noPermissionsReason(action, p)))
}

func writeRawJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(status)
	_ = json.MarshalEncode(jsontext.NewEncoder(w, jsonx.V1()), v)
}

// enforceFGAC authenticates and authorizes a data-plane request on a fine-grained-access-control
// domain, then serves it. It always handles the request.
func (h *Handler) enforceFGAC(w http.ResponseWriter, r *http.Request, target dataPlaneTarget, dom *Domain) {
	principal, ok := h.authenticateFGAC(r, dom)
	if !ok {
		writeUnauthorized(w)

		return
	}

	perms := h.Backend.EffectivePerms(dom.Name, principal)

	if sub, isSec := securityAPISubpath(target.path); isSec {
		h.serveSecurityAPI(w, r, dom.Name, perms, sub)

		return
	}

	op := analyzeDataRequest(r.Method, target.path)
	if denied := op.denied(perms); denied != "" {
		writeSecurityException(w, denied, principal)

		return
	}

	h.serveAuthorized(w, withFGACAuth(r, perms), op, perms)
}

func (h *Handler) serveAuthorized(w http.ResponseWriter, r *http.Request, op dataOp, perms effectivePerms) {
	var (
		res accessRestriction
		err error
	)

	if len(op.Indices) > 0 && (op.Kind == opKindSearch || op.Kind == opKindCount || op.Kind == opKindGet) {
		res, err = perms.restrictionFor(op.Indices[0].Index, op.Indices[0].Action)
		if err != nil {
			writeSecurityException(w, op.Indices[0].Action, perms.Principal)

			return
		}
	}

	if !res.restricted() {
		h.serveRest(w, r)

		return
	}

	h.serveRestricted(w, r, op, res)
}

// serveRestricted runs a search/count/get through the engine with DLS applied to the query and
// FLS / masking applied to the returned sources.
func (h *Handler) serveRestricted(w http.ResponseWriter, r *http.Request, op dataOp, res accessRestriction) {
	asCount := op.Kind == opKindCount && res.hasDLS

	if res.hasDLS && op.Kind != opKindGet {
		if !rewriteForDLS(r, res.dlsQuery(), asCount) {
			writeRawJSON(
				w,
				http.StatusBadRequest,
				map[string]any{jsonKeyError: "invalid request body", jsonKeyStatusLower: http.StatusBadRequest},
			)

			return
		}
	}

	cw := newCaptureWriter()
	h.serveRest(cw, r)

	body := cw.buf.Bytes()
	if cw.status == http.StatusOK {
		body = transformRestrictedResponse(body, op, res, asCount)
	}

	copyHeaders(w.Header(), cw.header)
	w.Header().Del("Content-Length")
	w.WriteHeader(cw.status)
	_, _ = w.Write(body)
}

// rewriteForDLS restricts the request's query to the DLS filter. A count becomes a search.
func rewriteForDLS(r *http.Request, dls map[string]any, asCount bool) bool {
	var req map[string]any

	raw, _ := httputils.ReadBody(r)
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &req, jsonx.V1()); err != nil {
			return false
		}
	}

	if req == nil {
		req = map[string]any{}
	}

	query := map[string]any{queryKeyBool: map[string]any{queryKeyFilter: []any{dls}}}
	if orig, ok := req["query"].(map[string]any); ok && len(orig) > 0 {
		query = map[string]any{queryKeyBool: map[string]any{"must": []any{orig}, queryKeyFilter: []any{dls}}}
	}

	req["query"] = query

	if asCount {
		r.URL.Path = strings.TrimSuffix(r.URL.Path, "/"+indexOpCount) + "/" + indexOpSearch
		r.Method = http.MethodPost
		req["size"] = 0
	}

	nb, _ := json.Marshal(req, jsonx.V1())
	r.Body = io.NopCloser(bytes.NewReader(nb))
	r.ContentLength = int64(len(nb))
	r.Header.Del("Content-Length")

	return true
}

func transformRestrictedResponse(body []byte, op dataOp, res accessRestriction, asCount bool) []byte {
	var resp map[string]any
	if err := json.Unmarshal(body, &resp, jsonx.V1()); err != nil {
		return body
	}

	switch {
	case asCount:
		resp = map[string]any{"count": searchTotal(resp), jsonKeyDocShards: readOpShards()}
	case op.Kind == opKindSearch:
		filterHits(resp, res)
	case op.Kind == opKindGet:
		filterGet(resp, res)
	}

	out, err := json.Marshal(resp, jsonx.V1())
	if err != nil {
		return body
	}

	return out
}

// searchTotal extracts hits.total.value from a search response.
func searchTotal(resp map[string]any) int {
	hits, _ := resp["hits"].(map[string]any)
	total, _ := hits["total"].(map[string]any)
	v, _ := total["value"].(float64)

	return int(v)
}

func filterHits(resp map[string]any, res accessRestriction) {
	outer, ok := resp["hits"].(map[string]any)
	if !ok {
		return
	}

	hits, _ := outer["hits"].([]any)
	for _, h := range hits {
		hit, isMap := h.(map[string]any)
		if !isMap {
			continue
		}

		if src, has := hit["_source"].(map[string]any); has {
			hit["_source"] = res.Apply(src)
		}
	}
}

func filterGet(resp map[string]any, res accessRestriction) {
	src, ok := resp["_source"].(map[string]any)
	if !ok {
		return
	}

	if !res.docVisible(src) {
		delete(resp, "_source")
		resp["found"] = false
		delete(resp, "_version")
		delete(resp, "_seq_no")
		delete(resp, "_primary_term")

		return
	}

	resp["_source"] = res.Apply(src)
}

// captureWriter buffers a handler's response.
type captureWriter struct {
	header http.Header
	buf    bytes.Buffer
	status int
}

func newCaptureWriter() *captureWriter {
	return &captureWriter{header: http.Header{}, status: http.StatusOK}
}

func (c *captureWriter) Header() http.Header         { return c.header }
func (c *captureWriter) Write(p []byte) (int, error) { return c.buf.Write(p) }
func (c *captureWriter) WriteHeader(status int)      { c.status = status }

func copyHeaders(dst, src http.Header) {
	for k, vs := range src {
		dst[k] = append([]string(nil), vs...)
	}
}

// authorizeBulkAction checks one bulk item against the caller's index permissions. A denied item
// yields a per-item 403 security_exception, as OpenSearch reports it; (nil, false) means allowed.
func (h *Handler) authorizeBulkAction(r *http.Request, defaultIndex string, act bulkAction) (map[string]any, bool) {
	auth, ok := fgacAuthOf(r)
	if !ok {
		return nil, false
	}

	index := act.Index
	if index == "" {
		index = defaultIndex
	}

	action := actionIndexDoc

	switch act.Op {
	case bulkOpUpdate:
		action = actionUpdateDoc
	case bulkOpDelete:
		action = actionDeleteDoc
	}

	if index == "" || auth.perms.AllowIndex(index, action) {
		return nil, false
	}

	return bulkErrorItem(act.Op, index, act.ID, http.StatusForbidden,
		securityExceptionType, noPermissionsReason(action, auth.perms.Principal)), true
}
