package ociapigateway

import (
	"bytes"
	"encoding/json"
	"mime"
	"net/http"
	"slices"
	"strings"
)

// Policy discriminators and limits from the Oracle deployment specification reference.
const (
	modeEnforcing  = "ENFORCING"
	modePermissive = "PERMISSIVE"
	modeDisabled   = "DISABLED"

	cacheLookupSimple = "SIMPLE_LOOKUP_POLICY"
	cacheStoreFixed   = "FIXED_TTL_STORE_POLICY"
	maxAllowedSans    = 10
	validationNone    = "NONE"
)

type paramItem struct {
	Required *bool  `json:"required"`
	Name     string `json:"name"`
}

// paramItems accepts the array the SDK models as well as the single object Oracle's JSON examples show.
type paramItems []paramItem

func (p *paramItems) UnmarshalJSON(raw []byte) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) > 0 && raw[0] == '{' {
		var one paramItem
		if err := json.Unmarshal(raw, &one); err != nil {
			return err
		}

		*p = paramItems{one}

		return nil
	}

	var many []paramItem

	if err := json.Unmarshal(raw, &many); err != nil {
		return err
	}

	*p = many

	return nil
}

type paramValidationDoc struct {
	Mode       string     `json:"validationMode"`
	Headers    paramItems `json:"headers"`
	Parameters paramItems `json:"parameters"`
}

type bodyValidationDoc struct {
	Required *bool `json:"required"`
	Content  map[string]struct {
		ValidationType string `json:"validationType"`
	} `json:"content"`
	Mode string `json:"validationMode"`
}

type cacheLookupDoc struct {
	IsEnabled *bool    `json:"isEnabled"`
	IsPrivate *bool    `json:"isPrivateCachingEnabled"`
	Type      string   `json:"type"`
	Additions []string `json:"cacheKeyAdditions"`
}

type cacheStoreDoc struct {
	TTL  *int   `json:"timeToLiveInSeconds"`
	Type string `json:"type"`
}

type mtlsDoc struct {
	IsVerifiedCertificateRequired *bool    `json:"isVerifiedCertificateRequired"`
	AllowedSans                   []string `json:"allowedSans"`
}

type usagePlansDoc struct {
	TokenLocations []string `json:"tokenLocations"`
}

type dynamicServerDoc struct {
	Detail *authDoc     `json:"authenticationServerDetail"`
	Key    selectionKey `json:"key"`
}

type dynamicAuthDoc struct {
	SelectionSource *struct {
		Type     string `json:"type"`
		Selector string `json:"selector"`
	} `json:"selectionSource"`
	Servers []dynamicServerDoc `json:"authenticationServers"`
}

func validMode(m string) bool {
	return m == "" || m == modeEnforcing || m == modePermissive || m == modeDisabled
}

func validateGlobalPolicies(p *requestPolicies) *APIError {
	if err := validateMTLS(p.MutualTLS); err != nil {
		return err
	}

	if u := p.UsagePlans; u != nil {
		if len(u.TokenLocations) == 0 {
			return errParam("usagePlans.tokenLocations must not be empty")
		}

		for _, loc := range u.TokenLocations {
			if kind, _, ok := parseVariable(loc); !ok || (kind != locationHeader && kind != locationQuery) {
				return errParam("usagePlans.tokenLocations entries must be request.headers[..] or request.query[..]")
			}
		}
	}

	return validateDynamicAuth(p.DynamicAuthentication)
}

func validateMTLS(m *mtlsDoc) *APIError {
	if m == nil {
		return nil
	}

	if len(m.AllowedSans) > maxAllowedSans {
		return errParam("mutualTls.allowedSans may hold at most 10 values")
	}

	for _, s := range m.AllowedSans {
		core := strings.TrimSuffix(strings.TrimPrefix(s, "*"), "*")
		if core == "" || strings.Contains(core, "*") {
			return errParam("mutualTls.allowedSans wildcards are only allowed at the start or end of a value")
		}
	}

	return nil
}

func validateDynamicAuth(d *dynamicAuthDoc) *APIError {
	if d == nil {
		return nil
	}

	if d.SelectionSource == nil || d.SelectionSource.Type != selectorSingle {
		return errParam("dynamicAuthentication.selectionSource must be of type SINGLE")
	}

	if _, _, ok := parseVariable(d.SelectionSource.Selector); !ok {
		return errParam("dynamicAuthentication selector must be request.path[..], request.query[..] or request.headers[..]")
	}

	if len(d.Servers) == 0 {
		return errParam("dynamicAuthentication requires authenticationServers")
	}

	for i := range d.Servers {
		s := &d.Servers[i]
		if s.Key.Name == "" || (s.Key.Type != anyOf && s.Key.Type != wildcardKey) {
			return errParam("authentication server keys need a name and type ANY_OF or WILDCARD")
		}

		if s.Detail == nil {
			return errMissing("authenticationServerDetail is required")
		}

		if err := validateAuth(s.Detail); err != nil {
			return err
		}
	}

	return nil
}

func validateRouteValidations(p *routeRequestPolicies) *APIError {
	for _, pv := range []*paramValidationDoc{p.QueryValidations, p.HeaderValidations} {
		if pv != nil && !validMode(pv.Mode) {
			return errParam("validationMode must be ENFORCING, PERMISSIVE or DISABLED")
		}
	}

	if hv := p.HeaderValidations; hv != nil {
		for _, h := range hv.Headers {
			if h.Name == "" {
				return errParam("headerValidations.headers[].name is required")
			}
		}
	}

	if qv := p.QueryValidations; qv != nil {
		for _, q := range qv.Parameters {
			if q.Name == "" {
				return errParam("queryParameterValidations.parameters[].name is required")
			}
		}
	}

	if err := validateBodyValidation(p.BodyValidation); err != nil {
		return err
	}

	return validateCacheLookup(p.ResponseCacheLookup)
}

func validateBodyValidation(bv *bodyValidationDoc) *APIError {
	if bv == nil {
		return nil
	}

	if !validMode(bv.Mode) {
		return errParam("validationMode must be ENFORCING, PERMISSIVE or DISABLED")
	}

	if len(bv.Content) == 0 {
		return errParam("bodyValidation.content must list at least one media type")
	}

	for k, v := range bv.Content {
		if _, _, err := mime.ParseMediaType(k); err != nil && k != "*/*" && !strings.HasSuffix(k, "/*") {
			return errParam("bodyValidation.content key " + k + " is not a media type")
		}

		if v.ValidationType != validationNone {
			return errParam("bodyValidation.content validationType must be NONE")
		}
	}

	return nil
}

func validateCacheLookup(c *cacheLookupDoc) *APIError {
	if c == nil {
		return nil
	}

	if c.Type != cacheLookupSimple {
		return errParam("responseCacheLookup.type must be SIMPLE_LOOKUP_POLICY")
	}

	for _, a := range c.Additions {
		if a == "request.host" {
			continue
		}

		if m := cacheAdditionRe.FindStringSubmatch(a); m == nil {
			return errParam("cacheKeyAdditions entries must be request.query[..], request.headers[..], request.auth[..] or request.host")
		}
	}

	return nil
}

func validateCacheStore(p *routeResponsePolicies) *APIError {
	if p == nil || p.ResponseCacheStore == nil {
		return nil
	}

	s := p.ResponseCacheStore
	if s.Type != cacheStoreFixed {
		return errParam("responseCacheStore.type must be FIXED_TTL_STORE_POLICY")
	}

	if s.TTL == nil || *s.TTL < 1 {
		return errParam("responseCacheStore.timeToLiveInSeconds must be a positive integer")
	}

	return nil
}

// failsParams reports the first required header or query parameter the request lacks.
func failsParams(r *http.Request, rp *routeRequestPolicies) string {
	if hv := rp.HeaderValidations; hv != nil && hv.Mode != modeDisabled {
		for _, h := range hv.Headers {
			if h.Required != nil && *h.Required && r.Header.Get(h.Name) == "" {
				return "missing required header " + h.Name
			}
		}
	}

	if qv := rp.QueryValidations; qv != nil && qv.Mode != modeDisabled {
		q := r.URL.Query()

		for _, p := range qv.Parameters {
			if p.Required == nil || !*p.Required {
				continue
			}

			if !slices.ContainsFunc(keysOf(q), func(k string) bool { return strings.EqualFold(k, p.Name) }) {
				return "missing required query parameter " + p.Name
			}
		}
	}

	return ""
}

func keysOf(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	return out
}

// contentTypeAllowed applies the most specific matching content key: exact, then type/*, then */*.
func contentTypeAllowed(content map[string]struct {
	ValidationType string `json:"validationType"`
}, header string,
) bool {
	mt, _, err := mime.ParseMediaType(header)
	if err != nil {
		return false
	}

	mt = strings.ToLower(mt)
	typ, _, _ := strings.Cut(mt, "/")

	for k := range content {
		k = strings.ToLower(k)
		if k == mt || k == typ+"/*" || k == "*/*" {
			return true
		}
	}

	return false
}

func failsBody(r *http.Request, bv *bodyValidationDoc, body []byte) string {
	if bv == nil || bv.Mode == modeDisabled {
		return ""
	}

	if len(body) == 0 {
		if bv.Required != nil && *bv.Required {
			return "the request body is required"
		}

		return ""
	}

	if !contentTypeAllowed(bv.Content, r.Header.Get("Content-Type")) {
		return "the request body content type is not allowed"
	}

	return ""
}

// validateRequest returns the failure message of the first failing validation in ENFORCING mode, and logs-only
// (returns empty) for PERMISSIVE. permissiveHit reports a PERMISSIVE failure for the caller to log.
func validateRequest(r *http.Request, rp *routeRequestPolicies, body []byte) (string, string) {
	if rp == nil {
		return "", ""
	}

	checks := []struct {
		mode string
		fail string
	}{
		{modeOf(rp.HeaderValidations), failsHeaderOnly(r, rp)},
		{modeOf(rp.QueryValidations), failsQueryOnly(r, rp)},
	}
	if rp.BodyValidation != nil {
		checks = append(checks, struct{ mode, fail string }{rp.BodyValidation.Mode, failsBody(r, rp.BodyValidation, body)})
	}

	permissive := ""

	for _, c := range checks {
		switch {
		case c.fail == "":
		case c.mode == modePermissive:
			permissive = c.fail
		default:
			return c.fail, permissive
		}
	}

	return "", permissive
}

func modeOf(p *paramValidationDoc) string {
	if p == nil {
		return modeEnforcing
	}

	return p.Mode
}

func failsHeaderOnly(r *http.Request, rp *routeRequestPolicies) string {
	return failsParams(r, &routeRequestPolicies{HeaderValidations: rp.HeaderValidations})
}

func failsQueryOnly(r *http.Request, rp *routeRequestPolicies) string {
	return failsParams(r, &routeRequestPolicies{QueryValidations: rp.QueryValidations})
}
