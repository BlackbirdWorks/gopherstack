package azurefile

import (
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

func (b *InMemoryBackend) corsRules(acct string) []corsRule {
	b.mu.RLock("CORS")
	defer b.mu.RUnlock()

	a := b.peek(acct)
	if a == nil || a.Props == nil || a.Props.CORS == nil {
		return nil
	}

	return append([]corsRule(nil), a.Props.CORS.Rules...)
}

func (b *InMemoryBackend) sharePolicy(acct, shr, id string) (accessPolicy, bool) {
	b.mu.RLock("SharePolicy")
	defer b.mu.RUnlock()

	sh, aerr := b.peek(acct).findShare(shr, "", false)
	if aerr != nil {
		return accessPolicy{}, false
	}

	for _, si := range sh.ACL {
		if si.ID == id {
			return si.Policy, true
		}
	}

	return accessPolicy{}, false
}

func (b *InMemoryBackend) fileExists(acct, shr, path string) bool {
	b.mu.RLock("FileExists")
	defer b.mu.RUnlock()

	sh, aerr := b.peek(acct).findShare(shr, "", false)
	if aerr != nil {
		return false
	}

	n, aerr := sh.lookup(path)

	return aerr == nil && !n.Dir
}

func splitCSV(v string) []string {
	var out []string

	for p := range strings.SplitSeq(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}

	return out
}

// wildMatch matches s against a pattern whose '*' stands for any run of characters.
func wildMatch(pattern, s string) bool {
	head, tail, wild := strings.Cut(pattern, "*")
	if !wild {
		return pattern == s
	}

	if !strings.HasPrefix(s, head) {
		return false
	}

	s = s[len(head):]
	if !strings.Contains(tail, "*") {
		return strings.HasSuffix(s, tail)
	}

	for i := 0; i <= len(s); i++ {
		if wildMatch(tail, s[i:]) {
			return true
		}
	}

	return false
}

// originRule is the first rule whose origin and method match; exact reports
// that it matched by listing the origin rather than "*".
func originRule(rules []corsRule, origin, method string) (corsRule, bool, bool) {
	for _, r := range rules {
		matched, exact := false, false

		for _, o := range splitCSV(r.AllowedOrigins) {
			if o == "*" {
				matched = true
			} else if wildMatch(o, origin) {
				matched, exact = true, true

				break
			}
		}

		if matched && methodAllowed(r, method) {
			return r, exact, true
		}
	}

	return corsRule{}, false, false
}

func methodAllowed(r corsRule, method string) bool {
	for _, m := range splitCSV(r.AllowedMethods) {
		if strs.Equal(m, method) {
			return true
		}
	}

	return false
}

func headerAllowed(r corsRule, name string) bool {
	for _, a := range splitCSV(r.AllowedHeaders) {
		if strings.HasSuffix(a, "*") {
			if strings.HasPrefix(strs.Fold(name), strs.Fold(strings.TrimSuffix(a, "*"))) {
				return true
			}
		} else if strs.Equal(a, name) {
			return true
		}
	}

	return false
}

// preflight answers an OPTIONS request from the account's CORS rules; it is
// evaluated against the service and needs no authorization.
func (h *Handler) preflight(rc *reqCtx) *resp {
	origin := rc.hdr("Origin")
	method := rc.hdr("Access-Control-Request-Method")

	if origin == "" || method == "" {
		return errResp(newErr(http.StatusBadRequest, "MissingRequiredHeader",
			"Required CORS headers 'Origin' and 'Access-Control-Request-Method' are not present."))
	}

	rule, exact, ok := originRule(h.Backend.corsRules(rc.acct), origin, method)
	reqHeaders := splitCSV(rc.hdr("Access-Control-Request-Headers"))

	for _, hd := range reqHeaders {
		ok = ok && headerAllowed(rule, hd)
	}

	if !ok {
		return errResp(newErr(http.StatusForbidden, "CorsPreflightFailure",
			"CORS not enabled or no matching rule found for this request."))
	}

	rs := newResp(http.StatusOK)
	setAllowOrigin(rs.hdr, origin, exact)
	rs.hdr.Set("Access-Control-Allow-Methods", method)
	rs.hdr.Set("Access-Control-Max-Age", strconv.Itoa(int(rule.MaxAgeInSeconds)))

	if len(reqHeaders) > 0 {
		rs.hdr.Set("Access-Control-Allow-Headers", strings.Join(reqHeaders, ","))
	}

	return rs
}

func setAllowOrigin(hdr http.Header, origin string, exact bool) {
	if exact {
		hdr.Set("Access-Control-Allow-Origin", origin)
		hdr.Set("Access-Control-Allow-Credentials", "true")
		hdr.Set("Vary", "Origin")

		return
	}

	hdr.Set("Access-Control-Allow-Origin", "*")
}

// applyCORS adds the Access-Control headers of an actual request, following
// the response matrix of the CORS support reference.
func (h *Handler) applyCORS(rc *reqCtx, hdr http.Header) {
	rules := h.Backend.corsRules(rc.acct)
	if len(rules) == 0 {
		return
	}

	origin, method := rc.hdr("Origin"), rc.r.Method
	readOnly := method == http.MethodGet || method == http.MethodHead

	if origin == "" {
		if rule, _, ok := wildcardRule(rules, method); ok {
			hdr.Set("Access-Control-Allow-Origin", "*")
			setExpose(hdr, rule)
		} else if readOnly {
			hdr.Set("Vary", "Origin")
		}

		return
	}

	rule, exact, ok := originRule(rules, origin, method)
	if !ok {
		if readOnly {
			hdr.Set("Vary", "Origin")
		}

		return
	}

	setAllowOrigin(hdr, origin, exact)

	if !readOnly {
		hdr.Del("Vary")
	}

	setExpose(hdr, rule)
}

func wildcardRule(rules []corsRule, method string) (corsRule, bool, bool) {
	for _, r := range rules {
		if methodAllowed(r, method) && slices.Contains(splitCSV(r.AllowedOrigins), "*") {
			return r, false, true
		}
	}

	return corsRule{}, false, false
}

func setExpose(hdr http.Header, r corsRule) {
	if v := strings.Join(splitCSV(r.ExposedHeaders), ","); v != "" {
		hdr.Set("Access-Control-Expose-Headers", v)
	}
}
