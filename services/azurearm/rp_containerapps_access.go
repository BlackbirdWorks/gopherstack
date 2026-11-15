package azurearm

import (
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
)

const acaForbiddenBody = "Ip Forbidden"

// acaParseRange reads an ipSecurityRestrictions range: a single address or a CIDR block.
func acaParseRange(s string) (netip.Prefix, bool) {
	s = strings.TrimSpace(s)

	if pfx, err := netip.ParsePrefix(s); err == nil {
		return pfx.Masked(), true
	}

	if addr, err := netip.ParseAddr(s); err == nil {
		return netip.PrefixFrom(addr, addr.BitLen()), true
	}

	return netip.Prefix{}, false
}

// acaClientAddr is the caller's address: the first X-Forwarded-For entry, else the connection's peer.
func acaClientAddr(r *http.Request) (netip.Addr, bool) {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		first, _, _ := strings.Cut(xff, ",")
		if a, err := netip.ParseAddr(strings.TrimSpace(first)); err == nil {
			return a.Unmap(), true
		}
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}

	a, perr := netip.ParseAddr(host)

	return a.Unmap(), perr == nil
}

// acaIPAllowed applies the ingress ipSecurityRestrictions: with Allow rules only matching callers pass, with Deny
// rules matching callers are refused (rules cannot mix, see acaCheckIPRules).
func acaIPAllowed(rules []any, r *http.Request) bool {
	if len(rules) == 0 {
		return true
	}

	addr, ok := acaClientAddr(r)
	allow := strings.EqualFold(strOf(mapOf(rules[0]), "action"), acaRuleAllow)
	matched := false

	for _, e := range rules {
		if pfx, valid := acaParseRange(strOf(mapOf(e), "ipAddressRange")); valid && ok && pfx.Contains(addr) {
			matched = true
		}
	}

	return matched == allow
}

// acaCORS answers a CORS preflight and decorates responses per ingress.corsPolicy. It reports whether it answered.
func acaCORS(w http.ResponseWriter, r *http.Request, policy map[string]any) bool {
	origin := r.Header.Get("Origin")
	if origin == "" || len(policy) == 0 {
		return false
	}

	allowed := false

	for _, o := range acaList(policy["allowedOrigins"]) {
		if v, _ := o.(string); v == "*" || strings.EqualFold(v, origin) {
			allowed = true
		}
	}

	if !allowed {
		return false
	}

	h := w.Header()
	h.Set("Access-Control-Allow-Origin", origin)
	h.Add("Vary", "Origin")

	if v, _ := policy["allowCredentials"].(bool); v {
		h.Set("Access-Control-Allow-Credentials", "true")
	}

	if exp := acaJoin(policy["exposeHeaders"]); exp != "" {
		h.Set("Access-Control-Expose-Headers", exp)
	}

	if r.Method != http.MethodOptions || r.Header.Get("Access-Control-Request-Method") == "" {
		return false
	}

	h.Set("Access-Control-Allow-Methods", acaJoin(policy["allowedMethods"]))
	h.Set("Access-Control-Allow-Headers", acaJoin(policy["allowedHeaders"]))

	if n, ok := acaNum(policy, "maxAge"); ok {
		h.Set("Access-Control-Max-Age", strconv.Itoa(int(n)))
	}

	w.WriteHeader(http.StatusNoContent)

	return true
}

func acaJoin(v any) string {
	var parts []string

	for _, e := range acaList(v) {
		if s, ok := e.(string); ok {
			parts = append(parts, s)
		}
	}

	return strings.Join(parts, ", ")
}
