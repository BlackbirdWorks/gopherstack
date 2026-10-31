package ociloadbalancer

import (
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"
)

// Rule actions.
const (
	actAddReqHeader    = "ADD_HTTP_REQUEST_HEADER"
	actAddRespHeader   = "ADD_HTTP_RESPONSE_HEADER"
	actExtendReqHdr    = "EXTEND_HTTP_REQUEST_HEADER_VALUE"
	actExtendRespHdr   = "EXTEND_HTTP_RESPONSE_HEADER_VALUE"
	actRemoveReqHdr    = "REMOVE_HTTP_REQUEST_HEADER"
	actRemoveRespHdr   = "REMOVE_HTTP_RESPONSE_HEADER"
	actAllow           = "ALLOW"
	actRedirect        = "REDIRECT"
	actControlAccess   = "CONTROL_ACCESS_USING_HTTP_METHODS"
	actHTTPHeader      = "HTTP_HEADER"
	actIPMaxConns      = "IP_BASED_MAX_CONNECTIONS"
	condSourceIP       = "SOURCE_IP_ADDRESS"
	condPath           = "PATH"
	defaultRedirect    = http.StatusFound
	defaultDeniedCode  = http.StatusMethodNotAllowed
	statusRangeStart   = 400
	statusRangeEnd     = 599
	forbiddenStatus    = http.StatusForbidden
	defaultHTTPPort    = 80
	defaultHTTPSPort   = 443
	schemeHTTPS        = "https"
	schemeHTTP         = "http"
	redirectTokenQuery = "{query}"
)

//nolint:gochecknoglobals // immutable
var restrictedHeaders = []string{
	"host",
	"content-length",
	"transfer-encoding",
	"connection",
	"upgrade",
	"te",
}

func checkHeaderName(h string) *APIError {
	if h == "" {
		return errMissing("rule header is required")
	}

	if !tokenRE.MatchString(h) {
		return errParam("rule header " + h + " is not a valid header name")
	}

	if slices.Contains(restrictedHeaders, strings.ToLower(h)) {
		return errParam("rule header " + h + " cannot be modified")
	}

	return nil
}

// normalizeRule validates a rule set item and applies its defaults.
func normalizeRule(r Rule) (Rule, *APIError) {
	switch r.Action {
	case actAddReqHeader, actAddRespHeader:
		if r.Value == nil {
			return r, errMissing(r.Action + " needs header and value")
		}

		return r, checkHeaderName(r.Header)
	case actExtendReqHdr, actExtendRespHdr:
		if r.Prefix == nil && r.Suffix == nil {
			return r, errMissing(r.Action + " needs prefix or suffix")
		}

		return r, checkHeaderName(r.Header)
	case actRemoveReqHdr, actRemoveRespHdr:
		return r, checkHeaderName(r.Header)
	case actAllow:
		return r, checkConditions(r, false)
	case actRedirect:
		return normalizeRedirect(r)
	case actControlAccess:
		return normalizeControlAccess(r)
	case actHTTPHeader:
		return normalizeHTTPHeader(r)
	case actIPMaxConns:
		return r, errUnsupported(actIPMaxConns)
	default:
		return r, errParam("unknown rule action " + r.Action)
	}
}

func normalizeRedirect(r Rule) (Rule, *APIError) {
	if err := checkConditions(r, true); err != nil {
		return r, err
	}

	code := defaultRedirect
	if r.ResponseCode != nil {
		code = *r.ResponseCode
	}

	if !slices.Contains([]int{301, 302, 303, 307, 308}, code) {
		return r, errParam("REDIRECT responseCode must be 301, 302, 303, 307 or 308")
	}

	r.ResponseCode = &code

	if r.RedirectURI == nil {
		return r, errMissing("REDIRECT needs redirectUri")
	}

	if p := r.RedirectURI.Protocol; p != nil &&
		!slices.Contains([]string{schemeHTTP, schemeHTTPS, "{protocol}"}, strings.ToLower(*p)) {
		return r, errParam("redirectUri protocol must be http, https or {protocol}")
	}

	if p := r.RedirectURI.Port; p != nil && (*p < 1 || *p > maxPort) {
		return r, errParam("redirectUri port must be between 1 and 65535")
	}

	return r, nil
}

func normalizeControlAccess(r Rule) (Rule, *APIError) {
	if len(r.AllowedMethods) == 0 {
		return r, errMissing(actControlAccess + " needs allowedMethods")
	}

	for i, m := range r.AllowedMethods {
		r.AllowedMethods[i] = strings.ToUpper(m)
		if !tokenRE.MatchString(m) {
			return r, errParam("allowedMethods contains an invalid method " + m)
		}
	}

	code := defaultDeniedCode
	if r.StatusCode != nil {
		code = *r.StatusCode
	}

	if code < statusRangeStart || code > statusRangeEnd {
		return r, errParam("statusCode must be a 4xx or 5xx status")
	}

	r.StatusCode = &code

	return r, nil
}

func normalizeHTTPHeader(r Rule) (Rule, *APIError) {
	if boolOr(r.InvalidCharsAllowed) {
		return r, errUnsupported("areInvalidCharactersAllowed")
	}

	if r.HTTPLargeHeaderSizeKB != nil && !slices.Contains([]int{8, 16, 32, 64}, *r.HTTPLargeHeaderSizeKB) {
		return r, errParam("httpLargeHeaderSizeInKB must be 8, 16, 32 or 64")
	}

	return r, nil
}

func checkConditions(r Rule, allowPath bool) *APIError {
	if len(r.Conditions) == 0 {
		return errMissing(r.Action + " needs conditions")
	}

	for _, c := range r.Conditions {
		switch c.AttributeName {
		case condSourceIP:
			if !validSource(c.AttributeValue) {
				return errParam("SOURCE_IP_ADDRESS value " + c.AttributeValue + " is not an IP address or CIDR")
			}
		case condPath:
			if !allowPath {
				return errParam(r.Action + " does not accept PATH conditions")
			}

			if !slices.Contains([]string{matchExact, matchLongest, matchPrefix, matchSuffix}, c.Operator) ||
				c.AttributeValue == "" {
				return errParam("PATH conditions need attributeValue and a match operator")
			}
		case "SOURCE_VCN_ID", "SOURCE_VCN_IP_ADDRESS":
			return errUnsupported("condition " + c.AttributeName + " (no VCN context at the listener)")
		default:
			return errParam("unknown condition attributeName " + c.AttributeName)
		}
	}

	return nil
}

func validSource(v string) bool {
	if _, err := netip.ParsePrefix(v); err == nil {
		return true
	}

	_, err := netip.ParseAddr(v)

	return err == nil
}

func sourceMatches(v string, ip netip.Addr) bool {
	if p, err := netip.ParsePrefix(v); err == nil {
		return p.Contains(ip)
	}

	a, err := netip.ParseAddr(v)

	return err == nil && a == ip
}

func conditionsMatch(conds []RuleCondition, r *http.Request, ip netip.Addr) bool {
	for _, c := range conds {
		switch c.AttributeName {
		case condSourceIP:
			if sourceMatches(c.AttributeValue, ip) {
				return true
			}
		case condPath:
			if pathMatches(
				PathRoute{Path: c.AttributeValue, PathMatchType: MatchType{MatchType: c.Operator}},
				r.URL.Path,
			) {
				return true
			}
		}
	}

	return false
}

// reqScope describes the client connection a request arrived on.
type reqScope struct {
	ip     netip.Addr
	scheme string
	port   int
}

func clientAddr(r *http.Request) netip.Addr {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}

	a, _ := netip.ParseAddr(host)

	return a.Unmap()
}

// directReply is a response the load balancer produces itself.
type directReply struct {
	location string
	status   int
}

// applyRequestRules runs the listener's request-phase rules in order and returns a direct reply when a rule
// short-circuits the request. Response-phase rules are returned for the caller to apply.
func applyRequestRules(sets []*RuleSet, r *http.Request, sc reqScope) (*directReply, []Rule) {
	var items, resp []Rule

	for _, s := range sets {
		items = append(items, s.Items...)
	}

	if !allowedByRules(items, r, sc.ip) {
		return &directReply{status: forbiddenStatus}, nil
	}

	for _, it := range items {
		switch it.Action {
		case actControlAccess:
			if !slices.Contains(it.AllowedMethods, r.Method) {
				return &directReply{status: *it.StatusCode}, nil
			}
		case actRedirect:
			if conditionsMatch(it.Conditions, r, sc.ip) {
				return &directReply{status: *it.ResponseCode, location: redirectTarget(it.RedirectURI, r, sc)}, nil
			}
		case actAddReqHeader:
			r.Header.Add(it.Header, *it.Value)
		case actRemoveReqHdr:
			r.Header.Del(it.Header)
		case actExtendReqHdr:
			extendHeader(r.Header, it)
		case actAddRespHeader, actRemoveRespHdr, actExtendRespHdr:
			resp = append(resp, it)
		}
	}

	return nil, resp
}

// allowedByRules reports whether the request passes the ALLOW rules: with none present everything passes.
func allowedByRules(items []Rule, r *http.Request, ip netip.Addr) bool {
	allows, allowed := 0, false

	for _, it := range items {
		if it.Action == actAllow {
			allows++

			allowed = allowed || conditionsMatch(it.Conditions, r, ip)
		}
	}

	return allows == 0 || allowed
}

func extendHeader(h http.Header, it Rule) {
	cur := h.Values(it.Header)
	if len(cur) == 0 {
		return
	}

	pre, suf := "", ""
	if it.Prefix != nil {
		pre = *it.Prefix
	}

	if it.Suffix != nil {
		suf = *it.Suffix
	}

	h.Set(it.Header, pre+strings.Join(cur, ",")+suf)
}

func applyResponseRules(h http.Header, rules []Rule) {
	for _, it := range rules {
		switch it.Action {
		case actAddRespHeader:
			h.Add(it.Header, *it.Value)
		case actRemoveRespHdr:
			h.Del(it.Header)
		case actExtendRespHdr:
			extendHeader(h, it)
		}
	}
}

func tokenOr(p *string, def string) string {
	if p == nil {
		return def
	}

	return *p
}

func redirectTarget(u *RedirectURI, r *http.Request, sc reqScope) string {
	host := r.Host
	if h, _, err := net.SplitHostPort(r.Host); err == nil {
		host = h
	}

	repl := strings.NewReplacer(
		"{protocol}", sc.scheme, "{host}", host, "{port}", strconv.Itoa(sc.port),
		"{path}", strings.TrimPrefix(r.URL.Path, "/"), "{query}", r.URL.RawQuery,
	)

	scheme := strings.ToLower(repl.Replace(tokenOr(u.Protocol, "{protocol}")))
	port := repl.Replace(tokenOr(nil, "{port}"))

	if u.Port != nil {
		port = strconv.Itoa(*u.Port)
	}

	h := repl.Replace(tokenOr(u.Host, "{host}"))
	if (scheme != schemeHTTP || port != strconv.Itoa(defaultHTTPPort)) &&
		(scheme != schemeHTTPS || port != strconv.Itoa(defaultHTTPSPort)) {
		h = net.JoinHostPort(strings.Trim(h, "[]"), port)
	}

	path := repl.Replace(tokenOr(u.Path, "/{path}"))
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	out := scheme + "://" + h + path

	if q := strings.TrimPrefix(repl.Replace(tokenOr(u.Query, redirectTokenQuery)), "?"); q != "" {
		out += "?" + q
	}

	return out
}
