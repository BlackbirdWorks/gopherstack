package elbv2

import (
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	actionTypeRedirect      = "redirect"
	actionTypeFixedResponse = "fixed-response"
	actionTypeAuthPrefix    = "authenticate-"
)

func (h *Handler) route(w http.ResponseWriter, req *dpRequest) {
	rules, err := req.be.DescribeRules(req.listener.ListenerArn, nil)
	if err != nil {
		writeDPError(w, http.StatusServiceUnavailable, msgNoTargets)

		return
	}

	var def *Rule

	for i := range rules {
		rule := &rules[i]
		if rule.IsDefault {
			def = rule

			continue
		}

		if ruleMatches(rule.Conditions, req) {
			h.runActions(w, req, rule.Actions, rule.Transforms)

			return
		}
	}

	actions := req.listener.DefaultActions
	if def != nil && len(def.Actions) > 0 {
		actions = def.Actions
	}

	h.runActions(w, req, actions, nil)
}

func (h *Handler) runActions(w http.ResponseWriter, req *dpRequest, actions []Action, tr []RuleTransform) {
	sorted := append([]Action(nil), actions...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Order < sorted[j].Order })

	for _, a := range sorted {
		switch {
		case a.Type == actionTypeRedirect && a.RedirectConfig != nil:
			serveRedirect(w, req, a.RedirectConfig)

			return
		case a.Type == actionTypeFixedResponse && a.FixedResponseConfig != nil:
			serveFixed(w, a.FixedResponseConfig)

			return
		case a.Type == actionTypeForward:
			applyTransforms(req, tr)
			h.serveForward(w, req, a)

			return
		case strings.HasPrefix(a.Type, actionTypeAuthPrefix) || a.Type == actionTypeJwtValidation:
			writeDPError(w, http.StatusNotImplemented, msgNotImplemented)

			return
		}
	}

	writeDPError(w, http.StatusServiceUnavailable, msgNoTargets)
}

func ruleMatches(conds []Condition, req *dpRequest) bool {
	for _, c := range conds {
		if !conditionMatches(c, req) {
			return false
		}
	}

	return true
}

func conditionMatches(c Condition, req *dpRequest) bool {
	switch c.Field {
	case condHostHeader:
		return matchAnyText(c.Values, c.RegexValues, req.host, true)
	case condPathPattern:
		return matchAnyText(c.Values, c.RegexValues, req.path, false)
	case condHTTPHeader:
		return headerMatches(c, req.r.Header)
	case condHTTPMethod:
		return matchMethod(c.Values, req.r.Method)
	case condQueryString:
		return queryMatches(c.QueryStringPairs, req.r.URL.Query())
	case condSourceIP:
		return sourceIPMatches(c.Values, req.client)
	}

	return false
}

func matchAnyText(patterns, regexes []string, s string, fold bool) bool {
	for _, p := range patterns {
		if wildcardMatch(p, s, fold) {
			return true
		}
	}

	for _, rx := range regexes {
		if re, err := regexp.Compile(rx); err == nil && re.MatchString(s) {
			return true
		}
	}

	return false
}

func headerMatches(c Condition, hdr http.Header) bool {
	for _, v := range hdr.Values(c.HTTPHeaderName) {
		if matchAnyText(c.Values, c.RegexValues, v, true) {
			return true
		}
	}

	return false
}

func matchMethod(values []string, method string) bool {
	for _, v := range values {
		if strings.EqualFold(v, method) {
			return true
		}
	}

	return false
}

func queryMatches(pairs []QueryStringPair, q url.Values) bool {
	for _, p := range pairs {
		for k, vs := range q {
			if p.Key != "" && !strings.EqualFold(p.Key, k) {
				continue
			}

			for _, v := range vs {
				if wildcardMatch(p.Value, v, true) {
					return true
				}
			}
		}
	}

	return false
}

func sourceIPMatches(cidrs []string, addr netip.Addr) bool {
	if !addr.IsValid() {
		return false
	}

	for _, c := range cidrs {
		if p, err := netip.ParsePrefix(c); err == nil && p.Contains(addr) {
			return true
		}
	}

	return false
}

// wildcardMatch matches s against a pattern where * is any run of characters and ? is exactly one.
func wildcardMatch(pattern, s string, fold bool) bool {
	if fold {
		pattern, s = strings.ToLower(pattern), strings.ToLower(s)
	}

	p, i := 0, 0
	star, mark := -1, 0

	for i < len(s) {
		switch {
		case p < len(pattern) && (pattern[p] == '?' || pattern[p] == s[i]) && pattern[p] != '*':
			p++
			i++
		case p < len(pattern) && pattern[p] == '*':
			star, mark = p, i
			p++
		case star >= 0:
			p = star + 1
			mark++
			i = mark
		default:
			return false
		}
	}

	for p < len(pattern) && pattern[p] == '*' {
		p++
	}

	return p == len(pattern)
}

func applyTransforms(req *dpRequest, tr []RuleTransform) {
	for _, t := range tr {
		switch {
		case t.URLRewriteConfig != nil:
			for _, rw := range t.URLRewriteConfig.Rewrites {
				if re, err := regexp.Compile(rw.Regex); err == nil && re.MatchString(req.path) {
					req.path = re.ReplaceAllString(req.path, rw.Replace)
					req.rawPath = (&url.URL{Path: req.path}).EscapedPath()

					break
				}
			}
		case t.HostHeaderRewriteConfig != nil:
			for _, rw := range t.HostHeaderRewriteConfig.Rewrites {
				if re, err := regexp.Compile(rw.Regex); err == nil && re.MatchString(req.host) {
					req.host = re.ReplaceAllString(req.host, rw.Replace)

					break
				}
			}
		}
	}
}

func listenerScheme(l *Listener) string { return strings.ToLower(l.Protocol) }

func serveFixed(w http.ResponseWriter, cfg *FixedResponseConfig) {
	status, err := strconv.Atoi(cfg.StatusCode)
	if err != nil {
		status = http.StatusOK
	}

	ct := cfg.ContentType
	if ct == "" {
		ct = "text/plain"
	}

	w.Header().Set("Content-Type", ct)
	w.WriteHeader(status)
	_, _ = w.Write([]byte(cfg.MessageBody))
}

func serveRedirect(w http.ResponseWriter, req *dpRequest, cfg *RedirectConfig) {
	repl := strings.NewReplacer(
		"#{protocol}", listenerScheme(req.listener),
		"#{host}", req.host,
		"#{port}", strconv.Itoa(int(req.listener.Port)),
		"#{path}", strings.TrimPrefix(req.rawPath, "/"),
		"#{query}", req.r.URL.RawQuery,
	)

	proto := orDefault(cfg.Protocol, "#{protocol}")
	host := orDefault(cfg.Host, "#{host}")
	port := orDefault(cfg.Port, "#{port}")
	path := orDefault(cfg.Path, "/#{path}")
	query := orDefault(cfg.Query, "#{query}")

	loc := strings.ToLower(
		repl.Replace(proto),
	) + "://" + repl.Replace(
		host,
	) + ":" + repl.Replace(
		port,
	) + repl.Replace(
		path,
	)
	if q := repl.Replace(query); q != "" {
		loc += "?" + q
	}

	status := http.StatusFound
	if cfg.StatusCode == "HTTP_301" {
		status = http.StatusMovedPermanently
	}

	w.Header().Set("Location", loc)
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(status)
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}

	return v
}
