package service

import (
	"net/http"
	"net/url"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	contentTypeHeader  = "Content-Type"
	userAgentHeader    = "User-Agent"
	amzUserAgentHeader = "X-Amz-User-Agent"
)

// AltGate admits a request when ANY alternative holds, for matchers that accept via
// several independent signals. Every list is a necessary-condition superset of what the
// matcher claims; an empty gate (or one holding an empty string) leaves the service ungated.
type AltGate struct {
	// Target: X-Amz-Target prefixes.
	Target []string
	// Path: path prefixes (after leading-slash collapsing).
	Path []string
	// PathSuffix: suffixes of the raw URL path.
	PathSuffix []string
	// Host: lowercase ASCII substrings of the Host header, matched case-insensitively.
	Host []string
	// UserAgent: lowercase ASCII substrings of User-Agent or X-Amz-User-Agent, matched case-insensitively.
	UserAgent []string
	// ContentType: lowercase ASCII substrings of Content-Type, matched case-insensitively.
	ContentType []string
	// Query: case-sensitive substrings of the raw query string.
	Query []string
	// Scope: exact SigV4 credential-scope service names (header or X-Amz-Credential query).
	Scope []string
}

func (g *AltGate) valid() bool {
	lists := [][]string{g.Target, g.Path, g.PathSuffix, g.Host, g.UserAgent, g.ContentType, g.Query, g.Scope}
	n := 0

	for _, l := range lists {
		n += len(l)

		if slices.Contains(l, "") {
			return false
		}
	}

	return n > 0
}

// lookupCtx carries the per-request values gates read, parsed at most once.
type lookupCtx struct {
	req       *http.Request
	target    string
	rawPath   string
	path      string
	cborName  string
	scope     string
	ctype     string
	ua        string
	amzUA     string
	scopeDone bool
	ctypeDone bool
	uaDone    bool
}

func (q *lookupCtx) contentType() string {
	if !q.ctypeDone {
		q.ctype = headerFirst(q.req.Header, contentTypeHeader)
		q.ctypeDone = true
	}

	return q.ctype
}

func (q *lookupCtx) userAgents() (string, string) {
	if !q.uaDone {
		q.ua = headerFirst(q.req.Header, userAgentHeader)
		q.amzUA = headerFirst(q.req.Header, amzUserAgentHeader)
		q.uaDone = true
	}

	return q.ua, q.amzUA
}

func (q *lookupCtx) scopeService() string {
	if !q.scopeDone {
		q.scope = httputils.ExtractServiceFromRequest(q.req)
		q.scopeDone = true
	}

	return q.scope
}

func (g *AltGate) admits(q *lookupCtx) bool {
	if q.req == nil {
		return true
	}

	if hasAnyPrefix(q.target, g.Target) || hasAnyPrefix(q.path, g.Path) || hasAnySuffix(q.rawPath, g.PathSuffix) {
		return true
	}

	if containsAnyFold(q.req.Host, g.Host) || q.matchesUserAgent(g.UserAgent) ||
		(len(g.ContentType) > 0 && containsAnyFold(q.contentType(), g.ContentType)) ||
		hasAnyContains(q.req.URL, g.Query) {
		return true
	}

	if len(g.Scope) == 0 {
		return false
	}

	return slices.Contains(g.Scope, q.scopeService())
}

// headerFirst is Header.Get for an already-canonical key, minus the key canonicalisation.
func headerFirst(h http.Header, canonicalKey string) string {
	if v := h[canonicalKey]; len(v) > 0 {
		return v[0]
	}

	return ""
}

func (q *lookupCtx) matchesUserAgent(subs []string) bool {
	if len(subs) == 0 {
		return false
	}

	ua, amzUA := q.userAgents()

	return containsAnyFold(ua, subs) || containsAnyFold(amzUA, subs)
}

func hasAnyContains(u *url.URL, subs []string) bool {
	if u == nil {
		return false
	}

	return slices.ContainsFunc(subs, func(x string) bool { return strings.Contains(u.RawQuery, x) })
}

func hasAnySuffix(s string, suffixes []string) bool {
	return slices.ContainsFunc(suffixes, func(x string) bool { return strings.HasSuffix(s, x) })
}

// containsAnyFold reports whether s may contain any lowercase-ASCII sub, ignoring case.
// Non-ASCII input is admitted: Unicode lowercasing can fold it onto ASCII letters.
func containsAnyFold(s string, subs []string) bool {
	if len(subs) == 0 || s == "" {
		return false
	}

	for i := range len(s) {
		if s[i] >= utf8.RuneSelf {
			return true
		}
	}

	return slices.ContainsFunc(subs, func(sub string) bool { return containsFoldASCII(s, sub) })
}

func containsFoldASCII(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		j := 0
		for j < len(sub) && lowerASCII(s[i+j]) == sub[j] {
			j++
		}

		if j == len(sub) {
			return true
		}
	}

	return false
}

func lowerASCII(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + ('a' - 'A')
	}

	return b
}
