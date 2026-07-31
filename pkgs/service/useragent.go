package service

import (
	"net/http"
	"strings"
)

// MatchesUserAgentMarker reports whether the request identified by h carries
// any of markers in its SDK user-agent identification, checked
// case-insensitively against both the User-Agent and X-Amz-User-Agent
// headers.
//
// Native SDKs (Go, Python, Java, ...) send their SDK identification in the
// standard User-Agent header. Browser JavaScript cannot: the Fetch spec
// forbids scripts from setting User-Agent (the browser controls it), so the
// AWS SDK for JavaScript puts its own SDK identification exclusively into
// X-Amz-User-Agent when running in a browser. A RouteMatcher that only
// inspects User-Agent will therefore never match a browser-originated
// request; checking both headers is what makes matching work for a browser
// dashboard as well as a native SDK caller.
//
// Matching is case-insensitive because the same logical marker differs in
// case depending on which SDK sent it: aws-sdk-go-v2 derives its marker from
// the (lowercase) Go module path -- e.g. "api/docdb" -- while the AWS SDK
// for JavaScript uses the API model's PascalCase serviceId verbatim -- e.g.
// "api/DocDB". Pass more than one marker to also cover an SDK that spells
// the same service differently: e.g. MediaStore Data's serviceId contains a
// space, which aws-sdk-go-v2 (module-path-derived: "mediastoredata") and the
// AWS SDK for JavaScript (space escaped to a hyphen: "MediaStore-Data")
// encode differently.
func MatchesUserAgentMarker(h http.Header, markers ...string) bool {
	userAgents := [2]string{
		strings.ToLower(h.Get("User-Agent")),
		strings.ToLower(h.Get("X-Amz-User-Agent")),
	}

	for _, marker := range markers {
		lowerMarker := strings.ToLower(marker)

		for _, ua := range userAgents {
			if ua != "" && strings.Contains(ua, lowerMarker) {
				return true
			}
		}
	}

	return false
}
