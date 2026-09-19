// Package testguarduseragentguarded is a synthetic fixture for
// TestIsGuarded_GuardShapes -- the User-Agent-marker shape (appsync's
// service.MatchesUserAgentMarker(header, "api/appsync")): the call is
// recognized by name alone, regardless of package qualifier.
package testguarduseragentguarded

import "strings"

const (
	widgetPrefix = "/widgets/"
	apiPrefix    = "/v2/apis"
)

type Handler struct{}

func (h *Handler) RouteMatcher() bool {
	path := ""

	if strings.HasPrefix(path, widgetPrefix) {
		return true
	}

	if strings.HasPrefix(path, apiPrefix) {
		return MatchesUserAgentMarker("", "api/widget")
	}

	return false
}

func (h *Handler) MatchPriority() int { return 60 }

func MatchesUserAgentMarker(userAgent, marker string) bool {
	return strings.Contains(userAgent, marker)
}
