// Package testguarduseragentunguarded is the near-miss counterpart to
// testguarduseragentguarded: it inspects a User-Agent-shaped string
// directly with strings.Contains instead of calling
// MatchesUserAgentMarker, and the marker literal itself isn't ARN-shaped,
// so no bullet fires.
package testguarduseragentunguarded

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
		return strings.Contains("", "api/widget")
	}

	return false
}

func (h *Handler) MatchPriority() int { return 60 }
