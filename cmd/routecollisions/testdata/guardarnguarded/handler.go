// Package testguardarnguarded is a synthetic fixture for
// TestIsGuarded_GuardShapes -- the ARN service-segment shape (amplify's
// strings.Contains(arn, amplifyServiceIdentifier), accessanalyzer's
// strings.Contains(after, ":"+accessAnalyzerService+":")): a same-package
// helper, reached one call-hop from RouteMatcher, tests a concatenation of
// consts against an ARN-shaped marker.
package testguardarnguarded

import "strings"

const (
	widgetPrefix     = "/widgets/"
	widgetTagsPrefix = "/tags/"
	widgetService    = "widget"
)

type Handler struct{}

func (h *Handler) RouteMatcher() bool {
	path := ""

	if strings.HasPrefix(path, widgetPrefix) {
		return true
	}

	if strings.HasPrefix(path, widgetTagsPrefix) {
		return matchesWidgetARN(path[len(widgetTagsPrefix):])
	}

	return false
}

func (h *Handler) MatchPriority() int { return 85 }

func matchesWidgetARN(rest string) bool {
	return strings.Contains(rest, ":"+widgetService+":")
}
