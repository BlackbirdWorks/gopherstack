// Package testguardarnunguarded is the near-miss counterpart to
// testguardarnguarded: the helper calls strings.Contains, but against a
// plain substring with no ARN-shaped marker (no leading/trailing colon, no
// "arn:aws:" prefix), so it must NOT be recognized as a guard.
package testguardarnunguarded

import "strings"

const (
	widgetPrefix     = "/widgets/"
	widgetTagsPrefix = "/tags/"
)

type Handler struct{}

func (h *Handler) RouteMatcher() bool {
	path := ""

	if strings.HasPrefix(path, widgetPrefix) {
		return true
	}

	if strings.HasPrefix(path, widgetTagsPrefix) {
		return matchesWidgetName(path[len(widgetTagsPrefix):])
	}

	return false
}

func (h *Handler) MatchPriority() int { return 85 }

func matchesWidgetName(rest string) bool {
	return strings.Contains(rest, "widget")
}
