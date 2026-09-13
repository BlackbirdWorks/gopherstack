// Package testpredicate is synthetic fixture data for
// TestAnalyzeDir_DelegationShapes -- it mirrors the "return isXPath(path)"
// shape (omics, apigateway, backup, codeartifact, elasticsearch,
// opensearch): RouteMatcher delegates to a predicate function defined
// elsewhere in the package, which itself delegates one hop further.
package testpredicate

import "strings"

const widgetPath = "/widgets"

const widgetExtra = "/widgets/extra"

type Handler struct{}

func (h *Handler) RouteMatcher() bool {
	return isWidgetPath("")
}

func (h *Handler) MatchPriority() int { return 85 }

func isWidgetPath(path string) bool {
	return path == widgetPath || strings.HasPrefix(path, widgetPath+"/") || isWidgetExtraPath(path)
}

func isWidgetExtraPath(path string) bool {
	return strings.HasPrefix(path, widgetExtra+"/")
}
