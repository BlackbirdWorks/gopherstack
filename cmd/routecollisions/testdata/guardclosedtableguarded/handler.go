// Package testguardclosedtableguarded is a synthetic fixture for
// TestIsGuarded_GuardShapes -- the closed-route-whitelist shape (polly's
// parseRoute(...).operation != opUnknown, a closed 5-route table): a
// same-package helper resolves the path into an operation, and
// RouteMatcher accepts only when that operation isn't the "Unknown"
// sentinel.
package testguardclosedtableguarded

const (
	opUnknown = "Unknown"
	pathFoo   = "/foo"
)

type route struct{ operation string }

type Handler struct{}

func (h *Handler) RouteMatcher() bool {
	path := ""

	return parseRoute(path).operation != opUnknown
}

func (h *Handler) MatchPriority() int { return 70 }

func parseRoute(path string) route {
	switch path {
	case pathFoo:
		return route{operation: "Foo"}
	default:
		return route{operation: opUnknown}
	}
}
