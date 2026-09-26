// Package testguardclosedtablecommaok is a synthetic fixture for
// TestIsGuarded_GuardShapes -- the other half of the closed-route-whitelist
// shape: `_, ok := routes[path]` gating the match on membership in a
// closed map, rather than a "!= opUnknown" sentinel comparison.
package testguardclosedtablecommaok

const pathBar = "/bar"

type Handler struct{}

func (h *Handler) RouteMatcher() bool {
	path := ""
	_, ok := routes()[path]

	return ok
}

func (h *Handler) MatchPriority() int { return 70 }

func routes() map[string]string {
	return map[string]string{pathBar: "Bar"}
}
