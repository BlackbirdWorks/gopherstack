// Package testguardexactguarded is a synthetic fixture for
// TestIsGuarded_GuardShapes -- the exact-match-vs-prefix-semantics shape
// (appsync's "/v1/tags" claim only ever matching AppSync's own literal
// "/v1/tags" exactly, never CodeArtifact's real "/v1/tags/{arn}"):
// RouteMatcher compares the full request path with "==", never HasPrefix.
package testguardexactguarded

const tagsExact = "/v1/tags"

type Handler struct{}

func (h *Handler) RouteMatcher() bool {
	path := ""

	return path == tagsExact
}

func (h *Handler) MatchPriority() int { return 80 }
