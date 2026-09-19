// Package testguardboundaryguarded is a synthetic fixture for
// TestIsGuarded_GuardShapes -- the path-boundary-concatenation shape
// (codeartifact's isDomainRepoPath, which requires a "/" boundary after
// "/v1/domain" so it never matches "/v1/domainnames"): the boundary check
// itself isn't independently recognized, but living inside an is\w*Path-
// named helper earns it bullet (a) anyway.
package testguardboundaryguarded

import "strings"

const domainBase = "/v1/domain"

type Handler struct{}

func (h *Handler) RouteMatcher() bool {
	path := ""

	return isDomainRepoPath(path)
}

func (h *Handler) MatchPriority() int { return 80 }

func isDomainRepoPath(path string) bool {
	return path == domainBase || strings.HasPrefix(path, domainBase+"/")
}
