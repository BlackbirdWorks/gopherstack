// Package testlocalslice is synthetic fixture data for
// TestAnalyzeDir_DelegationShapes -- it mirrors omics'/backup's local
// "exacts"/"prefixes" []string{...} literal shape (a slice literal scoped
// to the helper function itself, not a package-level var the existing
// sliceConsts regex table would see), whose bare-identifier elements name
// package consts.
package testlocalslice

const pathAlpha = "/alpha"

const pathBeta = "/beta"

type Handler struct{}

func (h *Handler) RouteMatcher() bool {
	return matchesPath("")
}

func (h *Handler) MatchPriority() int { return 85 }

func matchesPath(path string) bool {
	exacts := []string{pathAlpha, pathBeta}

	for _, p := range exacts {
		if path == p {
			return true
		}
	}

	return false
}
