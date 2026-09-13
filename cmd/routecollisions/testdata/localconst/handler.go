// Package testlocalconst is synthetic fixture data for
// TestAnalyzeDir_DelegationShapes -- it mirrors resourcegroups'
// isResourceTagsPath: a chased helper function declaring two *local* consts,
// one used in a HasPrefix comparison (must produce a claim, resolved
// through the comparison) and one used only in HasSuffix (must NOT also
// produce a bogus standalone claim merely from its own declaration line --
// this is a regression fixture for the false positive that shape produced
// before localConstTable/declRanges excluded it).
package testlocalconst

import "strings"

type Handler struct{}

func (h *Handler) RouteMatcher() bool {
	return isTaggedPath("")
}

func (h *Handler) MatchPriority() int { return 85 }

func isTaggedPath(path string) bool {
	const prefix = "/resources/"

	const suffix = "/tags"

	return strings.HasPrefix(path, prefix) && strings.HasSuffix(path, suffix) && len(path) > len(prefix)+len(suffix)
}
