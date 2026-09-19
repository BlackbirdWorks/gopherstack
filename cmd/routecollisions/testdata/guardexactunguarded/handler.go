// Package testguardexactunguarded is the near-miss counterpart to
// testguardexactguarded: the identical literal, but claimed with
// strings.HasPrefix instead of "==" -- no exact match anywhere, so it must
// NOT be recognized as a guard.
package testguardexactunguarded

import "strings"

const tagsPrefix = "/v1/tags"

type Handler struct{}

func (h *Handler) RouteMatcher() bool {
	path := ""

	return strings.HasPrefix(path, tagsPrefix)
}

func (h *Handler) MatchPriority() int { return 80 }
