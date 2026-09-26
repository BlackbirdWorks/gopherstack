// Package testguardboundaryunguarded is the near-miss counterpart to
// testguardboundaryguarded: the identical "/" boundary concatenation, but
// checked inline in RouteMatcher with no is\w*Path-named wrapper and no
// exact "==" comparison -- proving the boundary logic alone, without the
// naming convention, is not recognized as a guard.
package testguardboundaryunguarded

import "strings"

const domainBase = "/v1/domain"

type Handler struct{}

func (h *Handler) RouteMatcher() bool {
	path := ""

	return strings.HasPrefix(path, domainBase+"/")
}

func (h *Handler) MatchPriority() int { return 80 }
