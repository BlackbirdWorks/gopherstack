// Package testguardclosedtableunguarded is the near-miss counterpart to
// testguardclosedtableguarded: a bare prefix claim with no whitelist table,
// no "Unknown"-style sentinel comparison, and no comma-ok map lookup.
package testguardclosedtableunguarded

import "strings"

const pathFoo = "/foo"

type Handler struct{}

func (h *Handler) RouteMatcher() bool {
	path := ""
	if strings.HasPrefix(path, pathFoo+"/") {
		return true
	}

	return false
}

func (h *Handler) MatchPriority() int { return 70 }
