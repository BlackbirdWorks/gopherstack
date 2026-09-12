// Package testmapkeys is synthetic fixture data for
// TestAnalyzeDir_DelegationShapes -- it mirrors the map/route-table literal
// key shape (account's operationNames, resourcegroups' rgRESTPathOps):
// RouteMatcher indexes a package-level map var, keyed by both a literal
// string and an identifier naming a package const.
package testmapkeys

type Handler struct{}

const pathFoo = "/foo"

//nolint:gochecknoglobals // synthetic fixture, mirrors a real lookup table
var opPaths = map[string]string{
	"/bar":  "Bar",
	pathFoo: "Foo",
}

func (h *Handler) RouteMatcher() bool {
	_, ok := opPaths[""]

	return ok
}

func (h *Handler) MatchPriority() int { return 85 }
