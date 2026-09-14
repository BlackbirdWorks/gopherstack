// Package testroutetable is synthetic fixture data for
// TestAnalyzeDir_DelegationShapes -- it mirrors resiliencehub/mgn/
// networkmanager's multi-hop route-table shape: RouteMatcher calls a method
// that fans out through a merge helper into two more map-literal-returning
// methods. One table uses AWS's own "RPC over REST" operation-name-shaped
// keys (no leading "/", must NOT produce a claim); the other uses a real
// REST path (must produce one), proving the chase reaches two hops deep and
// the "/"-prefix filter still applies to whatever it finds there.
package testroutetable

type routeEntry struct{ op string }

type Handler struct{}

func (h *Handler) RouteMatcher() bool {
	_, ok := h.routes()["POST create-thing"]

	return ok
}

func (h *Handler) MatchPriority() int { return 85 }

func (h *Handler) routes() map[string]routeEntry {
	return mergeRoutes(h.routesA(), h.routesB())
}

func mergeRoutes(tables ...map[string]routeEntry) map[string]routeEntry {
	out := map[string]routeEntry{}

	for _, t := range tables {
		for k, v := range t {
			out[k] = v
		}
	}

	return out
}

func (h *Handler) routesA() map[string]routeEntry {
	return map[string]routeEntry{
		"POST create-thing": {op: "CreateThing"},
	}
}

func (h *Handler) routesB() map[string]routeEntry {
	return map[string]routeEntry{
		"/things/search": {op: "SearchThings"},
	}
}
