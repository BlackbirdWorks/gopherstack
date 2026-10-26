package gcpcompute

import (
	"net/http"
	"net/url"
	"strings"
)

const (
	nameInRegionOrZone = 2
	maxPathTail        = 3
	verbList           = "list"
	verbAggList        = "aggList"
	verbGet            = "get"
	verbInsert         = "insert"
	verbDelete         = "delete"
	verbPatch          = "patch"
	verbUpdate         = "update"
)

var apiPrefixes = []string{ //nolint:gochecknoglobals // read-only lookup table
	"/compute/v1/",
	"/compute/beta/",
	"/compute/alpha/",
}

type reqRoute struct {
	project    string
	scope      string
	loc        string
	collection string
	name       string
	action     string
	aggregated bool
}

func (r reqRoute) verb(method string) string {
	switch {
	case r.action != "":
		return r.action
	case r.aggregated:
		return verbAggList
	case r.name == "" && method == http.MethodGet:
		return verbList
	case r.name == "" && method == http.MethodPost:
		return verbInsert
	}

	switch method {
	case http.MethodGet:
		return verbGet
	case http.MethodDelete:
		return verbDelete
	case http.MethodPatch:
		return verbPatch
	case http.MethodPut:
		return verbUpdate
	}

	return ""
}

func splitPath(escaped string) ([]string, bool) {
	var raw string

	for _, p := range apiPrefixes {
		if after, ok := strings.CutPrefix(escaped, p); ok {
			raw = after

			break
		}
	}

	if raw == "" {
		return nil, false
	}

	parts := strings.Split(strings.Trim(raw, "/"), "/")
	for i, p := range parts {
		u, err := url.PathUnescape(p)
		if err != nil {
			return nil, false
		}

		parts[i] = u
	}

	if len(parts) < 2 || parts[0] != collProjects || parts[1] == "" {
		return nil, false
	}

	return parts, true
}

func parseRequest(escapedPath string) (reqRoute, bool) {
	parts, ok := splitPath(escapedPath)
	if !ok {
		return reqRoute{}, false
	}

	rt := reqRoute{project: parts[1]}
	rest := parts[2:]

	if len(rest) == 0 {
		rt.collection, rt.name = collProjects, rt.project

		return rt, true
	}

	switch rest[0] {
	case scopeGlobal:
		rt.scope, rest = scopeGlobal, rest[1:]
	case "aggregated":
		if len(rest) != nameInRegionOrZone {
			return reqRoute{}, false
		}

		rt.aggregated, rt.collection = true, rest[1]

		return rt, true
	case scopeRegion, scopeZone:
		if len(rest) <= nameInRegionOrZone {
			rt.collection = rest[0]
			if len(rest) == nameInRegionOrZone {
				rt.name = rest[1]
			}

			return rt, true
		}

		rt.scope, rt.loc, rest = rest[0], rest[1], rest[2:]
	default:
		if len(rest) == maxPathTail {
			// The SDK sends targetHttp(s)Proxies setUrlMap/setSslCertificates without the global segment.
			rt.scope = scopeGlobal

			return finishRoute(rt, rest)
		}

		rt.collection, rt.name, rt.action = collProjects, rt.project, rest[0]

		return rt, len(rest) == 1
	}

	return finishRoute(rt, rest)
}

func finishRoute(rt reqRoute, rest []string) (reqRoute, bool) {
	if len(rest) == 0 || len(rest) > maxPathTail {
		return reqRoute{}, false
	}

	rt.collection = rest[0]

	if len(rest) > 1 {
		rt.name = rest[1]
	}

	if len(rest) == maxPathTail {
		rt.action = rest[2]
	}

	if rt.collection == "images" && rt.name == "family" && rt.action != "" {
		rt.name, rt.action = rt.action, "getFromFamily"
	}

	return rt, true
}
