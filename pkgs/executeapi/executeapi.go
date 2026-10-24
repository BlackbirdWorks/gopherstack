// Package executeapi parses API Gateway invoke URLs that carry the API id in the host or an
// internal path prefix.
package executeapi

import (
	"net/http"
	"strings"
)

const (
	hostLabel  = "execute-api"
	pathPrefix = "/_aws/execute-api/"
)

// Parse extracts the API id and the remaining "/{stage}/{path}" from an invoke request, which is
// either {apiId}.execute-api.<host>[:port]/{stage}/{path} or /_aws/execute-api/{apiId}/{stage}/{path}.
func Parse(r *http.Request) (string, string, bool) {
	if rest, ok := strings.CutPrefix(r.URL.Path, pathPrefix); ok {
		id, tail, _ := strings.Cut(rest, "/")
		if id == "" {
			return "", "", false
		}

		return id, "/" + tail, true
	}

	if id, ok := hostAPIID(r.Host); ok {
		return id, r.URL.Path, true
	}

	return "", "", false
}

func hostAPIID(host string) (string, bool) {
	if i := strings.LastIndexByte(host, ':'); i >= 0 && !strings.Contains(host[i:], "]") {
		host = host[:i]
	}

	id, rest, ok := strings.Cut(host, "."+hostLabel+".")
	if !ok || id == "" || rest == "" || strings.Contains(id, ".") {
		return "", false
	}

	return id, true
}

// NoBasePath is the wire value AWS uses for a mapping at the domain root.
const NoBasePath = "(none)"

// HostName returns the lower-cased request host without its port.
func HostName(r *http.Request) string {
	host := r.Host
	if i := strings.LastIndexByte(host, ':'); i >= 0 && !strings.Contains(host[i:], "]") {
		host = host[:i]
	}

	return strings.ToLower(host)
}

// MatchBasePath picks the longest key that is a whole-segment prefix of path and returns it with
// the remaining path (always starting with "/"). The empty key and "(none)" match any path as
// the lowest-precedence fallback.
func MatchBasePath(keys []string, path string) (string, string, bool) {
	best, found := "", false
	rest := path

	for _, key := range keys {
		norm := strings.Trim(key, "/")
		if norm == NoBasePath {
			norm = ""
		}

		if norm == "" {
			if !found {
				best, found, rest = key, true, path
			}

			continue
		}

		tail, ok := strings.CutPrefix(path, "/"+norm)
		if !ok || (tail != "" && tail[0] != '/') {
			continue
		}

		if bn := strings.Trim(best, "/"); !found || bn == NoBasePath || len(norm) > len(bn) {
			best, found, rest = key, true, tail
		}
	}

	if rest == "" {
		rest = "/"
	}

	return best, rest, found
}
