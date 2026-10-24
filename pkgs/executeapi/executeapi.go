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
