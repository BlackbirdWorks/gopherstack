// Package testguardisnameunguarded is the near-miss counterpart to
// testguardisnameguarded: the exact same internal HasSuffix logic, but the
// helper is named registryPathMatches (not is\w*Path), so the naming
// convention doesn't fire and neither does any other bullet ("/manifests/"
// and "/blobs/" aren't ARN-shaped literals).
package testguardisnameunguarded

import "strings"

const registryPrefix = "/v2"

type Handler struct{}

func (h *Handler) RouteMatcher() bool {
	path := ""
	if strings.HasPrefix(path, registryPrefix) {
		return registryPathMatches(path)
	}

	return false
}

func (h *Handler) MatchPriority() int { return 90 }

func registryPathMatches(path string) bool {
	return strings.HasSuffix(path, "/manifests/") || strings.HasSuffix(path, "/blobs/")
}
