// Package testguardisnameguarded is a synthetic fixture for
// TestIsGuarded_GuardShapes -- the sibling-specific named-guard shape
// (ecr's isRegistryPath): RouteMatcher delegates to a helper whose NAME
// alone (is\w*Path) marks it as a recognized disambiguation check,
// regardless of what its body actually does.
package testguardisnameguarded

import "strings"

const registryPrefix = "/v2"

type Handler struct{}

func (h *Handler) RouteMatcher() bool {
	path := ""
	if strings.HasPrefix(path, registryPrefix) {
		return isRegistryPath(path)
	}

	return false
}

func (h *Handler) MatchPriority() int { return 90 }

func isRegistryPath(path string) bool {
	return strings.HasSuffix(path, "/manifests/") || strings.HasSuffix(path, "/blobs/")
}
