package registryauth

import "strings"

const (
	hubHost      = "docker.io"
	hubServerURL = "https://index.docker.io/v1/"
	hubLibrary   = "library/"
)

// imageRef is a parsed image reference split into registry host and remainder.
type imageRef struct {
	host   string
	path   string
	tag    string
	digest string
}

// normalizeHost strips scheme and path and folds the Docker Hub aliases.
func normalizeHost(h string) string {
	h = strings.TrimSpace(strings.ToLower(h))
	h = strings.TrimPrefix(strings.TrimPrefix(h, "https://"), "http://")

	if i := strings.IndexByte(h, '/'); i >= 0 {
		h = h[:i]
	}

	switch h {
	case "index.docker.io", "registry-1.docker.io", "registry.hub.docker.com", "":
		return hubHost
	}

	return h
}

func looksLikeHost(first string) bool {
	return strings.ContainsAny(first, ".:") || first == "localhost"
}

func parseRef(ref string) imageRef {
	var r imageRef

	rest := ref
	if i := strings.IndexByte(rest, '@'); i >= 0 {
		rest, r.digest = rest[:i], rest[i+1:]
	}

	first, tail, hasSlash := strings.Cut(rest, "/")
	if hasSlash && looksLikeHost(first) {
		r.host = normalizeHost(first)
		rest = tail
	} else {
		r.host = hubHost
	}

	if i := strings.LastIndexByte(rest, ':'); i >= 0 && !strings.Contains(rest[i:], "/") {
		rest, r.tag = rest[:i], rest[i+1:]
	}

	r.path = rest

	return r
}

// repoPath is the path with the implicit Docker Hub library/ prefix applied.
func (r imageRef) repoPath() string {
	if r.host == hubHost && !strings.Contains(r.path, "/") {
		return hubLibrary + r.path
	}

	return r.path
}

// String renders the reference using the explicit host.
func (r imageRef) String() string {
	s := r.host + "/" + r.repoPath()
	if r.tag != "" {
		s += ":" + r.tag
	}

	if r.digest != "" {
		s += "@" + r.digest
	}

	return s
}

// withHost swaps the registry host (which may carry a path prefix) keeping tag.
func (r imageRef) withHost(host string) string {
	s := strings.TrimSuffix(host, "/") + "/" + r.repoPath()
	if r.tag != "" {
		s += ":" + r.tag
	} else {
		s += ":latest"
	}

	return s
}

// tagged is the original reference with an explicit tag, for retagging.
func (r imageRef) tagged() string {
	t := r.tag
	if t == "" {
		t = "latest"
	}

	return r.host + "/" + r.repoPath() + ":" + t
}
