package gcpcompute

import "strings"

// ref is a parsed reference to a compute resource: a full URL, a project-relative path or a bare name.
type ref struct {
	project string
	scope   string
	loc     string
	name    string
}

func trimURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if i := strings.Index(raw, "/compute/"); i >= 0 {
		rest := raw[i+len("/compute/"):]
		if j := strings.IndexByte(rest, '/'); j >= 0 {
			return rest[j+1:]
		}
	}

	return strings.TrimPrefix(raw, "/")
}

// parseRef parses raw against the expected collection; project and location fall back to the given defaults.
func parseRef(raw, collection, defProject, defLoc string) (ref, bool) {
	parts := strings.Split(trimURL(raw), "/")
	r := ref{project: defProject, loc: defLoc}

	if len(parts) == 0 || parts[len(parts)-1] == "" {
		return r, false
	}

	r.name = parts[len(parts)-1]
	if len(parts) == 1 {
		return r, true
	}

	if parts[len(parts)-2] != collection {
		return r, false
	}

	rest := parts[:len(parts)-2]

	if len(rest) >= 2 && rest[0] == "projects" {
		r.project = rest[1]
		rest = rest[2:]
	}

	switch {
	case len(rest) == 0:
	case len(rest) == 1 && rest[0] == scopeGlobal:
		r.scope = scopeGlobal
	case len(rest) == 2 && (rest[0] == scopeRegion || rest[0] == scopeZone):
		r.scope, r.loc = rest[0], rest[1]
	default:
		return r, false
	}

	return r, true
}

func refGlobal(raw, collection, project string) (ref, *APIError) {
	r, ok := parseRef(raw, collection, project, "")
	if !ok || (r.scope != "" && r.scope != scopeGlobal) {
		return r, errField("resource."+collection, raw, "The URL is malformed.")
	}

	return r, nil
}

func refLocated(raw, collection, scope, project, defLoc string) (ref, *APIError) {
	r, ok := parseRef(raw, collection, project, defLoc)
	if !ok || (r.scope != "" && r.scope != scope) || r.loc == "" {
		return r, errField("resource."+collection, raw, "The URL is malformed.")
	}

	return r, nil
}
