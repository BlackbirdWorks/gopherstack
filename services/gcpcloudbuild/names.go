package gcpcloudbuild

import (
	"regexp"
	"strings"
)

const (
	globalLocation = "global"
	kwProjects     = "projects"
	kwLocations    = "locations"
)

var (
	idPattern       = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$`)
	locationPattern = regexp.MustCompile(`^[a-z][-a-z0-9]{0,62}$`)
)

// ref identifies a build, trigger or worker pool.
type ref struct {
	project  string
	location string
	id       string
}

func (r ref) key() string { return r.project + "/" + r.id }

func (r ref) name(kind string) string {
	return "projects/" + r.project + "/locations/" + r.location + "/" + kind + "/" + r.id
}

// parseParent accepts "projects/p/locations/l", "projects/p" or "" (with projectID).
func parseParent(parent, projectID string) (string, string, error) {
	if parent == "" {
		if projectID == "" {
			return "", "", errInvalid("A project is required.")
		}

		return projectID, globalLocation, nil
	}

	parts := strings.Split(parent, "/")

	switch {
	case len(parts) == 2 && parts[0] == kwProjects && parts[1] != "":
		return parts[1], globalLocation, nil
	case len(parts) == 4 && parts[0] == kwProjects && parts[2] == kwLocations &&
		parts[1] != "" && locationPattern.MatchString(parts[3]):
		return parts[1], parts[3], nil
	}

	return "", "", errInvalid(
		"Invalid parent %q: want projects/{project}/locations/{location}.",
		parent,
	)
}

// parseName parses projects/p/locations/l/<kind>/id or the legacy projects/p/<kind>/id.
func parseName(name, kind string) (ref, error) {
	parts := strings.Split(name, "/")

	switch {
	case len(parts) == 4 && parts[0] == kwProjects && parts[2] == kind && parts[1] != "" && parts[3] != "":
		return ref{project: parts[1], location: globalLocation, id: parts[3]}, nil
	case len(parts) == 6 && parts[0] == kwProjects && parts[2] == kwLocations && parts[4] == kind &&
		parts[1] != "" && parts[3] != "" && parts[5] != "":
		return ref{project: parts[1], location: parts[3], id: parts[5]}, nil
	}

	return ref{}, errInvalid("Invalid resource name %q.", name)
}

// resolveRef finds the resource named by name, or by the legacy project_id and id pair.
func resolveRef(name, project, id, kind string) (ref, error) {
	if name != "" {
		return parseName(name, kind)
	}

	if project == "" || id == "" {
		return ref{}, errInvalid("Either name or project_id and id are required.")
	}

	return ref{project: project, location: globalLocation, id: id}, nil
}

// projectNumber derives a stable 12-digit project number from a project id.
const projectNumberMod = 1_000_000_000_000

func projectNumber(project string) string {
	var h uint64 = 14695981039346656037

	for i := range len(project) {
		h ^= uint64(project[i])
		h *= 1099511628211
	}

	n := h % projectNumberMod

	const width = 12

	s := make([]byte, width)
	for i := width - 1; i >= 0; i-- {
		s[i] = byte('0' + n%10)
		n /= 10
	}

	return string(s)
}
