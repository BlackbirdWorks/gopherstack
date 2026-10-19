package gcpscheduler

import (
	"regexp"
	"strings"
)

var jobIDRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,500}$`)

const (
	projectsPrefix = "projects"
	projectParts   = 2
	locationParts  = 4
	jobParts       = 6
)

// parseProject validates projects/{p}.
func parseProject(name string) error {
	p := strings.Split(name, "/")
	if len(p) != projectParts || p[0] != projectsPrefix || p[1] == "" {
		return errInvalid("Invalid project name %q.", name)
	}

	return nil
}

// parseLocation validates projects/{p}/locations/{l}.
func parseLocation(name string) error {
	p := strings.Split(name, "/")
	if len(p) != locationParts || p[0] != projectsPrefix || p[2] != "locations" || p[1] == "" || p[3] == "" {
		return errInvalid("Invalid location name %q.", name)
	}

	return nil
}

// parseJobName validates projects/{p}/locations/{l}/jobs/{j} and returns its location.
func parseJobName(name string) (string, error) {
	p := strings.Split(name, "/")
	if len(p) != jobParts || p[4] != "jobs" || parseLocation(strings.Join(p[:locationParts], "/")) != nil {
		return "", errInvalid("Invalid job name %q.", name)
	}

	if !jobIDRe.MatchString(p[5]) {
		return "", errInvalid(
			"Job ID %q must contain only letters, numbers, underscores and hyphens (1-500 characters).", p[5])
	}

	return strings.Join(p[:locationParts], "/"), nil
}
