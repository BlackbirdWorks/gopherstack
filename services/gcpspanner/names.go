package gcpspanner

import (
	"regexp"
	"strings"
)

var (
	instanceIDRe = regexp.MustCompile(`^[a-z][-a-z0-9]{0,62}[a-z0-9]$`)
	databaseIDRe = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,28}[a-z0-9]$`)
)

const (
	projectsSegment = "projects"
	projectParts    = 2
	instanceParts   = 4
	databaseParts   = 6
)

// parseProject parses projects/{p}.
func parseProject(name string) (string, error) {
	p := strings.Split(name, "/")
	if len(p) != projectParts || p[0] != projectsSegment || p[1] == "" {
		return "", errInvalid("Invalid parent %q: want projects/{project}.", name)
	}

	return p[1], nil
}

// parseInstance parses projects/{p}/instances/{i} into the project and instance id.
func parseInstance(name string) (string, string, error) {
	p := strings.Split(name, "/")
	if len(p) != instanceParts || p[0] != projectsSegment || p[2] != "instances" || p[1] == "" || p[3] == "" {
		return "", "", errInvalid("Invalid instance name %q: want projects/{project}/instances/{instance}.", name)
	}

	return p[1], p[3], nil
}

// parseDatabase parses projects/{p}/instances/{i}/databases/{d}.
func parseDatabase(name string) (string, string, error) {
	p := strings.Split(name, "/")
	if len(p) != databaseParts || p[4] != "databases" || p[5] == "" {
		return "", "", errInvalid("Invalid database name %q.", name)
	}

	inst := strings.Join(p[:instanceParts], "/")
	if _, _, err := parseInstance(inst); err != nil {
		return "", "", err
	}

	return inst, p[5], nil
}

func instanceName(project, id string) string { return "projects/" + project + "/instances/" + id }

func configName(project, id string) string { return "projects/" + project + "/instanceConfigs/" + id }

func databaseName(instance, id string) string { return instance + "/databases/" + id }
