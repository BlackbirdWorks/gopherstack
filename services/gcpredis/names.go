package gcpredis

import (
	"regexp"
	"strings"
)

var idRe = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,38}[a-z0-9])?$`)

const (
	allLocs   = "-"
	nameParts = 6
	parentLen = 4
)

// resName is a parsed projects/{p}/locations/{l}[/instances/{i}] name.
type resName struct {
	project  string
	location string
	id       string
}

func (n resName) parent() string { return "projects/" + n.project + "/locations/" + n.location }

func (n resName) full() string { return n.parent() + "/instances/" + n.id }

func parseParent(name string) (resName, error) {
	p := strings.Split(name, "/")
	if len(p) != parentLen || p[0] != "projects" || p[2] != "locations" || p[1] == "" || p[3] == "" {
		return resName{}, errInvalid("Invalid resource name %q.", name)
	}

	return resName{project: p[1], location: p[3]}, nil
}

func parseName(name string) (resName, error) {
	p := strings.Split(name, "/")
	if len(p) != nameParts || p[4] != "instances" || p[5] == "" {
		return resName{}, errInvalid("Invalid resource name %q.", name)
	}

	n, err := parseParent(strings.Join(p[:parentLen], "/"))
	if err != nil || n.location == allLocs {
		return resName{}, errInvalid("Invalid resource name %q.", name)
	}

	n.id = p[5]

	return n, nil
}

func validateID(id string) error {
	if !idRe.MatchString(id) {
		return errInvalid("instance_id %q must be 1-40 lowercase letters, digits or hyphens, starting with a "+
			"letter and not ending with a hyphen.", id)
	}

	return nil
}
