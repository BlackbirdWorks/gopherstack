package gcpfunctions

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

var idRe = regexp.MustCompile(`^[a-z]([-a-z0-9]*[a-z0-9])?$`)

const (
	maxIDLen  = 63
	hashLen   = 10
	nameParts = 6
	allLocs   = "-"
)

// resName is a parsed projects/{p}/locations/{l}[/functions/{f}] name.
type resName struct {
	project  string
	location string
	id       string
}

func (n resName) parent() string { return "projects/" + n.project + "/locations/" + n.location }

func (n resName) full() string { return n.parent() + "/functions/" + n.id }

func parseParent(name string) (resName, error) {
	p := strings.Split(name, "/")
	if len(p) != 4 || p[0] != "projects" || p[2] != "locations" || p[1] == "" || p[3] == "" {
		return resName{}, errInvalid("Invalid resource name %q.", name)
	}

	return resName{project: p[1], location: p[3]}, nil
}

func parseName(name string) (resName, error) {
	p := strings.Split(name, "/")
	if len(p) != nameParts || p[4] != "functions" || p[5] == "" {
		return resName{}, errInvalid("Invalid resource name %q.", name)
	}

	n, err := parseParent(strings.Join(p[:4], "/"))
	if err != nil || n.location == allLocs {
		return resName{}, errInvalid("Invalid resource name %q.", name)
	}

	n.id = p[5]

	return n, nil
}

func validateID(id string) error {
	if len(id) > maxIDLen || !idRe.MatchString(id) {
		return errInvalid("function_id %q must be 1-%d lowercase letters, digits or hyphens, starting with a "+
			"letter and not ending with a hyphen.", id, maxIDLen)
	}

	return nil
}

func shortHash(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "/")))

	return hex.EncodeToString(sum[:])[:hashLen]
}

// hostLabel is the DNS label a function is served under: <function>-<hash>-<location>.
func hostLabel(n resName) string {
	return n.id + "-" + shortHash(n.project, n.location, n.id) + "-" + n.location
}

func lastSegment(name string) string {
	return name[strings.LastIndex(name, "/")+1:]
}
