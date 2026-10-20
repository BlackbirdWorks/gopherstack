package gcprun

import (
	"strconv"

	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"

	"google.golang.org/protobuf/types/known/timestamppb"
)

var idRe = regexp.MustCompile(`^[a-z]([-a-z0-9]*[a-z0-9])?$`)

const (
	maxIDLen     = 49
	hashLen      = 10
	baseParts    = 4
	namePairSize = 2
)

// resName is a parsed projects/{p}/locations/{l}/{kind}/{id}... resource name.
type resName struct {
	project  string
	location string
	ids      []string
}

func (n resName) parent() string { return "projects/" + n.project + "/locations/" + n.location }

// parseName validates a resource name whose collection path after the location is kinds.
func parseName(name string, kinds ...string) (resName, error) {
	p := strings.Split(name, "/")
	if len(p) != baseParts+namePairSize*len(kinds) || p[0] != "projects" || p[2] != "locations" ||
		p[1] == "" || p[3] == "" {
		return resName{}, errInvalid("Invalid resource name %q.", name)
	}

	n := resName{project: p[1], location: p[3]}

	for i, k := range kinds {
		kind, id := p[baseParts+namePairSize*i], p[baseParts+namePairSize*i+1]
		if kind != k || id == "" {
			return resName{}, errInvalid("Invalid resource name %q.", name)
		}

		n.ids = append(n.ids, id)
	}

	return n, nil
}

// parseParent validates projects/{p}/locations/{l}.
func parseParent(name string) (resName, error) { return parseName(name) }

func validateID(id, what string) error {
	if len(id) > maxIDLen || !idRe.MatchString(id) {
		return errInvalid("%s %q must be 1-%d lowercase letters, digits or hyphens, starting with a letter and "+
			"not ending with a hyphen.", what, id, maxIDLen)
	}

	return nil
}

func shortHash(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "/")))

	return hex.EncodeToString(sum[:])[:hashLen]
}

// hostLabel is the DNS label a service is served under: <service>-<hash>-<location>.
func hostLabel(n resName) string {
	return n.ids[0] + "-" + shortHash(n.project, n.location, n.ids[0]) + "-" + n.location
}

func lastSegment(name string) string {
	return name[strings.LastIndex(name, "/")+1:]
}

// etag derives the optimistic-concurrency tag of a resource generation.
func etag(uid string, generation int64, at *timestamppb.Timestamp) string {
	return shortHash(uid, strconv.FormatInt(generation, 10), strconv.FormatInt(at.AsTime().UnixNano(), 10))
}
