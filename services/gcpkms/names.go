package gcpkms

import (
	"regexp"
	"strconv"
	"strings"
)

const (
	depthLocation = 2
	depthKeyRing  = 3
	depthKey      = 4
	depthVersion  = 5
)

var idRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,63}$`)

func collections() []string {
	return []string{"projects", "locations", "keyRings", "cryptoKeys", "cryptoKeyVersions"}
}

// parseResource validates a name of exactly depth collection/id pairs and returns it unchanged.
func parseResource(name string, depth int) error {
	parts := strings.Split(name, "/")
	if len(parts) != depth*2 {
		return errInvalid("Invalid resource name %q.", name)
	}

	for i, c := range collections()[:depth] {
		if parts[2*i] != c || parts[2*i+1] == "" {
			return errInvalid("Invalid resource name %q.", name)
		}
	}

	return nil
}

func parseJobName(name string) error {
	parts := strings.Split(name, "/")
	if len(parts) != 2*depthKeyRing+2 || parts[2*depthKeyRing] != "importJobs" || parts[2*depthKeyRing+1] == "" {
		return errInvalid("Invalid import job name %q.", name)
	}

	return parseResource(strings.Join(parts[:2*depthKeyRing], "/"), depthKeyRing)
}

func parentOf(name string) string {
	rest, _, ok := strings.CutLast(name, "/")
	if !ok {
		return ""
	}

	parent, _, _ := strings.CutLast(rest, "/")

	return parent
}

func idOf(name string) string {
	_, id, _ := strings.CutLast(name, "/")

	return id
}

func validID(kind, id string) error {
	if !idRe.MatchString(id) {
		return errInvalid("Invalid %s ID %q. Must match [a-zA-Z0-9_-]{1,63}.", kind, id)
	}

	return nil
}

func versionName(key string, id int64) string {
	return key + "/cryptoKeyVersions/" + strconv.FormatInt(id, 10)
}

// splitVersion splits a cryptoKeyVersions name into its key name and numeric id.
func splitVersion(name string) (string, int64, error) {
	if err := parseResource(name, depthVersion); err != nil {
		return "", 0, err
	}

	id, err := strconv.ParseInt(idOf(name), 10, 64)
	if err != nil || id < 1 {
		return "", 0, errInvalid("Invalid CryptoKeyVersion name %q.", name)
	}

	return parentOf(name), id, nil
}
