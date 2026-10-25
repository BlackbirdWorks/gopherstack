package gcpeventarc

import (
	"regexp"
	"strings"
)

var (
	triggerIDRe = regexp.MustCompile(`^[a-z]([-a-z0-9]{1,61}[a-z0-9])$`)
	channelIDRe = regexp.MustCompile(`^[a-z]([-a-z0-9]{1,61}[a-z0-9])$`)
	connIDRe    = regexp.MustCompile(`^[a-z]([-a-z0-9]{1,61}[a-z0-9])$`)
	topicRe     = regexp.MustCompile(`^projects/([^/]+)/topics/([^/]+)$`)
	labelKeyRe  = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)
	labelValRe  = regexp.MustCompile(`^[a-z0-9_-]{0,63}$`)
)

const (
	wildcardLocation = "-"
	maxLabels        = 64
	googleChannelCfg = "googleChannelConfig"
	fieldName        = "name"
)

// parseRes splits a resource name into the ids that follow the given collection kinds.
func parseRes(name string, kinds ...string) ([]string, bool) {
	p := strings.Split(name, "/")
	if len(p) != 2*len(kinds) {
		return nil, false
	}

	ids := make([]string, len(kinds))

	for i, k := range kinds {
		if p[2*i] != k || p[2*i+1] == "" {
			return nil, false
		}

		ids[i] = p[2*i+1]
	}

	return ids, true
}

// parseParent returns the project and location of "projects/{p}/locations/{l}".
func parseParent(name string) (string, string, error) {
	ids, ok := parseRes(name, "projects", "locations")
	if !ok {
		return "", "", errInvalid("Invalid parent %q: want projects/{project}/locations/{location}.", name)
	}

	return ids[0], ids[1], nil
}

// parseChild returns the project, location and id of "projects/{p}/locations/{l}/{kind}/{id}".
func parseChild(name, kind string) (string, string, string, error) {
	ids, ok := parseRes(name, "projects", "locations", kind)
	if !ok {
		return "", "", "", errInvalid(
			"Invalid name %q: want projects/{project}/locations/{location}/%s/{id}.",
			name,
			kind,
		)
	}

	return ids[0], ids[1], ids[2], nil
}

func locationName(project, location string) string {
	return "projects/" + project + "/locations/" + location
}

func childName(project, location, kind, id string) string {
	return locationName(project, location) + "/" + kind + "/" + id
}

func validateLabels(labels map[string]string) error {
	if len(labels) > maxLabels {
		return errInvalid("At most %d labels are allowed.", maxLabels)
	}

	for k, v := range labels {
		if !labelKeyRe.MatchString(k) {
			return errInvalid("Invalid label key %q.", k)
		}

		if !labelValRe.MatchString(v) {
			return errInvalid("Invalid value %q for label %q.", v, k)
		}
	}

	return nil
}
