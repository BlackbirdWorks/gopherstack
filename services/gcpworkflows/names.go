package gcpworkflows

import (
	"hash/fnv"
	"regexp"
	"strconv"
	"strings"
)

var (
	workflowIDRe = regexp.MustCompile(`^[A-Za-z]([-_A-Za-z0-9]{0,62}[A-Za-z0-9])?$`)
	labelKeyRe   = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)
	labelValRe   = regexp.MustCompile(`^[a-z0-9_-]{0,63}$`)
	accountRe    = regexp.MustCompile(`^(projects/[^/]+/serviceAccounts/)?[^@\s/]+(@[^@\s/]+)?$`)
	cryptoKeyRe  = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+/keyRings/[^/]+/cryptoKeys/[^/]+$`)
)

const (
	kindWorkflows  = "workflows"
	kindExecutions = "executions"
	wildcardLoc    = "-"
	maxLabels      = 64
	maxDescription = 1000
	maxEnvVars     = 20
	maxEnvValue    = 4096
	maxArgument    = 32 * 1024
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

func parseParent(name string) (string, string, error) {
	ids, ok := parseRes(name, "projects", "locations")
	if !ok {
		return "", "", errInvalid("Invalid parent %q: want projects/{project}/locations/{location}.", name)
	}

	return ids[0], ids[1], nil
}

func parseWorkflow(name string) (string, string, string, error) {
	ids, ok := parseRes(name, "projects", "locations", kindWorkflows)
	if !ok {
		return "", "", "", errInvalid(
			"Invalid workflow name %q: want projects/{project}/locations/{location}/workflows/{workflow}.", name)
	}

	return ids[0], ids[1], ids[2], nil
}

func validateExecutionName(name string) error {
	if _, ok := parseRes(name, "projects", "locations", kindWorkflows, kindExecutions); !ok {
		return errInvalid(
			"Invalid execution name %q: want "+
				"projects/{project}/locations/{location}/workflows/{workflow}/executions/{execution}.",
			name,
		)
	}

	return nil
}

// lastSegment returns the text after the final slash.
func lastSegment(name string) string {
	if _, after, ok := strings.CutLast(name, "/"); ok {
		return after
	}

	return name
}

func locationName(project, location string) string {
	return "projects/" + project + "/locations/" + location
}

func workflowName(project, location, id string) string {
	return locationName(project, location) + "/workflows/" + id
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

const projectNumberMod = 900000000000

// projectNumber derives a stable 12-digit project number from the project id.
func projectNumber(project string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(project))

	return strconv.FormatUint(h.Sum64()%projectNumberMod+100000000000, 10)
}

func defaultServiceAccount(project string) string {
	return "projects/" + project + "/serviceAccounts/" + projectNumber(
		project,
	) + "-compute@developer.gserviceaccount.com"
}

// normalizeAccount returns the projects/{project}/serviceAccounts/{account} form of a service account.
func normalizeAccount(project, sa string) string {
	if sa == "" {
		return defaultServiceAccount(project)
	}

	if strings.HasPrefix(sa, "projects/") {
		return sa
	}

	return "projects/" + project + "/serviceAccounts/" + sa
}

// accountEmail extracts the account from a service account resource name.
func accountEmail(sa string) string { return lastSegment(sa) }
