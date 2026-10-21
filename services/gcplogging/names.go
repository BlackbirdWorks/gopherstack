package gcplogging

import (
	"net/url"
	"regexp"
	"strings"
)

const (
	defaultBucketID  = "_Default"
	requiredBucketID = "_Required"
	allLogsViewID    = "_AllLogs"
	globalLocation   = "global"
	maxIDLen         = 100
	projectNumberMod = 1_000_000_000_000

	pathDescription = "description"
	pathFilter      = "filter"
	pathDisabled    = "disabled"
	pathDestination = "destination"
	pathBigQuery    = "bigquery_options"
)

var (
	parentRe    = regexp.MustCompile(`^(projects|folders|organizations|billingAccounts)/[^/]+$`)
	logNameRe   = regexp.MustCompile(`^((?:projects|folders|organizations|billingAccounts)/[^/]+)/logs/([^/]+)$`)
	logIDRe     = regexp.MustCompile(`^[A-Za-z0-9/_.-]{1,512}$`)
	resIDRe     = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,100}$`)
	bucketIDRe  = regexp.MustCompile(`^[A-Za-z0-9_-]{1,100}$`)
	parentOfRe  = regexp.MustCompile(`^((?:projects|folders|organizations|billingAccounts)/[^/]+)(?:/|$)`)
	locParentRe = regexp.MustCompile(`^((?:projects|folders|organizations|billingAccounts)/[^/]+)/locations/([^/]+)$`)
	bucketRe    = regexp.MustCompile(
		`^((?:projects|folders|organizations|billingAccounts)/[^/]+)/locations/([^/]+)/buckets/([^/]+)$`)
	viewRe = regexp.MustCompile(
		`^((?:projects|folders|organizations|billingAccounts)/[^/]+/locations/[^/]+/buckets/[^/]+)/views/([^/]+)$`)
)

func parentOf(name string) string {
	m := parentOfRe.FindStringSubmatch(name)
	if m == nil {
		return ""
	}

	return m[1]
}

func checkParent(parent string) error {
	if !parentRe.MatchString(parent) {
		return errInvalid("Invalid parent [%s]: expected projects/[ID], folders/[ID], organizations/[ID] or "+
			"billingAccounts/[ID].", parent)
	}

	return nil
}

// childName validates name as parent/collection/id and returns the parent.
func childName(name, collection string) (string, string, error) {
	parent, rest, ok := strings.Cut(name, "/"+collection+"/")
	if !ok || checkParent(parent) != nil || rest == "" || strings.Contains(rest, "/") {
		return "", "", errInvalid("Invalid %s name [%s].", collection, name)
	}

	return parent, rest, nil
}

// splitLogName validates parent/logs/id and returns the parent.
func splitLogName(name string) (string, error) {
	m := logNameRe.FindStringSubmatch(name)
	if m == nil {
		return "", errInvalid("Invalid log name [%s]: expected [PARENT]/logs/[LOG_ID].", name)
	}

	id := m[2]
	if dec, err := url.PathUnescape(id); err == nil {
		id = dec
	}

	if !logIDRe.MatchString(id) {
		return "", errInvalid("Invalid log id [%s].", id)
	}

	return m[1], nil
}

func bucketName(parent, location, id string) string {
	return parent + "/locations/" + location + "/buckets/" + id
}

func sinkName(parent, id string) string { return parent + "/sinks/" + id }

func checkResourceID(kind, id string) error {
	if !resIDRe.MatchString(id) {
		return errInvalid("Invalid %s id [%s]: use up to %d letters, digits, '_', '-' or '.'.", kind, id, maxIDLen)
	}

	return nil
}
