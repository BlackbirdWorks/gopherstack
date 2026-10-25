package gcpbigtable

import (
	"regexp"
	"strings"
)

var (
	instanceIDRe = regexp.MustCompile(`^[a-z][a-z0-9-]+[a-z0-9]$`)
	clusterIDRe  = regexp.MustCompile(`^[a-z][-a-z0-9]*$`)
	profileIDRe  = regexp.MustCompile(`^[a-zA-Z0-9_.~-]+$`)
	tableIDRe    = regexp.MustCompile(`^[_a-zA-Z0-9][-_.a-zA-Z0-9]*$`)
	familyIDRe   = tableIDRe
	backupIDRe   = tableIDRe
	labelRe      = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)
	locationRe   = regexp.MustCompile(`^projects/([^/]+)/locations/([^/]+)$`)
)

const (
	maxInstanceIDLen = 33
	maxClusterIDLen  = 30
	maxTableIDLen    = 50
	maxFamilyIDLen   = 64
	maxLabels        = 64
	maxLabelLen      = 63
	wildcard         = "-"
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

func parseProject(name string) (string, error) {
	ids, ok := parseRes(name, "projects")
	if !ok {
		return "", errInvalid("Invalid parent %q: want projects/{project}.", name)
	}

	return ids[0], nil
}

func parseInstance(name string) (string, string, error) {
	ids, ok := parseRes(name, "projects", "instances")
	if !ok {
		return "", "", errInvalid("Invalid instance name %q: want projects/{project}/instances/{instance}.", name)
	}

	return ids[0], ids[1], nil
}

func parseCluster(name string) (string, error) {
	ids, ok := parseRes(name, "projects", "instances", "clusters")
	if !ok {
		return "", errInvalid("Invalid cluster name %q: want projects/{project}/instances/{instance}/clusters/{cluster}.", name)
	}

	return "projects/" + ids[0] + "/instances/" + ids[1], nil
}

func parseTable(name string) (string, string, error) {
	ids, ok := parseRes(name, "projects", "instances", "tables")
	if !ok {
		return "", "", errInvalid("Invalid table name %q: want projects/{project}/instances/{instance}/tables/{table}.", name)
	}

	return instanceName(ids[0], ids[1]), ids[2], nil
}

func parseProfile(name string) (string, string, error) {
	ids, ok := parseRes(name, "projects", "instances", "appProfiles")
	if !ok {
		return "", "", errInvalid("Invalid app profile name %q.", name)
	}

	return instanceName(ids[0], ids[1]), ids[2], nil
}

func parseBackup(name string) (string, string, string, error) {
	ids, ok := parseRes(name, "projects", "instances", "clusters", "backups")
	if !ok {
		return "", "", "", errInvalid("Invalid backup name %q.", name)
	}

	return instanceName(ids[0], ids[1]), ids[2], ids[3], nil
}

func instanceName(project, id string) string { return "projects/" + project + "/instances/" + id }

func clusterName(instance, id string) string { return instance + "/clusters/" + id }

func tableName(instance, id string) string { return instance + "/tables/" + id }

func profileName(instance, id string) string { return instance + "/appProfiles/" + id }

func backupName(instance, cluster, id string) string {
	return instance + "/clusters/" + cluster + "/backups/" + id
}

func lastSegment(name string) string {
	return name[strings.LastIndexByte(name, '/')+1:]
}
