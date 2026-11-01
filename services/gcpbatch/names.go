package gcpbatch

import (
	"regexp"
	"strconv"
	"strings"
)

const (
	kindJobs       = "jobs"
	kindGroups     = "taskGroups"
	kindTasks      = "tasks"
	kindOperations = "operations"
	anyLocation    = "-"
	groupName      = "group0"
	maxLabels      = 64
	maxPriority    = 100
)

var (
	jobIDRe    = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$`)
	locationRe = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
	labelKeyRe = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)
	labelValRe = regexp.MustCompile(`^[a-z0-9_-]{0,63}$`)
	envNameRe  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	secretRe   = regexp.MustCompile(`^projects/[^/]+/secrets/[^/]+/versions/[^/]+$`)
	saEmailRe  = regexp.MustCompile(`^[^@/\s]+@[^@/\s]+\.iam\.gserviceaccount\.com$`)
	requestRe  = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
)

// regions are the locations Batch lists; jobs may target any of them.
//
//nolint:gochecknoglobals // immutable lookup table
var regions = []string{
	"asia-east1", "asia-east2", "asia-northeast1", "asia-northeast2", "asia-northeast3", "asia-south1",
	"asia-southeast1", "asia-southeast2", "australia-southeast1", "europe-north1", "europe-west1",
	"europe-west2", "europe-west3", "europe-west4", "europe-west6", "northamerica-northeast1",
	"southamerica-east1", "us-central1", "us-east1", "us-east4", "us-west1", "us-west2", "us-west3", "us-west4",
}

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
	if !ok || (ids[1] != anyLocation && !locationRe.MatchString(ids[1])) {
		return "", "", errInvalid("Invalid parent %q: want projects/{project}/locations/{location}.", name)
	}

	return ids[0], ids[1], nil
}

func parseJobName(name string) (string, string, error) {
	ids, ok := parseRes(name, "projects", "locations", kindJobs)
	if !ok {
		return "", "", errInvalid("Invalid name %q: want projects/{project}/locations/{location}/jobs/{job}.", name)
	}

	return ids[0], ids[1], nil
}

// parseGroupName returns the job name of "{job}/taskGroups/{group}" and the group id.
func parseGroupName(name string) (string, string, error) {
	i := strings.Index(name, "/"+kindGroups+"/")
	if i < 0 {
		return "", "", errInvalid("Invalid parent %q: want {job}/taskGroups/{task_group}.", name)
	}

	job, group := name[:i], name[i+len(kindGroups)+2:]
	if _, _, err := parseJobName(job); err != nil || group == "" || strings.Contains(group, "/") {
		return "", "", errInvalid("Invalid parent %q: want {job}/taskGroups/{task_group}.", name)
	}

	return job, group, nil
}

// parseTaskName returns the job name, group id and task index of ".../taskGroups/{g}/tasks/{n}".
func parseTaskName(name string) (string, string, int, error) {
	i := strings.LastIndex(name, "/"+kindTasks+"/")
	if i < 0 {
		return "", "", 0, errInvalid("Invalid name %q: want {job}/taskGroups/{task_group}/tasks/{task}.", name)
	}

	job, group, err := parseGroupName(name[:i])
	if err != nil {
		return "", "", 0, errInvalid("Invalid name %q: want {job}/taskGroups/{task_group}/tasks/{task}.", name)
	}

	n, convErr := strconv.Atoi(name[i+len(kindTasks)+2:])
	if convErr != nil || n < 0 {
		return "", "", 0, errNotFound("Task %q not found.", name)
	}

	return job, group, n, nil
}

func locationName(project, location string) string {
	return "projects/" + project + "/locations/" + location
}

func jobName(project, location, id string) string {
	return locationName(project, location) + "/" + kindJobs + "/" + id
}

func groupFullName(job string) string { return job + "/" + kindGroups + "/" + groupName }

func taskName(job string, idx int) string {
	return groupFullName(job) + "/" + kindTasks + "/" + strconv.Itoa(idx)
}

func itoa(i int) string { return strconv.Itoa(i) }
