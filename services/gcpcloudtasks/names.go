package gcpcloudtasks

import (
	"regexp"
	"strings"
)

var (
	queueIDRe = regexp.MustCompile(`^[a-zA-Z0-9-]{1,100}$`)
	taskIDRe  = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,500}$`)
)

const (
	locationParts = 4
	queueParts    = 6
	taskParts     = 8
)

// parseLocation validates projects/{p}/locations/{l}.
func parseLocation(name string) error {
	p := strings.Split(name, "/")
	if len(p) != locationParts || p[0] != "projects" || p[2] != "locations" || p[1] == "" || p[3] == "" {
		return errInvalid("Invalid location name %q.", name)
	}

	return nil
}

// parseQueueName validates projects/{p}/locations/{l}/queues/{q} and returns its location.
func parseQueueName(name string) (string, error) {
	p := strings.Split(name, "/")
	if len(p) != queueParts || p[4] != "queues" || parseLocation(strings.Join(p[:locationParts], "/")) != nil {
		return "", errInvalid("Invalid queue name %q.", name)
	}

	if !queueIDRe.MatchString(p[5]) {
		return "", errInvalid("Queue ID %q must contain only letters, numbers and hyphens (1-100 characters).", p[5])
	}

	return strings.Join(p[:locationParts], "/"), nil
}

// parseTaskName validates a full task name and returns its queue name.
func parseTaskName(name string) (string, error) {
	p := strings.Split(name, "/")
	if len(p) != taskParts || p[6] != "tasks" {
		return "", errInvalid("Invalid task name %q.", name)
	}

	queue := strings.Join(p[:queueParts], "/")
	if _, err := parseQueueName(queue); err != nil {
		return "", err
	}

	if !taskIDRe.MatchString(p[7]) {
		return "", errInvalid(
			"Task ID %q must contain only letters, numbers, underscores and hyphens (1-500 characters).",
			p[7],
		)
	}

	return queue, nil
}

func taskNameOf(queue, id string) string { return queue + "/tasks/" + id }

// parseCmekName validates projects/{p}/locations/{l}/cmekConfig.
func parseCmekName(name string) error {
	loc, ok := strings.CutSuffix(name, "/cmekConfig")
	if !ok {
		return errInvalid("Invalid CMEK config name %q.", name)
	}

	return parseLocation(loc)
}
