package ecs

import (
	"crypto/rand"
	"math/big"
	"strconv"
	"strings"
	"time"
)

const (
	propagateTagsTaskDefinition = "TASK_DEFINITION"
	propagateTagsService        = "SERVICE"
	propagateTagsNone           = "NONE"
)

// resolveTaskTags computes the final tag list for a task, applying propagateTags
// semantics and optionally injecting ECS-managed system tags.
func resolveTaskTags(
	explicitTags []Tag,
	propagateTags string,
	enableECSManagedTags bool,
	clusterName string,
	serviceName string,
	td *TaskDefinition,
	tdTags []Tag,
	svcTags []Tag,
) []Tag {
	var base []Tag

	switch strings.ToUpper(propagateTags) {
	case propagateTagsTaskDefinition:
		base = copyTags(tdTags)
	case propagateTagsService:
		base = copyTags(svcTags)
	default:
		base = copyTags(explicitTags)
	}

	// Explicit tags override propagated tags (merge, explicit wins).
	if propagateTags != "" && propagateTags != propagateTagsNone {
		base = mergeTags(base, explicitTags)
	}

	if enableECSManagedTags {
		managed := eCSManagedTags(clusterName, serviceName, td)
		base = mergeTags(base, managed)
	}

	return base
}

// eCSManagedTags returns the aws:ecs:* system tags injected by ECS.
func eCSManagedTags(clusterName, serviceName string, td *TaskDefinition) []Tag {
	tags := []Tag{
		{Key: "aws:ecs:clusterName", Value: clusterName},
	}

	if serviceName != "" {
		tags = append(tags, Tag{Key: "aws:ecs:serviceName", Value: serviceName})
	}

	if td != nil {
		tags = append(
			tags,
			Tag{Key: "aws:ecs:taskDefinitionFamily", Value: td.Family},
			Tag{Key: "aws:ecs:taskDefinitionRevision", Value: strconv.Itoa(td.Revision)},
		)
	}

	return tags
}

// mergeTags merges override tags into base, with overrides winning on key conflicts.
func mergeTags(base, overrides []Tag) []Tag {
	if len(overrides) == 0 {
		return base
	}

	idx := make(map[string]int, len(base))

	for i, t := range base {
		idx[t.Key] = i
	}

	// Use append-only slice to avoid makezero lint warning (non-zero init + append).
	out := append([]Tag(nil), base...)

	for _, t := range overrides {
		if i, ok := idx[t.Key]; ok {
			out[i] = t
		} else {
			idx[t.Key] = len(out)
			out = append(out, t)
		}
	}

	return out
}

// placementViolatesDistinctInstance returns true if placing a task on a given
// container instance would violate the distinctInstance constraint (i.e., a task
// from the same service already runs on that instance).
func placementViolatesDistinctInstance(
	clusterTasks []*Task,
	instanceArn string,
	serviceName string,
) bool {
	group := "service:" + serviceName

	for _, t := range clusterTasks {
		if t.ContainerInstanceArn == instanceArn &&
			t.Group == group &&
			t.LastStatus == statusRunning {
			return true
		}
	}

	return false
}

// selectContainerInstance picks a container instance for task placement according
// to the service's PlacementStrategy. Returns "" when no suitable instance is found.
//
// Supported strategies (evaluated in order):
//   - random  → uniform random selection among eligible instances
//   - spread  → fewest-tasks-first among eligible instances (field ignored)
//   - binpack → most-tasks-first among eligible instances (field ignored)
//
// Falls back to random when the strategy list is empty or the type is unrecognized.
func selectContainerInstance(
	instances []*ContainerInstance,
	clusterTasks []*Task,
	constraints []PlacementConstraint,
	strategies []PlacementStrategy,
	serviceName string,
) string {
	// Collect eligible instances (ACTIVE + not violating constraints).
	eligible := make([]string, 0, len(instances))

	for _, ci := range instances {
		arn := ci.ContainerInstanceArn
		if ci.Status != statusActive {
			continue
		}

		violates := false

		for _, c := range constraints {
			if strings.EqualFold(c.Type, "distinctInstance") {
				if placementViolatesDistinctInstance(clusterTasks, arn, serviceName) {
					violates = true

					break
				}
			}
		}

		if !violates {
			eligible = append(eligible, arn)
		}
	}

	if len(eligible) == 0 {
		return ""
	}

	if len(strategies) == 0 {
		return cryptoRandChoice(eligible)
	}

	switch strings.ToLower(strategies[0].Type) {
	case "random":
		return cryptoRandChoice(eligible)
	case "spread":
		return leastLoadedInstance(eligible, clusterTasks)
	case "binpack":
		return mostLoadedInstance(eligible, clusterTasks)
	default:
		return cryptoRandChoice(eligible)
	}
}

// cryptoRandChoice picks a random element from a non-empty slice using crypto/rand.
// Placement selection does not require cryptographic security, but we use
// crypto/rand to satisfy the G404 linter rule.
func cryptoRandChoice(items []string) string {
	n, err := rand.Int(rand.Reader, big.NewInt(int64(len(items))))
	if err != nil {
		return items[0]
	}

	return items[n.Int64()]
}

// taskCountOnInstance counts running tasks assigned to a container instance.
func taskCountOnInstance(clusterTasks []*Task, instanceArn string) int {
	count := 0

	for _, t := range clusterTasks {
		if t.ContainerInstanceArn == instanceArn && t.LastStatus == statusRunning {
			count++
		}
	}

	return count
}

// leastLoadedInstance returns the instance with the fewest running tasks.
func leastLoadedInstance(eligible []string, clusterTasks []*Task) string {
	best := eligible[0]
	bestCount := taskCountOnInstance(clusterTasks, best)

	for _, arn := range eligible[1:] {
		if n := taskCountOnInstance(clusterTasks, arn); n < bestCount {
			best = arn
			bestCount = n
		}
	}

	return best
}

// mostLoadedInstance returns the instance with the most running tasks.
func mostLoadedInstance(eligible []string, clusterTasks []*Task) string {
	best := eligible[0]
	bestCount := taskCountOnInstance(clusterTasks, best)

	for _, arn := range eligible[1:] {
		if n := taskCountOnInstance(clusterTasks, arn); n > bestCount {
			best = arn
			bestCount = n
		}
	}

	return best
}

// float64UnixNow returns the current Unix timestamp as float64 (seconds).
func float64UnixNow() float64 {
	return float64(time.Now().Unix())
}

// mergeConstraints combines task-definition constraints with run-time override
// constraints, deduplicating by (type, expression) pair. The task-definition
// constraints take precedence (appear first).
func mergeConstraints(tdConstraints, inputConstraints []PlacementConstraint) []PlacementConstraint {
	if len(inputConstraints) == 0 {
		return tdConstraints
	}

	if len(tdConstraints) == 0 {
		return inputConstraints
	}

	seen := make(map[string]struct{}, len(tdConstraints))
	// Pre-size to the task-definition constraints; append grows for any extra
	// input constraints (avoids a flagged len+len capacity expression).
	merged := make([]PlacementConstraint, 0, len(tdConstraints))

	for _, c := range tdConstraints {
		key := strings.ToLower(c.Type) + "|" + c.Expression
		seen[key] = struct{}{}
		merged = append(merged, c)
	}

	for _, c := range inputConstraints {
		key := strings.ToLower(c.Type) + "|" + c.Expression
		if _, dup := seen[key]; !dup {
			merged = append(merged, c)
		}
	}

	return merged
}
