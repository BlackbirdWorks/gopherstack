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

// ledgerView lets placement consult the container-instance resource ledger.
type ledgerView struct {
	shortfall func(ci *ContainerInstance) string
	free      func(ci *ContainerInstance) (cpu, mem int64, ok bool)
}

// selectContainerInstance picks a container instance for task placement according
// to the service's PlacementStrategy. It returns "" when no instance is suitable;
// resourceReason then names the CPU/MEMORY shortfall that excluded instances, if any.
//
// Strategies (first entry decides):
//   - random  -> uniform random among eligible instances
//   - spread  -> fewest-tasks-first
//   - binpack -> least remaining cpu/memory first when the field names one, else most-tasks-first
//
// Falls back to random when the strategy list is empty or the type is unrecognized.
func selectContainerInstance(
	instances []*ContainerInstance,
	clusterTasks []*Task,
	constraints []PlacementConstraint,
	strategies []PlacementStrategy,
	serviceName string,
	skip map[string]bool,
	ledger ledgerView,
) (string, string) {
	eligible, reason := eligibleInstances(instances, clusterTasks, constraints, serviceName, skip, ledger)
	if len(eligible) == 0 {
		return "", reason
	}

	arns := make([]string, len(eligible))
	for i, ci := range eligible {
		arns[i] = ci.ContainerInstanceArn
	}

	if len(strategies) == 0 {
		return cryptoRandChoice(arns), ""
	}

	switch strings.ToLower(strategies[0].Type) {
	case "spread":
		return leastLoadedInstance(arns, clusterTasks), ""
	case "binpack":
		if arn, ok := binpackByLedger(eligible, strategies[0].Field, ledger); ok {
			return arn, ""
		}

		return mostLoadedInstance(arns, clusterTasks), ""
	default:
		return cryptoRandChoice(arns), ""
	}
}

// eligibleInstances filters to ACTIVE instances that satisfy constraints and the resource ledger.
func eligibleInstances(
	instances []*ContainerInstance,
	clusterTasks []*Task,
	constraints []PlacementConstraint,
	serviceName string,
	skip map[string]bool,
	ledger ledgerView,
) ([]*ContainerInstance, string) {
	eligible := make([]*ContainerInstance, 0, len(instances))
	reason := ""

	for _, ci := range instances {
		arn := ci.ContainerInstanceArn
		if ci.Status != statusActive || skip[arn] ||
			violatesDistinctInstance(constraints, clusterTasks, arn, serviceName) {
			continue
		}

		if ledger.shortfall != nil {
			if r := ledger.shortfall(ci); r != "" {
				reason = pickShortfall(reason, r)

				continue
			}
		}

		eligible = append(eligible, ci)
	}

	return eligible, reason
}

func violatesDistinctInstance(cs []PlacementConstraint, clusterTasks []*Task, arn, serviceName string) bool {
	for _, c := range cs {
		if strings.EqualFold(c.Type, "distinctInstance") &&
			placementViolatesDistinctInstance(clusterTasks, arn, serviceName) {
			return true
		}
	}

	return false
}

// pickShortfall keeps the more specific reason: CPU over MEMORY.
func pickShortfall(cur, next string) string {
	if cur == failureReasonCPU || next == "" {
		return cur
	}

	return next
}

// binpackByLedger picks the eligible instance with the least remaining cpu or memory (per field).
func binpackByLedger(eligible []*ContainerInstance, field string, ledger ledgerView) (string, bool) {
	f := strings.ToLower(field)
	if (f != "cpu" && f != "memory") || ledger.free == nil {
		return "", false
	}

	best, bestFree, found := "", int64(0), false

	for _, ci := range eligible {
		cpu, mem, ok := ledger.free(ci)
		if !ok {
			continue
		}

		v := mem
		if f == "cpu" {
			v = cpu
		}

		if !found || v < bestFree {
			best, bestFree, found = ci.ContainerInstanceArn, v, true
		}
	}

	return best, found
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
