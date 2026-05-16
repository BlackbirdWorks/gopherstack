package ecs

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ---- Container runtime status ----

// NetworkInterface holds the network interface details for a running container.
type NetworkInterface struct {
	AttachmentID       string `json:"attachmentId,omitempty"`
	PrivateIpv4Address string `json:"privateIpv4Address,omitempty"`
	Ipv6Address        string `json:"ipv6Address,omitempty"`
}

// NetworkBinding maps a container port to a host port.
type NetworkBinding struct {
	BindIP        string `json:"bindIP,omitempty"`
	ContainerPort int    `json:"containerPort,omitempty"`
	HostPort      int    `json:"hostPort,omitempty"`
	Protocol      string `json:"protocol,omitempty"`
}

// Container holds the runtime status of a single container within a task.
type Container struct {
	ContainerArn      string             `json:"containerArn,omitempty"`
	TaskArn           string             `json:"taskArn,omitempty"`
	Name              string             `json:"name"`
	Image             string             `json:"image,omitempty"`
	ImageDigest       string             `json:"imageDigest,omitempty"`
	RuntimeID         string             `json:"runtimeId,omitempty"`
	LastStatus        string             `json:"lastStatus"`
	HealthStatus      string             `json:"healthStatus,omitempty"`
	CPU               string             `json:"cpu,omitempty"`
	Memory            string             `json:"memory,omitempty"`
	MemoryReservation string             `json:"memoryReservation,omitempty"`
	Reason            string             `json:"reason,omitempty"`
	ExitCode          *int               `json:"exitCode,omitempty"`
	NetworkInterfaces []NetworkInterface `json:"networkInterfaces,omitempty"`
	NetworkBindings   []NetworkBinding   `json:"networkBindings,omitempty"`
}

// buildContainersForTask creates the initial Container slice from a task's
// ContainerDefinitions. Status matches the task's initial status.
func buildContainersForTask(task *Task, td *TaskDefinition) []Container {
	containers := make([]Container, 0, len(td.ContainerDefinitions))

	for _, cd := range td.ContainerDefinitions {
		c := Container{
			ContainerArn: fmt.Sprintf(
				"arn:aws:ecs:%s",
				strings.TrimPrefix(task.TaskArn, "arn:aws:ecs:"),
			) + "/container/" + uuid.NewString(),
			TaskArn:    task.TaskArn,
			Name:       cd.Name,
			Image:      cd.Image,
			LastStatus: task.LastStatus,
		}

		if cd.CPU != 0 {
			c.CPU = fmt.Sprintf("%d", cd.CPU)
		}

		if cd.Memory != 0 {
			c.Memory = fmt.Sprintf("%d", cd.Memory)
		}

		if cd.MemoryReservation != 0 {
			c.MemoryReservation = fmt.Sprintf("%d", cd.MemoryReservation)
		}

		if cd.HealthCheck != nil {
			c.HealthStatus = "UNKNOWN"
		}

		// Populate network bindings from port mappings.
		for _, pm := range cd.PortMappings {
			if pm.ContainerPort != 0 {
				nb := NetworkBinding{
					BindIP:        "0.0.0.0",
					ContainerPort: pm.ContainerPort,
					HostPort:      pm.HostPort,
					Protocol:      pm.Protocol,
				}

				if nb.Protocol == "" {
					nb.Protocol = "tcp"
				}

				if nb.HostPort == 0 {
					nb.HostPort = pm.ContainerPort
				}

				c.NetworkBindings = append(c.NetworkBindings, nb)
			}
		}

		containers = append(containers, c)
	}

	return containers
}

// syncContainerStatuses updates Container.LastStatus to match the task's LastStatus.
// Called after RunTask completes (success or failure) to keep containers in sync.
func syncContainerStatuses(task *Task, exitCode *int) {
	for i := range task.Containers {
		task.Containers[i].LastStatus = task.LastStatus

		if task.LastStatus == statusStopped && exitCode != nil {
			task.Containers[i].ExitCode = exitCode
		}

		if task.LastStatus == statusRunning {
			task.Containers[i].RuntimeID = uuid.NewString()[:12]

			if task.Containers[i].HealthStatus == "UNKNOWN" {
				task.Containers[i].HealthStatus = "HEALTHY"
			}
		}
	}
}

// ---- Deployment (active deployment record on a Service) ----

// Deployment represents an active or completed deployment on an ECS service.
type Deployment struct {
	CreatedAt          *float64 `json:"createdAt,omitempty"`
	UpdatedAt          *float64 `json:"updatedAt,omitempty"`
	ID                 string   `json:"id"`
	Status             string   `json:"status"`
	TaskDefinition     string   `json:"taskDefinition"`
	LaunchType         string   `json:"launchType,omitempty"`
	PlatformVersion    string   `json:"platformVersion,omitempty"`
	RolloutState       string   `json:"rolloutState,omitempty"`
	RolloutStateReason string   `json:"rolloutStateReason,omitempty"`
	DesiredCount       int      `json:"desiredCount"`
	PendingCount       int      `json:"pendingCount"`
	RunningCount       int      `json:"runningCount"`
	FailedTasks        int      `json:"failedTasks"`
}

// newPrimaryDeployment builds the initial PRIMARY deployment for a newly created service.
func newPrimaryDeployment(svc *Service) Deployment {
	now := float64UnixNow()

	return Deployment{
		ID:                 "ecs-svc/" + uuid.NewString(),
		Status:             "PRIMARY",
		TaskDefinition:     svc.TaskDefinition,
		LaunchType:         svc.LaunchType,
		PlatformVersion:    "LATEST",
		RolloutState:       "IN_PROGRESS",
		RolloutStateReason: "ECS deployment ecs-svc created.",
		DesiredCount:       svc.DesiredCount,
		CreatedAt:          &now,
		UpdatedAt:          &now,
	}
}

// newActiveDeployment builds a new active deployment on service update.
// The previous PRIMARY deployment is expected to be demoted to ACTIVE separately.
func newActiveDeployment(svc *Service) Deployment {
	now := float64UnixNow()

	return Deployment{
		ID:                 "ecs-svc/" + uuid.NewString(),
		Status:             "PRIMARY",
		TaskDefinition:     svc.TaskDefinition,
		LaunchType:         svc.LaunchType,
		PlatformVersion:    "LATEST",
		RolloutState:       "IN_PROGRESS",
		RolloutStateReason: "ECS deployment ecs-svc created.",
		DesiredCount:       svc.DesiredCount,
		CreatedAt:          &now,
		UpdatedAt:          &now,
	}
}

// ---- PropagateTags logic ----

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
		tags = append(tags,
			Tag{Key: "aws:ecs:taskDefinitionFamily", Value: td.Family},
			Tag{Key: "aws:ecs:taskDefinitionRevision", Value: fmt.Sprintf("%d", td.Revision)},
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

	out := make([]Tag, len(base))
	copy(out, base)

	for _, t := range overrides {
		if i, ok := idx[t.Key]; ok {
			out[i] = t
		} else {
			out = append(out, t)
			idx[t.Key] = len(out) - 1
		}
	}

	return out
}

// ---- Placement enforcement ----

// placementViolatesDistinctInstance returns true if placing a task on a given
// container instance would violate the distinctInstance constraint (i.e., a task
// from the same service already runs on that instance).
func placementViolatesDistinctInstance(
	clusterTasks map[string]*Task,
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
	instances map[string]*ContainerInstance,
	clusterTasks map[string]*Task,
	constraints []PlacementConstraint,
	strategies []PlacementStrategy,
	serviceName string,
) string {
	// Collect eligible instances (ACTIVE + not violating constraints).
	eligible := make([]string, 0, len(instances))

	for arn, ci := range instances {
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
		return eligible[rand.IntN(len(eligible))]
	}

	switch strings.ToLower(strategies[0].Type) {
	case "random":
		return eligible[rand.IntN(len(eligible))]

	case "spread":
		// Choose the instance with the fewest running tasks.
		return leastLoadedInstance(eligible, clusterTasks)

	case "binpack":
		// Choose the instance with the most running tasks (pack before spilling).
		return mostLoadedInstance(eligible, clusterTasks)

	default:
		return eligible[rand.IntN(len(eligible))]
	}
}

// taskCountOnInstance counts running tasks assigned to a container instance.
func taskCountOnInstance(clusterTasks map[string]*Task, instanceArn string) int {
	count := 0

	for _, t := range clusterTasks {
		if t.ContainerInstanceArn == instanceArn && t.LastStatus == statusRunning {
			count++
		}
	}

	return count
}

// leastLoadedInstance returns the instance with the fewest running tasks.
func leastLoadedInstance(eligible []string, clusterTasks map[string]*Task) string {
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
func mostLoadedInstance(eligible []string, clusterTasks map[string]*Task) string {
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
