package ecs

import (
	"crypto/rand"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	containerHealthStatusUnknown     = "UNKNOWN"
	containerHealthStatusHealthy     = "HEALTHY"
	deploymentRolloutStateInProgress = "IN_PROGRESS"
	deploymentRolloutStateCompleted  = "COMPLETED"
	deploymentRolloutStateFailed     = "FAILED"
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
	Protocol      string `json:"protocol,omitempty"`
	ContainerPort int    `json:"containerPort,omitempty"`
	HostPort      int    `json:"hostPort,omitempty"`
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

// buildContainerArn constructs a container ARN from a task ARN.
func buildContainerArn(taskArn string) string {
	return "arn:aws:ecs:" + strings.TrimPrefix(
		taskArn,
		"arn:aws:ecs:",
	) + "/container/" + uuid.NewString()
}

// buildNetworkBindingsForContainer converts a container definition's port mappings
// to NetworkBinding records, defaulting the protocol to "tcp" when unset.
func buildNetworkBindingsForContainer(cd ContainerDefinition) []NetworkBinding {
	if len(cd.PortMappings) == 0 {
		return nil
	}

	bindings := make([]NetworkBinding, 0, len(cd.PortMappings))

	for _, pm := range cd.PortMappings {
		if pm.ContainerPort == 0 {
			continue
		}

		proto := pm.Protocol
		if proto == "" {
			proto = transportTCP
		}

		hostPort := pm.HostPort
		if hostPort == 0 {
			hostPort = pm.ContainerPort
		}

		bindings = append(bindings, NetworkBinding{
			BindIP:        "0.0.0.0",
			ContainerPort: pm.ContainerPort,
			HostPort:      hostPort,
			Protocol:      proto,
		})
	}

	return bindings
}

// buildContainerFromDef builds a Container from a ContainerDefinition.
func buildContainerFromDef(taskArn string, initialStatus string, cd ContainerDefinition) Container {
	c := Container{
		ContainerArn: buildContainerArn(taskArn),
		TaskArn:      taskArn,
		Name:         cd.Name,
		Image:        cd.Image,
		LastStatus:   initialStatus,
	}

	if cd.CPU != 0 {
		c.CPU = strconv.Itoa(cd.CPU)
	}

	if cd.Memory != 0 {
		c.Memory = strconv.Itoa(cd.Memory)
	}

	if cd.MemoryReservation != 0 {
		c.MemoryReservation = strconv.Itoa(cd.MemoryReservation)
	}

	if cd.HealthCheck != nil {
		c.HealthStatus = containerHealthStatusUnknown
	}

	c.NetworkBindings = buildNetworkBindingsForContainer(cd)

	return c
}

// buildContainersForTask creates the initial Container slice from a task's
// ContainerDefinitions. Status matches the task's initial status.
func buildContainersForTask(task *Task, td *TaskDefinition) []Container {
	containers := make([]Container, 0, len(td.ContainerDefinitions))

	for _, cd := range td.ContainerDefinitions {
		containers = append(containers, buildContainerFromDef(task.TaskArn, task.LastStatus, cd))
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

			if task.Containers[i].HealthStatus == containerHealthStatusUnknown {
				task.Containers[i].HealthStatus = containerHealthStatusHealthy
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
	ServiceRevisionArn string   `json:"serviceRevisionArn,omitempty"`
	DesiredCount       int      `json:"desiredCount"`
	PendingCount       int      `json:"pendingCount"`
	RunningCount       int      `json:"runningCount"`
	FailedTasks        int      `json:"failedTasks"`
}

// serviceRevisionArnFor derives the ARN of the service revision captured by a
// deployment from the owning service's ARN, following the
// arn:aws:ecs:region:account:service-revision/cluster/service/id scheme.
func serviceRevisionArnFor(svc *Service, revisionID string) string {
	return strings.Replace(svc.ServiceArn, ":service/", ":service-revision/", 1) + "/" + revisionID
}

// newPrimaryDeployment builds the initial PRIMARY deployment for a newly created service.
func newPrimaryDeployment(svc *Service) Deployment {
	now := float64UnixNow()
	revisionID := uuid.NewString()

	return Deployment{
		ID:                 "ecs-svc/" + uuid.NewString(),
		Status:             deploymentStatusPrimary,
		TaskDefinition:     svc.TaskDefinition,
		LaunchType:         svc.LaunchType,
		PlatformVersion:    platformVersionLatest,
		RolloutState:       deploymentRolloutStateInProgress,
		RolloutStateReason: "ECS deployment ecs-svc created.",
		ServiceRevisionArn: serviceRevisionArnFor(svc, revisionID),
		DesiredCount:       svc.DesiredCount,
		CreatedAt:          &now,
		UpdatedAt:          &now,
	}
}

// newActiveDeployment builds a new primary deployment on service update.
// The previous PRIMARY deployment is expected to be demoted to ACTIVE separately.
func newActiveDeployment(svc *Service) Deployment {
	now := float64UnixNow()
	revisionID := uuid.NewString()

	return Deployment{
		ID:                 "ecs-svc/" + uuid.NewString(),
		Status:             deploymentStatusPrimary,
		TaskDefinition:     svc.TaskDefinition,
		LaunchType:         svc.LaunchType,
		PlatformVersion:    platformVersionLatest,
		RolloutState:       deploymentRolloutStateInProgress,
		RolloutStateReason: "ECS deployment ecs-svc created.",
		ServiceRevisionArn: serviceRevisionArnFor(svc, revisionID),
		DesiredCount:       svc.DesiredCount,
		CreatedAt:          &now,
		UpdatedAt:          &now,
	}
}

// ---- ServiceRevision (DescribeServiceRevisions) ----

// ServiceRevision describes the workload configuration captured by a single
// deployment of an ECS service, as returned by DescribeServiceRevisions.
type ServiceRevision struct {
	CreatedAt                   *float64                       `json:"createdAt,omitempty"`
	NetworkConfiguration        *NetworkConfiguration          `json:"networkConfiguration,omitempty"`
	ServiceConnectConfiguration *ServiceConnectConfiguration   `json:"serviceConnectConfiguration,omitempty"`
	ClusterArn                  string                         `json:"clusterArn,omitempty"`
	LaunchType                  string                         `json:"launchType,omitempty"`
	PlatformVersion             string                         `json:"platformVersion,omitempty"`
	ServiceArn                  string                         `json:"serviceArn,omitempty"`
	ServiceRevisionArn          string                         `json:"serviceRevisionArn,omitempty"`
	TaskDefinition              string                         `json:"taskDefinition,omitempty"`
	CapacityProviderStrategy    []CapacityProviderStrategyItem `json:"capacityProviderStrategy,omitempty"`
	LoadBalancers               []LoadBalancer                 `json:"loadBalancers,omitempty"`
	ServiceRegistries           []ServiceRegistry              `json:"serviceRegistries,omitempty"`
}

// buildServiceRevision projects a service's current configuration and a specific
// deployment record into the ServiceRevision shape returned by DescribeServiceRevisions.
func buildServiceRevision(svc *Service, d Deployment) ServiceRevision {
	return ServiceRevision{
		CapacityProviderStrategy:    svc.CapacityProviderStrategy,
		ClusterArn:                  svc.ClusterArn,
		CreatedAt:                   d.CreatedAt,
		LaunchType:                  d.LaunchType,
		LoadBalancers:               svc.LoadBalancers,
		NetworkConfiguration:        svc.NetworkConfiguration,
		PlatformVersion:             d.PlatformVersion,
		ServiceArn:                  svc.ServiceArn,
		ServiceConnectConfiguration: svc.ServiceConnectConfiguration,
		ServiceRegistries:           svc.ServiceRegistries,
		ServiceRevisionArn:          d.ServiceRevisionArn,
		TaskDefinition:              d.TaskDefinition,
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

// ---- Placement enforcement ----

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
