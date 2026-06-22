package ecs

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrDaemonNotFound is returned when a daemon does not exist.
	ErrDaemonNotFound = awserr.New("DaemonNotFoundException", awserr.ErrNotFound)
	// ErrDaemonAlreadyExists is returned when a daemon with the same name exists.
	ErrDaemonAlreadyExists = awserr.New("DaemonAlreadyExistsException", awserr.ErrAlreadyExists)
	// ErrDaemonTaskDefinitionNotFound is returned when a daemon task definition does not exist.
	ErrDaemonTaskDefinitionNotFound = awserr.New(
		"DaemonTaskDefinitionNotFoundException",
		awserr.ErrNotFound,
	)
	// ErrServiceRevisionNotFound is returned when a service revision does not exist.
	ErrServiceRevisionNotFound = awserr.New("ServiceRevisionNotFoundException", awserr.ErrNotFound)
)

// Daemon represents an ECS daemon — a task definition that runs one task per
// container instance in a cluster (similar to a Kubernetes DaemonSet).
type Daemon struct {
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
	DaemonArn      string    `json:"daemonArn"`
	DaemonName     string    `json:"daemonName"`
	ClusterArn     string    `json:"clusterArn"`
	TaskDefinition string    `json:"taskDefinition,omitempty"`
	Status         string    `json:"status"`
}

// DaemonTaskDefinition represents a task definition registered for a daemon.
type DaemonTaskDefinition struct {
	RegisteredAt            time.Time `json:"registeredAt"`
	DaemonTaskDefinitionArn string    `json:"daemonTaskDefinitionArn"`
	DaemonArn               string    `json:"daemonArn"`
	Family                  string    `json:"family"`
	TaskDefinitionArn       string    `json:"taskDefinitionArn,omitempty"`
	Status                  string    `json:"status"`
	Revision                int       `json:"revision"`
}

// DaemonDeployment represents a daemon deployment event.
type DaemonDeployment struct {
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
	ID           string    `json:"id"`
	DaemonArn    string    `json:"daemonArn"`
	Status       string    `json:"status"`
	FailedTasks  int       `json:"failedTasks"`
	PendingTasks int       `json:"pendingTasks"`
	RunningTasks int       `json:"runningTasks"`
}

// DaemonRevision is a point-in-time snapshot of a daemon's task definition.
type DaemonRevision struct {
	CreatedAt         time.Time `json:"createdAt"`
	DaemonRevisionArn string    `json:"daemonRevisionArn"`
	DaemonArn         string    `json:"daemonArn"`
	TaskDefinitionArn string    `json:"taskDefinitionArn"`
	Revision          int       `json:"revision"`
}

// ServiceRevision is a point-in-time snapshot of a service's configuration.
// Created when a service is created or updated, enabling rollback and audit.
type ServiceRevision struct {
	CreatedAt                time.Time                      `json:"createdAt"`
	NetworkConfiguration     *NetworkConfiguration          `json:"networkConfiguration,omitempty"`
	DeploymentConfiguration  *DeploymentConfiguration       `json:"deploymentConfiguration,omitempty"`
	ServiceRevisionArn       string                         `json:"serviceRevisionArn"`
	ServiceArn               string                         `json:"serviceArn"`
	ClusterArn               string                         `json:"clusterArn"`
	TaskDefinition           string                         `json:"taskDefinition"`
	LaunchType               string                         `json:"launchType,omitempty"`
	PlatformVersion          string                         `json:"platformVersion,omitempty"`
	PlatformFamily           string                         `json:"platformFamily,omitempty"`
	CapacityProviderStrategy []CapacityProviderStrategyItem `json:"capacityProviderStrategy,omitempty"`
	LoadBalancers            []LoadBalancer                 `json:"loadBalancers,omitempty"`
	ServiceRegistries        []ServiceRegistry              `json:"serviceRegistries,omitempty"`
}

// AttachmentStateChange represents a single attachment status update from the container agent.
type AttachmentStateChange struct {
	AttachmentArn string `json:"attachmentArn"`
	Status        string `json:"status"`
}

// CreateDaemonInput holds parameters for CreateDaemon.
type CreateDaemonInput struct {
	ClusterName    string
	DaemonName     string
	TaskDefinition string
}

// UpdateDaemonInput holds parameters for UpdateDaemon.
type UpdateDaemonInput struct {
	ClusterName    string
	DaemonName     string
	TaskDefinition string
}

// RegisterDaemonTaskDefinitionInput holds parameters for RegisterDaemonTaskDefinition.
type RegisterDaemonTaskDefinitionInput struct {
	DaemonName        string
	Family            string
	TaskDefinitionArn string
}

// SubmitTaskStateChangeInput holds parameters from the container agent.
type SubmitTaskStateChangeInput struct {
	PullStartedAt      *time.Time
	PullStoppedAt      *time.Time
	ExecutionStoppedAt *time.Time
	Cluster            string
	Task               string
	Status             string
	Reason             string
}

// SubmitContainerStateChangeInput holds parameters from the container agent.
type SubmitContainerStateChangeInput struct {
	ExitCode      *int
	Cluster       string
	Task          string
	ContainerName string
	RuntimeID     string
	Status        string
	Reason        string
}

// GetRegion returns the AWS region this backend is configured for.
func (b *InMemoryBackend) GetRegion() string {
	return b.region
}

// ---- Daemon CRUD ----

// CreateDaemon creates a new ECS daemon in the given cluster.
func (b *InMemoryBackend) CreateDaemon(input CreateDaemonInput) (*Daemon, error) {
	if input.DaemonName == "" {
		return nil, fmt.Errorf("%w: daemonName is required", ErrInvalidParameter)
	}
	if input.ClusterName == "" {
		return nil, fmt.Errorf("%w: cluster is required", ErrInvalidParameter)
	}

	clusterName := clusterKey(b.resolveCluster(input.ClusterName))

	b.mu.Lock("CreateDaemon")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, input.ClusterName)
	}

	if b.daemons[clusterName] == nil {
		b.daemons[clusterName] = make(map[string]*Daemon)
	}

	if _, exists := b.daemons[clusterName][input.DaemonName]; exists {
		return nil, fmt.Errorf("%w: %s", ErrDaemonAlreadyExists, input.DaemonName)
	}

	clusterArn := b.clusters[clusterName].ClusterArn
	daemonArn := arn.Build(
		"ecs",
		b.region,
		b.accountID,
		fmt.Sprintf("daemon/%s/%s", clusterName, input.DaemonName),
	)
	now := time.Now()

	d := &Daemon{
		DaemonArn:      daemonArn,
		DaemonName:     input.DaemonName,
		ClusterArn:     clusterArn,
		TaskDefinition: input.TaskDefinition,
		Status:         statusActive,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	b.daemons[clusterName][input.DaemonName] = d

	// Create initial deployment for the new daemon.
	dep := b.newDaemonDeploymentLocked(daemonArn, clusterName)
	b.daemonDeployments[dep.ID] = dep

	cp := *d

	return &cp, nil
}

// DeleteDaemon removes a daemon from the cluster.
func (b *InMemoryBackend) DeleteDaemon(clusterName, daemonName string) (*Daemon, error) {
	key := clusterKey(b.resolveCluster(clusterName))

	b.mu.Lock("DeleteDaemon")
	defer b.mu.Unlock()

	if _, ok := b.clusters[key]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, clusterName)
	}

	d, ok := b.daemons[key][daemonName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrDaemonNotFound, daemonName)
	}

	delete(b.daemons[key], daemonName)
	delete(b.daemonTaskDefs, d.DaemonArn)
	delete(b.daemonRevisions, d.DaemonArn)

	cp := *d
	cp.Status = statusInactive

	return &cp, nil
}

// DescribeDaemon returns a single daemon by name.
func (b *InMemoryBackend) DescribeDaemon(clusterName, daemonName string) (*Daemon, error) {
	key := clusterKey(b.resolveCluster(clusterName))

	b.mu.RLock("DescribeDaemon")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[key]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, clusterName)
	}

	d, ok := b.daemons[key][daemonName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrDaemonNotFound, daemonName)
	}

	cp := *d

	return &cp, nil
}

// ListDaemons returns all daemons in the cluster.
func (b *InMemoryBackend) ListDaemons(clusterName string) ([]Daemon, error) {
	key := clusterKey(b.resolveCluster(clusterName))

	b.mu.RLock("ListDaemons")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[key]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, clusterName)
	}

	out := make([]Daemon, 0, len(b.daemons[key]))
	for _, d := range b.daemons[key] {
		out = append(out, *d)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].DaemonName < out[j].DaemonName
	})

	return out, nil
}

// UpdateDaemon updates a daemon's task definition.
func (b *InMemoryBackend) UpdateDaemon(input UpdateDaemonInput) (*Daemon, error) {
	if input.DaemonName == "" {
		return nil, fmt.Errorf("%w: daemonName is required", ErrInvalidParameter)
	}

	key := clusterKey(b.resolveCluster(input.ClusterName))

	b.mu.Lock("UpdateDaemon")
	defer b.mu.Unlock()

	if _, ok := b.clusters[key]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, input.ClusterName)
	}

	d, ok := b.daemons[key][input.DaemonName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrDaemonNotFound, input.DaemonName)
	}

	d.TaskDefinition = input.TaskDefinition
	d.UpdatedAt = time.Now()

	// Create a new deployment for the update.
	dep := b.newDaemonDeploymentLocked(d.DaemonArn, key)
	b.daemonDeployments[dep.ID] = dep

	cp := *d

	return &cp, nil
}

// ---- Daemon task definitions ----

// RegisterDaemonTaskDefinition registers a task definition for a daemon.
func (b *InMemoryBackend) RegisterDaemonTaskDefinition(
	input RegisterDaemonTaskDefinitionInput,
) (*DaemonTaskDefinition, error) {
	if input.DaemonName == "" {
		return nil, fmt.Errorf("%w: daemon is required", ErrInvalidParameter)
	}

	b.mu.Lock("RegisterDaemonTaskDefinition")
	defer b.mu.Unlock()

	daemonArn := b.resolveDaemonArnLocked(input.DaemonName)
	if daemonArn == "" {
		return nil, fmt.Errorf("%w: %s", ErrDaemonNotFound, input.DaemonName)
	}

	revisions := b.daemonTaskDefs[daemonArn]
	revision := 1
	if len(revisions) > 0 {
		revision = revisions[len(revisions)-1].Revision + 1
	}

	family := input.Family
	if family == "" {
		family = input.DaemonName
	}

	defArn := fmt.Sprintf(
		"arn:aws:ecs:%s:%s:daemon-task-definition/%s:%d",
		b.region, b.accountID, family, revision,
	)

	def := &DaemonTaskDefinition{
		RegisteredAt:            time.Now(),
		DaemonTaskDefinitionArn: defArn,
		DaemonArn:               daemonArn,
		Family:                  family,
		TaskDefinitionArn:       input.TaskDefinitionArn,
		Status:                  statusActive,
		Revision:                revision,
	}

	b.daemonTaskDefs[daemonArn] = append(revisions, def)

	// Record the revision snapshot.
	rev := &DaemonRevision{
		CreatedAt: def.RegisteredAt,
		DaemonRevisionArn: fmt.Sprintf(
			"arn:aws:ecs:%s:%s:daemon-revision/%s/%d",
			b.region,
			b.accountID,
			input.DaemonName,
			revision,
		),
		DaemonArn:         daemonArn,
		TaskDefinitionArn: input.TaskDefinitionArn,
		Revision:          revision,
	}
	b.daemonRevisions[daemonArn] = append(b.daemonRevisions[daemonArn], rev)

	cp := *def

	return &cp, nil
}

// DescribeDaemonTaskDefinition returns the latest task definition for a daemon.
func (b *InMemoryBackend) DescribeDaemonTaskDefinition(
	daemonName string,
) (*DaemonTaskDefinition, error) {
	b.mu.RLock("DescribeDaemonTaskDefinition")
	defer b.mu.RUnlock()

	daemonArn := b.resolveDaemonArnLocked(daemonName)
	if daemonArn == "" {
		return nil, fmt.Errorf("%w: %s", ErrDaemonNotFound, daemonName)
	}

	revisions := b.daemonTaskDefs[daemonArn]
	if len(revisions) == 0 {
		return nil, fmt.Errorf(
			"%w: no task definition registered for daemon %s",
			ErrDaemonTaskDefinitionNotFound,
			daemonName,
		)
	}

	cp := *revisions[len(revisions)-1]

	return &cp, nil
}

// ListDaemonTaskDefinitions returns all registered task definitions for a daemon.
func (b *InMemoryBackend) ListDaemonTaskDefinitions(
	daemonName string,
) ([]DaemonTaskDefinition, error) {
	b.mu.RLock("ListDaemonTaskDefinitions")
	defer b.mu.RUnlock()

	daemonArn := b.resolveDaemonArnLocked(daemonName)
	if daemonArn == "" {
		return nil, fmt.Errorf("%w: %s", ErrDaemonNotFound, daemonName)
	}

	revisions := b.daemonTaskDefs[daemonArn]
	out := make([]DaemonTaskDefinition, 0, len(revisions))
	for _, r := range revisions {
		out = append(out, *r)
	}

	return out, nil
}

// DeleteDaemonTaskDefinition deregisters a daemon task definition by ARN.
func (b *InMemoryBackend) DeleteDaemonTaskDefinition(daemonTaskDefinitionArn string) error {
	b.mu.Lock("DeleteDaemonTaskDefinition")
	defer b.mu.Unlock()

	for daemonArn, revisions := range b.daemonTaskDefs {
		kept := revisions[:0]
		found := false
		for _, r := range revisions {
			if r.DaemonTaskDefinitionArn == daemonTaskDefinitionArn {
				found = true
			} else {
				kept = append(kept, r)
			}
		}
		if found {
			if len(kept) == 0 {
				delete(b.daemonTaskDefs, daemonArn)
			} else {
				b.daemonTaskDefs[daemonArn] = kept
			}

			return nil
		}
	}

	return fmt.Errorf("%w: %s", ErrDaemonTaskDefinitionNotFound, daemonTaskDefinitionArn)
}

// ---- Daemon deployments ----

// DescribeDaemonDeployments returns deployments for a daemon, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeDaemonDeployments(
	daemonArn string,
	deploymentIDs []string,
) ([]DaemonDeployment, error) {
	b.mu.RLock("DescribeDaemonDeployments")
	defer b.mu.RUnlock()

	var out []DaemonDeployment

	if len(deploymentIDs) > 0 {
		for _, id := range deploymentIDs {
			dep, ok := b.daemonDeployments[id]
			if ok && dep.DaemonArn == daemonArn {
				out = append(out, *dep)
			}
		}
	} else {
		for _, dep := range b.daemonDeployments {
			if dep.DaemonArn == daemonArn {
				out = append(out, *dep)
			}
		}
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})

	return out, nil
}

// ListDaemonDeployments returns deployment IDs for a daemon in a cluster.
func (b *InMemoryBackend) ListDaemonDeployments(clusterName, daemonName string) ([]string, error) {
	key := clusterKey(b.resolveCluster(clusterName))

	b.mu.RLock("ListDaemonDeployments")
	defer b.mu.RUnlock()

	if _, ok := b.clusters[key]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, clusterName)
	}

	d, ok := b.daemons[key][daemonName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrDaemonNotFound, daemonName)
	}

	var ids []string
	for id, dep := range b.daemonDeployments {
		if dep.DaemonArn == d.DaemonArn {
			ids = append(ids, id)
		}
	}

	sort.Strings(ids)

	return ids, nil
}

// DescribeDaemonRevisions returns revision snapshots for a daemon.
func (b *InMemoryBackend) DescribeDaemonRevisions(
	daemonName string,
	revisionNums []int,
) ([]DaemonRevision, error) {
	b.mu.RLock("DescribeDaemonRevisions")
	defer b.mu.RUnlock()

	daemonArn := b.resolveDaemonArnLocked(daemonName)
	if daemonArn == "" {
		return nil, fmt.Errorf("%w: %s", ErrDaemonNotFound, daemonName)
	}

	all := b.daemonRevisions[daemonArn]

	if len(revisionNums) == 0 {
		out := make([]DaemonRevision, 0, len(all))
		for _, r := range all {
			out = append(out, *r)
		}

		return out, nil
	}

	set := make(map[int]bool, len(revisionNums))
	for _, n := range revisionNums {
		set[n] = true
	}

	var out []DaemonRevision
	for _, r := range all {
		if set[r.Revision] {
			out = append(out, *r)
		}
	}

	return out, nil
}

// ---- Service revisions ----

// DescribeServiceRevisions returns service revisions by their ARNs.
func (b *InMemoryBackend) DescribeServiceRevisions(
	serviceRevisionArns []string,
) ([]ServiceRevision, []Failure, error) {
	b.mu.RLock("DescribeServiceRevisions")
	defer b.mu.RUnlock()

	out := make([]ServiceRevision, 0, len(serviceRevisionArns))
	failures := make([]Failure, 0)

	for _, revArn := range serviceRevisionArns {
		rev, ok := b.serviceRevisionsByArn[revArn]
		if !ok {
			failures = append(failures, Failure{
				Arn:    revArn,
				Reason: statusMissing,
				Detail: fmt.Sprintf("service revision %s not found", revArn),
			})

			continue
		}
		out = append(out, *rev)
	}

	return out, failures, nil
}

// addServiceRevisionLocked records a new service revision snapshot.
// Must be called with write lock held.
func (b *InMemoryBackend) addServiceRevisionLocked(svc *Service) {
	revisions := b.serviceRevisions[svc.ServiceArn]
	revisionNum := len(revisions) + 1

	// Extract cluster name from cluster ARN for the revision ARN.
	clusterName := clusterKey(svc.ClusterArn)

	revArn := fmt.Sprintf(
		"arn:aws:ecs:%s:%s:service-revision/%s/%s/%d",
		b.region, b.accountID, clusterName, svc.ServiceName, revisionNum,
	)

	rev := &ServiceRevision{
		CreatedAt:                time.Now(),
		ServiceRevisionArn:       revArn,
		ServiceArn:               svc.ServiceArn,
		ClusterArn:               svc.ClusterArn,
		TaskDefinition:           svc.TaskDefinition,
		LaunchType:               svc.LaunchType,
		NetworkConfiguration:     svc.NetworkConfiguration,
		DeploymentConfiguration:  svc.DeploymentConfiguration,
		CapacityProviderStrategy: svc.CapacityProviderStrategy,
		LoadBalancers:            svc.LoadBalancers,
		ServiceRegistries:        svc.ServiceRegistries,
	}

	b.serviceRevisions[svc.ServiceArn] = append(revisions, rev)
	b.serviceRevisionsByArn[revArn] = rev
}

// ---- Submit state changes ----

// SubmitTaskStateChange processes a task state change from the container agent.
// Validates that the cluster and task exist, then updates the task status.
func (b *InMemoryBackend) SubmitTaskStateChange(input SubmitTaskStateChangeInput) error {
	clusterName := clusterKey(b.resolveCluster(input.Cluster))

	b.mu.Lock("SubmitTaskStateChange")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return fmt.Errorf("%w: %s", ErrClusterNotFound, input.Cluster)
	}

	task, ok := b.tasks[clusterName][input.Task]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTaskNotFound, input.Task)
	}

	if input.Status != "" {
		task.LastStatus = strings.ToUpper(input.Status)
		if input.Status == statusStopped {
			now := time.Now()
			task.DesiredStatus = statusStopped
			task.StoppedAt = &now
			if input.Reason != "" {
				task.StoppedReason = input.Reason
			}
		}
	}

	if input.PullStartedAt != nil {
		task.ConnectivityAt = input.PullStartedAt
	}

	return nil
}

// SubmitContainerStateChange processes a container state change from the container agent.
// Validates cluster and task exist, then updates the matching container's status.
func (b *InMemoryBackend) SubmitContainerStateChange(input SubmitContainerStateChangeInput) error {
	clusterName := clusterKey(b.resolveCluster(input.Cluster))

	b.mu.Lock("SubmitContainerStateChange")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return fmt.Errorf("%w: %s", ErrClusterNotFound, input.Cluster)
	}

	task, ok := b.tasks[clusterName][input.Task]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTaskNotFound, input.Task)
	}

	for i := range task.Containers {
		if task.Containers[i].Name == input.ContainerName {
			if input.Status != "" {
				task.Containers[i].LastStatus = strings.ToUpper(input.Status)
			}
			if input.RuntimeID != "" {
				task.Containers[i].RuntimeID = input.RuntimeID
			}
			if input.ExitCode != nil {
				task.Containers[i].ExitCode = input.ExitCode
			}
			if input.Reason != "" {
				task.Containers[i].Reason = input.Reason
			}

			return nil
		}
	}

	return nil
}

// SubmitAttachmentStateChanges processes attachment state changes from the container agent.
// Validates the cluster exists, then updates matching task attachment statuses.
func (b *InMemoryBackend) SubmitAttachmentStateChanges(
	clusterRef string,
	changes []AttachmentStateChange,
) error {
	clusterName := clusterKey(b.resolveCluster(clusterRef))

	b.mu.Lock("SubmitAttachmentStateChanges")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return fmt.Errorf("%w: %s", ErrClusterNotFound, clusterRef)
	}

	if len(changes) == 0 {
		return nil
	}

	// Build a lookup from attachmentArn → new status.
	changeMap := make(map[string]string, len(changes))
	for _, ch := range changes {
		changeMap[ch.AttachmentArn] = ch.Status
	}

	// Scan all tasks in the cluster and update matching attachments.
	for _, task := range b.tasks[clusterName] {
		for i := range task.Attachments {
			if newStatus, ok := changeMap[task.Attachments[i].ID]; ok {
				task.Attachments[i].Status = newStatus
			}
		}
	}

	return nil
}

// ---- Helpers ----

// resolveDaemonArnLocked finds the ARN of a daemon by name or ARN across all clusters.
// Must be called with at least an RLock held.
func (b *InMemoryBackend) resolveDaemonArnLocked(nameOrArn string) string {
	if strings.HasPrefix(nameOrArn, "arn:") {
		// Verify the ARN exists.
		for _, clusterDaemons := range b.daemons {
			for _, d := range clusterDaemons {
				if d.DaemonArn == nameOrArn {
					return nameOrArn
				}
			}
		}

		return ""
	}

	// Search by name.
	for _, clusterDaemons := range b.daemons {
		if d, ok := clusterDaemons[nameOrArn]; ok {
			return d.DaemonArn
		}
	}

	return ""
}

// newDaemonDeploymentLocked creates a new daemon deployment record.
// Must be called with write lock held.
func (b *InMemoryBackend) newDaemonDeploymentLocked(
	daemonArn, clusterName string,
) *DaemonDeployment {
	now := time.Now()
	id := fmt.Sprintf("daemon-deploy-%s-%d", clusterName, now.UnixNano())

	// Count running tasks for this daemon across the cluster.
	running := 0
	for _, t := range b.tasks[clusterName] {
		if t.Group == "daemon:"+daemonName(daemonArn) && t.LastStatus == statusRunning {
			running++
		}
	}

	return &DaemonDeployment{
		ID:           id,
		DaemonArn:    daemonArn,
		Status:       statusActive,
		RunningTasks: running,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
}

// daemonName extracts the daemon name from a daemon ARN.
// ARN format: arn:aws:ecs:{region}:{account}:daemon/{clusterName}/{daemonName}.
func daemonName(daemonArn string) string {
	// Find last slash.
	idx := strings.LastIndex(daemonArn, "/")
	if idx < 0 {
		return daemonArn
	}

	return daemonArn[idx+1:]
}
