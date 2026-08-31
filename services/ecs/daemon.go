package ecs

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Daemon status values (types.DaemonStatus only models these two).
const (
	daemonStatusActive           = "ACTIVE"
	daemonStatusDeleteInProgress = "DELETE_IN_PROGRESS"
)

// DaemonTaskDefinition status values (types.DaemonTaskDefinitionStatus).
const (
	daemonTaskDefStatusActive           = "ACTIVE"
	daemonTaskDefStatusDeleteInProgress = "DELETE_IN_PROGRESS"
	daemonTaskDefStatusDeleted          = "DELETED"
)

// DaemonDeployment status values (types.DaemonDeploymentStatus).
const (
	daemonDeploymentStatusSuccessful = "SUCCESSFUL"
)

// maxDaemonTaskDefinitionRevisions caps the number of retained revisions per
// daemon task definition family, mirroring the ordinary task definition cap.
const maxDaemonTaskDefinitionRevisions = 100

var (
	// ErrDaemonNotFound is returned when a daemon does not exist. This mirrors
	// types.DaemonNotFoundException in aws-sdk-go-v2/service/ecs.
	ErrDaemonNotFound = awserr.New("DaemonNotFoundException", awserr.ErrNotFound)
	// ErrDaemonNotActive is returned when attempting to update a daemon that is
	// not ACTIVE. This mirrors types.DaemonNotActiveException.
	ErrDaemonNotActive = awserr.New("DaemonNotActiveException", awserr.ErrConflict)
	// ErrDaemonTaskDefinitionNotFound is returned when a daemon task definition
	// does not exist. Amazon ECS does not model a dedicated NotFound exception
	// shape for daemon task definitions; it uses the generic ClientException
	// (mirrors real DescribeTaskDefinition / DeregisterTaskDefinition behavior).
	ErrDaemonTaskDefinitionNotFound = awserr.New("ClientException", awserr.ErrNotFound)
	// ErrDaemonAlreadyExists is returned when a daemon with the same name
	// already exists in the cluster. Amazon ECS does not model a dedicated
	// AlreadyExists exception for daemons; it uses the generic ResourceInUseException.
	ErrDaemonAlreadyExists = awserr.New("ResourceInUseException", awserr.ErrAlreadyExists)
)

// DaemonAlarmConfiguration configures CloudWatch alarm-based rollback for a
// daemon deployment.
type DaemonAlarmConfiguration struct {
	AlarmNames []string `json:"alarmNames,omitempty"`
	Enable     bool     `json:"enable,omitempty"`
}

// DaemonDeploymentConfiguration controls how a daemon rolls out updates.
type DaemonDeploymentConfiguration struct {
	Alarms            *DaemonAlarmConfiguration `json:"alarms,omitempty"`
	BakeTimeInMinutes int                       `json:"bakeTimeInMinutes,omitempty"`
	DrainPercent      float64                   `json:"drainPercent,omitempty"`
}

// Daemon represents an Amazon ECS Managed Daemon.
type Daemon struct {
	CreatedAt                time.Time                      `json:"createdAt"`
	UpdatedAt                time.Time                      `json:"updatedAt"`
	DeploymentConfiguration  *DaemonDeploymentConfiguration `json:"deploymentConfiguration,omitempty"`
	DeploymentArn            string                         `json:"deploymentArn,omitempty"`
	ClusterArn               string                         `json:"clusterArn"`
	DaemonTaskDefinitionArn  string                         `json:"daemonTaskDefinitionArn"`
	DaemonName               string                         `json:"daemonName"`
	CurrentDaemonRevisionArn string                         `json:"currentDaemonRevisionArn,omitempty"`
	Status                   string                         `json:"status"`
	DaemonArn                string                         `json:"daemonArn"`
	PropagateTags            string                         `json:"propagateTags,omitempty"`
	CapacityProviderArns     []string                       `json:"capacityProviderArns,omitempty"`
	Tags                     []Tag                          `json:"tags,omitempty"`
	EnableECSManagedTags     bool                           `json:"enableECSManagedTags,omitempty"`
	EnableExecuteCommand     bool                           `json:"enableExecuteCommand,omitempty"`
}

// DaemonContainerDefinition is a container definition within a daemon task definition.
type DaemonContainerDefinition struct {
	HealthCheck            *HealthCheck           `json:"healthCheck,omitempty"`
	LogConfiguration       *LogConfiguration      `json:"logConfiguration,omitempty"`
	RepositoryCredentials  *RepositoryCredentials `json:"repositoryCredentials,omitempty"`
	FirelensConfiguration  *FirelensConfiguration `json:"firelensConfiguration,omitempty"`
	Name                   string                 `json:"name"`
	Image                  string                 `json:"image"`
	WorkingDirectory       string                 `json:"workingDirectory,omitempty"`
	User                   string                 `json:"user,omitempty"`
	Command                []string               `json:"command,omitempty"`
	EntryPoint             []string               `json:"entryPoint,omitempty"`
	Environment            []KeyValuePair         `json:"environment,omitempty"`
	EnvironmentFiles       []EnvironmentFile      `json:"environmentFiles,omitempty"`
	Secrets                []SecretReference      `json:"secrets,omitempty"`
	MountPoints            []MountPoint           `json:"mountPoints,omitempty"`
	DependsOn              []ContainerDependency  `json:"dependsOn,omitempty"`
	CPU                    int                    `json:"cpu,omitempty"`
	Memory                 int                    `json:"memory,omitempty"`
	MemoryReservation      int                    `json:"memoryReservation,omitempty"`
	StartTimeout           int                    `json:"startTimeout,omitempty"`
	StopTimeout            int                    `json:"stopTimeout,omitempty"`
	Essential              bool                   `json:"essential"`
	Interactive            bool                   `json:"interactive,omitempty"`
	PseudoTerminal         bool                   `json:"pseudoTerminal,omitempty"`
	Privileged             bool                   `json:"privileged,omitempty"`
	ReadonlyRootFilesystem bool                   `json:"readonlyRootFilesystem,omitempty"`
}

// DaemonVolume is a data volume definition for a daemon task.
type DaemonVolume struct {
	Host *HostVolumeProperties `json:"host,omitempty"`
	Name string                `json:"name"`
}

// DaemonTaskDefinition is a template describing the containers that form a daemon.
type DaemonTaskDefinition struct {
	RegisteredAt            time.Time                   `json:"registeredAt"`
	DeleteRequestedAt       *time.Time                  `json:"deleteRequestedAt,omitempty"`
	DaemonTaskDefinitionArn string                      `json:"daemonTaskDefinitionArn"`
	Family                  string                      `json:"family"`
	CPU                     string                      `json:"cpu,omitempty"`
	Memory                  string                      `json:"memory,omitempty"`
	ExecutionRoleArn        string                      `json:"executionRoleArn,omitempty"`
	TaskRoleArn             string                      `json:"taskRoleArn,omitempty"`
	RegisteredBy            string                      `json:"registeredBy,omitempty"`
	Status                  string                      `json:"status"`
	ContainerDefinitions    []DaemonContainerDefinition `json:"containerDefinitions"`
	Volumes                 []DaemonVolume              `json:"volumes,omitempty"`
	Revision                int                         `json:"revision"`
}

// DaemonDeployment represents the progressive rollout of a daemon change.
type DaemonDeployment struct {
	CreatedAt               time.Time                      `json:"createdAt"`
	StartedAt               *time.Time                     `json:"startedAt,omitempty"`
	FinishedAt              *time.Time                     `json:"finishedAt,omitempty"`
	DeploymentConfiguration *DaemonDeploymentConfiguration `json:"deploymentConfiguration,omitempty"`
	DaemonDeploymentArn     string                         `json:"daemonDeploymentArn"`
	DaemonArn               string                         `json:"daemonArn"`
	ClusterArn              string                         `json:"clusterArn"`
	Status                  string                         `json:"status"`
	StatusReason            string                         `json:"statusReason,omitempty"`
	TargetDaemonRevisionArn string                         `json:"targetDaemonRevisionArn,omitempty"`
}

// DaemonRevision is a snapshot of a daemon's configuration at deployment time.
type DaemonRevision struct {
	CreatedAt               time.Time `json:"createdAt"`
	ClusterArn              string    `json:"clusterArn,omitempty"`
	DaemonArn               string    `json:"daemonArn"`
	DaemonRevisionArn       string    `json:"daemonRevisionArn"`
	DaemonTaskDefinitionArn string    `json:"daemonTaskDefinitionArn"`
	PropagateTags           string    `json:"propagateTags,omitempty"`
	EnableECSManagedTags    bool      `json:"enableECSManagedTags"`
	EnableExecuteCommand    bool      `json:"enableExecuteCommand"`
}

// CreateDaemonInput holds input for CreateDaemon.
type CreateDaemonInput struct {
	DeploymentConfiguration *DaemonDeploymentConfiguration
	DaemonName              string
	ClusterArn              string
	DaemonTaskDefinitionArn string
	PropagateTags           string
	CapacityProviderArns    []string
	Tags                    []Tag
	EnableECSManagedTags    bool
	EnableExecuteCommand    bool
}

// UpdateDaemonInput holds input for UpdateDaemon.
type UpdateDaemonInput struct {
	DeploymentConfiguration *DaemonDeploymentConfiguration
	DaemonArn               string
	DaemonTaskDefinitionArn string
	PropagateTags           string
	CapacityProviderArns    []string
	EnableECSManagedTags    bool
	EnableExecuteCommand    bool
}

// RegisterDaemonTaskDefinitionInput holds input for RegisterDaemonTaskDefinition.
type RegisterDaemonTaskDefinitionInput struct {
	Family               string
	CPU                  string
	Memory               string
	ExecutionRoleArn     string
	TaskRoleArn          string
	ContainerDefinitions []DaemonContainerDefinition
	Volumes              []DaemonVolume
	Tags                 []Tag
}

// daemonARN builds the ARN for a daemon within a cluster.
func (b *InMemoryBackend) daemonARN(clusterName, daemonName string) string {
	return fmt.Sprintf("arn:aws:ecs:%s:%s:daemon/%s/%s", b.region, b.accountID, clusterName, daemonName)
}

// daemonTaskDefinitionARN builds the ARN for a daemon task definition revision.
func (b *InMemoryBackend) daemonTaskDefinitionARN(family string, revision int) string {
	return fmt.Sprintf(
		"arn:aws:ecs:%s:%s:daemon-task-definition/%s:%d", b.region, b.accountID, family, revision,
	)
}

// daemonDeploymentARN builds the ARN for a daemon deployment.
func (b *InMemoryBackend) daemonDeploymentARN(clusterName, daemonName string) string {
	return fmt.Sprintf(
		"arn:aws:ecs:%s:%s:daemon-deployment/%s/%s/%s",
		b.region, b.accountID, clusterName, daemonName, uuid.NewString(),
	)
}

// daemonRevisionARN builds the ARN for a daemon revision.
func (b *InMemoryBackend) daemonRevisionARN(clusterName, daemonName string) string {
	return fmt.Sprintf(
		"arn:aws:ecs:%s:%s:daemon-revision/%s/%s/%s",
		b.region, b.accountID, clusterName, daemonName, uuid.NewString(),
	)
}

// parseDaemonArn extracts the cluster name and daemon name embedded in a
// daemon ARN of the form "arn:aws:ecs:region:account:daemon/clusterName/daemonName".
// Used so daemons can be looked up in the cluster-scoped b.daemons index (see
// InMemoryBackend.daemons in backend.go, and purge.go's purgeDaemonsLocked,
// which cleans up that index per-cluster) in O(1) from a bare ARN.
func parseDaemonArn(daemonArn string) (string, string, bool) {
	const marker = ":daemon/"

	_, rest, found := strings.Cut(daemonArn, marker)
	if !found {
		return "", "", false
	}

	clusterName, daemonName, found := strings.Cut(rest, "/")
	if !found || clusterName == "" || daemonName == "" {
		return "", "", false
	}

	return clusterName, daemonName, true
}

// findDaemonLocked resolves a daemon by ARN. Must be called with the lock held.
func (b *InMemoryBackend) findDaemonLocked(daemonArn string) (*Daemon, error) {
	clusterName, daemonName, ok := parseDaemonArn(daemonArn)
	if ok {
		if d, exists := b.daemons.Get(scopedKey(clusterName, daemonName)); exists {
			return d, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrDaemonNotFound, daemonArn)
}

// createDaemonRevisionLocked snapshots the daemon's current configuration into
// a new DaemonRevision and returns it. Must be called with the lock held.
func (b *InMemoryBackend) createDaemonRevisionLocked(clusterName string, d *Daemon) *DaemonRevision {
	rev := &DaemonRevision{
		CreatedAt:               time.Now(),
		ClusterArn:              d.ClusterArn,
		DaemonArn:               d.DaemonArn,
		DaemonRevisionArn:       b.daemonRevisionARN(clusterName, d.DaemonName),
		DaemonTaskDefinitionArn: d.DaemonTaskDefinitionArn,
		PropagateTags:           d.PropagateTags,
		EnableECSManagedTags:    d.EnableECSManagedTags,
		EnableExecuteCommand:    d.EnableExecuteCommand,
	}

	b.daemonRevisions.Put(rev)

	return rev
}

// createDaemonDeploymentLocked creates a new daemon deployment record targeting
// the given daemon revision. Since this emulator has no real container-instance
// draining infrastructure, deployments complete synchronously as SUCCESSFUL.
// Must be called with the lock held.
func (b *InMemoryBackend) createDaemonDeploymentLocked(
	clusterName string, d *Daemon, rev *DaemonRevision,
) *DaemonDeployment {
	now := time.Now()

	dep := &DaemonDeployment{
		CreatedAt:               now,
		StartedAt:               &now,
		FinishedAt:              &now,
		DaemonDeploymentArn:     b.daemonDeploymentARN(clusterName, d.DaemonName),
		DaemonArn:               d.DaemonArn,
		ClusterArn:              d.ClusterArn,
		Status:                  daemonDeploymentStatusSuccessful,
		TargetDaemonRevisionArn: rev.DaemonRevisionArn,
		DeploymentConfiguration: d.DeploymentConfiguration,
	}

	b.daemonDeployments.Put(dep)

	return dep
}

// CreateDaemon creates a new daemon and its initial deployment.
func (b *InMemoryBackend) CreateDaemon(input CreateDaemonInput) (*Daemon, error) {
	if input.DaemonName == "" {
		return nil, fmt.Errorf("%w: daemonName is required", ErrInvalidParameter)
	}

	if input.DaemonTaskDefinitionArn == "" {
		return nil, fmt.Errorf("%w: daemonTaskDefinitionArn is required", ErrInvalidParameter)
	}

	if len(input.CapacityProviderArns) == 0 {
		return nil, fmt.Errorf("%w: capacityProviderArns is required", ErrInvalidParameter)
	}

	clusterName := clusterKey(b.resolveCluster(input.ClusterArn))

	b.mu.Lock("CreateDaemon")
	defer b.mu.Unlock()

	if _, err := b.findDaemonTaskDefinitionLocked(input.DaemonTaskDefinitionArn); err != nil {
		return nil, err
	}

	daemonArn := b.daemonARN(clusterName, input.DaemonName)
	if b.daemons.Has(scopedKey(clusterName, input.DaemonName)) {
		return nil, fmt.Errorf("%w: %s", ErrDaemonAlreadyExists, input.DaemonName)
	}

	now := time.Now()

	d := &Daemon{
		CreatedAt:               now,
		UpdatedAt:               now,
		DaemonArn:               daemonArn,
		DaemonName:              input.DaemonName,
		ClusterArn:              fmt.Sprintf("arn:aws:ecs:%s:%s:cluster/%s", b.region, b.accountID, clusterName),
		DaemonTaskDefinitionArn: input.DaemonTaskDefinitionArn,
		Status:                  daemonStatusActive,
		CapacityProviderArns:    input.CapacityProviderArns,
		DeploymentConfiguration: input.DeploymentConfiguration,
		PropagateTags:           input.PropagateTags,
		EnableECSManagedTags:    input.EnableECSManagedTags,
		EnableExecuteCommand:    input.EnableExecuteCommand,
		Tags:                    copyTags(input.Tags),
	}

	rev := b.createDaemonRevisionLocked(clusterName, d)
	dep := b.createDaemonDeploymentLocked(clusterName, d, rev)
	d.DeploymentArn = dep.DaemonDeploymentArn
	d.CurrentDaemonRevisionArn = rev.DaemonRevisionArn

	b.daemons.Put(d)

	out := *d
	out.Tags = copyTags(d.Tags)

	return &out, nil
}

// DeleteDaemon deletes a daemon. The returned snapshot reflects the
// DELETE_IN_PROGRESS transitional status returned synchronously by real AWS.
func (b *InMemoryBackend) DeleteDaemon(daemonArn string) (*Daemon, error) {
	b.mu.Lock("DeleteDaemon")
	defer b.mu.Unlock()

	d, err := b.findDaemonLocked(daemonArn)
	if err != nil {
		return nil, err
	}

	out := *d
	out.Status = daemonStatusDeleteInProgress
	out.UpdatedAt = time.Now()

	clusterName, daemonName, ok := parseDaemonArn(daemonArn)
	if ok {
		b.daemons.Delete(scopedKey(clusterName, daemonName))
	}

	b.deleteDaemonAncillaryLocked(daemonArn)

	return &out, nil
}

// deleteDaemonAncillaryLocked removes daemonRevisions and daemonDeployments rows
// belonging to daemonArn. Both tables are keyed by their own ARN (not
// DaemonArn), so matching entries must be found by scanning and filtering on the
// .DaemonArn field -- skip this and DeleteDaemon leaks one daemonRevisions row
// per UpdateDaemon call and one daemonDeployments row per deployment ever made.
// Shared with purgeDaemonsLocked (purge.go). Must be called with the write lock held.
func (b *InMemoryBackend) deleteDaemonAncillaryLocked(daemonArn string) {
	for _, rev := range b.daemonRevisions.All() {
		if rev.DaemonArn == daemonArn {
			b.daemonRevisions.Delete(rev.DaemonRevisionArn)
		}
	}

	for _, dep := range b.daemonDeployments.All() {
		if dep.DaemonArn == daemonArn {
			b.daemonDeployments.Delete(dep.DaemonDeploymentArn)
		}
	}
}

// DescribeDaemon returns the full detail of a daemon.
func (b *InMemoryBackend) DescribeDaemon(daemonArn string) (*Daemon, error) {
	b.mu.RLock("DescribeDaemon")
	defer b.mu.RUnlock()

	d, err := b.findDaemonLocked(daemonArn)
	if err != nil {
		return nil, err
	}

	out := *d
	out.Tags = copyTags(d.Tags)

	return &out, nil
}

// UpdateDaemon updates a daemon's configuration and triggers a new deployment.
func (b *InMemoryBackend) UpdateDaemon(input UpdateDaemonInput) (*Daemon, error) {
	if input.DaemonTaskDefinitionArn == "" {
		return nil, fmt.Errorf("%w: daemonTaskDefinitionArn is required", ErrInvalidParameter)
	}

	if len(input.CapacityProviderArns) == 0 {
		return nil, fmt.Errorf("%w: capacityProviderArns is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateDaemon")
	defer b.mu.Unlock()

	d, err := b.findDaemonLocked(input.DaemonArn)
	if err != nil {
		return nil, err
	}

	if d.Status != daemonStatusActive {
		return nil, fmt.Errorf("%w: %s", ErrDaemonNotActive, input.DaemonArn)
	}

	if _, tdErr := b.findDaemonTaskDefinitionLocked(input.DaemonTaskDefinitionArn); tdErr != nil {
		return nil, tdErr
	}

	d.DaemonTaskDefinitionArn = input.DaemonTaskDefinitionArn
	d.CapacityProviderArns = input.CapacityProviderArns
	d.DeploymentConfiguration = input.DeploymentConfiguration
	d.PropagateTags = input.PropagateTags
	d.EnableECSManagedTags = input.EnableECSManagedTags
	d.EnableExecuteCommand = input.EnableExecuteCommand
	d.UpdatedAt = time.Now()

	clusterName := clusterKey(b.resolveCluster(d.ClusterArn))
	rev := b.createDaemonRevisionLocked(clusterName, d)
	dep := b.createDaemonDeploymentLocked(clusterName, d, rev)
	d.DeploymentArn = dep.DaemonDeploymentArn
	d.CurrentDaemonRevisionArn = rev.DaemonRevisionArn

	out := *d
	out.Tags = copyTags(d.Tags)

	return &out, nil
}

// ListDaemonsInput holds optional filters for ListDaemons.
type ListDaemonsInput struct {
	ClusterArn           string
	CapacityProviderArns []string
}

// ListDaemons returns daemons, optionally filtered by cluster or capacity
// provider. Per ListDaemonsInput.ClusterArn's doc ("If you do not specify a
// cluster, the default cluster is assumed."), an unset ClusterArn scopes to
// the "default" cluster rather than every cluster.
func (b *InMemoryBackend) ListDaemons(input ListDaemonsInput) ([]Daemon, error) {
	b.mu.RLock("ListDaemons")
	defer b.mu.RUnlock()

	wantCluster := fmt.Sprintf(
		"arn:aws:ecs:%s:%s:cluster/%s", b.region, b.accountID, clusterKey(b.resolveCluster(input.ClusterArn)),
	)

	wantCP := make(map[string]bool, len(input.CapacityProviderArns))
	for _, cp := range input.CapacityProviderArns {
		wantCP[cp] = true
	}

	all := b.daemons.All()
	out := make([]Daemon, 0, len(all))

	for _, d := range all {
		if d.ClusterArn != wantCluster {
			continue
		}

		if len(wantCP) > 0 && !daemonHasAnyCapacityProvider(d, wantCP) {
			continue
		}

		out = append(out, *d)
	}

	return out, nil
}

// daemonHasAnyCapacityProvider reports whether d is associated with at least
// one of the capacity providers in want.
func daemonHasAnyCapacityProvider(d *Daemon, want map[string]bool) bool {
	for _, cp := range d.CapacityProviderArns {
		if want[cp] {
			return true
		}
	}

	return false
}

// findDaemonTaskDefinitionLocked resolves a daemon task definition by family
// (latest ACTIVE revision), "family:revision", or full ARN. Must be called
// with the lock held.
func (b *InMemoryBackend) findDaemonTaskDefinitionLocked(familyOrArn string) (*DaemonTaskDefinition, error) {
	if revs, ok := b.daemonTaskDefinitions[familyOrArn]; ok {
		for i := range slices.Backward(revs) {
			if revs[i].Status == daemonTaskDefStatusActive {
				cp := *revs[i]

				return &cp, nil
			}
		}
	}

	if td, ok := b.daemonTaskDefByArn.Get(familyOrArn); ok {
		cp := *td

		return &cp, nil
	}

	if idx := strings.LastIndex(familyOrArn, ":"); idx > 0 {
		family := familyOrArn[:idx]

		revNum, err := strconv.Atoi(familyOrArn[idx+1:])
		if err == nil {
			for _, td := range b.daemonTaskDefinitions[family] {
				if td.Revision == revNum {
					cp := *td

					return &cp, nil
				}
			}
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrDaemonTaskDefinitionNotFound, familyOrArn)
}

// RegisterDaemonTaskDefinition registers a new daemon task definition revision.
func (b *InMemoryBackend) RegisterDaemonTaskDefinition(
	input RegisterDaemonTaskDefinitionInput,
) (*DaemonTaskDefinition, error) {
	if input.Family == "" {
		return nil, fmt.Errorf("%w: family is required", ErrInvalidParameter)
	}

	if len(input.ContainerDefinitions) == 0 {
		return nil, fmt.Errorf("%w: containerDefinitions is required", ErrInvalidParameter)
	}

	b.mu.Lock("RegisterDaemonTaskDefinition")
	defer b.mu.Unlock()

	revisions := b.daemonTaskDefinitions[input.Family]

	revision := 1
	if len(revisions) > 0 {
		revision = revisions[len(revisions)-1].Revision + 1
	}

	td := &DaemonTaskDefinition{
		RegisteredAt:            time.Now(),
		DaemonTaskDefinitionArn: b.daemonTaskDefinitionARN(input.Family, revision),
		Family:                  input.Family,
		CPU:                     input.CPU,
		Memory:                  input.Memory,
		ExecutionRoleArn:        input.ExecutionRoleArn,
		TaskRoleArn:             input.TaskRoleArn,
		Status:                  daemonTaskDefStatusActive,
		ContainerDefinitions:    input.ContainerDefinitions,
		Volumes:                 input.Volumes,
		Revision:                revision,
	}

	revisions = append(revisions, td)

	if len(revisions) > maxDaemonTaskDefinitionRevisions {
		excess := len(revisions) - maxDaemonTaskDefinitionRevisions

		for _, evicted := range revisions[:excess] {
			b.daemonTaskDefByArn.Delete(evicted.DaemonTaskDefinitionArn)
		}

		revisions = revisions[excess:]
	}

	b.daemonTaskDefinitions[input.Family] = revisions
	b.daemonTaskDefByArn.Put(td)

	if len(input.Tags) > 0 {
		copied := make([]Tag, len(input.Tags))
		copy(copied, input.Tags)
		b.resourceTags[resourceTagKey(td.DaemonTaskDefinitionArn)] = copied
	}

	cp := *td

	return &cp, nil
}

// DescribeDaemonTaskDefinition returns a daemon task definition by family, ARN, or family:revision.
func (b *InMemoryBackend) DescribeDaemonTaskDefinition(familyOrArn string) (*DaemonTaskDefinition, error) {
	b.mu.RLock("DescribeDaemonTaskDefinition")
	defer b.mu.RUnlock()

	return b.findDaemonTaskDefinitionLocked(familyOrArn)
}

// DeleteDaemonTaskDefinition marks a daemon task definition revision as DELETED.
func (b *InMemoryBackend) DeleteDaemonTaskDefinition(familyOrArn string) (*DaemonTaskDefinition, error) {
	b.mu.Lock("DeleteDaemonTaskDefinition")
	defer b.mu.Unlock()

	td, err := b.findDaemonTaskDefinitionLocked(familyOrArn)
	if err != nil {
		return nil, err
	}

	for _, revs := range b.daemonTaskDefinitions {
		for _, r := range revs {
			if r.DaemonTaskDefinitionArn == td.DaemonTaskDefinitionArn {
				now := time.Now()
				r.Status = daemonTaskDefStatusDeleted
				r.DeleteRequestedAt = &now
				cp := *r

				return &cp, nil
			}
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrDaemonTaskDefinitionNotFound, familyOrArn)
}

// ListDaemonTaskDefinitionsInput holds optional filters for ListDaemonTaskDefinitions.
type ListDaemonTaskDefinitionsInput struct {
	Family       string
	FamilyPrefix string
	Status       string // "", "ACTIVE" (default), "DELETE_IN_PROGRESS", or "ALL"
}

// ListDaemonTaskDefinitions returns daemon task definition summaries,
// unsorted; the handler applies the documented family/revision order (see
// handleListDaemonTaskDefinitions).
func (b *InMemoryBackend) ListDaemonTaskDefinitions(
	input ListDaemonTaskDefinitionsInput,
) ([]DaemonTaskDefinition, error) {
	b.mu.RLock("ListDaemonTaskDefinitions")
	defer b.mu.RUnlock()

	wantStatus := strings.ToUpper(input.Status)
	if wantStatus == "" {
		wantStatus = daemonTaskDefStatusActive
	}

	out := make([]DaemonTaskDefinition, 0, len(b.daemonTaskDefinitions))

	for family, revs := range b.daemonTaskDefinitions {
		if input.Family != "" && family != input.Family {
			continue
		}

		if input.FamilyPrefix != "" && !strings.HasPrefix(family, input.FamilyPrefix) {
			continue
		}

		for _, td := range revs {
			if wantStatus != "ALL" && td.Status != wantStatus {
				continue
			}

			out = append(out, *td)
		}
	}

	return out, nil
}

// DescribeDaemonDeployments returns daemon deployments for the given ARNs,
// reporting a Failure for each ARN that does not exist.
func (b *InMemoryBackend) DescribeDaemonDeployments(arns []string) ([]DaemonDeployment, []Failure, error) {
	b.mu.RLock("DescribeDaemonDeployments")
	defer b.mu.RUnlock()

	out := make([]DaemonDeployment, 0, len(arns))
	failures := make([]Failure, 0, len(arns))

	for _, arn := range arns {
		dep, ok := b.daemonDeployments.Get(arn)
		if !ok {
			failures = append(failures, Failure{
				Arn:    arn,
				Reason: statusMissing,
				Detail: fmt.Sprintf("daemon deployment %s not found", arn),
			})

			continue
		}

		out = append(out, *dep)
	}

	return out, failures, nil
}

// DescribeDaemonRevisions returns daemon revisions for the given ARNs,
// reporting a Failure for each ARN that does not exist.
func (b *InMemoryBackend) DescribeDaemonRevisions(arns []string) ([]DaemonRevision, []Failure, error) {
	b.mu.RLock("DescribeDaemonRevisions")
	defer b.mu.RUnlock()

	out := make([]DaemonRevision, 0, len(arns))
	failures := make([]Failure, 0, len(arns))

	for _, arn := range arns {
		rev, ok := b.daemonRevisions.Get(arn)
		if !ok {
			failures = append(failures, Failure{
				Arn:    arn,
				Reason: statusMissing,
				Detail: fmt.Sprintf("daemon revision %s not found", arn),
			})

			continue
		}

		out = append(out, *rev)
	}

	return out, failures, nil
}

// ListDaemonDeploymentsInput holds filters for ListDaemonDeployments.
type ListDaemonDeploymentsInput struct {
	DaemonArn string
	Status    []string
}

// ListDaemonDeployments returns deployment summaries for a daemon, optionally
// filtered by status.
func (b *InMemoryBackend) ListDaemonDeployments(input ListDaemonDeploymentsInput) ([]DaemonDeployment, error) {
	b.mu.RLock("ListDaemonDeployments")
	defer b.mu.RUnlock()

	wantStatus := make(map[string]bool, len(input.Status))
	for _, s := range input.Status {
		wantStatus[strings.ToUpper(s)] = true
	}

	all := b.daemonDeployments.All()
	out := make([]DaemonDeployment, 0, len(all))

	for _, dep := range all {
		if dep.DaemonArn != input.DaemonArn {
			continue
		}

		if len(wantStatus) > 0 && !wantStatus[dep.Status] {
			continue
		}

		out = append(out, *dep)
	}

	return out, nil
}

// addServiceRevisionLocked is a compatibility hook for callers that record a
// ServiceRevision snapshot whenever a service's Deployments change. This backend
// derives ServiceRevision snapshots on demand from each deployment's
// ServiceRevisionArn instead (see DescribeServiceRevisions/buildServiceRevision
// in services.go), so no bookkeeping is required here. Must be called with the
// write lock held.
func (b *InMemoryBackend) addServiceRevisionLocked(_ *Service) {}
