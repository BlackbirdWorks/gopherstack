package ecs

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// Snapshottable is an optional interface that a Backend may implement to
// support snapshot/restore for persistence or test isolation.
type Snapshottable interface {
	Snapshot(ctx context.Context) []byte
	Restore(context.Context, []byte) error
}

type backendSnapshot struct {
	Clusters               map[string]*Cluster                      `json:"clusters"`
	TaskDefinitions        map[string][]*TaskDefinition             `json:"taskDefinitions"`
	Services               map[string]map[string]*Service           `json:"services"`
	Tasks                  map[string]map[string]*Task              `json:"tasks"`
	ContainerInstances     map[string]map[string]*ContainerInstance `json:"containerInstances"`
	TaskSets               map[string]map[string]*TaskSet           `json:"taskSets"`
	CapacityProviders      map[string]*CapacityProvider             `json:"capacityProviders"`
	AccountSettings        map[string]*AccountSetting               `json:"accountSettings"`
	Attributes             map[string]map[string]*Attribute         `json:"attributes"`
	ServiceDeployments     map[string]*ServiceDeployment            `json:"serviceDeployments"`
	ExpressGatewayServices map[string]*ExpressGatewayService        `json:"expressGatewayServices"`
	TaskProtections        map[string]*TaskProtection               `json:"taskProtections"`
	Daemons                map[string]*Daemon                       `json:"daemons"`
	DaemonTaskDefinitions  map[string][]*DaemonTaskDefinition       `json:"daemonTaskDefinitions"`
	DaemonDeployments      map[string]*DaemonDeployment             `json:"daemonDeployments"`
	DaemonRevisions        map[string]*DaemonRevision               `json:"daemonRevisions"`
}

func snapshotClusters(src map[string]*Cluster) map[string]*Cluster {
	dst := make(map[string]*Cluster, len(src))
	for k, v := range src {
		cp := *v
		dst[k] = &cp
	}

	return dst
}

func snapshotTaskDefinitions(src map[string][]*TaskDefinition) map[string][]*TaskDefinition {
	dst := make(map[string][]*TaskDefinition, len(src))
	for family, revs := range src {
		revsCp := make([]*TaskDefinition, len(revs))
		for i, td := range revs {
			cp := *td
			revsCp[i] = &cp
		}
		dst[family] = revsCp
	}

	return dst
}

func snapshotServices(src map[string]map[string]*Service) map[string]map[string]*Service {
	dst := make(map[string]map[string]*Service, len(src))
	for cluster, svcMap := range src {
		cp := make(map[string]*Service, len(svcMap))
		for name, svc := range svcMap {
			s := *svc
			cp[name] = &s
		}
		dst[cluster] = cp
	}

	return dst
}

func snapshotTasks(src map[string]map[string]*Task) map[string]map[string]*Task {
	dst := make(map[string]map[string]*Task, len(src))
	for cluster, taskMap := range src {
		cp := make(map[string]*Task, len(taskMap))
		for arn, task := range taskMap {
			t := *task
			cp[arn] = &t
		}
		dst[cluster] = cp
	}

	return dst
}

func snapshotContainerInstances(
	src map[string]map[string]*ContainerInstance,
) map[string]map[string]*ContainerInstance {
	dst := make(map[string]map[string]*ContainerInstance, len(src))
	for cluster, ciMap := range src {
		cp := make(map[string]*ContainerInstance, len(ciMap))
		for arn, ci := range ciMap {
			c := *ci
			cp[arn] = &c
		}
		dst[cluster] = cp
	}

	return dst
}

func snapshotTaskSets(src map[string]map[string]*TaskSet) map[string]map[string]*TaskSet {
	dst := make(map[string]map[string]*TaskSet, len(src))
	for key, tsMap := range src {
		cp := make(map[string]*TaskSet, len(tsMap))
		for arn, ts := range tsMap {
			t := *ts
			cp[arn] = &t
		}
		dst[key] = cp
	}

	return dst
}

func snapshotCapacityProviders(src map[string]*CapacityProvider) map[string]*CapacityProvider {
	dst := make(map[string]*CapacityProvider, len(src))
	for k, v := range src {
		cp := *v
		cp.Tags = copyTags(v.Tags)
		dst[k] = &cp
	}

	return dst
}

func snapshotAttributes(src map[string]map[string]*Attribute) map[string]map[string]*Attribute {
	dst := make(map[string]map[string]*Attribute, len(src))
	for cluster, attrMap := range src {
		cp := make(map[string]*Attribute, len(attrMap))
		for key, attr := range attrMap {
			a := *attr
			cp[key] = &a
		}
		dst[cluster] = cp
	}

	return dst
}

func snapshotTaskProtections(src map[string]*TaskProtection) map[string]*TaskProtection {
	dst := make(map[string]*TaskProtection, len(src))
	for k, v := range src {
		cp := *v
		dst[k] = &cp
	}

	return dst
}

func snapshotExpressGatewayServices(
	src map[string]*ExpressGatewayService,
) map[string]*ExpressGatewayService {
	dst := make(map[string]*ExpressGatewayService, len(src))
	for k, v := range src {
		cp := *v
		cp.Tags = copyTags(v.Tags)
		dst[k] = &cp
	}

	return dst
}

// snapshotDaemons flattens the cluster-nested in-memory daemon index (see
// InMemoryBackend.daemons in backend.go, keyed by clusterName then daemonName)
// into the flat ARN-keyed shape persisted on disk, preserving the existing
// "daemons" snapshot field shape. restoreDaemons performs the inverse.
func snapshotDaemons(src map[string]map[string]*Daemon) map[string]*Daemon {
	dst := make(map[string]*Daemon)
	for _, clusterDaemons := range src {
		for _, v := range clusterDaemons {
			cp := *v
			cp.Tags = copyTags(v.Tags)
			dst[cp.DaemonArn] = &cp
		}
	}

	return dst
}

// restoreDaemons re-nests a flat ARN-keyed daemon snapshot (see snapshotDaemons)
// back into the clusterName -> daemonName -> Daemon index used at runtime.
func restoreDaemons(src map[string]*Daemon) map[string]map[string]*Daemon {
	dst := make(map[string]map[string]*Daemon)

	for arn, d := range src {
		clusterName, daemonName, ok := parseDaemonArn(arn)
		if !ok {
			continue
		}

		if dst[clusterName] == nil {
			dst[clusterName] = make(map[string]*Daemon)
		}

		dst[clusterName][daemonName] = d
	}

	return dst
}

func snapshotDaemonTaskDefinitions(src map[string][]*DaemonTaskDefinition) map[string][]*DaemonTaskDefinition {
	dst := make(map[string][]*DaemonTaskDefinition, len(src))
	for family, revs := range src {
		revsCp := make([]*DaemonTaskDefinition, len(revs))
		for i, td := range revs {
			cp := *td
			revsCp[i] = &cp
		}
		dst[family] = revsCp
	}

	return dst
}

func snapshotDaemonDeployments(src map[string]*DaemonDeployment) map[string]*DaemonDeployment {
	dst := make(map[string]*DaemonDeployment, len(src))
	for k, v := range src {
		cp := *v
		dst[k] = &cp
	}

	return dst
}

func snapshotDaemonRevisions(src map[string]*DaemonRevision) map[string]*DaemonRevision {
	dst := make(map[string]*DaemonRevision, len(src))
	for k, v := range src {
		cp := *v
		dst[k] = &cp
	}

	return dst
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	accountSettings := make(map[string]*AccountSetting, len(b.accountSettings))
	for k, v := range b.accountSettings {
		cp := *v
		accountSettings[k] = &cp
	}

	serviceDeployments := make(map[string]*ServiceDeployment, len(b.serviceDeployments))
	for k, v := range b.serviceDeployments {
		cp := *v
		serviceDeployments[k] = &cp
	}

	snap := backendSnapshot{
		Clusters:               snapshotClusters(b.clusters),
		TaskDefinitions:        snapshotTaskDefinitions(b.taskDefinitions),
		Services:               snapshotServices(b.services),
		Tasks:                  snapshotTasks(b.tasks),
		ContainerInstances:     snapshotContainerInstances(b.containerInstances),
		TaskSets:               snapshotTaskSets(b.taskSets),
		CapacityProviders:      snapshotCapacityProviders(b.capacityProviders),
		AccountSettings:        accountSettings,
		Attributes:             snapshotAttributes(b.attributes),
		ServiceDeployments:     serviceDeployments,
		ExpressGatewayServices: snapshotExpressGatewayServices(b.expressGatewayServices),
		TaskProtections:        snapshotTaskProtections(b.taskProtections),
		Daemons:                snapshotDaemons(b.daemons),
		DaemonTaskDefinitions:  snapshotDaemonTaskDefinitions(b.daemonTaskDefinitions),
		DaemonDeployments:      snapshotDaemonDeployments(b.daemonDeployments),
		DaemonRevisions:        snapshotDaemonRevisions(b.daemonRevisions),
	}

	return persistence.MarshalSnapshot(ctx, "ecs", snap)
}

// initSnapshotDefaults ensures all maps in the snapshot are non-nil so callers
// can safely assign them to the backend without nil-map panics.
func initSnapshotDefaults(snap *backendSnapshot) {
	if snap.Clusters == nil {
		snap.Clusters = make(map[string]*Cluster)
	}

	if snap.TaskDefinitions == nil {
		snap.TaskDefinitions = make(map[string][]*TaskDefinition)
	}

	if snap.Services == nil {
		snap.Services = make(map[string]map[string]*Service)
	}

	if snap.Tasks == nil {
		snap.Tasks = make(map[string]map[string]*Task)
	}

	if snap.ContainerInstances == nil {
		snap.ContainerInstances = make(map[string]map[string]*ContainerInstance)
	}

	if snap.TaskSets == nil {
		snap.TaskSets = make(map[string]map[string]*TaskSet)
	}

	if snap.CapacityProviders == nil {
		snap.CapacityProviders = make(map[string]*CapacityProvider)
	}

	if snap.AccountSettings == nil {
		snap.AccountSettings = make(map[string]*AccountSetting)
	}

	if snap.Attributes == nil {
		snap.Attributes = make(map[string]map[string]*Attribute)
	}

	if snap.ServiceDeployments == nil {
		snap.ServiceDeployments = make(map[string]*ServiceDeployment)
	}

	if snap.ExpressGatewayServices == nil {
		snap.ExpressGatewayServices = make(map[string]*ExpressGatewayService)
	}

	if snap.TaskProtections == nil {
		snap.TaskProtections = make(map[string]*TaskProtection)
	}

	initDaemonSnapshotDefaults(snap)
}

// initDaemonSnapshotDefaults ensures the daemon-related maps in the snapshot
// are non-nil. Split out from initSnapshotDefaults to keep cyclomatic
// complexity within limits.
func initDaemonSnapshotDefaults(snap *backendSnapshot) {
	if snap.Daemons == nil {
		snap.Daemons = make(map[string]*Daemon)
	}

	if snap.DaemonTaskDefinitions == nil {
		snap.DaemonTaskDefinitions = make(map[string][]*DaemonTaskDefinition)
	}

	if snap.DaemonDeployments == nil {
		snap.DaemonDeployments = make(map[string]*DaemonDeployment)
	}

	if snap.DaemonRevisions == nil {
		snap.DaemonRevisions = make(map[string]*DaemonRevision)
	}
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "ecs", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	initSnapshotDefaults(&snap)

	b.clusters = snap.Clusters
	b.taskDefinitions = snap.TaskDefinitions
	b.services = snap.Services
	b.tasks = snap.Tasks
	b.containerInstances = snap.ContainerInstances
	b.taskSets = snap.TaskSets
	b.capacityProviders = snap.CapacityProviders
	b.accountSettings = snap.AccountSettings
	b.attributes = snap.Attributes
	b.serviceDeployments = snap.ServiceDeployments
	b.expressGatewayServices = snap.ExpressGatewayServices
	b.taskProtections = snap.TaskProtections
	b.daemons = restoreDaemons(snap.Daemons)
	b.daemonTaskDefinitions = snap.DaemonTaskDefinitions
	b.daemonDeployments = snap.DaemonDeployments
	b.daemonRevisions = snap.DaemonRevisions

	// Rebuild the ARN→TaskDefinition cache from restored task definitions.
	b.taskDefByArn = make(map[string]*TaskDefinition)
	for _, revs := range b.taskDefinitions {
		for _, td := range revs {
			b.taskDefByArn[td.TaskDefinitionArn] = td
		}
	}

	// Rebuild the ARN→DaemonTaskDefinition cache from restored daemon task definitions.
	b.daemonTaskDefByArn = make(map[string]*DaemonTaskDefinition)
	for _, revs := range b.daemonTaskDefinitions {
		for _, td := range revs {
			b.daemonTaskDefByArn[td.DaemonTaskDefinitionArn] = td
		}
	}

	return nil
}

// Snapshot implements Snapshottable by delegating to the backend when it
// supports snapshotting. Returns nil for non-snapshottable backends.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements Snapshottable by delegating to the backend when it
// supports snapshotting. Non-snapshottable backends are skipped.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Restore(ctx, data)
	}

	return nil
}
