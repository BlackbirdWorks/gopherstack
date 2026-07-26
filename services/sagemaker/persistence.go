package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// sagemakerSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change would make an older snapshot unsafe to decode
// as the current shape. Restore compares this against the persisted value
// and discards (rather than attempts to partially decode) any mismatch — see
// Restore below. This is bumped from the pre-Phase-3.3 format, which had no
// Version field at all and persisted resources as region-nested raw maps
// rather than store.Registry tables, so any snapshot taken before this
// pkgs/store conversion is treated as "missing/incompatible" and discarded
// cleanly rather than misinterpreted.
const sagemakerSnapshotVersion = 1

// persistedCluster is a serialisable DTO for Cluster. It exists — mirroring
// the SQS pilot's queueSnapshot — because Cluster is a "dirty" struct for
// persistence purposes: Nodes carries `json:"-"` (hidden from the AWS wire
// shape returned to API callers) but must still be persisted, and
// CreationTime is persisted as an explicit RFC3339 string (this backend's
// pre-conversion snapshot format) rather than store.Table's default
// json.Marshal-of-time.Time encoding.
type persistedCluster struct {
	CreationTime   string                  `json:"CreationTime"`
	Nodes          map[string]*ClusterNode `json:"Nodes"`
	Tags           map[string]string       `json:"Tags,omitempty"`
	ClusterArn     string                  `json:"ClusterArn"`
	ClusterName    string                  `json:"ClusterName"`
	ClusterStatus  string                  `json:"ClusterStatus"`
	NodeRecovery   string                  `json:"NodeRecovery,omitempty"`
	InstanceGroups []ClusterInstanceGroup  `json:"InstanceGroups,omitempty"`
}

// toPersistedCluster converts a live Cluster to its persisted DTO shape.
func toPersistedCluster(c *Cluster) *persistedCluster {
	pc := &persistedCluster{
		CreationTime:   c.CreationTime.Format(time.RFC3339),
		ClusterArn:     c.ClusterArn,
		ClusterName:    c.ClusterName,
		ClusterStatus:  c.ClusterStatus,
		NodeRecovery:   c.NodeRecovery,
		Tags:           c.Tags,
		InstanceGroups: c.InstanceGroups,
		Nodes:          make(map[string]*ClusterNode, len(c.Nodes)),
	}

	for nk, nv := range c.Nodes {
		nodeCopy := *nv
		pc.Nodes[nk] = &nodeCopy
	}

	return pc
}

// fromPersistedCluster rebuilds a live Cluster from its persisted DTO shape.
func fromPersistedCluster(ctx context.Context, pc *persistedCluster) *Cluster {
	t, err := time.Parse(time.RFC3339, pc.CreationTime)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx,
			"sagemaker: failed to parse cluster creation time", "cluster", pc.ClusterName, "error", err)
	}

	c := &Cluster{
		ClusterArn:     pc.ClusterArn,
		ClusterName:    pc.ClusterName,
		ClusterStatus:  pc.ClusterStatus,
		NodeRecovery:   pc.NodeRecovery,
		Tags:           pc.Tags,
		InstanceGroups: pc.InstanceGroups,
		CreationTime:   t,
		Nodes:          make(map[string]*ClusterNode, len(pc.Nodes)),
	}

	for nk, nv := range pc.Nodes {
		nodeCopy := *nv
		c.Nodes[nk] = &nodeCopy
	}

	return c
}

// clusterTablePrefix names the per-region entries carved out of
// backendSnapshot.Tables for DTO-based Cluster encoding — see Snapshot/Restore.
const clusterTablePrefix = "clusters:"

// hubContentKeyString serialises a hubContentKey to a single delimited string
// for use as the store.Table primary key for b.hubContents.
func hubContentKeyString(k hubContentKey) string {
	return k.HubName + "|" + k.HubContentType + "|" + k.HubContentName + "|" + k.HubContentVersion
}

// backendSnapshot is the top-level on-disk shape for the SageMaker backend.
//
// Tables holds one JSON-encoded array per registered store.Table (produced by
// [store.Registry.SnapshotAll]) — one entry per (resource, region) pair,
// e.g. "models:us-east-1", "endpointConfigs:us-east-1". Clusters are the one
// exception: their table entries are re-encoded through [persistedCluster]
// (see Snapshot/Restore) rather than left as the registry's generic
// json.Marshal-of-Cluster output.
//
// The five fields below are not (yet) backed by store.Table — see the doc
// comments on the corresponding InMemoryBackend fields in backend.go for why
// each one doesn't fit the Table[V] keyed-by-string-derived-from-V model —
// so they are persisted directly here instead of via Tables.
type backendSnapshot struct {
	Tables                 map[string]json.RawMessage                  `json:"tables"`
	ImageVersions          map[string]map[string]map[int]*ImageVersion `json:"imageVersions,omitempty"`
	ImageVersionCounts     map[string]map[string]int                   `json:"imageVersionCounts,omitempty"`
	MonitoringAlertHistory map[string][]*MonitoringAlertHistoryEntry   `json:"monitoringAlertHistory,omitempty"`
	PipelineVersions       map[string]map[string][]*PipelineVersion    `json:"pipelineVersions,omitempty"`
	SCPortfolioEnabled     map[string]bool                             `json:"servicecatalogPortfolioEnabled,omitempty"`
	AccountID              string                                      `json:"accountID"`
	Region                 string                                      `json:"region"`
	Version                int                                         `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "sagemaker: snapshot table marshal failed", "error", err)

		return nil
	}

	// Overwrite the generic per-region cluster table encoding (which would
	// silently drop Nodes, since Cluster.Nodes carries `json:"-"`) with the
	// persistedCluster DTO shape.
	for region, tbl := range b.clusters {
		clusters := tbl.Snapshot()
		dtos := make([]*persistedCluster, len(clusters))

		for i, c := range clusters {
			dtos[i] = toPersistedCluster(c)
		}

		data, marshalErr := json.Marshal(dtos)
		if marshalErr != nil {
			logger.Load(ctx).WarnContext(ctx,
				"sagemaker: snapshot cluster table marshal failed", "region", region, "error", marshalErr)

			return nil
		}

		tables[clusterTablePrefix+region] = data
	}

	snap := backendSnapshot{
		Version:                sagemakerSnapshotVersion,
		Tables:                 tables,
		ImageVersions:          b.imageVersions,
		ImageVersionCounts:     b.imageVersionCounts,
		MonitoringAlertHistory: b.monitoringAlertHistory,
		PipelineVersions:       b.pipelineVersions,
		SCPortfolioEnabled:     b.servicecatalogPortfolioEnabled,
		AccountID:              b.accountID,
		Region:                 b.region,
	}

	return persistence.MarshalSnapshot(ctx, "sagemaker", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "sagemaker", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != sagemakerSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape — that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across this Phase 3.3 snapshot-format
		// change), not data corruption.
		logger.Load(ctx).WarnContext(ctx,
			"sagemaker: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", sagemakerSnapshotVersion)

		b.registry.ResetAll()
		b.resetNonTableFieldsLocked()
		b.resetLifecycleContext()

		return nil
	}

	// Cluster entries need DTO decoding (see Snapshot): pull them out of the
	// raw table map before delegating the rest to registry.RestoreAll, then
	// restore each region's store.Table[Cluster] from the decoded DTOs
	// afterward. Any region not present in the snapshot is left absent from
	// b.clusters here; RestoreAll's "reset tables absent from data" behavior
	// (since we've stripped every "clusters:*" key from snap.Tables) clears
	// any such region's table if it was already registered.
	clusterTables := make(map[string]json.RawMessage)

	for name, raw := range snap.Tables {
		if region, ok := strings.CutPrefix(name, clusterTablePrefix); ok {
			clusterTables[region] = raw

			delete(snap.Tables, name)
		}
	}

	// registry.RestoreAll only visits tables already registered in the
	// registry; every resource table is registered lazily per-region on
	// first use (see e.g. modelsStore), so a freshly constructed backend —
	// the common Restore target — starts with none registered. Without this,
	// RestoreAll would silently skip every region the backend hasn't
	// happened to touch yet, dropping that data on the floor.
	b.ensureTablesRegistered(snap.Tables)

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("sagemaker: restore snapshot tables: %w", err)
	}

	for region, raw := range clusterTables {
		var dtos []*persistedCluster

		if err := json.Unmarshal(raw, &dtos); err != nil {
			return fmt.Errorf("sagemaker: restore cluster table %q: %w", region, err)
		}

		clusters := make([]*Cluster, len(dtos))
		for i, pc := range dtos {
			clusters[i] = fromPersistedCluster(ctx, pc)
		}

		b.clustersStore(region).Restore(clusters)
	}

	b.imageVersions = snap.ImageVersions
	b.imageVersionCounts = snap.ImageVersionCounts
	b.monitoringAlertHistory = snap.MonitoringAlertHistory
	b.pipelineVersions = snap.PipelineVersions
	b.servicecatalogPortfolioEnabled = snap.SCPortfolioEnabled

	if b.imageVersions == nil {
		b.imageVersions = make(map[string]map[string]map[int]*ImageVersion)
	}

	if b.imageVersionCounts == nil {
		b.imageVersionCounts = make(map[string]map[string]int)
	}

	if b.monitoringAlertHistory == nil {
		b.monitoringAlertHistory = make(map[string][]*MonitoringAlertHistoryEntry)
	}

	if b.pipelineVersions == nil {
		b.pipelineVersions = make(map[string]map[string][]*PipelineVersion)
	}

	if b.servicecatalogPortfolioEnabled == nil {
		b.servicecatalogPortfolioEnabled = make(map[string]bool)
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	b.resetLifecycleContext()
	b.rebuildARNIndexes()

	return nil
}

// resetNonTableFieldsLocked clears the handful of resource collections not
// backed by store.Table (and therefore not covered by registry.ResetAll),
// plus the ARN indexes (which are pure caches derived from the Table-backed
// resource maps). Callers must hold b.mu.
func (b *InMemoryBackend) resetNonTableFieldsLocked() {
	b.imageVersions = make(map[string]map[string]map[int]*ImageVersion)
	b.imageVersionCounts = make(map[string]map[string]int)
	b.monitoringAlertHistory = make(map[string][]*MonitoringAlertHistoryEntry)
	b.pipelineVersions = make(map[string]map[string][]*PipelineVersion)
	b.servicecatalogPortfolioEnabled = make(map[string]bool)
	b.rebuildARNIndexes()
}

// buildARNIndex reconstructs a region -> ARN -> name index from a region ->
// store.Table[V] resource map. arnFn derives both the resource's own primary
// name/key and its ARN from the stored value (rather than from the table's
// internal key, which [store.Table] does not expose) so the index can be
// rebuilt purely by scanning each table's contents after a restore.
func buildARNIndex[V any](
	src map[string]*store.Table[V],
	arnFn func(*V) (name, arn string),
) map[string]map[string]string {
	idx := make(map[string]map[string]string, len(src))

	for region, tbl := range src {
		regionIdx := make(map[string]string, tbl.Len())

		for _, item := range tbl.All() {
			name, arn := arnFn(item)
			regionIdx[arn] = name
		}

		idx[region] = regionIdx
	}

	return idx
}

// rebuildARNIndexes reconstructs all ARN-to-name indexes after a restore
// (called with lock held). These indexes are pure derived caches over the
// Table-backed resource maps (see the "modelARNIndex" family of fields in
// backend.go) — never persisted themselves.
func (b *InMemoryBackend) rebuildARNIndexes() {
	b.modelARNIndex = buildARNIndex(b.models, func(m *Model) (string, string) { return m.ModelName, m.ModelARN })
	b.endpointConfigARNIndex = buildARNIndex(
		b.endpointConfigs,
		func(ec *EndpointConfig) (string, string) { return ec.EndpointConfigName, ec.EndpointConfigARN },
	)
	b.actionARNIndex = buildARNIndex(b.actions, func(a *Action) (string, string) { return a.ActionName, a.ActionArn })
	b.contextARNIndex = buildARNIndex(
		b.contexts,
		func(c *Context) (string, string) { return c.ContextName, c.ContextArn },
	)
	b.algorithmARNIndex = buildARNIndex(
		b.algorithms,
		func(al *Algorithm) (string, string) { return al.AlgorithmName, al.AlgorithmArn },
	)
	b.clusterARNIndex = buildARNIndex(
		b.clusters,
		func(c *Cluster) (string, string) { return c.ClusterName, c.ClusterArn },
	)
	// ModelPackage's own primary store.Table key is already its ARN (see
	// modelsStore-family doc in backend.go), so this index degenerates to
	// ARN -> ARN — preserved exactly from the pre-conversion behavior.
	b.modelPackageARNIndex = buildARNIndex(
		b.modelPackages,
		func(mp *ModelPackage) (string, string) { return mp.ModelPackageArn, mp.ModelPackageArn },
	)
	b.endpointARNIndex = buildARNIndex(
		b.endpoints,
		func(ep *Endpoint) (string, string) { return ep.EndpointName, ep.EndpointArn },
	)
	b.trainingJobARNIndex = buildARNIndex(
		b.trainingJobs,
		func(tj *TrainingJob) (string, string) { return tj.TrainingJobName, tj.TrainingJobArn },
	)
	b.notebookARNIndex = buildARNIndex(
		b.notebooks,
		func(nb *NotebookInstance) (string, string) { return nb.NotebookInstanceName, nb.NotebookInstanceArn },
	)
	b.hpTuningJobARNIndex = buildARNIndex(
		b.hpTuningJobs,
		func(j *HyperParameterTuningJob) (string, string) {
			return j.HyperParameterTuningJobName, j.HyperParameterTuningJobArn
		},
	)
	b.processingJobARNIndex = buildARNIndex(
		b.processingJobs,
		func(pj *ProcessingJob) (string, string) { return pj.ProcessingJobName, pj.ProcessingJobArn },
	)
	b.transformJobARNIndex = buildARNIndex(
		b.transformJobs,
		func(tj *TransformJob) (string, string) { return tj.TransformJobName, tj.TransformJobArn },
	)
	b.aiWorkloadConfigARNIndex = buildARNIndex(
		b.aiWorkloadConfigs,
		func(c *AIWorkloadConfig) (string, string) { return c.AIWorkloadConfigName, c.AIWorkloadConfigArn },
	)
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

// tableRegistrars maps each store.Table-backed resource's field name (the
// part of a "field:region" registry table name before the colon) to a
// function that lazily registers that field's table for a given region —
// i.e. the same xxxStore(region) helper the backend's own operations call.
//
// Restore needs this because store.Registry.RestoreAll only visits tables
// ALREADY registered in the registry; a freshly constructed backend (the
// common Restore target) has none registered until first touched, so
// without this pre-registration step every resource's snapshot data would
// be silently dropped for any region the fresh backend hasn't used yet.
//
//nolint:gochecknoglobals // read-only lookup table initialized once at package load
var tableRegistrars = map[string]func(*InMemoryBackend, string){
	"actions":                        func(b *InMemoryBackend, r string) { b.actionsStore(r) },
	"aiBenchmarkJobs":                func(b *InMemoryBackend, r string) { b.aiBenchmarkJobsStore(r) },
	"aiRecommendationJobs":           func(b *InMemoryBackend, r string) { b.aiRecommendationJobsStore(r) },
	"aiWorkloadConfigs":              func(b *InMemoryBackend, r string) { b.aiWorkloadConfigsStore(r) },
	"algorithms":                     func(b *InMemoryBackend, r string) { b.algorithmsStore(r) },
	"appImageConfigs":                func(b *InMemoryBackend, r string) { b.appImageConfigsStore(r) },
	"apps":                           func(b *InMemoryBackend, r string) { b.appsStore(r) },
	"artifacts":                      func(b *InMemoryBackend, r string) { b.artifactsStore(r) },
	"associations":                   func(b *InMemoryBackend, r string) { b.associationsStore(r) },
	"autoMLJobs":                     func(b *InMemoryBackend, r string) { b.autoMLJobsStore(r) },
	"clusterSchedulerConfigs":        func(b *InMemoryBackend, r string) { b.clusterSchedulerConfigsStore(r) },
	"clusters":                       func(b *InMemoryBackend, r string) { b.clustersStore(r) },
	"codeRepositories":               func(b *InMemoryBackend, r string) { b.codeRepositoriesStore(r) },
	"compilationJobs":                func(b *InMemoryBackend, r string) { b.compilationJobsStore(r) },
	"computeQuotas":                  func(b *InMemoryBackend, r string) { b.computeQuotasStore(r) },
	"contexts":                       func(b *InMemoryBackend, r string) { b.contextsStore(r) },
	"dataQualityJobDefs":             func(b *InMemoryBackend, r string) { b.dataQualityJobDefsStore(r) },
	"deviceFleets":                   func(b *InMemoryBackend, r string) { b.deviceFleetsStore(r) },
	"devices":                        func(b *InMemoryBackend, r string) { b.devicesStore(r) },
	"domains":                        func(b *InMemoryBackend, r string) { b.domainsStore(r) },
	"edgeDeploymentPlans":            func(b *InMemoryBackend, r string) { b.edgeDeploymentPlansStore(r) },
	"edgePackagingJobs":              func(b *InMemoryBackend, r string) { b.edgePackagingJobsStore(r) },
	"endpointConfigs":                func(b *InMemoryBackend, r string) { b.endpointConfigsStore(r) },
	"endpoints":                      func(b *InMemoryBackend, r string) { b.endpointsStore(r) },
	"experiments":                    func(b *InMemoryBackend, r string) { b.experimentsStore(r) },
	"featureGroups":                  func(b *InMemoryBackend, r string) { b.featureGroupsStore(r) },
	"featureMetadata":                func(b *InMemoryBackend, r string) { b.featureMetadataStore(r) },
	"featureRecords":                 func(b *InMemoryBackend, r string) { b.featureRecordsStore(r) },
	"flowDefinitions":                func(b *InMemoryBackend, r string) { b.flowDefinitionsStore(r) },
	"hpTuningJobs":                   func(b *InMemoryBackend, r string) { b.hpTuningJobsStore(r) },
	"hubContents":                    func(b *InMemoryBackend, r string) { b.hubContentsStore(r) },
	"hubs":                           func(b *InMemoryBackend, r string) { b.hubsStore(r) },
	"humanTaskUis":                   func(b *InMemoryBackend, r string) { b.humanTaskUisStore(r) },
	"inferenceComponents":            func(b *InMemoryBackend, r string) { b.inferenceComponentsStore(r) },
	"inferenceExperiments":           func(b *InMemoryBackend, r string) { b.inferenceExperimentsStore(r) },
	"inferenceRecommendationsJobs":   func(b *InMemoryBackend, r string) { b.inferenceRecommendationsJobsStore(r) },
	"jobs":                           func(b *InMemoryBackend, r string) { b.jobsStore(r) },
	"labelingJobs":                   func(b *InMemoryBackend, r string) { b.labelingJobsStore(r) },
	"mlflowApps":                     func(b *InMemoryBackend, r string) { b.mlflowAppsStore(r) },
	"mlflowTrackingServers":          func(b *InMemoryBackend, r string) { b.mlflowTrackingServersStore(r) },
	"modelBiasJobDefs":               func(b *InMemoryBackend, r string) { b.modelBiasJobDefsStore(r) },
	"modelCardExportJobs":            func(b *InMemoryBackend, r string) { b.modelCardExportJobsStore(r) },
	"modelCards":                     func(b *InMemoryBackend, r string) { b.modelCardsStore(r) },
	"modelExplainJobDefs":            func(b *InMemoryBackend, r string) { b.modelExplainJobDefsStore(r) },
	"modelPackageGroups":             func(b *InMemoryBackend, r string) { b.modelPackageGroupsStore(r) },
	"modelPackages":                  func(b *InMemoryBackend, r string) { b.modelPackagesStore(r) },
	"modelQualityJobDefs":            func(b *InMemoryBackend, r string) { b.modelQualityJobDefsStore(r) },
	"models":                         func(b *InMemoryBackend, r string) { b.modelsStore(r) },
	"monitoringExecutions":           func(b *InMemoryBackend, r string) { b.monitoringExecutionsStore(r) },
	"monitoringSchedules":            func(b *InMemoryBackend, r string) { b.monitoringSchedulesStore(r) },
	"notebookLifecycleConfigs":       func(b *InMemoryBackend, r string) { b.notebookLifecycleConfigsStore(r) },
	"notebooks":                      func(b *InMemoryBackend, r string) { b.notebooksStore(r) },
	"optimizationJobs":               func(b *InMemoryBackend, r string) { b.optimizationJobsStore(r) },
	"partnerApps":                    func(b *InMemoryBackend, r string) { b.partnerAppsStore(r) },
	"pipelineExecSteps":              func(b *InMemoryBackend, r string) { b.pipelineExecStepsStore(r) },
	"pipelineExecutions":             func(b *InMemoryBackend, r string) { b.pipelineExecutionsStore(r) },
	"pipelines":                      func(b *InMemoryBackend, r string) { b.pipelinesStore(r) },
	"processingJobs":                 func(b *InMemoryBackend, r string) { b.processingJobsStore(r) },
	"projects":                       func(b *InMemoryBackend, r string) { b.projectsStore(r) },
	"reservedCapacities":             func(b *InMemoryBackend, r string) { b.reservedCapacitiesStore(r) },
	"smImages":                       func(b *InMemoryBackend, r string) { b.smImagesStore(r) },
	"spaces":                         func(b *InMemoryBackend, r string) { b.spacesStore(r) },
	"studioLifecycleConfigs":         func(b *InMemoryBackend, r string) { b.studioLifecycleConfigsStore(r) },
	"trainingJobs":                   func(b *InMemoryBackend, r string) { b.trainingJobsStore(r) },
	"trainingPlanExtensionOfferings": func(b *InMemoryBackend, r string) { b.trainingPlanExtensionOfferingsStore(r) },
	"trainingPlans":                  func(b *InMemoryBackend, r string) { b.trainingPlansStore(r) },
	"transformJobs":                  func(b *InMemoryBackend, r string) { b.transformJobsStore(r) },
	"trialComponentAssociations":     func(b *InMemoryBackend, r string) { b.trialComponentAssociationsStore(r) },
	"trialComponents":                func(b *InMemoryBackend, r string) { b.trialComponentsStore(r) },
	"trials":                         func(b *InMemoryBackend, r string) { b.trialsStore(r) },
	"userProfiles":                   func(b *InMemoryBackend, r string) { b.userProfilesStore(r) },
	"workforces":                     func(b *InMemoryBackend, r string) { b.workforcesStore(r) },
	"workteams":                      func(b *InMemoryBackend, r string) { b.workteamsStore(r) },
	"monitoringAlerts":               func(b *InMemoryBackend, r string) { b.monitoringAlertsStore(r) },
}

// ensureTablesRegistered pre-registers a store.Table for every "field:region"
// key present in raw — see tableRegistrars — so the subsequent
// registry.RestoreAll call actually receives every table the snapshot
// contains, not just whichever ones happen to already be registered.
func (b *InMemoryBackend) ensureTablesRegistered(raw map[string]json.RawMessage) {
	for name := range raw {
		field, region, cut := strings.Cut(name, ":")
		if !cut {
			continue
		}

		if fn, ok := tableRegistrars[field]; ok {
			fn(b, region)
		}
	}
}
