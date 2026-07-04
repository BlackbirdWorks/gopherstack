package glue

import (
	"context"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	Databases                 map[string]*Database                      `json:"databases"`
	Tables                    map[string]*Table                         `json:"tables"`
	Crawlers                  map[string]*Crawler                       `json:"crawlers"`
	Jobs                      map[string]*Job                           `json:"jobs"`
	Partitions                map[string]*Partition                     `json:"partitions"`
	PartitionIndexes          map[string]*PartitionIndex                `json:"partitionIndexes"`
	TableVersions             map[string]*TableVersion                  `json:"tableVersions"`
	Connections               map[string]*Connection                    `json:"connections"`
	Blueprints                map[string]*Blueprint                     `json:"blueprints"`
	CustomEntityTypes         map[string]*CustomEntityType              `json:"customEntityTypes"`
	DataQualityResult         map[string]*DataQualityResult             `json:"dataQualityResult"`
	DevEndpoints              map[string]*DevEndpoint                   `json:"devEndpoints"`
	JobRuns                   map[string][]*JobRun                      `json:"jobRuns"`
	JobBookmarks              map[string]*JobBookmark                   `json:"jobBookmarks"`
	DataQualityRulesets       map[string]*DataQualityRuleset            `json:"dataQualityRulesets"`
	DataQualityEvalRuns       map[string]*DataQualityEvaluationRun      `json:"dataQualityEvalRuns"`
	Triggers                  map[string]*Trigger                       `json:"triggers"`
	Workflows                 map[string]*Workflow                      `json:"workflows"`
	WorkflowRuns              map[string][]*WorkflowRun                 `json:"workflowRuns"`
	Classifiers               map[string]*Classifier                    `json:"classifiers"`
	Registries                map[string]*Registry                      `json:"registries"`
	Schemas                   map[string]*Schema                        `json:"schemas"`
	SchemaVersions            map[string][]*SchemaVersion               `json:"schemaVersions"`
	UDFs                      map[string]*UserDefinedFunction           `json:"udfs"`
	SecurityConfigs           map[string]*SecurityConfiguration         `json:"securityConfigs"`
	Sessions                  map[string]*Session                       `json:"sessions"`
	SessionStatements         map[string][]*Statement                   `json:"sessionStatements"`
	TableOptimizers           map[string]*TableOptimizer                `json:"tableOptimizers"`
	TableColumnStats          map[string]*ColumnStatistics              `json:"tableColumnStats"`
	PartitionColumnStats      map[string]*ColumnStatistics              `json:"partitionColumnStats"`
	ResourcePolicies          map[string]*resourcePolicyEntry           `json:"resourcePolicies"`
	MLTransforms              map[string]*MLTransform                   `json:"mlTransforms"`
	Catalogs                  map[string]*CatalogEntry                  `json:"catalogs"`
	CatalogEncryptionSettings map[string]*DataCatalogEncryptionSettings `json:"catalogEncryptionSettings"`
	UsageProfiles             map[string]*UsageProfile                  `json:"usageProfiles"`
	BlueprintRuns             map[string]*BlueprintRun                  `json:"blueprintRuns"`
	DQRecommendationRuns      map[string]*DQRuleRecommendationRun       `json:"dqRecommendationRuns"`
	ColumnStatTaskSettings    map[string]*ColumnStatisticsTaskSettings  `json:"columnStatTaskSettings"`
	ColumnStatTaskRuns        map[string]*ColumnStatisticsTaskRun       `json:"columnStatTaskRuns"`
	MaterializedViewRuns      map[string]*MaterializedViewRefreshRun    `json:"materializedViewRuns"`
	Integrations              map[string]*Integration                   `json:"integrations"`
	CrawlHistory              map[string][]*CrawlHistoryEntry           `json:"crawlHistory"`
	DQStatisticAnnotations    map[string]*StatisticAnnotation           `json:"dqStatisticAnnotations"`
	GlueIdentityCenterConfig  *IdentityCenterConfig                     `json:"glueIdentityCenterConfig,omitempty"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Databases: copyMap(b.databases, cloneDatabase),
		Tables: copyMap(b.tables, func(t *Table) *Table {
			cp := *t

			return &cp
		}),
		Crawlers: copyMap(b.crawlers, cloneCrawler),
		Jobs:     copyMap(b.jobs, cloneJob),
		Partitions: copyMap(b.partitions, func(p *Partition) *Partition {
			cp := *p
			cp.Values = append([]string(nil), p.Values...)

			return &cp
		}),
		TableVersions: copyMap(b.tableVersions, func(tv *TableVersion) *TableVersion {
			cp := *tv

			return &cp
		}),
		Connections: copyMap(b.connections, cloneConnection),
		Blueprints:  maps.Clone(b.blueprints),
		CustomEntityTypes: copyMap(b.customEntityTypes, func(c *CustomEntityType) *CustomEntityType {
			cp := *c
			cp.ContextWords = append([]string(nil), c.ContextWords...)

			return &cp
		}),
		DataQualityResult: maps.Clone(b.dataQualityResult),
		DevEndpoints:      maps.Clone(b.devEndpoints),
		JobRuns:           copyJobRunsMap(b.jobRuns),
		JobBookmarks:      maps.Clone(b.jobBookmarks),
		DataQualityRulesets: copyMap(b.dataQualityRulesets, func(r *DataQualityRuleset) *DataQualityRuleset {
			cp := *r
			cp.Tags = maps.Clone(r.Tags)

			return &cp
		}),
		DataQualityEvalRuns: copyMap(
			b.dataQualityEvalRuns,
			func(run *DataQualityEvaluationRun) *DataQualityEvaluationRun {
				cp := *run
				cp.RulesetNames = append([]string(nil), run.RulesetNames...)

				return &cp
			},
		),
	}
	addExtendedSnapshotState(&snap, b)

	return persistence.MarshalSnapshot(ctx, "glue", snap)
}

func addExtendedSnapshotState(snap *backendSnapshot, b *InMemoryBackend) {
	snap.PartitionIndexes = b.partitionIndexes
	snap.Triggers = b.triggers
	snap.Workflows = b.workflows
	snap.WorkflowRuns = b.workflowRuns
	snap.Classifiers = b.classifiers
	snap.Registries = b.registries
	snap.Schemas = b.schemas
	snap.SchemaVersions = b.schemaVersions
	snap.UDFs = b.udfs
	snap.SecurityConfigs = b.securityConfigs
	snap.Sessions = b.sessions
	snap.SessionStatements = b.sessionStatements
	snap.TableOptimizers = b.tableOptimizers
	snap.TableColumnStats = b.tableColumnStats
	snap.PartitionColumnStats = b.partitionColumnStats
	snap.ResourcePolicies = b.resourcePolicies
	snap.MLTransforms = b.mlTransforms
	snap.Catalogs = b.catalogs
	snap.CatalogEncryptionSettings = b.catalogEncryptionSettings
	snap.UsageProfiles = b.usageProfiles
	snap.BlueprintRuns = b.blueprintRuns
	snap.DQRecommendationRuns = b.dqRecommendationRuns
	snap.ColumnStatTaskSettings = b.columnStatTaskSettings
	snap.ColumnStatTaskRuns = b.columnStatTaskRuns
	snap.MaterializedViewRuns = b.materializedViewRuns
	snap.Integrations = b.integrations
	snap.CrawlHistory = b.crawlHistory
	snap.DQStatisticAnnotations = b.dqStatisticAnnotations
	snap.GlueIdentityCenterConfig = b.glueIdentityCenterConfig
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "glue", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	initSnapshotDefaults(&snap)
	b.restoreFromSnapshot(snap)

	return nil
}

// initSnapshotDefaults ensures all snapshot maps are non-nil.
func initSnapshotDefaults(snap *backendSnapshot) {
	initSnapshotCoreDefaults(snap)
	initSnapshotExtDefaults(snap)
	initSnapshotOperationalDefaults(snap)
	initSnapshotCatalogDefaults(snap)
	initSnapshotTaskDefaults(snap)
}

// initSnapshotCoreDefaults initializes core maps to non-nil.
func initSnapshotCoreDefaults(snap *backendSnapshot) {
	if snap.Databases == nil {
		snap.Databases = make(map[string]*Database)
	}
	if snap.Tables == nil {
		snap.Tables = make(map[string]*Table)
	}
	if snap.Crawlers == nil {
		snap.Crawlers = make(map[string]*Crawler)
	}
	if snap.Jobs == nil {
		snap.Jobs = make(map[string]*Job)
	}
	if snap.Partitions == nil {
		snap.Partitions = make(map[string]*Partition)
	}
	if snap.PartitionIndexes == nil {
		snap.PartitionIndexes = make(map[string]*PartitionIndex)
	}
	if snap.TableVersions == nil {
		snap.TableVersions = make(map[string]*TableVersion)
	}
	if snap.Connections == nil {
		snap.Connections = make(map[string]*Connection)
	}
	if snap.Blueprints == nil {
		snap.Blueprints = make(map[string]*Blueprint)
	}
}

// initSnapshotExtDefaults initializes extended maps to non-nil.
func initSnapshotExtDefaults(snap *backendSnapshot) {
	if snap.CustomEntityTypes == nil {
		snap.CustomEntityTypes = make(map[string]*CustomEntityType)
	}
	if snap.DataQualityResult == nil {
		snap.DataQualityResult = make(map[string]*DataQualityResult)
	}
	if snap.DevEndpoints == nil {
		snap.DevEndpoints = make(map[string]*DevEndpoint)
	}
	if snap.JobRuns == nil {
		snap.JobRuns = make(map[string][]*JobRun)
	}
	if snap.JobBookmarks == nil {
		snap.JobBookmarks = make(map[string]*JobBookmark)
	}
	if snap.DataQualityRulesets == nil {
		snap.DataQualityRulesets = make(map[string]*DataQualityRuleset)
	}
	if snap.DataQualityEvalRuns == nil {
		snap.DataQualityEvalRuns = make(map[string]*DataQualityEvaluationRun)
	}
}

func initSnapshotOperationalDefaults(snap *backendSnapshot) {
	if snap.Triggers == nil {
		snap.Triggers = make(map[string]*Trigger)
	}
	if snap.Workflows == nil {
		snap.Workflows = make(map[string]*Workflow)
	}
	if snap.WorkflowRuns == nil {
		snap.WorkflowRuns = make(map[string][]*WorkflowRun)
	}
	if snap.Classifiers == nil {
		snap.Classifiers = make(map[string]*Classifier)
	}
	if snap.UDFs == nil {
		snap.UDFs = make(map[string]*UserDefinedFunction)
	}
	if snap.SecurityConfigs == nil {
		snap.SecurityConfigs = make(map[string]*SecurityConfiguration)
	}
	if snap.Sessions == nil {
		snap.Sessions = make(map[string]*Session)
	}
	if snap.SessionStatements == nil {
		snap.SessionStatements = make(map[string][]*Statement)
	}
	if snap.TableOptimizers == nil {
		snap.TableOptimizers = make(map[string]*TableOptimizer)
	}
	if snap.TableColumnStats == nil {
		snap.TableColumnStats = make(map[string]*ColumnStatistics)
	}
	if snap.PartitionColumnStats == nil {
		snap.PartitionColumnStats = make(map[string]*ColumnStatistics)
	}
}

func initSnapshotCatalogDefaults(snap *backendSnapshot) {
	if snap.Registries == nil {
		snap.Registries = make(map[string]*Registry)
	}
	if snap.Schemas == nil {
		snap.Schemas = make(map[string]*Schema)
	}
	if snap.SchemaVersions == nil {
		snap.SchemaVersions = make(map[string][]*SchemaVersion)
	}
	if snap.ResourcePolicies == nil {
		snap.ResourcePolicies = make(map[string]*resourcePolicyEntry)
	}
	if snap.MLTransforms == nil {
		snap.MLTransforms = make(map[string]*MLTransform)
	}
	if snap.Catalogs == nil {
		snap.Catalogs = make(map[string]*CatalogEntry)
	}
	if snap.CatalogEncryptionSettings == nil {
		snap.CatalogEncryptionSettings = make(map[string]*DataCatalogEncryptionSettings)
	}
	if snap.UsageProfiles == nil {
		snap.UsageProfiles = make(map[string]*UsageProfile)
	}
}

func initSnapshotTaskDefaults(snap *backendSnapshot) {
	if snap.BlueprintRuns == nil {
		snap.BlueprintRuns = make(map[string]*BlueprintRun)
	}
	if snap.DQRecommendationRuns == nil {
		snap.DQRecommendationRuns = make(map[string]*DQRuleRecommendationRun)
	}
	if snap.ColumnStatTaskSettings == nil {
		snap.ColumnStatTaskSettings = make(map[string]*ColumnStatisticsTaskSettings)
	}
	if snap.ColumnStatTaskRuns == nil {
		snap.ColumnStatTaskRuns = make(map[string]*ColumnStatisticsTaskRun)
	}
	if snap.MaterializedViewRuns == nil {
		snap.MaterializedViewRuns = make(map[string]*MaterializedViewRefreshRun)
	}
	if snap.Integrations == nil {
		snap.Integrations = make(map[string]*Integration)
	}
	if snap.CrawlHistory == nil {
		snap.CrawlHistory = make(map[string][]*CrawlHistoryEntry)
	}
	if snap.DQStatisticAnnotations == nil {
		snap.DQStatisticAnnotations = make(map[string]*StatisticAnnotation)
	}
}

// restoreFromSnapshot copies snapshot data into the backend (caller holds lock).
func (b *InMemoryBackend) restoreFromSnapshot(snap backendSnapshot) {
	b.databases = copyMap(snap.Databases, cloneDatabase)
	b.tables = copyMap(snap.Tables, func(t *Table) *Table {
		cp := *t

		return &cp
	})
	b.crawlers = copyMap(snap.Crawlers, cloneCrawler)
	b.jobs = copyMap(snap.Jobs, cloneJob)
	b.partitions = copyMap(snap.Partitions, func(p *Partition) *Partition {
		cp := *p
		cp.Values = append([]string(nil), p.Values...)

		return &cp
	})
	b.partitionIndexes = snap.PartitionIndexes
	b.tableVersions = copyMap(snap.TableVersions, func(tv *TableVersion) *TableVersion {
		cp := *tv

		return &cp
	})
	b.connections = copyMap(snap.Connections, cloneConnection)
	b.blueprints = maps.Clone(snap.Blueprints)
	b.customEntityTypes = copyMap(snap.CustomEntityTypes, func(c *CustomEntityType) *CustomEntityType {
		cp := *c
		cp.ContextWords = append([]string(nil), c.ContextWords...)

		return &cp
	})
	b.dataQualityResult = maps.Clone(snap.DataQualityResult)
	b.devEndpoints = maps.Clone(snap.DevEndpoints)
	b.jobRuns = copyJobRunsMap(snap.JobRuns)
	b.jobBookmarks = maps.Clone(snap.JobBookmarks)
	b.dataQualityRulesets = copyMap(snap.DataQualityRulesets, func(r *DataQualityRuleset) *DataQualityRuleset {
		cp := *r
		cp.Tags = maps.Clone(r.Tags)

		return &cp
	})
	b.dataQualityEvalRuns = copyMap(
		snap.DataQualityEvalRuns,
		func(run *DataQualityEvaluationRun) *DataQualityEvaluationRun {
			cp := *run
			cp.RulesetNames = append([]string(nil), run.RulesetNames...)

			return &cp
		},
	)
	b.restoreExtendedSnapshotState(snap)
}

func (b *InMemoryBackend) restoreExtendedSnapshotState(snap backendSnapshot) {
	b.triggers = snap.Triggers
	b.workflows = snap.Workflows
	b.workflowRuns = snap.WorkflowRuns
	b.classifiers = snap.Classifiers
	b.registries = snap.Registries
	b.schemas = snap.Schemas
	b.schemaVersions = snap.SchemaVersions
	b.udfs = snap.UDFs
	b.securityConfigs = snap.SecurityConfigs
	b.sessions = snap.Sessions
	b.sessionStatements = snap.SessionStatements
	b.tableOptimizers = snap.TableOptimizers
	b.tableColumnStats = snap.TableColumnStats
	b.partitionColumnStats = snap.PartitionColumnStats
	b.resourcePolicies = snap.ResourcePolicies
	b.mlTransforms = snap.MLTransforms
	b.catalogs = snap.Catalogs
	b.catalogEncryptionSettings = snap.CatalogEncryptionSettings
	b.usageProfiles = snap.UsageProfiles
	b.blueprintRuns = snap.BlueprintRuns
	b.dqRecommendationRuns = snap.DQRecommendationRuns
	b.columnStatTaskSettings = snap.ColumnStatTaskSettings
	b.columnStatTaskRuns = snap.ColumnStatTaskRuns
	b.materializedViewRuns = snap.MaterializedViewRuns
	b.integrations = snap.Integrations
	b.crawlHistory = snap.CrawlHistory
	b.dqStatisticAnnotations = snap.DQStatisticAnnotations
	b.glueIdentityCenterConfig = snap.GlueIdentityCenterConfig
}

// copyMap creates a deep copy of a map using the provided clone function.
func copyMap[V any](src map[string]V, clone func(V) V) map[string]V {
	dst := make(map[string]V, len(src))
	for k, v := range src {
		dst[k] = clone(v)
	}

	return dst
}

// Snapshot implements Snapshottable by delegating to the backend when it
// supports it.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements Snapshottable by delegating to the backend when it
// supports it.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Restore(ctx, data)
	}

	return nil
}

// copyJobRunsMap deep-copies the jobRuns map (map[string][]*JobRun).
func copyJobRunsMap(src map[string][]*JobRun) map[string][]*JobRun {
	dst := make(map[string][]*JobRun, len(src))
	for k, runs := range src {
		cp := make([]*JobRun, len(runs))
		for i, r := range runs {
			rc := *r
			rc.Arguments = maps.Clone(r.Arguments)
			cp[i] = &rc
		}
		dst[k] = cp
	}

	return dst
}
