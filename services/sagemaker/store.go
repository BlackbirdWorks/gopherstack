package sagemaker

import (
	"context"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend is an in-memory store for SageMaker resources.
//
// All resource maps are nested by region (outer key = region) so that
// same-named resources are isolated across regions. The per-region inner maps
// are created lazily via the *Store helpers. Callers must hold b.mu while
// accessing the inner maps.
type InMemoryBackend struct {
	models                     map[string]*store.Table[Model]
	endpointConfigs            map[string]*store.Table[EndpointConfig]
	endpoints                  map[string]*store.Table[Endpoint]
	trainingJobs               map[string]*store.Table[TrainingJob]
	notebooks                  map[string]*store.Table[NotebookInstance]
	hpTuningJobs               map[string]*store.Table[HyperParameterTuningJob]
	associations               map[string]*store.Table[Association]
	trialComponentAssociations map[string]*store.Table[TrialComponentAssociation]
	actions                    map[string]*store.Table[Action]
	artifacts                  map[string]*store.Table[Artifact] // region -> ArtifactArn -> Artifact
	contexts                   map[string]*store.Table[Context]  // region -> ContextName -> Context
	algorithms                 map[string]*store.Table[Algorithm]
	clusters                   map[string]*store.Table[Cluster]
	modelPackages              map[string]*store.Table[ModelPackage]
	modelPackageGroups         map[string]*store.Table[ModelPackageGroup]
	autoMLJobs                 map[string]*store.Table[AutoMLJob]
	codeRepositories           map[string]*store.Table[CodeRepository]
	projects                   map[string]*store.Table[Project]
	spaces                     map[string]*store.Table[Space]
	smImages                   map[string]*store.Table[SMImage]
	imageVersions              map[string]map[string]map[int]*ImageVersion // region → imageName → version → ImageVersion
	imageVersionCounts         map[string]map[string]int                   // region → imageName → latest version number
	compilationJobs            map[string]*store.Table[CompilationJob]
	monitoringSchedules        map[string]*store.Table[MonitoringSchedule]
	workteams                  map[string]*store.Table[Workteam]
	labelingJobs               map[string]*store.Table[LabelingJob]
	dataQualityJobDefs         map[string]*store.Table[JobDefinition]
	modelBiasJobDefs           map[string]*store.Table[JobDefinition]
	modelQualityJobDefs        map[string]*store.Table[JobDefinition]
	modelExplainJobDefs        map[string]*store.Table[JobDefinition]
	// monitoringAlerts is region -> store.Table[MonitoringAlert], keyed by
	// monitoringAlertKey(scheduleName, alertName).
	monitoringAlerts map[string]*store.Table[MonitoringAlert]
	// monitoringAlertHistory is region -> history entries.
	monitoringAlertHistory map[string][]*MonitoringAlertHistoryEntry
	// monitoringExecutions is region -> "scheduleName|processingJobArn" -> execution.
	monitoringExecutions   map[string]*store.Table[MonitoringExecution]
	humanTaskUis           map[string]*store.Table[HumanTaskUI]
	workforces             map[string]*store.Table[Workforce]
	flowDefinitions        map[string]*store.Table[FlowDefinition]
	appImageConfigs        map[string]*store.Table[AppImageConfig]
	inferenceExperiments   map[string]*store.Table[InferenceExperiment]
	mlflowTrackingServers  map[string]*store.Table[MlflowTrackingServer]
	mlflowApps             map[string]*store.Table[MlflowApp]
	modelCards             map[string]*store.Table[ModelCard]
	optimizationJobs       map[string]*store.Table[OptimizationJob]
	studioLifecycleConfigs map[string]*store.Table[StudioLifecycleConfig]
	partnerApps            map[string]*store.Table[PartnerApp]
	trainingPlans          map[string]*store.Table[TrainingPlan]
	reservedCapacities     map[string]*store.Table[ReservedCapacity]
	// trainingPlanExtensionOfferings is region -> extensionOfferingID -> pending extension offer.
	trainingPlanExtensionOfferings map[string]*store.Table[pendingTrainingPlanExtension]
	// modelCardExportJobs is region -> ModelCardExportJobArn -> job.
	modelCardExportJobs          map[string]*store.Table[ModelCardExportJob]
	modelARNIndex                map[string]map[string]string // region → ARN → model name
	endpointConfigARNIndex       map[string]map[string]string // region → ARN → endpoint config name
	endpointARNIndex             map[string]map[string]string // region → ARN → endpoint name
	trainingJobARNIndex          map[string]map[string]string // region → ARN → training job name
	notebookARNIndex             map[string]map[string]string // region → ARN → notebook instance name
	hpTuningJobARNIndex          map[string]map[string]string // region → ARN → HP tuning job name
	actionARNIndex               map[string]map[string]string // region → ARN → action name
	contextARNIndex              map[string]map[string]string // region → ARN → context name
	algorithmARNIndex            map[string]map[string]string // region → ARN → algorithm name
	clusterARNIndex              map[string]map[string]string // region → ARN → cluster name
	modelPackageARNIndex         map[string]map[string]string // region → ARN → model package ARN
	processingJobARNIndex        map[string]map[string]string // region → ARN → job name
	transformJobARNIndex         map[string]map[string]string // region → ARN → job name
	domains                      map[string]*store.Table[Domain]
	userProfiles                 map[string]*store.Table[UserProfile]
	apps                         map[string]*store.Table[App]
	featureGroups                map[string]*store.Table[FeatureGroup]
	featureRecords               map[string]*store.Table[FeatureRecord]
	featureMetadata              map[string]*store.Table[FeatureMetadata]
	pipelines                    map[string]*store.Table[Pipeline]
	pipelineExecutions           map[string]*store.Table[PipelineExecution]
	pipelineExecSteps            map[string]*store.Table[PipelineExecutionStep]
	experiments                  map[string]*store.Table[Experiment]
	trials                       map[string]*store.Table[Trial]
	trialComponents              map[string]*store.Table[TrialComponent]
	notebookLifecycleConfigs     map[string]*store.Table[NotebookInstanceLifecycleConfig]
	processingJobs               map[string]*store.Table[ProcessingJob]
	transformJobs                map[string]*store.Table[TransformJob]
	edgePackagingJobs            map[string]*store.Table[EdgePackagingJob]
	edgeDeploymentPlans          map[string]*store.Table[EdgeDeploymentPlan]
	inferenceRecommendationsJobs map[string]*store.Table[InferenceRecommendationsJob]
	deviceFleets                 map[string]*store.Table[DeviceFleet]
	devices                      map[string]*store.Table[Device]
	inferenceComponents          map[string]*store.Table[InferenceComponent]
	clusterSchedulerConfigs      map[string]*store.Table[ClusterSchedulerConfig]
	computeQuotas                map[string]*store.Table[ComputeQuota]
	hubs                         map[string]*store.Table[Hub]
	hubContents                  map[string]*store.Table[HubContent]
	aiBenchmarkJobs              map[string]*store.Table[AIBenchmarkJob]
	aiRecommendationJobs         map[string]*store.Table[AIRecommendationJob]
	aiWorkloadConfigs            map[string]*store.Table[AIWorkloadConfig]
	aiWorkloadConfigARNIndex     map[string]map[string]string // region → ARN → AI workload config name
	jobs                         map[string]*store.Table[Job]
	// pipelineVersions is region -> pipelineName -> versions, ordered oldest-first.
	pipelineVersions map[string]map[string][]*PipelineVersion
	// servicecatalogPortfolioEnabled is region -> whether the SageMaker
	// Service Catalog portfolio has been enabled via
	// EnableSagemakerServicecatalogPortfolio. Absent/false means Disabled.
	servicecatalogPortfolioEnabled map[string]bool
	// registry lets Reset collapse the per-region store.Table lifecycle for
	// every resource collection below to one registry.ResetAll() call, and
	// backs Snapshot/Restore via registry.SnapshotAll()/RestoreAll(). Each
	// resource field above is itself a map[string]*store.Table[T] keyed by
	// region (one store.Table per region, registered lazily on first use of
	// that region) rather than a single flat store.Table[T], because these
	// resources are natively region-partitioned collections; see the
	// per-resource Store(r) helpers below (modelsStore, endpointsStore and
	// friends) for the lazy-create-and-register point.
	registry        *store.Registry
	lifecycleParent context.Context
	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
	mu              *lockmetrics.RWMutex
	accountID       string
	region          string
	wg              sync.WaitGroup
}

// ---------------------------------------------------------------------------
// Per-region store helpers — lazy inner-map initialisation.
// Callers must hold b.mu.
// ---------------------------------------------------------------------------

func (b *InMemoryBackend) modelsStore(r string) *store.Table[Model] {
	if b.models[r] == nil {
		b.models[r] = store.Register(b.registry, "models:"+r, store.New(func(v *Model) string { return v.ModelName }))
	}

	return b.models[r]
}

// modelsStoreRO returns the region-scoped models table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelsStoreRO(r string) *store.Table[Model] {
	if v := b.models[r]; v != nil {
		return v
	}

	return store.New(func(v *Model) string { return v.ModelName })
}

func (b *InMemoryBackend) endpointConfigsStore(r string) *store.Table[EndpointConfig] {
	if b.endpointConfigs[r] == nil {
		b.endpointConfigs[r] = store.Register(
			b.registry,
			"endpointConfigs:"+r,
			store.New(func(v *EndpointConfig) string { return v.EndpointConfigName }),
		)
	}

	return b.endpointConfigs[r]
}

// endpointConfigsStoreRO returns the region-scoped endpointConfigs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) endpointConfigsStoreRO(r string) *store.Table[EndpointConfig] {
	if v := b.endpointConfigs[r]; v != nil {
		return v
	}

	return store.New(func(v *EndpointConfig) string { return v.EndpointConfigName })
}

func (b *InMemoryBackend) endpointsStore(r string) *store.Table[Endpoint] {
	if b.endpoints[r] == nil {
		b.endpoints[r] = store.Register(
			b.registry,
			"endpoints:"+r,
			store.New(func(v *Endpoint) string { return v.EndpointName }),
		)
	}

	return b.endpoints[r]
}

// endpointsStoreRO returns the region-scoped endpoints table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) endpointsStoreRO(r string) *store.Table[Endpoint] {
	if v := b.endpoints[r]; v != nil {
		return v
	}

	return store.New(func(v *Endpoint) string { return v.EndpointName })
}

func (b *InMemoryBackend) trainingJobsStore(r string) *store.Table[TrainingJob] {
	if b.trainingJobs[r] == nil {
		b.trainingJobs[r] = store.Register(
			b.registry,
			"trainingJobs:"+r,
			store.New(func(v *TrainingJob) string { return v.TrainingJobName }),
		)
	}

	return b.trainingJobs[r]
}

// trainingJobsStoreRO returns the region-scoped trainingJobs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) trainingJobsStoreRO(r string) *store.Table[TrainingJob] {
	if v := b.trainingJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *TrainingJob) string { return v.TrainingJobName })
}

func (b *InMemoryBackend) notebooksStore(r string) *store.Table[NotebookInstance] {
	if b.notebooks[r] == nil {
		b.notebooks[r] = store.Register(
			b.registry,
			"notebooks:"+r,
			store.New(func(v *NotebookInstance) string { return v.NotebookInstanceName }),
		)
	}

	return b.notebooks[r]
}

// notebooksStoreRO returns the region-scoped notebooks table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) notebooksStoreRO(r string) *store.Table[NotebookInstance] {
	if v := b.notebooks[r]; v != nil {
		return v
	}

	return store.New(func(v *NotebookInstance) string { return v.NotebookInstanceName })
}

func (b *InMemoryBackend) hpTuningJobsStore(r string) *store.Table[HyperParameterTuningJob] {
	if b.hpTuningJobs[r] == nil {
		b.hpTuningJobs[r] = store.Register(
			b.registry,
			"hpTuningJobs:"+r,
			store.New(func(v *HyperParameterTuningJob) string { return v.HyperParameterTuningJobName }),
		)
	}

	return b.hpTuningJobs[r]
}

// hpTuningJobsStoreRO returns the region-scoped hpTuningJobs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) hpTuningJobsStoreRO(r string) *store.Table[HyperParameterTuningJob] {
	if v := b.hpTuningJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *HyperParameterTuningJob) string { return v.HyperParameterTuningJobName })
}

func (b *InMemoryBackend) associationsStore(r string) *store.Table[Association] {
	if b.associations[r] == nil {
		b.associations[r] = store.Register(
			b.registry,
			"associations:"+r,
			store.New(func(v *Association) string { return associationKey(v.SourceArn, v.DestinationArn) }),
		)
	}

	return b.associations[r]
}

// associationsStoreRO returns the region-scoped associations table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) associationsStoreRO(r string) *store.Table[Association] {
	if v := b.associations[r]; v != nil {
		return v
	}

	return store.New(func(v *Association) string { return associationKey(v.SourceArn, v.DestinationArn) })
}

func (b *InMemoryBackend) trialComponentAssociationsStore(r string) *store.Table[TrialComponentAssociation] {
	if b.trialComponentAssociations[r] == nil {
		b.trialComponentAssociations[r] = store.Register(
			b.registry,
			"trialComponentAssociations:"+r,
			store.New(
				func(v *TrialComponentAssociation) string { return trialComponentKey(v.TrialName, v.TrialComponentName) },
			),
		)
	}

	return b.trialComponentAssociations[r]
}

// trialComponentAssociationsStoreRO returns the region-scoped trialComponentAssociations table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) trialComponentAssociationsStoreRO(r string) *store.Table[TrialComponentAssociation] {
	if v := b.trialComponentAssociations[r]; v != nil {
		return v
	}

	return store.New(
		func(v *TrialComponentAssociation) string { return trialComponentKey(v.TrialName, v.TrialComponentName) },
	)
}

func (b *InMemoryBackend) actionsStore(r string) *store.Table[Action] {
	if b.actions[r] == nil {
		b.actions[r] = store.Register(
			b.registry,
			"actions:"+r,
			store.New(func(v *Action) string { return v.ActionName }),
		)
	}

	return b.actions[r]
}

// actionsStoreRO returns the region-scoped actions table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) actionsStoreRO(r string) *store.Table[Action] {
	if v := b.actions[r]; v != nil {
		return v
	}

	return store.New(func(v *Action) string { return v.ActionName })
}

func (b *InMemoryBackend) algorithmsStore(r string) *store.Table[Algorithm] {
	if b.algorithms[r] == nil {
		b.algorithms[r] = store.Register(
			b.registry,
			"algorithms:"+r,
			store.New(func(v *Algorithm) string { return v.AlgorithmName }),
		)
	}

	return b.algorithms[r]
}

// algorithmsStoreRO returns the region-scoped algorithms table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) algorithmsStoreRO(r string) *store.Table[Algorithm] {
	if v := b.algorithms[r]; v != nil {
		return v
	}

	return store.New(func(v *Algorithm) string { return v.AlgorithmName })
}

func (b *InMemoryBackend) clustersStore(r string) *store.Table[Cluster] {
	if b.clusters[r] == nil {
		b.clusters[r] = store.Register(
			b.registry,
			"clusters:"+r,
			store.New(func(v *Cluster) string { return v.ClusterName }),
		)
	}

	return b.clusters[r]
}

// clustersStoreRO returns the region-scoped clusters table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) clustersStoreRO(r string) *store.Table[Cluster] {
	if v := b.clusters[r]; v != nil {
		return v
	}

	return store.New(func(v *Cluster) string { return v.ClusterName })
}

func (b *InMemoryBackend) modelPackagesStore(r string) *store.Table[ModelPackage] {
	if b.modelPackages[r] == nil {
		b.modelPackages[r] = store.Register(
			b.registry,
			"modelPackages:"+r,
			store.New(func(v *ModelPackage) string { return v.ModelPackageArn }),
		)
	}

	return b.modelPackages[r]
}

// modelPackagesStoreRO returns the region-scoped modelPackages table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelPackagesStoreRO(r string) *store.Table[ModelPackage] {
	if v := b.modelPackages[r]; v != nil {
		return v
	}

	return store.New(func(v *ModelPackage) string { return v.ModelPackageArn })
}
func (b *InMemoryBackend) modelPackageGroupsStore(r string) *store.Table[ModelPackageGroup] {
	if b.modelPackageGroups[r] == nil {
		b.modelPackageGroups[r] = store.Register(
			b.registry,
			"modelPackageGroups:"+r,
			store.New(func(v *ModelPackageGroup) string { return v.ModelPackageGroupName }),
		)
	}

	return b.modelPackageGroups[r]
}

// modelPackageGroupsStoreRO returns the region-scoped modelPackageGroups table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelPackageGroupsStoreRO(r string) *store.Table[ModelPackageGroup] {
	if v := b.modelPackageGroups[r]; v != nil {
		return v
	}

	return store.New(func(v *ModelPackageGroup) string { return v.ModelPackageGroupName })
}

func (b *InMemoryBackend) autoMLJobsStore(r string) *store.Table[AutoMLJob] {
	if b.autoMLJobs[r] == nil {
		b.autoMLJobs[r] = store.Register(
			b.registry,
			"autoMLJobs:"+r,
			store.New(func(v *AutoMLJob) string { return v.AutoMLJobName }),
		)
	}

	return b.autoMLJobs[r]
}

// autoMLJobsStoreRO returns the region-scoped autoMLJobs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) autoMLJobsStoreRO(r string) *store.Table[AutoMLJob] {
	if v := b.autoMLJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *AutoMLJob) string { return v.AutoMLJobName })
}

func (b *InMemoryBackend) codeRepositoriesStore(r string) *store.Table[CodeRepository] {
	if b.codeRepositories[r] == nil {
		b.codeRepositories[r] = store.Register(
			b.registry,
			"codeRepositories:"+r,
			store.New(func(v *CodeRepository) string { return v.CodeRepositoryName }),
		)
	}

	return b.codeRepositories[r]
}

// codeRepositoriesStoreRO returns the region-scoped codeRepositories table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) codeRepositoriesStoreRO(r string) *store.Table[CodeRepository] {
	if v := b.codeRepositories[r]; v != nil {
		return v
	}

	return store.New(func(v *CodeRepository) string { return v.CodeRepositoryName })
}

func (b *InMemoryBackend) projectsStore(r string) *store.Table[Project] {
	if b.projects[r] == nil {
		b.projects[r] = store.Register(
			b.registry,
			"projects:"+r,
			store.New(func(v *Project) string { return v.ProjectName }),
		)
	}

	return b.projects[r]
}

// projectsStoreRO returns the region-scoped projects table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) projectsStoreRO(r string) *store.Table[Project] {
	if v := b.projects[r]; v != nil {
		return v
	}

	return store.New(func(v *Project) string { return v.ProjectName })
}

func (b *InMemoryBackend) spacesStore(r string) *store.Table[Space] {
	if b.spaces[r] == nil {
		b.spaces[r] = store.Register(b.registry, "spaces:"+r, store.New(func(v *Space) string {
			return spaceKey(v.DomainID, v.SpaceName)
		}))
	}

	return b.spaces[r]
}

// spacesStoreRO returns the region-scoped spaces table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) spacesStoreRO(r string) *store.Table[Space] {
	if v := b.spaces[r]; v != nil {
		return v
	}

	return store.New(func(v *Space) string {
		return spaceKey(v.DomainID, v.SpaceName)
	})
}

func (b *InMemoryBackend) smImagesStore(r string) *store.Table[SMImage] {
	if b.smImages[r] == nil {
		b.smImages[r] = store.Register(
			b.registry,
			"smImages:"+r,
			store.New(func(v *SMImage) string { return v.ImageName }),
		)
	}

	return b.smImages[r]
}

// smImagesStoreRO returns the region-scoped smImages table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) smImagesStoreRO(r string) *store.Table[SMImage] {
	if v := b.smImages[r]; v != nil {
		return v
	}

	return store.New(func(v *SMImage) string { return v.ImageName })
}

func (b *InMemoryBackend) imageVersionsStore(r string) map[string]map[int]*ImageVersion {
	if b.imageVersions[r] == nil {
		b.imageVersions[r] = make(map[string]map[int]*ImageVersion)
	}

	return b.imageVersions[r]
}

// imageVersionsStoreRO returns the region-scoped imageVersions table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) imageVersionsStoreRO(r string) map[string]map[int]*ImageVersion {
	if v := b.imageVersions[r]; v != nil {
		return v
	}

	return make(map[string]map[int]*ImageVersion)
}

func (b *InMemoryBackend) imageVersionCountsStore(r string) map[string]int {
	if b.imageVersionCounts[r] == nil {
		b.imageVersionCounts[r] = make(map[string]int)
	}

	return b.imageVersionCounts[r]
}

func (b *InMemoryBackend) compilationJobsStore(r string) *store.Table[CompilationJob] {
	if b.compilationJobs[r] == nil {
		b.compilationJobs[r] = store.Register(
			b.registry,
			"compilationJobs:"+r,
			store.New(func(v *CompilationJob) string { return v.CompilationJobName }),
		)
	}

	return b.compilationJobs[r]
}

// compilationJobsStoreRO returns the region-scoped compilationJobs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) compilationJobsStoreRO(r string) *store.Table[CompilationJob] {
	if v := b.compilationJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *CompilationJob) string { return v.CompilationJobName })
}

func (b *InMemoryBackend) monitoringSchedulesStore(r string) *store.Table[MonitoringSchedule] {
	if b.monitoringSchedules[r] == nil {
		b.monitoringSchedules[r] = store.Register(
			b.registry,
			"monitoringSchedules:"+r,
			store.New(func(v *MonitoringSchedule) string { return v.MonitoringScheduleName }),
		)
	}

	return b.monitoringSchedules[r]
}

// monitoringSchedulesStoreRO returns the region-scoped monitoringSchedules table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) monitoringSchedulesStoreRO(r string) *store.Table[MonitoringSchedule] {
	if v := b.monitoringSchedules[r]; v != nil {
		return v
	}

	return store.New(func(v *MonitoringSchedule) string { return v.MonitoringScheduleName })
}

func (b *InMemoryBackend) workteamsStore(r string) *store.Table[Workteam] {
	if b.workteams[r] == nil {
		b.workteams[r] = store.Register(
			b.registry,
			"workteams:"+r,
			store.New(func(v *Workteam) string { return v.WorkteamName }),
		)
	}

	return b.workteams[r]
}

// workteamsStoreRO returns the region-scoped workteams table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) workteamsStoreRO(r string) *store.Table[Workteam] {
	if v := b.workteams[r]; v != nil {
		return v
	}

	return store.New(func(v *Workteam) string { return v.WorkteamName })
}
