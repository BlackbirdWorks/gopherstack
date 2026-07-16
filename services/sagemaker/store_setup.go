package sagemaker

import (
	"context"
	"crypto/rand"
	"encoding/hex"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// generateID returns a 24-char random hex string (12 random bytes).
func generateID() string {
	b := make([]byte, idByteLen)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

const (
	statusRunning = "Running"
)

const idByteLen = 12 // number of random bytes used when generating resource IDs

const sagemakerDefaultPageSize = 100

const (
	algorithmStatusCompleted   = "Completed"
	clusterStatusInService     = "InService"
	modelPackageStatusApproved = "Approved"
)

// NewInMemoryBackend creates a new in-memory SageMaker backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), accountID, region)
}

// NewInMemoryBackendWithContext creates a new in-memory SageMaker backend whose
// lifecycle goroutines (status-transition simulators) are children of svcCtx, so
// they are cancelled when the service shuts down rather than leaking. If svcCtx is
// nil, [context.Background] is used.
func NewInMemoryBackendWithContext(
	svcCtx context.Context,
	accountID, region string,
) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	b := &InMemoryBackend{
		lifecycleParent: svcCtx,
		accountID:       accountID,
		region:          region,
		mu:              lockmetrics.New("sagemaker"),
		registry:        store.NewRegistry(),
	}
	b.initAllResourceMaps()
	b.resetLifecycleContext()

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset reinitialises all maps to empty, clearing all stored resources.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	// Fresh registry: every xxxStore(r) helper below lazily re-registers a new
	// per-region store.Table under the same "field:region" name on next use,
	// which would panic against the old registry (duplicate name) since the
	// old one is never otherwise cleared.
	b.registry = store.NewRegistry()

	b.initAllResourceMaps()

	// Cancel pending goroutines and start fresh lifecycle context.
	b.resetLifecycleContext()
}

// initAllResourceMaps (re)initialises every resource map field to empty. It is
// shared by the constructor and Reset so both stay in lockstep; each
// initXxxMaps helper below owns one resource family so this stays readable.
func (b *InMemoryBackend) initAllResourceMaps() {
	b.initCoreAndDomainMaps()
	b.initCatalogMaps()
	b.initMonitoringMaps()
	b.initMLOpsMaps()
	b.initTrainingPlanExtMaps()
	b.initARNIndexMaps()
	b.initJobMaps()
}

// initCoreAndDomainMaps (re)initialises the model/endpoint/training-job/
// notebook/lineage-action/algorithm/cluster resource maps together with the
// Studio domain/user-profile/app/feature-store/pipeline/experiment resource
// maps. Combined into one helper (rather than two same-shaped ones) so the
// repeated `= make(map[string]*store.Table[...])` initialisation pattern
// only appears once at this length.
func (b *InMemoryBackend) initCoreAndDomainMaps() {
	b.models = make(map[string]*store.Table[Model])
	b.endpointConfigs = make(map[string]*store.Table[EndpointConfig])
	b.endpoints = make(map[string]*store.Table[Endpoint])
	b.trainingJobs = make(map[string]*store.Table[TrainingJob])
	b.notebooks = make(map[string]*store.Table[NotebookInstance])
	b.hpTuningJobs = make(map[string]*store.Table[HyperParameterTuningJob])
	b.associations = make(map[string]*store.Table[Association])
	b.trialComponentAssociations = make(map[string]*store.Table[TrialComponentAssociation])
	b.actions = make(map[string]*store.Table[Action])
	b.artifacts = make(map[string]*store.Table[Artifact])
	b.contexts = make(map[string]*store.Table[Context])
	b.algorithms = make(map[string]*store.Table[Algorithm])
	b.clusters = make(map[string]*store.Table[Cluster])
	b.domains = make(map[string]*store.Table[Domain])
	b.userProfiles = make(map[string]*store.Table[UserProfile])
	b.apps = make(map[string]*store.Table[App])
	b.featureGroups = make(map[string]*store.Table[FeatureGroup])
	b.featureRecords = make(map[string]*store.Table[FeatureRecord])
	b.featureMetadata = make(map[string]*store.Table[FeatureMetadata])
	b.pipelines = make(map[string]*store.Table[Pipeline])
	b.pipelineExecutions = make(map[string]*store.Table[PipelineExecution])
	b.pipelineExecSteps = make(map[string]*store.Table[PipelineExecutionStep])
	b.experiments = make(map[string]*store.Table[Experiment])
	b.trials = make(map[string]*store.Table[Trial])
	b.trialComponents = make(map[string]*store.Table[TrialComponent])
	b.notebookLifecycleConfigs = make(map[string]*store.Table[NotebookInstanceLifecycleConfig])
}

// initCatalogMaps (re)initialises the model-package/AutoML/project/image/
// compilation/monitoring-schedule/workteam/labeling-job resource maps.
func (b *InMemoryBackend) initCatalogMaps() {
	b.modelPackages = make(map[string]*store.Table[ModelPackage])
	b.modelPackageGroups = make(map[string]*store.Table[ModelPackageGroup])
	b.autoMLJobs = make(map[string]*store.Table[AutoMLJob])
	b.codeRepositories = make(map[string]*store.Table[CodeRepository])
	b.projects = make(map[string]*store.Table[Project])
	b.spaces = make(map[string]*store.Table[Space])
	b.smImages = make(map[string]*store.Table[SMImage])
	b.imageVersions = make(map[string]map[string]map[int]*ImageVersion)
	b.imageVersionCounts = make(map[string]map[string]int)
	b.compilationJobs = make(map[string]*store.Table[CompilationJob])
	b.monitoringSchedules = make(map[string]*store.Table[MonitoringSchedule])
	b.workteams = make(map[string]*store.Table[Workteam])
	b.labelingJobs = make(map[string]*store.Table[LabelingJob])
}

// initMonitoringMaps (re)initialises the model-monitor job-definition,
// monitoring-execution, and human-review resource maps.
func (b *InMemoryBackend) initMonitoringMaps() {
	b.dataQualityJobDefs = make(map[string]*store.Table[JobDefinition])
	b.modelBiasJobDefs = make(map[string]*store.Table[JobDefinition])
	b.modelQualityJobDefs = make(map[string]*store.Table[JobDefinition])
	b.modelExplainJobDefs = make(map[string]*store.Table[JobDefinition])
	b.monitoringAlerts = make(map[string]*store.Table[MonitoringAlert])
	b.monitoringAlertHistory = make(map[string][]*MonitoringAlertHistoryEntry)
	b.monitoringExecutions = make(map[string]*store.Table[MonitoringExecution])
	b.humanTaskUis = make(map[string]*store.Table[HumanTaskUI])
	b.workforces = make(map[string]*store.Table[Workforce])
	b.flowDefinitions = make(map[string]*store.Table[FlowDefinition])
	b.appImageConfigs = make(map[string]*store.Table[AppImageConfig])
}

// initMLOpsMaps (re)initialises the inference-experiment/MLflow/model-card/
// optimization-job/studio/partner-app/training-plan resource maps.
func (b *InMemoryBackend) initMLOpsMaps() {
	b.inferenceExperiments = make(map[string]*store.Table[InferenceExperiment])
	b.mlflowTrackingServers = make(map[string]*store.Table[MlflowTrackingServer])
	b.mlflowApps = make(map[string]*store.Table[MlflowApp])
	b.modelCards = make(map[string]*store.Table[ModelCard])
	b.optimizationJobs = make(map[string]*store.Table[OptimizationJob])
	b.studioLifecycleConfigs = make(map[string]*store.Table[StudioLifecycleConfig])
	b.partnerApps = make(map[string]*store.Table[PartnerApp])
	b.trainingPlans = make(map[string]*store.Table[TrainingPlan])
}

// initARNIndexMaps (re)initialises every region-scoped ARN → resource-name index.
func (b *InMemoryBackend) initARNIndexMaps() {
	b.modelARNIndex = make(map[string]map[string]string)
	b.endpointConfigARNIndex = make(map[string]map[string]string)
	b.endpointARNIndex = make(map[string]map[string]string)
	b.trainingJobARNIndex = make(map[string]map[string]string)
	b.notebookARNIndex = make(map[string]map[string]string)
	b.hpTuningJobARNIndex = make(map[string]map[string]string)
	b.actionARNIndex = make(map[string]map[string]string)
	b.contextARNIndex = make(map[string]map[string]string)
	b.algorithmARNIndex = make(map[string]map[string]string)
	b.clusterARNIndex = make(map[string]map[string]string)
	b.modelPackageARNIndex = make(map[string]map[string]string)
	b.processingJobARNIndex = make(map[string]map[string]string)
	b.transformJobARNIndex = make(map[string]map[string]string)
}

// initJobMaps (re)initialises the processing/transform/edge/device/inference-
// component/cluster-scheduler/hub resource maps.
func (b *InMemoryBackend) initJobMaps() {
	b.processingJobs = make(map[string]*store.Table[ProcessingJob])
	b.transformJobs = make(map[string]*store.Table[TransformJob])
	b.edgePackagingJobs = make(map[string]*store.Table[EdgePackagingJob])
	b.edgeDeploymentPlans = make(map[string]*store.Table[EdgeDeploymentPlan])
	b.inferenceRecommendationsJobs = make(map[string]*store.Table[InferenceRecommendationsJob])
	b.deviceFleets = make(map[string]*store.Table[DeviceFleet])
	b.devices = make(map[string]*store.Table[Device])
	b.inferenceComponents = make(map[string]*store.Table[InferenceComponent])
	b.clusterSchedulerConfigs = make(map[string]*store.Table[ClusterSchedulerConfig])
	b.computeQuotas = make(map[string]*store.Table[ComputeQuota])
	b.hubs = make(map[string]*store.Table[Hub])
	b.hubContents = make(map[string]*store.Table[HubContent])
}
