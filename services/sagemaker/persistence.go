package sagemaker

import (
	"encoding/json"
	"log/slog"
	"time"
)

// persistedCluster is a serialisable version of Cluster that includes Nodes.
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

// backendSnapshot holds the serialisable state of InMemoryBackend.
// All resource maps are nested by region (outer key = region).
type backendSnapshot struct {
	Models                     map[string]map[string]*Model                     `json:"models"`
	EndpointConfigs            map[string]map[string]*EndpointConfig            `json:"endpointConfigs"`
	Endpoints                  map[string]map[string]*Endpoint                  `json:"endpoints"`
	TrainingJobs               map[string]map[string]*TrainingJob               `json:"trainingJobs"`
	Notebooks                  map[string]map[string]*NotebookInstance          `json:"notebooks"`
	HPTuningJobs               map[string]map[string]*HyperParameterTuningJob   `json:"hpTuningJobs"`
	Associations               map[string]map[string]*Association               `json:"associations"`
	TrialComponentAssociations map[string]map[string]*TrialComponentAssociation `json:"trialComponentAssociations"`
	Actions                    map[string]map[string]*Action                    `json:"actions"`
	Artifacts                  map[string]map[string]*Artifact                  `json:"artifacts"`
	Contexts                   map[string]map[string]*Context                   `json:"contexts"`
	Algorithms                 map[string]map[string]*Algorithm                 `json:"algorithms"`
	Clusters                   map[string]map[string]*persistedCluster          `json:"clusters"`
	ModelPackages              map[string]map[string]*ModelPackage              `json:"modelPackages"`
	Domains                    map[string]map[string]*Domain                    `json:"domains"`
	// UserProfiles is stored as region → "domainID|profileName" → UserProfile.
	UserProfiles map[string]map[string]*UserProfile `json:"userProfiles"`
	// Apps is stored as region → "domainID|userProfileName|appType|appName" → App.
	Apps                     map[string]map[string]*App                             `json:"apps"`
	FeatureGroups            map[string]map[string]*FeatureGroup                    `json:"featureGroups"`
	Pipelines                map[string]map[string]*Pipeline                        `json:"pipelines"`
	PipelineExecutions       map[string]map[string]*PipelineExecution               `json:"pipelineExecutions"`
	PipelineExecSteps        map[string]map[string]*PipelineExecutionStep           `json:"pipelineExecSteps"`
	Experiments              map[string]map[string]*Experiment                      `json:"experiments"`
	Trials                   map[string]map[string]*Trial                           `json:"trials"`
	TrialComponents          map[string]map[string]*TrialComponent                  `json:"trialComponents"`
	NotebookLifecycleConfigs map[string]map[string]*NotebookInstanceLifecycleConfig `json:"notebookLifecycleConfigs"`
	ProcessingJobs           map[string]map[string]*ProcessingJob                   `json:"processingJobs"`
	TransformJobs            map[string]map[string]*TransformJob                    `json:"transformJobs"`
	EdgeDeploymentPlans      map[string]map[string]*EdgeDeploymentPlan              `json:"edgeDeploymentPlans"`
	FeatureRecords           map[string]map[string]*FeatureRecord                   `json:"featureRecords"`
	FeatureMetadata          map[string]map[string]*FeatureMetadata                 `json:"featureMetadata"`
	Hubs                     map[string]map[string]*Hub                             `json:"hubs"`
	// HubContents is stored as region → "hubName|contentType|contentName|contentVersion" → HubContent.
	HubContents map[string]map[string]*HubContent `json:"hubContents"`
	// Model Monitor job definitions (Create/Describe/Delete/List*JobDefinitions).
	DataQualityJobDefs  map[string]map[string]*JobDefinition `json:"dataQualityJobDefs"`
	ModelBiasJobDefs    map[string]map[string]*JobDefinition `json:"modelBiasJobDefs"`
	ModelQualityJobDefs map[string]map[string]*JobDefinition `json:"modelQualityJobDefs"`
	ModelExplainJobDefs map[string]map[string]*JobDefinition `json:"modelExplainJobDefs"`
	// MonitoringAlerts is stored as region → monitoringScheduleName → alertName → MonitoringAlert.
	MonitoringAlerts map[string]map[string]map[string]*MonitoringAlert `json:"monitoringAlerts"`
	// MonitoringAlertHistory is stored as region → history entries (across all schedules/alerts).
	MonitoringAlertHistory map[string][]*MonitoringAlertHistoryEntry `json:"monitoringAlertHistory"`
	// MonitoringExecutions is stored as region → "scheduleName|processingJobArn" → MonitoringExecution.
	MonitoringExecutions  map[string]map[string]*MonitoringExecution  `json:"monitoringExecutions"`
	Workteams             map[string]map[string]*Workteam             `json:"workteams"`
	Workforces            map[string]map[string]*Workforce            `json:"workforces"`
	LabelingJobs          map[string]map[string]*LabelingJob          `json:"labelingJobs"`
	MlflowTrackingServers map[string]map[string]*MlflowTrackingServer `json:"mlflowTrackingServers"`
	MlflowApps            map[string]map[string]*MlflowApp            `json:"mlflowApps"`
	PartnerApps           map[string]map[string]*PartnerApp           `json:"partnerApps"`
	AccountID             string                                      `json:"accountID"`
	Region                string                                      `json:"region"`
}

// snapshotClusters converts map[string]map[string]*Cluster →
// map[string]map[string]*persistedCluster.
func snapshotClusters(b *InMemoryBackend) map[string]map[string]*persistedCluster {
	clusters := make(map[string]map[string]*persistedCluster, len(b.clusters))

	for region, regionClusters := range b.clusters {
		clusters[region] = make(map[string]*persistedCluster, len(regionClusters))
		for k, c := range regionClusters {
			pc := &persistedCluster{
				CreationTime:   c.CreationTime.Format("2006-01-02T15:04:05Z07:00"),
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

			clusters[region][k] = pc
		}
	}

	return clusters
}

// snapshotUserProfiles converts map[string]map[userProfileKey]*UserProfile →
// map[string]map[string]*UserProfile (inner key = "domainID|profileName").
func snapshotUserProfiles(b *InMemoryBackend) map[string]map[string]*UserProfile {
	userProfiles := make(map[string]map[string]*UserProfile, len(b.userProfiles))

	for region, regionProfiles := range b.userProfiles {
		userProfiles[region] = make(map[string]*UserProfile, len(regionProfiles))
		for k, v := range regionProfiles {
			cp := *v
			userProfiles[region][k.DomainID+"|"+k.UserProfileName] = &cp
		}
	}

	return userProfiles
}

// snapshotApps converts map[string]map[appKey]*App → map[string]map[string]*App
// (inner key = "domainID|userProfileName|appType|appName").
func snapshotApps(b *InMemoryBackend) map[string]map[string]*App {
	apps := make(map[string]map[string]*App, len(b.apps))

	for region, regionApps := range b.apps {
		apps[region] = make(map[string]*App, len(regionApps))
		for k, v := range regionApps {
			cp := *v
			apps[region][k.DomainID+"|"+k.UserProfileName+"|"+k.AppType+"|"+k.AppName] = &cp
		}
	}

	return apps
}

// snapshotHubContents converts map[string]map[hubContentKey]*HubContent →
// map[string]map[string]*HubContent (inner key =
// "hubName|contentType|contentName|contentVersion").
func snapshotHubContents(b *InMemoryBackend) map[string]map[string]*HubContent {
	hubContents := make(map[string]map[string]*HubContent, len(b.hubContents))

	for region, regionContents := range b.hubContents {
		hubContents[region] = make(map[string]*HubContent, len(regionContents))
		for k, v := range regionContents {
			cp := *v
			hubContents[region][hubContentKeyString(k)] = &cp
		}
	}

	return hubContents
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	clusters := snapshotClusters(b)
	userProfiles := snapshotUserProfiles(b)
	apps := snapshotApps(b)
	hubContents := snapshotHubContents(b)

	snap := backendSnapshot{
		Models:                     b.models,
		EndpointConfigs:            b.endpointConfigs,
		Endpoints:                  b.endpoints,
		TrainingJobs:               b.trainingJobs,
		Notebooks:                  b.notebooks,
		HPTuningJobs:               b.hpTuningJobs,
		Associations:               b.associations,
		TrialComponentAssociations: b.trialComponentAssociations,
		Actions:                    b.actions,
		Artifacts:                  b.artifacts,
		Contexts:                   b.contexts,
		Algorithms:                 b.algorithms,
		Clusters:                   clusters,
		ModelPackages:              b.modelPackages,
		Domains:                    b.domains,
		UserProfiles:               userProfiles,
		Apps:                       apps,
		FeatureGroups:              b.featureGroups,
		Pipelines:                  b.pipelines,
		PipelineExecutions:         b.pipelineExecutions,
		PipelineExecSteps:          b.pipelineExecSteps,
		Experiments:                b.experiments,
		Trials:                     b.trials,
		TrialComponents:            b.trialComponents,
		NotebookLifecycleConfigs:   b.notebookLifecycleConfigs,
		ProcessingJobs:             b.processingJobs,
		TransformJobs:              b.transformJobs,
		EdgeDeploymentPlans:        b.edgeDeploymentPlans,
		FeatureRecords:             b.featureRecords,
		FeatureMetadata:            b.featureMetadata,
		Hubs:                       b.hubs,
		HubContents:                hubContents,
		DataQualityJobDefs:         b.dataQualityJobDefs,
		ModelBiasJobDefs:           b.modelBiasJobDefs,
		ModelQualityJobDefs:        b.modelQualityJobDefs,
		ModelExplainJobDefs:        b.modelExplainJobDefs,
		MonitoringAlerts:           b.monitoringAlerts,
		MonitoringAlertHistory:     b.monitoringAlertHistory,
		MonitoringExecutions:       b.monitoringExecutions,
		Workteams:                  b.workteams,
		Workforces:                 b.workforces,
		LabelingJobs:               b.labelingJobs,
		MlflowTrackingServers:      b.mlflowTrackingServers,
		MlflowApps:                 b.mlflowApps,
		PartnerApps:                b.partnerApps,
		AccountID:                  b.accountID,
		Region:                     b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("sagemaker: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)
	fixNilTagMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.restoreFields(&snap)
	b.resetLifecycleContext() // cancel pending goroutines, create fresh context
	b.rebuildARNIndexes()

	return nil
}

// restoreFields assigns deserialized maps to backend fields (called with lock held).
func restoreUserProfiles(snap *backendSnapshot) map[string]map[userProfileKey]*UserProfile {
	result := make(map[string]map[userProfileKey]*UserProfile, len(snap.UserProfiles))
	for region, regionProfiles := range snap.UserProfiles {
		result[region] = make(map[userProfileKey]*UserProfile, len(regionProfiles))
		for _, v := range regionProfiles {
			key := userProfileKey{DomainID: v.DomainID, UserProfileName: v.UserProfileName}
			cp := *v
			result[region][key] = &cp
		}
	}

	return result
}

func restoreApps(snap *backendSnapshot) map[string]map[appKey]*App {
	result := make(map[string]map[appKey]*App, len(snap.Apps))
	for region, regionApps := range snap.Apps {
		result[region] = make(map[appKey]*App, len(regionApps))
		for _, v := range regionApps {
			key := appKey{
				DomainID:        v.DomainID,
				UserProfileName: v.UserProfileName,
				AppType:         v.AppType,
				AppName:         v.AppName,
			}
			cp := *v
			result[region][key] = &cp
		}
	}

	return result
}

// hubContentKeyString serialises a hubContentKey to a single delimited string
// for use as a JSON map key.
func hubContentKeyString(k hubContentKey) string {
	return k.HubName + "|" + k.HubContentType + "|" + k.HubContentName + "|" + k.HubContentVersion
}

func restoreHubContents(snap *backendSnapshot) map[string]map[hubContentKey]*HubContent {
	result := make(map[string]map[hubContentKey]*HubContent, len(snap.HubContents))
	for region, regionContents := range snap.HubContents {
		result[region] = make(map[hubContentKey]*HubContent, len(regionContents))
		for _, v := range regionContents {
			key := hubContentKey{
				HubName: v.HubName, HubContentType: v.HubContentType,
				HubContentName: v.HubContentName, HubContentVersion: v.HubContentVersion,
			}
			cp := *v
			result[region][key] = &cp
		}
	}

	return result
}

func restoreClusters(snap *backendSnapshot) map[string]map[string]*Cluster {
	result := make(map[string]map[string]*Cluster, len(snap.Clusters))
	for region, regionClusters := range snap.Clusters {
		result[region] = make(map[string]*Cluster, len(regionClusters))
		for k, pc := range regionClusters {
			t, err := time.Parse("2006-01-02T15:04:05Z07:00", pc.CreationTime)
			if err != nil {
				slog.Default().Warn("sagemaker: failed to parse cluster creation time", "cluster", k, "error", err)
			}
			c := &Cluster{
				ClusterArn:     pc.ClusterArn,
				ClusterName:    pc.ClusterName,
				ClusterStatus:  pc.ClusterStatus,
				NodeRecovery:   pc.NodeRecovery,
				Tags:           ensureSageTagMap(pc.Tags),
				InstanceGroups: pc.InstanceGroups,
				CreationTime:   t,
				Nodes:          make(map[string]*ClusterNode, len(pc.Nodes)),
			}
			for nk, nv := range pc.Nodes {
				nodeCopy := *nv
				c.Nodes[nk] = &nodeCopy
			}
			result[region][k] = c
		}
	}

	return result
}

func (b *InMemoryBackend) restoreFields(snap *backendSnapshot) {
	b.models = snap.Models
	b.endpointConfigs = snap.EndpointConfigs
	b.endpoints = snap.Endpoints
	b.trainingJobs = snap.TrainingJobs
	b.notebooks = snap.Notebooks
	b.hpTuningJobs = snap.HPTuningJobs
	b.associations = snap.Associations
	b.trialComponentAssociations = snap.TrialComponentAssociations
	b.actions = snap.Actions
	b.artifacts = snap.Artifacts
	b.contexts = snap.Contexts
	b.algorithms = snap.Algorithms
	b.modelPackages = snap.ModelPackages
	b.domains = snap.Domains
	b.featureGroups = snap.FeatureGroups
	b.pipelines = snap.Pipelines
	b.pipelineExecutions = snap.PipelineExecutions
	b.pipelineExecSteps = snap.PipelineExecSteps
	b.experiments = snap.Experiments
	b.trials = snap.Trials
	b.trialComponents = snap.TrialComponents
	b.notebookLifecycleConfigs = snap.NotebookLifecycleConfigs
	b.processingJobs = snap.ProcessingJobs
	b.transformJobs = snap.TransformJobs
	b.edgeDeploymentPlans = snap.EdgeDeploymentPlans
	b.featureRecords = snap.FeatureRecords
	b.featureMetadata = snap.FeatureMetadata
	b.hubs = snap.Hubs
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.userProfiles = restoreUserProfiles(snap)
	b.apps = restoreApps(snap)
	b.clusters = restoreClusters(snap)
	b.hubContents = restoreHubContents(snap)
	b.dataQualityJobDefs = snap.DataQualityJobDefs
	b.modelBiasJobDefs = snap.ModelBiasJobDefs
	b.modelQualityJobDefs = snap.ModelQualityJobDefs
	b.modelExplainJobDefs = snap.ModelExplainJobDefs
	b.monitoringAlerts = snap.MonitoringAlerts
	b.monitoringAlertHistory = snap.MonitoringAlertHistory
	b.monitoringExecutions = snap.MonitoringExecutions
	b.workteams = snap.Workteams
	b.workforces = snap.Workforces
	b.labelingJobs = snap.LabelingJobs
	b.mlflowTrackingServers = snap.MlflowTrackingServers
	b.mlflowApps = snap.MlflowApps
	b.partnerApps = snap.PartnerApps
}

func buildARNIndex[V any](src map[string]map[string]V, arnFn func(string, V) string) map[string]map[string]string {
	idx := make(map[string]map[string]string, len(src))
	for region, regionItems := range src {
		regionIdx := make(map[string]string, len(regionItems))
		for name, item := range regionItems {
			regionIdx[arnFn(name, item)] = name
		}
		idx[region] = regionIdx
	}

	return idx
}

func fixNestedTagsSage[V any](nested map[string]map[string]V, fix func(V)) {
	for _, region := range nested {
		for _, item := range region {
			fix(item)
		}
	}
}

func ensureSageTagMap(m map[string]string) map[string]string {
	if m == nil {
		return make(map[string]string)
	}

	return m
}

// rebuildARNIndexes reconstructs all ARN-to-name indexes after a restore (called with lock held).
func (b *InMemoryBackend) rebuildARNIndexes() {
	b.modelARNIndex = buildARNIndex(b.models, func(_ string, m *Model) string { return m.ModelARN })
	b.endpointConfigARNIndex = buildARNIndex(
		b.endpointConfigs,
		func(_ string, ec *EndpointConfig) string { return ec.EndpointConfigARN },
	)
	b.actionARNIndex = buildARNIndex(b.actions, func(_ string, a *Action) string { return a.ActionArn })
	b.contextARNIndex = buildARNIndex(b.contexts, func(_ string, c *Context) string { return c.ContextArn })
	b.algorithmARNIndex = buildARNIndex(b.algorithms, func(_ string, al *Algorithm) string { return al.AlgorithmArn })
	b.clusterARNIndex = buildARNIndex(b.clusters, func(_ string, c *Cluster) string { return c.ClusterArn })
	b.modelPackageARNIndex = buildARNIndex(b.modelPackages, func(name string, _ *ModelPackage) string { return name })
	b.endpointARNIndex = buildARNIndex(b.endpoints, func(_ string, ep *Endpoint) string { return ep.EndpointArn })
	b.trainingJobARNIndex = buildARNIndex(
		b.trainingJobs,
		func(_ string, tj *TrainingJob) string { return tj.TrainingJobArn },
	)
	b.notebookARNIndex = buildARNIndex(
		b.notebooks,
		func(_ string, nb *NotebookInstance) string { return nb.NotebookInstanceArn },
	)
	b.hpTuningJobARNIndex = buildARNIndex(
		b.hpTuningJobs,
		func(_ string, j *HyperParameterTuningJob) string { return j.HyperParameterTuningJobArn },
	)
	b.processingJobARNIndex = buildARNIndex(
		b.processingJobs,
		func(_ string, pj *ProcessingJob) string { return pj.ProcessingJobArn },
	)
	b.transformJobARNIndex = buildARNIndex(
		b.transformJobs,
		func(_ string, tj *TransformJob) string { return tj.TransformJobArn },
	)
}

func ensureNonNilMaps(snap *backendSnapshot) {
	ensureCoreResourceMaps(snap)
	ensureJobMaps(snap)
	ensureConfigMaps(snap)
	ensureMetadataMaps(snap)
	ensureLineageMaps(snap)
	ensureHubMaps(snap)
	ensureMonitorMaps(snap)
}

// ensureMonitorMaps initialises the Model Monitor job definition and
// alert/execution maps if a snapshot predates their introduction.
func ensureMonitorMaps(snap *backendSnapshot) {
	if snap.DataQualityJobDefs == nil {
		snap.DataQualityJobDefs = make(map[string]map[string]*JobDefinition)
	}
	if snap.ModelBiasJobDefs == nil {
		snap.ModelBiasJobDefs = make(map[string]map[string]*JobDefinition)
	}
	if snap.ModelQualityJobDefs == nil {
		snap.ModelQualityJobDefs = make(map[string]map[string]*JobDefinition)
	}
	if snap.ModelExplainJobDefs == nil {
		snap.ModelExplainJobDefs = make(map[string]map[string]*JobDefinition)
	}
	if snap.MonitoringAlerts == nil {
		snap.MonitoringAlerts = make(map[string]map[string]map[string]*MonitoringAlert)
	}
	if snap.MonitoringAlertHistory == nil {
		snap.MonitoringAlertHistory = make(map[string][]*MonitoringAlertHistoryEntry)
	}
	if snap.MonitoringExecutions == nil {
		snap.MonitoringExecutions = make(map[string]map[string]*MonitoringExecution)
	}
	if snap.Workteams == nil {
		snap.Workteams = make(map[string]map[string]*Workteam)
	}
	if snap.Workforces == nil {
		snap.Workforces = make(map[string]map[string]*Workforce)
	}
	if snap.LabelingJobs == nil {
		snap.LabelingJobs = make(map[string]map[string]*LabelingJob)
	}
	if snap.MlflowTrackingServers == nil {
		snap.MlflowTrackingServers = make(map[string]map[string]*MlflowTrackingServer)
	}
	if snap.MlflowApps == nil {
		snap.MlflowApps = make(map[string]map[string]*MlflowApp)
	}
	if snap.PartnerApps == nil {
		snap.PartnerApps = make(map[string]map[string]*PartnerApp)
	}
}

func ensureHubMaps(snap *backendSnapshot) {
	if snap.Hubs == nil {
		snap.Hubs = make(map[string]map[string]*Hub)
	}

	if snap.HubContents == nil {
		snap.HubContents = make(map[string]map[string]*HubContent)
	}
}

func ensureLineageMaps(snap *backendSnapshot) {
	if snap.Artifacts == nil {
		snap.Artifacts = make(map[string]map[string]*Artifact)
	}
	if snap.Contexts == nil {
		snap.Contexts = make(map[string]map[string]*Context)
	}
}

func ensureCoreResourceMaps(snap *backendSnapshot) {
	if snap.Models == nil {
		snap.Models = make(map[string]map[string]*Model)
	}
	if snap.EndpointConfigs == nil {
		snap.EndpointConfigs = make(map[string]map[string]*EndpointConfig)
	}
	if snap.Endpoints == nil {
		snap.Endpoints = make(map[string]map[string]*Endpoint)
	}
	if snap.Actions == nil {
		snap.Actions = make(map[string]map[string]*Action)
	}
	if snap.Algorithms == nil {
		snap.Algorithms = make(map[string]map[string]*Algorithm)
	}
	if snap.ModelPackages == nil {
		snap.ModelPackages = make(map[string]map[string]*ModelPackage)
	}
}

// ensureNestedMap initialises *m to an empty map if it is nil. It is used to
// backfill region-nested resource maps that may be absent from snapshots
// taken before the corresponding resource family was introduced.
func ensureNestedMap[K comparable, V any](m *map[string]map[K]V) {
	if *m == nil {
		*m = make(map[string]map[K]V)
	}
}

func ensureJobMaps(snap *backendSnapshot) {
	ensureNestedMap(&snap.TrainingJobs)
	ensureNestedMap(&snap.Notebooks)
	ensureNestedMap(&snap.HPTuningJobs)
	ensureNestedMap(&snap.ProcessingJobs)
	ensureNestedMap(&snap.TransformJobs)
	ensureNestedMap(&snap.EdgeDeploymentPlans)
	ensureNestedMap(&snap.FeatureRecords)
	ensureNestedMap(&snap.FeatureMetadata)
}

func ensureConfigMaps(snap *backendSnapshot) {
	if snap.Domains == nil {
		snap.Domains = make(map[string]map[string]*Domain)
	}
	if snap.UserProfiles == nil {
		snap.UserProfiles = make(map[string]map[string]*UserProfile)
	}
	if snap.Apps == nil {
		snap.Apps = make(map[string]map[string]*App)
	}
	if snap.FeatureGroups == nil {
		snap.FeatureGroups = make(map[string]map[string]*FeatureGroup)
	}
	if snap.NotebookLifecycleConfigs == nil {
		snap.NotebookLifecycleConfigs = make(map[string]map[string]*NotebookInstanceLifecycleConfig)
	}
	if snap.Clusters == nil {
		snap.Clusters = make(map[string]map[string]*persistedCluster)
	}
}

func ensureMetadataMaps(snap *backendSnapshot) {
	ensureNestedMap(&snap.Pipelines)
	ensureNestedMap(&snap.PipelineExecutions)
	ensureNestedMap(&snap.PipelineExecSteps)
	ensureNestedMap(&snap.Experiments)
	ensureNestedMap(&snap.Trials)
	ensureNestedMap(&snap.TrialComponents)
	ensureNestedMap(&snap.Associations)
	ensureNestedMap(&snap.TrialComponentAssociations)
}

func fixNilTagMaps(snap *backendSnapshot) {
	fixNilTagMapsCoreResources(snap)
	fixNilTagMapsNewResources(snap)
}

func fixNilTagMapsCoreResources(snap *backendSnapshot) {
	fixNestedTagsSage(snap.Models, func(m *Model) { m.Tags = ensureSageTagMap(m.Tags) })
	fixNestedTagsSage(snap.EndpointConfigs, func(ec *EndpointConfig) { ec.Tags = ensureSageTagMap(ec.Tags) })
	fixNestedTagsSage(snap.Actions, func(a *Action) { a.Tags = ensureSageTagMap(a.Tags) })
	fixNestedTagsSage(snap.Artifacts, func(ar *Artifact) { ar.Tags = ensureSageTagMap(ar.Tags) })
	fixNestedTagsSage(snap.Contexts, func(c *Context) { c.Tags = ensureSageTagMap(c.Tags) })
	fixNestedTagsSage(snap.Algorithms, func(al *Algorithm) { al.Tags = ensureSageTagMap(al.Tags) })
	fixNestedTagsSage(
		snap.MlflowTrackingServers,
		func(s *MlflowTrackingServer) { s.Tags = ensureSageTagMap(s.Tags) },
	)
	fixNestedTagsSage(snap.MlflowApps, func(m *MlflowApp) { m.Tags = ensureSageTagMap(m.Tags) })
	fixNestedTagsSage(snap.PartnerApps, func(p *PartnerApp) { p.Tags = ensureSageTagMap(p.Tags) })
	fixNestedTagsSage(snap.ModelPackages, func(mp *ModelPackage) { mp.Tags = ensureSageTagMap(mp.Tags) })
}

func fixNilTagMapsNewResources(snap *backendSnapshot) {
	fixNestedTagsSage(snap.Endpoints, func(ep *Endpoint) { ep.Tags = ensureSageTagMap(ep.Tags) })
	fixNestedTagsSage(snap.TrainingJobs, func(tj *TrainingJob) { tj.Tags = ensureSageTagMap(tj.Tags) })
	fixNestedTagsSage(snap.Notebooks, func(nb *NotebookInstance) { nb.Tags = ensureSageTagMap(nb.Tags) })
	fixNestedTagsSage(snap.HPTuningJobs, func(j *HyperParameterTuningJob) { j.Tags = ensureSageTagMap(j.Tags) })
	fixNestedTagsSage(snap.Hubs, func(h *Hub) { h.Tags = ensureSageTagMap(h.Tags) })
	fixNestedTagsSage(snap.HubContents, func(hc *HubContent) { hc.Tags = ensureSageTagMap(hc.Tags) })
	fixNestedTagsSage(snap.DataQualityJobDefs, func(j *JobDefinition) { j.Tags = ensureSageTagMap(j.Tags) })
	fixNestedTagsSage(snap.ModelBiasJobDefs, func(j *JobDefinition) { j.Tags = ensureSageTagMap(j.Tags) })
	fixNestedTagsSage(snap.ModelQualityJobDefs, func(j *JobDefinition) { j.Tags = ensureSageTagMap(j.Tags) })
	fixNestedTagsSage(snap.ModelExplainJobDefs, func(j *JobDefinition) { j.Tags = ensureSageTagMap(j.Tags) })
	fixNestedTagsSage(snap.EdgeDeploymentPlans, func(p *EdgeDeploymentPlan) { p.Tags = ensureSageTagMap(p.Tags) })
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
