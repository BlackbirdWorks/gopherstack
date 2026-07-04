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
	FeatureRecords           map[string]map[string]*FeatureRecord                   `json:"featureRecords"`
	FeatureMetadata          map[string]map[string]*FeatureMetadata                 `json:"featureMetadata"`
	AccountID                string                                                 `json:"accountID"`
	Region                   string                                                 `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	// Convert clusters: map[string]map[string]*Cluster → map[string]map[string]*persistedCluster
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

	// Convert userProfiles: map[string]map[userProfileKey]*UserProfile
	// → map[string]map[string]*UserProfile (inner key = "domainID|profileName")
	userProfiles := make(map[string]map[string]*UserProfile, len(b.userProfiles))
	for region, regionProfiles := range b.userProfiles {
		userProfiles[region] = make(map[string]*UserProfile, len(regionProfiles))
		for k, v := range regionProfiles {
			cp := *v
			userProfiles[region][k.DomainID+"|"+k.UserProfileName] = &cp
		}
	}

	// Convert apps: map[string]map[appKey]*App
	// → map[string]map[string]*App (inner key = "domainID|userProfileName|appType|appName")
	apps := make(map[string]map[string]*App, len(b.apps))
	for region, regionApps := range b.apps {
		apps[region] = make(map[string]*App, len(regionApps))
		for k, v := range regionApps {
			cp := *v
			apps[region][k.DomainID+"|"+k.UserProfileName+"|"+k.AppType+"|"+k.AppName] = &cp
		}
	}

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
		FeatureRecords:             b.featureRecords,
		FeatureMetadata:            b.featureMetadata,
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
	b.featureRecords = snap.FeatureRecords
	b.featureMetadata = snap.FeatureMetadata
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.userProfiles = restoreUserProfiles(snap)
	b.apps = restoreApps(snap)
	b.clusters = restoreClusters(snap)
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

func ensureJobMaps(snap *backendSnapshot) {
	if snap.TrainingJobs == nil {
		snap.TrainingJobs = make(map[string]map[string]*TrainingJob)
	}
	if snap.Notebooks == nil {
		snap.Notebooks = make(map[string]map[string]*NotebookInstance)
	}
	if snap.HPTuningJobs == nil {
		snap.HPTuningJobs = make(map[string]map[string]*HyperParameterTuningJob)
	}
	if snap.ProcessingJobs == nil {
		snap.ProcessingJobs = make(map[string]map[string]*ProcessingJob)
	}
	if snap.TransformJobs == nil {
		snap.TransformJobs = make(map[string]map[string]*TransformJob)
	}
	if snap.FeatureRecords == nil {
		snap.FeatureRecords = make(map[string]map[string]*FeatureRecord)
	}
	if snap.FeatureMetadata == nil {
		snap.FeatureMetadata = make(map[string]map[string]*FeatureMetadata)
	}
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
	if snap.Pipelines == nil {
		snap.Pipelines = make(map[string]map[string]*Pipeline)
	}
	if snap.PipelineExecutions == nil {
		snap.PipelineExecutions = make(map[string]map[string]*PipelineExecution)
	}
	if snap.PipelineExecSteps == nil {
		snap.PipelineExecSteps = make(map[string]map[string]*PipelineExecutionStep)
	}
	if snap.Experiments == nil {
		snap.Experiments = make(map[string]map[string]*Experiment)
	}
	if snap.Trials == nil {
		snap.Trials = make(map[string]map[string]*Trial)
	}
	if snap.TrialComponents == nil {
		snap.TrialComponents = make(map[string]map[string]*TrialComponent)
	}
	if snap.Associations == nil {
		snap.Associations = make(map[string]map[string]*Association)
	}
	if snap.TrialComponentAssociations == nil {
		snap.TrialComponentAssociations = make(map[string]map[string]*TrialComponentAssociation)
	}
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
	fixNestedTagsSage(snap.ModelPackages, func(mp *ModelPackage) { mp.Tags = ensureSageTagMap(mp.Tags) })
}

func fixNilTagMapsNewResources(snap *backendSnapshot) {
	fixNestedTagsSage(snap.Endpoints, func(ep *Endpoint) { ep.Tags = ensureSageTagMap(ep.Tags) })
	fixNestedTagsSage(snap.TrainingJobs, func(tj *TrainingJob) { tj.Tags = ensureSageTagMap(tj.Tags) })
	fixNestedTagsSage(snap.Notebooks, func(nb *NotebookInstance) { nb.Tags = ensureSageTagMap(nb.Tags) })
	fixNestedTagsSage(snap.HPTuningJobs, func(j *HyperParameterTuningJob) { j.Tags = ensureSageTagMap(j.Tags) })
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
