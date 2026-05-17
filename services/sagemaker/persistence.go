package sagemaker

import (
	"encoding/json"
	"log/slog"
	"time"
)

// persistedCluster is a serialisable version of Cluster that includes Nodes.
type persistedCluster struct {
	CreationTime  string                  `json:"CreationTime"`
	Nodes         map[string]*ClusterNode `json:"Nodes"`
	ClusterArn    string                  `json:"ClusterArn"`
	ClusterName   string                  `json:"ClusterName"`
	ClusterStatus string                  `json:"ClusterStatus"`
}

// backendSnapshot holds the serialisable state of InMemoryBackend.
type backendSnapshot struct {
	Models                     map[string]*Model                           `json:"models"`
	EndpointConfigs            map[string]*EndpointConfig                  `json:"endpointConfigs"`
	Endpoints                  map[string]*Endpoint                        `json:"endpoints"`
	TrainingJobs               map[string]*TrainingJob                     `json:"trainingJobs"`
	Notebooks                  map[string]*NotebookInstance                `json:"notebooks"`
	HPTuningJobs               map[string]*HyperParameterTuningJob         `json:"hpTuningJobs"`
	Associations               map[string]*Association                     `json:"associations"`
	TrialComponentAssociations map[string]*TrialComponentAssociation       `json:"trialComponentAssociations"`
	Actions                    map[string]*Action                          `json:"actions"`
	Algorithms                 map[string]*Algorithm                       `json:"algorithms"`
	Clusters                   map[string]*persistedCluster                `json:"clusters"`
	ModelPackages              map[string]*ModelPackage                    `json:"modelPackages"`
	Domains                    map[string]*Domain                          `json:"domains"`
	UserProfiles               map[string]*UserProfile                     `json:"userProfiles"`
	Apps                       map[string]*App                             `json:"apps"`
	FeatureGroups              map[string]*FeatureGroup                    `json:"featureGroups"`
	Pipelines                  map[string]*Pipeline                        `json:"pipelines"`
	PipelineExecutions         map[string]*PipelineExecution               `json:"pipelineExecutions"`
	PipelineExecSteps          map[string]*PipelineExecutionStep           `json:"pipelineExecSteps"`
	Experiments                map[string]*Experiment                      `json:"experiments"`
	Trials                     map[string]*Trial                           `json:"trials"`
	TrialComponents            map[string]*TrialComponent                  `json:"trialComponents"`
	NotebookLifecycleConfigs   map[string]*NotebookInstanceLifecycleConfig `json:"notebookLifecycleConfigs"`
	ProcessingJobs             map[string]*ProcessingJob                   `json:"processingJobs"`
	AccountID                  string                                      `json:"accountID"`
	Region                     string                                      `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	clusters := make(map[string]*persistedCluster, len(b.clusters))

	for k, c := range b.clusters {
		pc := &persistedCluster{
			CreationTime:  c.CreationTime.Format("2006-01-02T15:04:05Z07:00"),
			ClusterArn:    c.ClusterArn,
			ClusterName:   c.ClusterName,
			ClusterStatus: c.ClusterStatus,
			Nodes:         make(map[string]*ClusterNode, len(c.Nodes)),
		}

		for nk, nv := range c.Nodes {
			nodeCopy := *nv
			pc.Nodes[nk] = &nodeCopy
		}

		clusters[k] = pc
	}

	// Serialise userProfiles and apps maps (composite key → string key).
	userProfiles := make(map[string]*UserProfile, len(b.userProfiles))
	for k, v := range b.userProfiles {
		cp := *v
		userProfiles[k.DomainID+"|"+k.UserProfileName] = &cp
	}
	apps := make(map[string]*App, len(b.apps))
	for k, v := range b.apps {
		cp := *v
		apps[k.DomainID+"|"+k.UserProfileName+"|"+k.AppType+"|"+k.AppName] = &cp
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
	b.rebuildARNIndexes()

	return nil
}

// restoreFields assigns deserialized maps to backend fields (called with lock held).
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
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Restore composite-key maps (string key → composite key).
	b.userProfiles = make(map[userProfileKey]*UserProfile, len(snap.UserProfiles))
	for _, v := range snap.UserProfiles {
		key := userProfileKey{DomainID: v.DomainID, UserProfileName: v.UserProfileName}
		cp := *v
		b.userProfiles[key] = &cp
	}
	b.apps = make(map[appKey]*App, len(snap.Apps))
	for _, v := range snap.Apps {
		key := appKey{
			DomainID:        v.DomainID,
			UserProfileName: v.UserProfileName,
			AppType:         v.AppType,
			AppName:         v.AppName,
		}
		cp := *v
		b.apps[key] = &cp
	}

	// Restore clusters, converting persistedCluster back to Cluster.
	b.clusters = make(map[string]*Cluster, len(snap.Clusters))

	for k, pc := range snap.Clusters {
		t, err := time.Parse("2006-01-02T15:04:05Z07:00", pc.CreationTime)
		if err != nil {
			slog.Default().
				Warn("sagemaker: failed to parse cluster creation time", "cluster", k, "error", err)
		}

		c := &Cluster{
			ClusterArn:    pc.ClusterArn,
			ClusterName:   pc.ClusterName,
			ClusterStatus: pc.ClusterStatus,
			CreationTime:  t,
			Nodes:         make(map[string]*ClusterNode, len(pc.Nodes)),
		}

		for nk, nv := range pc.Nodes {
			nodeCopy := *nv
			c.Nodes[nk] = &nodeCopy
		}

		b.clusters[k] = c
	}
}

// rebuildARNIndexes reconstructs all ARN-to-name indexes after a restore (called with lock held).
func (b *InMemoryBackend) rebuildARNIndexes() {
	b.modelARNIndex = make(map[string]string, len(b.models))

	for name, m := range b.models {
		b.modelARNIndex[m.ModelARN] = name
	}

	b.endpointConfigARNIndex = make(map[string]string, len(b.endpointConfigs))

	for name, ec := range b.endpointConfigs {
		b.endpointConfigARNIndex[ec.EndpointConfigARN] = name
	}

	b.actionARNIndex = make(map[string]string, len(b.actions))

	for name, a := range b.actions {
		b.actionARNIndex[a.ActionArn] = name
	}

	b.algorithmARNIndex = make(map[string]string, len(b.algorithms))

	for name, al := range b.algorithms {
		b.algorithmARNIndex[al.AlgorithmArn] = name
	}

	b.clusterARNIndex = make(map[string]string, len(b.clusters))

	for name, c := range b.clusters {
		b.clusterARNIndex[c.ClusterArn] = name
	}

	b.modelPackageARNIndex = make(map[string]string, len(b.modelPackages))

	for arnStr := range b.modelPackages {
		b.modelPackageARNIndex[arnStr] = arnStr
	}

	b.endpointARNIndex = make(map[string]string, len(b.endpoints))

	for name, ep := range b.endpoints {
		b.endpointARNIndex[ep.EndpointArn] = name
	}

	b.trainingJobARNIndex = make(map[string]string, len(b.trainingJobs))

	for name, tj := range b.trainingJobs {
		b.trainingJobARNIndex[tj.TrainingJobArn] = name
	}

	b.notebookARNIndex = make(map[string]string, len(b.notebooks))

	for name, nb := range b.notebooks {
		b.notebookARNIndex[nb.NotebookInstanceArn] = name
	}

	b.hpTuningJobARNIndex = make(map[string]string, len(b.hpTuningJobs))

	for name, j := range b.hpTuningJobs {
		b.hpTuningJobARNIndex[j.HyperParameterTuningJobArn] = name
	}

	b.processingJobARNIndex = make(map[string]string, len(b.processingJobs))

	for name, pj := range b.processingJobs {
		b.processingJobARNIndex[pj.ProcessingJobArn] = name
	}
}

func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Models == nil {
		snap.Models = make(map[string]*Model)
	}

	if snap.EndpointConfigs == nil {
		snap.EndpointConfigs = make(map[string]*EndpointConfig)
	}

	if snap.Endpoints == nil {
		snap.Endpoints = make(map[string]*Endpoint)
	}

	if snap.TrainingJobs == nil {
		snap.TrainingJobs = make(map[string]*TrainingJob)
	}

	if snap.Notebooks == nil {
		snap.Notebooks = make(map[string]*NotebookInstance)
	}

	if snap.HPTuningJobs == nil {
		snap.HPTuningJobs = make(map[string]*HyperParameterTuningJob)
	}

	if snap.Associations == nil {
		snap.Associations = make(map[string]*Association)
	}

	if snap.TrialComponentAssociations == nil {
		snap.TrialComponentAssociations = make(map[string]*TrialComponentAssociation)
	}

	if snap.Actions == nil {
		snap.Actions = make(map[string]*Action)
	}

	if snap.Algorithms == nil {
		snap.Algorithms = make(map[string]*Algorithm)
	}

	if snap.Clusters == nil {
		snap.Clusters = make(map[string]*persistedCluster)
	}

	if snap.ModelPackages == nil {
		snap.ModelPackages = make(map[string]*ModelPackage)
	}
	if snap.Domains == nil {
		snap.Domains = make(map[string]*Domain)
	}
	if snap.UserProfiles == nil {
		snap.UserProfiles = make(map[string]*UserProfile)
	}
	if snap.Apps == nil {
		snap.Apps = make(map[string]*App)
	}
	if snap.FeatureGroups == nil {
		snap.FeatureGroups = make(map[string]*FeatureGroup)
	}
	if snap.Pipelines == nil {
		snap.Pipelines = make(map[string]*Pipeline)
	}
	if snap.PipelineExecutions == nil {
		snap.PipelineExecutions = make(map[string]*PipelineExecution)
	}
	if snap.PipelineExecSteps == nil {
		snap.PipelineExecSteps = make(map[string]*PipelineExecutionStep)
	}
	if snap.Experiments == nil {
		snap.Experiments = make(map[string]*Experiment)
	}
	if snap.Trials == nil {
		snap.Trials = make(map[string]*Trial)
	}
	if snap.TrialComponents == nil {
		snap.TrialComponents = make(map[string]*TrialComponent)
	}
	if snap.NotebookLifecycleConfigs == nil {
		snap.NotebookLifecycleConfigs = make(map[string]*NotebookInstanceLifecycleConfig)
	}
	if snap.ProcessingJobs == nil {
		snap.ProcessingJobs = make(map[string]*ProcessingJob)
	}
}

func fixNilTagMaps(snap *backendSnapshot) {
	fixNilTagMapsCoreResources(snap)
	fixNilTagMapsNewResources(snap)
}

func fixNilTagMapsCoreResources(snap *backendSnapshot) {
	for _, m := range snap.Models {
		if m.Tags == nil {
			m.Tags = make(map[string]string)
		}
	}

	for _, ec := range snap.EndpointConfigs {
		if ec.Tags == nil {
			ec.Tags = make(map[string]string)
		}
	}

	for _, a := range snap.Actions {
		if a.Tags == nil {
			a.Tags = make(map[string]string)
		}
	}

	for _, al := range snap.Algorithms {
		if al.Tags == nil {
			al.Tags = make(map[string]string)
		}
	}

	for _, mp := range snap.ModelPackages {
		if mp.Tags == nil {
			mp.Tags = make(map[string]string)
		}
	}
}

func fixNilTagMapsNewResources(snap *backendSnapshot) {
	for _, ep := range snap.Endpoints {
		if ep.Tags == nil {
			ep.Tags = make(map[string]string)
		}
	}

	for _, tj := range snap.TrainingJobs {
		if tj.Tags == nil {
			tj.Tags = make(map[string]string)
		}
	}

	for _, nb := range snap.Notebooks {
		if nb.Tags == nil {
			nb.Tags = make(map[string]string)
		}
	}

	for _, j := range snap.HPTuningJobs {
		if j.Tags == nil {
			j.Tags = make(map[string]string)
		}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
