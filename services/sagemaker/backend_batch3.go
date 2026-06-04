package sagemaker

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

var (
	// ErrDataQualityJobDefNotFound is returned when a data quality job definition does not exist.
	ErrDataQualityJobDefNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrModelBiasJobDefNotFound is returned when a model bias job definition does not exist.
	ErrModelBiasJobDefNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrModelQualityJobDefNotFound is returned when a model quality job definition does not exist.
	ErrModelQualityJobDefNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrModelExplainJobDefNotFound is returned when a model explainability job definition does not exist.
	ErrModelExplainJobDefNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrHumanTaskUINotFound is returned when a human task UI does not exist.
	ErrHumanTaskUINotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrWorkforceNotFound is returned when a workforce does not exist.
	ErrWorkforceNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrFlowDefinitionNotFound is returned when a flow definition does not exist.
	ErrFlowDefinitionNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrAppImageConfigNotFound is returned when an app image config does not exist.
	ErrAppImageConfigNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrInferenceExperimentNotFound is returned when an inference experiment does not exist.
	ErrInferenceExperimentNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrMlflowTrackingServerNotFound is returned when an MLflow tracking server does not exist.
	ErrMlflowTrackingServerNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrModelCardNotFound is returned when a model card does not exist.
	ErrModelCardNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrOptimizationJobNotFound is returned when an optimization job does not exist.
	ErrOptimizationJobNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrStudioLifecycleConfigNotFound is returned when a studio lifecycle config does not exist.
	ErrStudioLifecycleConfigNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrPartnerAppNotFound is returned when a partner app does not exist.
	ErrPartnerAppNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrTrainingPlanNotFound is returned when a training plan does not exist.
	ErrTrainingPlanNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
)

// ---------------------------------------------------------------------------
// JobDefinition — shared struct for Data Quality / Model Bias / Quality / Explainability
// ---------------------------------------------------------------------------

// JobDefinition is a shared struct for the four monitoring job definition types.
type JobDefinition struct {
	CreationTime      time.Time         `json:"CreationTime"`
	Tags              map[string]string `json:"Tags,omitempty"`
	JobDefinitionName string            `json:"JobDefinitionName"`
	JobDefinitionArn  string            `json:"JobDefinitionArn"`
	JobDefinitionType string            `json:"JobDefinitionType"`
	RoleArn           string            `json:"RoleArn,omitempty"`
}

func cloneJobDefinition(j *JobDefinition) *JobDefinition {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)

	return &cp
}

func (b *InMemoryBackend) createJobDefinition(
	store map[string]*JobDefinition,
	defType, name, roleArn string,
	tags map[string]string,
	resourceType string,
) (*JobDefinition, error) {
	b.mu.Lock("createJobDefinition")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: %sJobDefinitionName is required", ErrValidation, defType)
	}

	if _, ok := store[name]; ok {
		return nil, fmt.Errorf("%w: %s job definition %q already exists", ErrValidation, defType, name)
	}

	defARN := arn.Build("sagemaker", b.region, b.accountID, resourceType+"/"+name)

	j := &JobDefinition{
		JobDefinitionName: name,
		JobDefinitionArn:  defARN,
		JobDefinitionType: defType,
		RoleArn:           roleArn,
		Tags:              mergeTags(nil, tags),
		CreationTime:      time.Now(),
	}
	store[name] = j

	return cloneJobDefinition(j), nil
}

func (b *InMemoryBackend) describeJobDefinition(
	store map[string]*JobDefinition,
	name string,
	notFound error,
) (*JobDefinition, error) {
	b.mu.RLock("describeJobDefinition")
	defer b.mu.RUnlock()

	j, ok := store[name]
	if !ok {
		return nil, fmt.Errorf("%w: job definition %q not found", notFound, name)
	}

	return cloneJobDefinition(j), nil
}

func (b *InMemoryBackend) deleteJobDefinition(
	store map[string]*JobDefinition,
	name string,
	notFound error,
) error {
	b.mu.Lock("deleteJobDefinition")
	defer b.mu.Unlock()

	if _, ok := store[name]; !ok {
		return fmt.Errorf("%w: job definition %q not found", notFound, name)
	}

	delete(store, name)

	return nil
}

// ---------------------------------------------------------------------------
// DataQualityJobDefinition
// ---------------------------------------------------------------------------

// CreateDataQualityJobDefinition creates a data quality job definition.
func (b *InMemoryBackend) CreateDataQualityJobDefinition(
	name, roleArn string,
	tags map[string]string,
) (*JobDefinition, error) {
	return b.createJobDefinition(
		b.dataQualityJobDefs, "DataQuality", name, roleArn, tags, "data-quality-job-definition",
	)
}

// DescribeDataQualityJobDefinition returns a data quality job definition by name.
func (b *InMemoryBackend) DescribeDataQualityJobDefinition(name string) (*JobDefinition, error) {
	return b.describeJobDefinition(b.dataQualityJobDefs, name, ErrDataQualityJobDefNotFound)
}

// DeleteDataQualityJobDefinition removes a data quality job definition by name.
func (b *InMemoryBackend) DeleteDataQualityJobDefinition(name string) error {
	return b.deleteJobDefinition(b.dataQualityJobDefs, name, ErrDataQualityJobDefNotFound)
}

// ---------------------------------------------------------------------------
// ModelBiasJobDefinition
// ---------------------------------------------------------------------------

// CreateModelBiasJobDefinition creates a model bias job definition.
func (b *InMemoryBackend) CreateModelBiasJobDefinition(
	name, roleArn string,
	tags map[string]string,
) (*JobDefinition, error) {
	return b.createJobDefinition(
		b.modelBiasJobDefs, "ModelBias", name, roleArn, tags, "model-bias-job-definition",
	)
}

// DescribeModelBiasJobDefinition returns a model bias job definition by name.
func (b *InMemoryBackend) DescribeModelBiasJobDefinition(name string) (*JobDefinition, error) {
	return b.describeJobDefinition(b.modelBiasJobDefs, name, ErrModelBiasJobDefNotFound)
}

// DeleteModelBiasJobDefinition removes a model bias job definition by name.
func (b *InMemoryBackend) DeleteModelBiasJobDefinition(name string) error {
	return b.deleteJobDefinition(b.modelBiasJobDefs, name, ErrModelBiasJobDefNotFound)
}

// ---------------------------------------------------------------------------
// ModelQualityJobDefinition
// ---------------------------------------------------------------------------

// CreateModelQualityJobDefinition creates a model quality job definition.
func (b *InMemoryBackend) CreateModelQualityJobDefinition(
	name, roleArn string,
	tags map[string]string,
) (*JobDefinition, error) {
	return b.createJobDefinition(
		b.modelQualityJobDefs, "ModelQuality", name, roleArn, tags, "model-quality-job-definition",
	)
}

// DescribeModelQualityJobDefinition returns a model quality job definition by name.
func (b *InMemoryBackend) DescribeModelQualityJobDefinition(name string) (*JobDefinition, error) {
	return b.describeJobDefinition(b.modelQualityJobDefs, name, ErrModelQualityJobDefNotFound)
}

// DeleteModelQualityJobDefinition removes a model quality job definition by name.
func (b *InMemoryBackend) DeleteModelQualityJobDefinition(name string) error {
	return b.deleteJobDefinition(b.modelQualityJobDefs, name, ErrModelQualityJobDefNotFound)
}

// ---------------------------------------------------------------------------
// ModelExplainabilityJobDefinition
// ---------------------------------------------------------------------------

// CreateModelExplainabilityJobDefinition creates a model explainability job definition.
func (b *InMemoryBackend) CreateModelExplainabilityJobDefinition(
	name, roleArn string,
	tags map[string]string,
) (*JobDefinition, error) {
	return b.createJobDefinition(
		b.modelExplainJobDefs,
		"ModelExplainability",
		name,
		roleArn,
		tags,
		"model-explainability-job-definition",
	)
}

// DescribeModelExplainabilityJobDefinition returns a model explainability job definition by name.
func (b *InMemoryBackend) DescribeModelExplainabilityJobDefinition(name string) (*JobDefinition, error) {
	return b.describeJobDefinition(b.modelExplainJobDefs, name, ErrModelExplainJobDefNotFound)
}

// DeleteModelExplainabilityJobDefinition removes a model explainability job definition by name.
func (b *InMemoryBackend) DeleteModelExplainabilityJobDefinition(name string) error {
	return b.deleteJobDefinition(b.modelExplainJobDefs, name, ErrModelExplainJobDefNotFound)
}

// ---------------------------------------------------------------------------
// HumanTaskUI
// ---------------------------------------------------------------------------

// HumanTaskUI represents a SageMaker human task UI.
type HumanTaskUI struct {
	CreationTime      time.Time         `json:"CreationTime"`
	Tags              map[string]string `json:"Tags,omitempty"`
	HumanTaskUIName   string            `json:"HumanTaskUiName"`
	HumanTaskUIArn    string            `json:"HumanTaskUiArn"`
	HumanTaskUIStatus string            `json:"HumanTaskUiStatus"`
}

func cloneHumanTaskUI(h *HumanTaskUI) *HumanTaskUI {
	cp := *h
	cp.Tags = maps.Clone(h.Tags)

	return &cp
}

// CreateHumanTaskUI creates a human task UI.
func (b *InMemoryBackend) CreateHumanTaskUI(
	name string,
	tags map[string]string,
) (*HumanTaskUI, error) {
	b.mu.Lock("CreateHumanTaskUI")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: HumanTaskUiName is required", ErrValidation)
	}

	if _, ok := b.humanTaskUis[name]; ok {
		return nil, fmt.Errorf("%w: human task UI %q already exists", ErrValidation, name)
	}

	uiARN := arn.Build("sagemaker", b.region, b.accountID, "human-task-ui/"+name)

	ui := &HumanTaskUI{
		HumanTaskUIName:   name,
		HumanTaskUIArn:    uiARN,
		HumanTaskUIStatus: statusActive,
		Tags:              mergeTags(nil, tags),
		CreationTime:      time.Now(),
	}
	b.humanTaskUis[name] = ui

	return cloneHumanTaskUI(ui), nil
}

// DescribeHumanTaskUI returns a human task UI by name.
func (b *InMemoryBackend) DescribeHumanTaskUI(name string) (*HumanTaskUI, error) {
	b.mu.RLock("DescribeHumanTaskUI")
	defer b.mu.RUnlock()

	ui, ok := b.humanTaskUis[name]
	if !ok {
		return nil, fmt.Errorf("%w: human task UI %q not found", ErrHumanTaskUINotFound, name)
	}

	return cloneHumanTaskUI(ui), nil
}

// DeleteHumanTaskUI removes a human task UI by name.
func (b *InMemoryBackend) DeleteHumanTaskUI(name string) error {
	b.mu.Lock("DeleteHumanTaskUI")
	defer b.mu.Unlock()

	if _, ok := b.humanTaskUis[name]; !ok {
		return fmt.Errorf("%w: human task UI %q not found", ErrHumanTaskUINotFound, name)
	}

	delete(b.humanTaskUis, name)

	return nil
}

// ---------------------------------------------------------------------------
// Workforce
// ---------------------------------------------------------------------------

// Workforce represents a SageMaker workforce.
type Workforce struct {
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	WorkforceName    string            `json:"WorkforceName"`
	WorkforceArn     string            `json:"WorkforceArn"`
	Status           string            `json:"Status"`
}

func cloneWorkforce(w *Workforce) *Workforce {
	cp := *w
	cp.Tags = maps.Clone(w.Tags)

	return &cp
}

// CreateWorkforce creates a workforce.
func (b *InMemoryBackend) CreateWorkforce(
	name string,
	tags map[string]string,
) (*Workforce, error) {
	b.mu.Lock("CreateWorkforce")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: WorkforceName is required", ErrValidation)
	}

	if _, ok := b.workforces[name]; ok {
		return nil, fmt.Errorf("%w: workforce %q already exists", ErrValidation, name)
	}

	workforceARN := arn.Build("sagemaker", b.region, b.accountID, "workforce/"+name)

	w := &Workforce{
		WorkforceName:    name,
		WorkforceArn:     workforceARN,
		Status:           statusActive,
		Tags:             mergeTags(nil, tags),
		LastModifiedTime: time.Now(),
	}
	b.workforces[name] = w

	return cloneWorkforce(w), nil
}

// DescribeWorkforce returns a workforce by name.
func (b *InMemoryBackend) DescribeWorkforce(name string) (*Workforce, error) {
	b.mu.RLock("DescribeWorkforce")
	defer b.mu.RUnlock()

	w, ok := b.workforces[name]
	if !ok {
		return nil, fmt.Errorf("%w: workforce %q not found", ErrWorkforceNotFound, name)
	}

	return cloneWorkforce(w), nil
}

// UpdateWorkforce updates a workforce (marks it modified).
func (b *InMemoryBackend) UpdateWorkforce(name string) (*Workforce, error) {
	b.mu.Lock("UpdateWorkforce")
	defer b.mu.Unlock()

	w, ok := b.workforces[name]
	if !ok {
		return nil, fmt.Errorf("%w: workforce %q not found", ErrWorkforceNotFound, name)
	}

	w.LastModifiedTime = time.Now()

	return cloneWorkforce(w), nil
}

// ---------------------------------------------------------------------------
// FlowDefinition
// ---------------------------------------------------------------------------

// FlowDefinition represents a SageMaker Augmented AI flow definition.
type FlowDefinition struct {
	CreationTime         time.Time         `json:"CreationTime"`
	Tags                 map[string]string `json:"Tags,omitempty"`
	FlowDefinitionName   string            `json:"FlowDefinitionName"`
	FlowDefinitionArn    string            `json:"FlowDefinitionArn"`
	FlowDefinitionStatus string            `json:"FlowDefinitionStatus"`
	RoleArn              string            `json:"RoleArn,omitempty"`
}

func cloneFlowDefinition(f *FlowDefinition) *FlowDefinition {
	cp := *f
	cp.Tags = maps.Clone(f.Tags)

	return &cp
}

// CreateFlowDefinition creates a flow definition.
func (b *InMemoryBackend) CreateFlowDefinition(
	name, roleArn string,
	tags map[string]string,
) (*FlowDefinition, error) {
	b.mu.Lock("CreateFlowDefinition")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: FlowDefinitionName is required", ErrValidation)
	}

	if _, ok := b.flowDefinitions[name]; ok {
		return nil, fmt.Errorf("%w: flow definition %q already exists", ErrValidation, name)
	}

	flowARN := arn.Build("sagemaker", b.region, b.accountID, "flow-definition/"+name)

	f := &FlowDefinition{
		FlowDefinitionName:   name,
		FlowDefinitionArn:    flowARN,
		FlowDefinitionStatus: statusActive,
		RoleArn:              roleArn,
		Tags:                 mergeTags(nil, tags),
		CreationTime:         time.Now(),
	}
	b.flowDefinitions[name] = f

	return cloneFlowDefinition(f), nil
}

// DescribeFlowDefinition returns a flow definition by name.
func (b *InMemoryBackend) DescribeFlowDefinition(name string) (*FlowDefinition, error) {
	b.mu.RLock("DescribeFlowDefinition")
	defer b.mu.RUnlock()

	f, ok := b.flowDefinitions[name]
	if !ok {
		return nil, fmt.Errorf("%w: flow definition %q not found", ErrFlowDefinitionNotFound, name)
	}

	return cloneFlowDefinition(f), nil
}

// DeleteFlowDefinition removes a flow definition by name.
func (b *InMemoryBackend) DeleteFlowDefinition(name string) error {
	b.mu.Lock("DeleteFlowDefinition")
	defer b.mu.Unlock()

	if _, ok := b.flowDefinitions[name]; !ok {
		return fmt.Errorf("%w: flow definition %q not found", ErrFlowDefinitionNotFound, name)
	}

	delete(b.flowDefinitions, name)

	return nil
}

// ---------------------------------------------------------------------------
// AppImageConfig
// ---------------------------------------------------------------------------

// AppImageConfig represents a SageMaker app image configuration.
type AppImageConfig struct {
	CreationTime       time.Time         `json:"CreationTime"`
	LastModifiedTime   time.Time         `json:"LastModifiedTime"`
	Tags               map[string]string `json:"Tags,omitempty"`
	AppImageConfigName string            `json:"AppImageConfigName"`
	AppImageConfigArn  string            `json:"AppImageConfigArn"`
}

func cloneAppImageConfig(a *AppImageConfig) *AppImageConfig {
	cp := *a
	cp.Tags = maps.Clone(a.Tags)

	return &cp
}

// CreateAppImageConfig creates an app image config.
func (b *InMemoryBackend) CreateAppImageConfig(
	name string,
	tags map[string]string,
) (*AppImageConfig, error) {
	b.mu.Lock("CreateAppImageConfig")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: AppImageConfigName is required", ErrValidation)
	}

	if _, ok := b.appImageConfigs[name]; ok {
		return nil, fmt.Errorf("%w: app image config %q already exists", ErrValidation, name)
	}

	configARN := arn.Build("sagemaker", b.region, b.accountID, "app-image-config/"+name)
	now := time.Now()

	a := &AppImageConfig{
		AppImageConfigName: name,
		AppImageConfigArn:  configARN,
		Tags:               mergeTags(nil, tags),
		CreationTime:       now,
		LastModifiedTime:   now,
	}
	b.appImageConfigs[name] = a

	return cloneAppImageConfig(a), nil
}

// DescribeAppImageConfig returns an app image config by name.
func (b *InMemoryBackend) DescribeAppImageConfig(name string) (*AppImageConfig, error) {
	b.mu.RLock("DescribeAppImageConfig")
	defer b.mu.RUnlock()

	a, ok := b.appImageConfigs[name]
	if !ok {
		return nil, fmt.Errorf("%w: app image config %q not found", ErrAppImageConfigNotFound, name)
	}

	return cloneAppImageConfig(a), nil
}

// UpdateAppImageConfig updates an app image config (marks it modified).
func (b *InMemoryBackend) UpdateAppImageConfig(name string) (*AppImageConfig, error) {
	b.mu.Lock("UpdateAppImageConfig")
	defer b.mu.Unlock()

	a, ok := b.appImageConfigs[name]
	if !ok {
		return nil, fmt.Errorf("%w: app image config %q not found", ErrAppImageConfigNotFound, name)
	}

	a.LastModifiedTime = time.Now()

	return cloneAppImageConfig(a), nil
}

// DeleteAppImageConfig removes an app image config by name.
func (b *InMemoryBackend) DeleteAppImageConfig(name string) error {
	b.mu.Lock("DeleteAppImageConfig")
	defer b.mu.Unlock()

	if _, ok := b.appImageConfigs[name]; !ok {
		return fmt.Errorf("%w: app image config %q not found", ErrAppImageConfigNotFound, name)
	}

	delete(b.appImageConfigs, name)

	return nil
}

// ---------------------------------------------------------------------------
// InferenceExperiment
// ---------------------------------------------------------------------------

// InferenceExperiment represents a SageMaker inference experiment.
type InferenceExperiment struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	Name             string            `json:"Name"`
	Arn              string            `json:"Arn"`
	Status           string            `json:"Status"`
	Type             string            `json:"Type,omitempty"`
	RoleArn          string            `json:"RoleArn,omitempty"`
}

func cloneInferenceExperiment(e *InferenceExperiment) *InferenceExperiment {
	cp := *e
	cp.Tags = maps.Clone(e.Tags)

	return &cp
}

// CreateInferenceExperiment creates an inference experiment.
func (b *InMemoryBackend) CreateInferenceExperiment(
	name, expType, roleArn string,
	tags map[string]string,
) (*InferenceExperiment, error) {
	b.mu.Lock("CreateInferenceExperiment")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if _, ok := b.inferenceExperiments[name]; ok {
		return nil, fmt.Errorf("%w: inference experiment %q already exists", ErrValidation, name)
	}

	expARN := arn.Build("sagemaker", b.region, b.accountID, "inference-experiment/"+name)
	now := time.Now()

	e := &InferenceExperiment{
		Name:             name,
		Arn:              expARN,
		Status:           "Running",
		Type:             expType,
		RoleArn:          roleArn,
		Tags:             mergeTags(nil, tags),
		CreationTime:     now,
		LastModifiedTime: now,
	}
	b.inferenceExperiments[name] = e

	return cloneInferenceExperiment(e), nil
}

// DescribeInferenceExperiment returns an inference experiment by name.
func (b *InMemoryBackend) DescribeInferenceExperiment(name string) (*InferenceExperiment, error) {
	b.mu.RLock("DescribeInferenceExperiment")
	defer b.mu.RUnlock()

	e, ok := b.inferenceExperiments[name]
	if !ok {
		return nil, fmt.Errorf("%w: inference experiment %q not found", ErrInferenceExperimentNotFound, name)
	}

	return cloneInferenceExperiment(e), nil
}

// StopInferenceExperiment sets an inference experiment status to "Cancelled".
func (b *InMemoryBackend) StopInferenceExperiment(name string) error {
	b.mu.Lock("StopInferenceExperiment")
	defer b.mu.Unlock()

	e, ok := b.inferenceExperiments[name]
	if !ok {
		return fmt.Errorf("%w: inference experiment %q not found", ErrInferenceExperimentNotFound, name)
	}

	e.Status = "Cancelled"
	e.LastModifiedTime = time.Now()

	return nil
}

// DeleteInferenceExperiment removes an inference experiment by name.
func (b *InMemoryBackend) DeleteInferenceExperiment(name string) error {
	b.mu.Lock("DeleteInferenceExperiment")
	defer b.mu.Unlock()

	if _, ok := b.inferenceExperiments[name]; !ok {
		return fmt.Errorf("%w: inference experiment %q not found", ErrInferenceExperimentNotFound, name)
	}

	delete(b.inferenceExperiments, name)

	return nil
}

// ---------------------------------------------------------------------------
// MlflowTrackingServer
// ---------------------------------------------------------------------------

// MlflowTrackingServer represents a SageMaker MLflow tracking server.
type MlflowTrackingServer struct {
	CreationTime         time.Time         `json:"CreationTime"`
	LastModifiedTime     time.Time         `json:"LastModifiedTime"`
	Tags                 map[string]string `json:"Tags,omitempty"`
	TrackingServerName   string            `json:"TrackingServerName"`
	TrackingServerArn    string            `json:"TrackingServerArn"`
	TrackingServerStatus string            `json:"TrackingServerStatus"`
	RoleArn              string            `json:"RoleArn,omitempty"`
	MlflowVersion        string            `json:"MlflowVersion,omitempty"`
}

func cloneMlflowTrackingServer(s *MlflowTrackingServer) *MlflowTrackingServer {
	cp := *s
	cp.Tags = maps.Clone(s.Tags)

	return &cp
}

// CreateMlflowTrackingServer creates an MLflow tracking server.
func (b *InMemoryBackend) CreateMlflowTrackingServer(
	name, roleArn, mlflowVersion string,
	tags map[string]string,
) (*MlflowTrackingServer, error) {
	b.mu.Lock("CreateMlflowTrackingServer")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: TrackingServerName is required", ErrValidation)
	}

	if _, ok := b.mlflowTrackingServers[name]; ok {
		return nil, fmt.Errorf("%w: MLflow tracking server %q already exists", ErrValidation, name)
	}

	serverARN := arn.Build("sagemaker", b.region, b.accountID, "mlflow-tracking-server/"+name)
	now := time.Now()

	s := &MlflowTrackingServer{
		TrackingServerName:   name,
		TrackingServerArn:    serverARN,
		TrackingServerStatus: "Created",
		RoleArn:              roleArn,
		MlflowVersion:        mlflowVersion,
		Tags:                 mergeTags(nil, tags),
		CreationTime:         now,
		LastModifiedTime:     now,
	}
	b.mlflowTrackingServers[name] = s

	return cloneMlflowTrackingServer(s), nil
}

// DescribeMlflowTrackingServer returns an MLflow tracking server by name.
func (b *InMemoryBackend) DescribeMlflowTrackingServer(name string) (*MlflowTrackingServer, error) {
	b.mu.RLock("DescribeMlflowTrackingServer")
	defer b.mu.RUnlock()

	s, ok := b.mlflowTrackingServers[name]
	if !ok {
		return nil, fmt.Errorf("%w: MLflow tracking server %q not found", ErrMlflowTrackingServerNotFound, name)
	}

	return cloneMlflowTrackingServer(s), nil
}

// DeleteMlflowTrackingServer removes an MLflow tracking server by name.
func (b *InMemoryBackend) DeleteMlflowTrackingServer(name string) error {
	b.mu.Lock("DeleteMlflowTrackingServer")
	defer b.mu.Unlock()

	if _, ok := b.mlflowTrackingServers[name]; !ok {
		return fmt.Errorf("%w: MLflow tracking server %q not found", ErrMlflowTrackingServerNotFound, name)
	}

	delete(b.mlflowTrackingServers, name)

	return nil
}

// StartMlflowTrackingServer sets an MLflow tracking server status to "Running".
func (b *InMemoryBackend) StartMlflowTrackingServer(name string) error {
	b.mu.Lock("StartMlflowTrackingServer")
	defer b.mu.Unlock()

	s, ok := b.mlflowTrackingServers[name]
	if !ok {
		return fmt.Errorf("%w: MLflow tracking server %q not found", ErrMlflowTrackingServerNotFound, name)
	}

	s.TrackingServerStatus = "Running"
	s.LastModifiedTime = time.Now()

	return nil
}

// StopMlflowTrackingServer sets an MLflow tracking server status to "Stopped".
func (b *InMemoryBackend) StopMlflowTrackingServer(name string) error {
	b.mu.Lock("StopMlflowTrackingServer")
	defer b.mu.Unlock()

	s, ok := b.mlflowTrackingServers[name]
	if !ok {
		return fmt.Errorf("%w: MLflow tracking server %q not found", ErrMlflowTrackingServerNotFound, name)
	}

	s.TrackingServerStatus = pipelineStatusStopped
	s.LastModifiedTime = time.Now()

	return nil
}

// ---------------------------------------------------------------------------
// ModelCard
// ---------------------------------------------------------------------------

// ModelCard represents a SageMaker model card.
type ModelCard struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	ModelCardName    string            `json:"ModelCardName"`
	ModelCardArn     string            `json:"ModelCardArn"`
	ModelCardStatus  string            `json:"ModelCardStatus"`
	Content          string            `json:"Content,omitempty"`
	ModelCardVersion int               `json:"ModelCardVersion"`
}

func cloneModelCard(c *ModelCard) *ModelCard {
	cp := *c
	cp.Tags = maps.Clone(c.Tags)

	return &cp
}

// CreateModelCard creates a model card.
func (b *InMemoryBackend) CreateModelCard(
	name, content string,
	tags map[string]string,
) (*ModelCard, error) {
	b.mu.Lock("CreateModelCard")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", ErrValidation)
	}

	if _, ok := b.modelCards[name]; ok {
		return nil, fmt.Errorf("%w: model card %q already exists", ErrValidation, name)
	}

	cardARN := arn.Build("sagemaker", b.region, b.accountID, "model-card/"+name)
	now := time.Now()

	c := &ModelCard{
		ModelCardName:    name,
		ModelCardArn:     cardARN,
		ModelCardStatus:  "Draft",
		ModelCardVersion: 1,
		Content:          content,
		Tags:             mergeTags(nil, tags),
		CreationTime:     now,
		LastModifiedTime: now,
	}
	b.modelCards[name] = c

	return cloneModelCard(c), nil
}

// DescribeModelCard returns a model card by name.
func (b *InMemoryBackend) DescribeModelCard(name string) (*ModelCard, error) {
	b.mu.RLock("DescribeModelCard")
	defer b.mu.RUnlock()

	c, ok := b.modelCards[name]
	if !ok {
		return nil, fmt.Errorf("%w: model card %q not found", ErrModelCardNotFound, name)
	}

	return cloneModelCard(c), nil
}

// UpdateModelCard updates a model card content and increments its version.
func (b *InMemoryBackend) UpdateModelCard(name, content string) (*ModelCard, error) {
	b.mu.Lock("UpdateModelCard")
	defer b.mu.Unlock()

	c, ok := b.modelCards[name]
	if !ok {
		return nil, fmt.Errorf("%w: model card %q not found", ErrModelCardNotFound, name)
	}

	c.Content = content
	c.ModelCardVersion++
	c.LastModifiedTime = time.Now()

	return cloneModelCard(c), nil
}

// DeleteModelCard removes a model card by name.
func (b *InMemoryBackend) DeleteModelCard(name string) error {
	b.mu.Lock("DeleteModelCard")
	defer b.mu.Unlock()

	if _, ok := b.modelCards[name]; !ok {
		return fmt.Errorf("%w: model card %q not found", ErrModelCardNotFound, name)
	}

	delete(b.modelCards, name)

	return nil
}

// ---------------------------------------------------------------------------
// OptimizationJob
// ---------------------------------------------------------------------------

// OptimizationJob represents a SageMaker optimization job.
type OptimizationJob struct {
	CreationTime          time.Time         `json:"CreationTime"`
	LastModifiedTime      time.Time         `json:"LastModifiedTime"`
	Tags                  map[string]string `json:"Tags,omitempty"`
	OptimizationJobName   string            `json:"OptimizationJobName"`
	OptimizationJobArn    string            `json:"OptimizationJobArn"`
	OptimizationJobStatus string            `json:"OptimizationJobStatus"`
	RoleArn               string            `json:"RoleArn,omitempty"`
}

func cloneOptimizationJob(j *OptimizationJob) *OptimizationJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)

	return &cp
}

// CreateOptimizationJob creates an optimization job.
func (b *InMemoryBackend) CreateOptimizationJob(
	name, roleArn string,
	tags map[string]string,
) (*OptimizationJob, error) {
	b.mu.Lock("CreateOptimizationJob")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: OptimizationJobName is required", ErrValidation)
	}

	if _, ok := b.optimizationJobs[name]; ok {
		return nil, fmt.Errorf("%w: optimization job %q already exists", ErrValidation, name)
	}

	jobARN := arn.Build("sagemaker", b.region, b.accountID, "optimization-job/"+name)
	now := time.Now()

	j := &OptimizationJob{
		OptimizationJobName:   name,
		OptimizationJobArn:    jobARN,
		OptimizationJobStatus: "COMPLETED",
		RoleArn:               roleArn,
		Tags:                  mergeTags(nil, tags),
		CreationTime:          now,
		LastModifiedTime:      now,
	}
	b.optimizationJobs[name] = j

	return cloneOptimizationJob(j), nil
}

// DescribeOptimizationJob returns an optimization job by name.
func (b *InMemoryBackend) DescribeOptimizationJob(name string) (*OptimizationJob, error) {
	b.mu.RLock("DescribeOptimizationJob")
	defer b.mu.RUnlock()

	j, ok := b.optimizationJobs[name]
	if !ok {
		return nil, fmt.Errorf("%w: optimization job %q not found", ErrOptimizationJobNotFound, name)
	}

	return cloneOptimizationJob(j), nil
}

// DeleteOptimizationJob removes an optimization job by name.
func (b *InMemoryBackend) DeleteOptimizationJob(name string) error {
	b.mu.Lock("DeleteOptimizationJob")
	defer b.mu.Unlock()

	if _, ok := b.optimizationJobs[name]; !ok {
		return fmt.Errorf("%w: optimization job %q not found", ErrOptimizationJobNotFound, name)
	}

	delete(b.optimizationJobs, name)

	return nil
}

// StopOptimizationJob sets an optimization job status to "STOPPED".
func (b *InMemoryBackend) StopOptimizationJob(name string) error {
	b.mu.Lock("StopOptimizationJob")
	defer b.mu.Unlock()

	j, ok := b.optimizationJobs[name]
	if !ok {
		return fmt.Errorf("%w: optimization job %q not found", ErrOptimizationJobNotFound, name)
	}

	j.OptimizationJobStatus = jobStatusStopped
	j.LastModifiedTime = time.Now()

	return nil
}

// ---------------------------------------------------------------------------
// StudioLifecycleConfig
// ---------------------------------------------------------------------------

// StudioLifecycleConfig represents a SageMaker Studio lifecycle configuration.
type StudioLifecycleConfig struct {
	CreationTime                 time.Time         `json:"CreationTime"`
	LastModifiedTime             time.Time         `json:"LastModifiedTime"`
	Tags                         map[string]string `json:"Tags,omitempty"`
	StudioLifecycleConfigName    string            `json:"StudioLifecycleConfigName"`
	StudioLifecycleConfigArn     string            `json:"StudioLifecycleConfigArn"`
	StudioLifecycleConfigAppType string            `json:"StudioLifecycleConfigAppType,omitempty"`
}

func cloneStudioLifecycleConfig(s *StudioLifecycleConfig) *StudioLifecycleConfig {
	cp := *s
	cp.Tags = maps.Clone(s.Tags)

	return &cp
}

// CreateStudioLifecycleConfig creates a Studio lifecycle configuration.
func (b *InMemoryBackend) CreateStudioLifecycleConfig(
	name, appType string,
	tags map[string]string,
) (*StudioLifecycleConfig, error) {
	b.mu.Lock("CreateStudioLifecycleConfig")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: StudioLifecycleConfigName is required", ErrValidation)
	}

	if _, ok := b.studioLifecycleConfigs[name]; ok {
		return nil, fmt.Errorf("%w: Studio lifecycle config %q already exists", ErrValidation, name)
	}

	configARN := arn.Build("sagemaker", b.region, b.accountID, "studio-lifecycle-config/"+name)
	now := time.Now()

	s := &StudioLifecycleConfig{
		StudioLifecycleConfigName:    name,
		StudioLifecycleConfigArn:     configARN,
		StudioLifecycleConfigAppType: appType,
		Tags:                         mergeTags(nil, tags),
		CreationTime:                 now,
		LastModifiedTime:             now,
	}
	b.studioLifecycleConfigs[name] = s

	return cloneStudioLifecycleConfig(s), nil
}

// DescribeStudioLifecycleConfig returns a Studio lifecycle configuration by name.
func (b *InMemoryBackend) DescribeStudioLifecycleConfig(name string) (*StudioLifecycleConfig, error) {
	b.mu.RLock("DescribeStudioLifecycleConfig")
	defer b.mu.RUnlock()

	s, ok := b.studioLifecycleConfigs[name]
	if !ok {
		return nil, fmt.Errorf("%w: Studio lifecycle config %q not found", ErrStudioLifecycleConfigNotFound, name)
	}

	return cloneStudioLifecycleConfig(s), nil
}

// DeleteStudioLifecycleConfig removes a Studio lifecycle configuration by name.
func (b *InMemoryBackend) DeleteStudioLifecycleConfig(name string) error {
	b.mu.Lock("DeleteStudioLifecycleConfig")
	defer b.mu.Unlock()

	if _, ok := b.studioLifecycleConfigs[name]; !ok {
		return fmt.Errorf("%w: Studio lifecycle config %q not found", ErrStudioLifecycleConfigNotFound, name)
	}

	delete(b.studioLifecycleConfigs, name)

	return nil
}

// ---------------------------------------------------------------------------
// PartnerApp
// ---------------------------------------------------------------------------

// PartnerApp represents a SageMaker partner app.
type PartnerApp struct {
	CreationTime time.Time         `json:"CreationTime"`
	Tags         map[string]string `json:"Tags,omitempty"`
	Name         string            `json:"Name"`
	Arn          string            `json:"Arn"`
	Status       string            `json:"Status"`
	Type         string            `json:"Type,omitempty"`
}

func clonePartnerApp(p *PartnerApp) *PartnerApp {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)

	return &cp
}

// CreatePartnerApp creates a partner app. Stores by ARN; returns both name and ARN.
func (b *InMemoryBackend) CreatePartnerApp(
	name, appType string,
	tags map[string]string,
) (*PartnerApp, error) {
	b.mu.Lock("CreatePartnerApp")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	appARN := arn.Build("sagemaker", b.region, b.accountID, "partner-app/"+name)

	if _, ok := b.partnerApps[appARN]; ok {
		return nil, fmt.Errorf("%w: partner app %q already exists", ErrValidation, name)
	}

	p := &PartnerApp{
		Name:         name,
		Arn:          appARN,
		Status:       "Available",
		Type:         appType,
		Tags:         mergeTags(nil, tags),
		CreationTime: time.Now(),
	}
	b.partnerApps[appARN] = p

	return clonePartnerApp(p), nil
}

// DescribePartnerApp returns a partner app by ARN.
func (b *InMemoryBackend) DescribePartnerApp(arnStr string) (*PartnerApp, error) {
	b.mu.RLock("DescribePartnerApp")
	defer b.mu.RUnlock()

	p, ok := b.partnerApps[arnStr]
	if !ok {
		return nil, fmt.Errorf("%w: partner app %q not found", ErrPartnerAppNotFound, arnStr)
	}

	return clonePartnerApp(p), nil
}

// DeletePartnerApp removes a partner app by ARN.
func (b *InMemoryBackend) DeletePartnerApp(arnStr string) error {
	b.mu.Lock("DeletePartnerApp")
	defer b.mu.Unlock()

	if _, ok := b.partnerApps[arnStr]; !ok {
		return fmt.Errorf("%w: partner app %q not found", ErrPartnerAppNotFound, arnStr)
	}

	delete(b.partnerApps, arnStr)

	return nil
}

// ---------------------------------------------------------------------------
// TrainingPlan
// ---------------------------------------------------------------------------

// TrainingPlan represents a SageMaker training plan.
type TrainingPlan struct {
	CreationTime     time.Time         `json:"CreationTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	TrainingPlanName string            `json:"TrainingPlanName"`
	TrainingPlanArn  string            `json:"TrainingPlanArn"`
	Status           string            `json:"Status"`
}

func cloneTrainingPlan(t *TrainingPlan) *TrainingPlan {
	cp := *t
	cp.Tags = maps.Clone(t.Tags)

	return &cp
}

// CreateTrainingPlan creates a training plan.
func (b *InMemoryBackend) CreateTrainingPlan(
	name string,
	tags map[string]string,
) (*TrainingPlan, error) {
	b.mu.Lock("CreateTrainingPlan")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: TrainingPlanName is required", ErrValidation)
	}

	if _, ok := b.trainingPlans[name]; ok {
		return nil, fmt.Errorf("%w: training plan %q already exists", ErrValidation, name)
	}

	planARN := arn.Build("sagemaker", b.region, b.accountID, "training-plan/"+name)

	t := &TrainingPlan{
		TrainingPlanName: name,
		TrainingPlanArn:  planARN,
		Status:           statusActive,
		Tags:             mergeTags(nil, tags),
		CreationTime:     time.Now(),
	}
	b.trainingPlans[name] = t

	return cloneTrainingPlan(t), nil
}

// DescribeTrainingPlan returns a training plan by name.
func (b *InMemoryBackend) DescribeTrainingPlan(name string) (*TrainingPlan, error) {
	b.mu.RLock("DescribeTrainingPlan")
	defer b.mu.RUnlock()

	t, ok := b.trainingPlans[name]
	if !ok {
		return nil, fmt.Errorf("%w: training plan %q not found", ErrTrainingPlanNotFound, name)
	}

	return cloneTrainingPlan(t), nil
}
