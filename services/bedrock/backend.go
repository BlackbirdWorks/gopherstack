package bedrock

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	textTypeText = "TEXT"
)

const bedrockDefaultPageSize = 100

// Resource lifecycle status constants.
const (
	statusCreating   = "Creating"
	statusInService  = "InService"
	statusInProgress = "InProgress"
	statusCompleted  = "Completed"
	statusStopped    = "Stopped"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when request validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// Tag represents a key-value tag on a Bedrock resource.
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Guardrail represents an Amazon Bedrock guardrail.
type Guardrail struct {
	CreatedAt               time.Time `json:"createdAt"`
	UpdatedAt               time.Time `json:"updatedAt"`
	GuardrailID             string    `json:"guardrailId"`
	GuardrailArn            string    `json:"guardrailArn"`
	Name                    string    `json:"name"`
	Description             string    `json:"description,omitempty"`
	Status                  string    `json:"status"`
	Version                 string    `json:"version"`
	BlockedInputMessaging   string    `json:"blockedInputMessaging,omitempty"`
	BlockedOutputsMessaging string    `json:"blockedOutputsMessaging,omitempty"`
	Tags                    []Tag     `json:"tags,omitempty"`
	// versionCounter tracks the next version number for this specific guardrail.
	versionCounter int
}

// GuardrailSummary is used in list operations.
type GuardrailSummary struct {
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
	GuardrailID string    `json:"id"`
	Arn         string    `json:"arn"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Status      string    `json:"status"`
	Version     string    `json:"version"`
}

// ProvisionedModelThroughput represents a provisioned model throughput resource.
type ProvisionedModelThroughput struct {
	CreationTime         time.Time `json:"creationTime"`
	LastModifiedTime     time.Time `json:"lastModifiedTime"`
	ProvisionedModelArn  string    `json:"provisionedModelArn"`
	ProvisionedModelName string    `json:"provisionedModelName"`
	ModelArn             string    `json:"modelArn"`
	DesiredModelArn      string    `json:"desiredModelArn"`
	FoundationModelArn   string    `json:"foundationModelArn"`
	Status               string    `json:"status"`
	CommitmentDuration   string    `json:"commitmentDuration,omitempty"`
	Tags                 []Tag     `json:"tags,omitempty"`
	ModelUnits           int32     `json:"modelUnits"`
	DesiredModelUnits    int32     `json:"desiredModelUnits"`
}

// FoundationModelSummary represents a foundation model.
type FoundationModelSummary struct {
	ModelArn         string   `json:"modelArn"`
	ModelID          string   `json:"modelId"`
	ModelName        string   `json:"modelName"`
	ProviderName     string   `json:"providerName"`
	InputModalities  []string `json:"inputModalities,omitempty"`
	OutputModalities []string `json:"outputModalities,omitempty"`
}

// EvaluationJob represents a model evaluation job.
type EvaluationJob struct {
	CreationTime     time.Time `json:"creationTime"`
	LastModifiedTime time.Time `json:"lastModifiedTime"`
	JobArn           string    `json:"jobArn"`
	JobName          string    `json:"jobName"`
	Status           string    `json:"status"`
	Tags             []Tag     `json:"tags,omitempty"`
}

// AutomatedReasoningPolicy represents an Automated Reasoning policy.
type AutomatedReasoningPolicy struct {
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
	PolicyArn   string    `json:"policyArn"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Status      string    `json:"status"`
	Tags        []Tag     `json:"tags,omitempty"`
}

// AutomatedReasoningPolicyBuildWorkflow represents a build workflow for a policy.
type AutomatedReasoningPolicyBuildWorkflow struct {
	BuildWorkflowID string `json:"buildWorkflowId"`
	PolicyArn       string `json:"policyArn"`
	Status          string `json:"status"`
}

// AutomatedReasoningPolicyTestCase represents a test case for a policy.
type AutomatedReasoningPolicyTestCase struct {
	TestCaseID string `json:"testCaseId"`
	PolicyArn  string `json:"policyArn"`
}

// AutomatedReasoningPolicyVersion represents a version of a policy.
type AutomatedReasoningPolicyVersion struct {
	CreatedAt      time.Time `json:"createdAt"`
	PolicyArn      string    `json:"policyArn"`
	Name           string    `json:"name"`
	DefinitionHash string    `json:"definitionHash"`
	Version        string    `json:"version"`
}

// CustomModel represents a custom model.
type CustomModel struct {
	CreationTime time.Time `json:"creationTime"`
	ModelArn     string    `json:"modelArn"`
	ModelName    string    `json:"modelName"`
	Tags         []Tag     `json:"tags,omitempty"`
}

// CustomModelDeployment represents a custom model deployment.
type CustomModelDeployment struct {
	CreationTime             time.Time `json:"creationTime"`
	LastModifiedTime         time.Time `json:"lastModifiedTime"`
	CustomModelDeploymentArn string    `json:"customModelDeploymentArn"`
	ModelDeploymentName      string    `json:"modelDeploymentName"`
	ModelArn                 string    `json:"modelArn"`
	Status                   string    `json:"status"`
	Tags                     []Tag     `json:"tags,omitempty"`
}

// FoundationModelAgreement represents an agreement for foundation model access.
type FoundationModelAgreement struct {
	ModelID string `json:"modelId"`
}

// GuardrailVersion represents a specific version of a guardrail.
type GuardrailVersion struct {
	GuardrailID string `json:"guardrailId"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
}

// ModelCopyJob represents a model copy job.
type ModelCopyJob struct {
	CreationTime     time.Time `json:"creationTime"`
	LastModifiedTime time.Time `json:"lastModifiedTime"`
	JobArn           string    `json:"jobArn"`
	SourceModelArn   string    `json:"sourceModelArn"`
	TargetModelArn   string    `json:"targetModelArn"`
	Status           string    `json:"status"`
	FailureMessage   string    `json:"failureMessage,omitempty"`
	Tags             []Tag     `json:"tags,omitempty"`
}

// ModelImportJob represents a model import job.
type ModelImportJob struct {
	CreationTime     time.Time  `json:"creationTime"`
	LastModifiedTime time.Time  `json:"lastModifiedTime"`
	EndTime          *time.Time `json:"endTime,omitempty"`
	JobArn           string     `json:"jobArn"`
	JobName          string     `json:"jobName"`
	ImportedModelArn string     `json:"importedModelArn"`
	Status           string     `json:"status"`
	Tags             []Tag      `json:"tags,omitempty"`
}

// ModelCustomizationJob represents a model customization job.
type ModelCustomizationJob struct {
	CreationTime      time.Time `json:"creationTime"`
	LastModifiedTime  time.Time `json:"lastModifiedTime"`
	EndTime           time.Time `json:"endTime"`
	JobArn            string    `json:"jobArn"`
	JobName           string    `json:"jobName"`
	BaseModelArn      string    `json:"baseModelArn"`
	OutputModelArn    string    `json:"outputModelArn"`
	Status            string    `json:"status"`
	CustomizationType string    `json:"customizationType,omitempty"`
	Tags              []Tag     `json:"tags,omitempty"`
}

// InferenceProfile represents an inference profile resource.
type InferenceProfile struct {
	CreatedAt            time.Time `json:"createdAt"`
	UpdatedAt            time.Time `json:"updatedAt"`
	InferenceProfileArn  string    `json:"inferenceProfileArn"`
	InferenceProfileID   string    `json:"inferenceProfileId"`
	InferenceProfileName string    `json:"inferenceProfileName"`
	Status               string    `json:"status"`
	Type                 string    `json:"type"`
	Description          string    `json:"description,omitempty"`
	Tags                 []Tag     `json:"tags,omitempty"`
}

// MarketplaceModelEndpoint represents a marketplace model endpoint.
type MarketplaceModelEndpoint struct {
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
	EndpointArn   string    `json:"endpointArn"`
	EndpointName  string    `json:"endpointName"`
	ModelSourceID string    `json:"modelSourceIdentifier"`
	Status        string    `json:"status"`
	Tags          []Tag     `json:"tags,omitempty"`
}

// ModelInvocationLoggingConfiguration represents the logging configuration.
type ModelInvocationLoggingConfiguration struct {
	S3BucketName   string `json:"s3BucketName,omitempty"`
	LoggingEnabled bool   `json:"loggingEnabled"`
}

// InMemoryBackend stores Amazon Bedrock state in memory.
type InMemoryBackend struct {
	guardrails                  map[string]*Guardrail
	guardrailVersions           map[string]*GuardrailVersion // guardrailID+":"+version → version
	provisionedModelThroughputs map[string]*ProvisionedModelThroughput
	evaluationJobs              map[string]*EvaluationJob
	automatedReasoningPolicies  map[string]*AutomatedReasoningPolicy
	arpBuildWorkflows           map[string]*AutomatedReasoningPolicyBuildWorkflow // workflowID → workflow
	arpTestCases                map[string]*AutomatedReasoningPolicyTestCase      // testCaseID → test case
	arpVersions                 map[string]*AutomatedReasoningPolicyVersion       // policyArn+":"+version → version
	arpVersionCountByPolicy     map[string]int                                    // policyARN → per-policy version counter
	customModels                map[string]*CustomModel                           // modelArn → model
	customModelDeployments      map[string]*CustomModelDeployment                 // deploymentArn → deployment
	foundationModelAgreements   map[string]*FoundationModelAgreement              // modelID → agreement
	modelCustomizationJobs      map[string]*ModelCustomizationJob                 // jobArn → job
	modelCopyJobs               map[string]*ModelCopyJob                          // jobArn → job
	modelImportJobs             map[string]*ModelImportJob                        // jobArn → job
	inferenceProfiles           map[string]*InferenceProfile                      // profileArn → profile
	marketplaceEndpoints        map[string]*MarketplaceModelEndpoint              // endpointArn → endpoint
	loggingConfig               *ModelInvocationLoggingConfiguration
	guardrailsByName            map[string]string // guardrail name → ID
	guardrailsByARN             map[string]string // guardrail ARN → ID
	pmtsByName                  map[string]string // PMT name → ARN
	arpByName                   map[string]string // policy name → ARN
	customModelsByName          map[string]string // model name → ARN
	customModelDeployByName     map[string]string // deployment name → ARN
	evaluationJobsByName        map[string]string // job name → ARN
	customizationJobsByName     map[string]string // job name → ARN
	inferenceProfilesByName     map[string]string // profile name → ARN
	marketplaceEndpointsByName  map[string]string // endpoint name → ARN
	// Agents
	agents                     map[string]*Agent
	agentsByName               map[string]string                         // agentName → agentID
	agentActionGroups          map[string]*AgentActionGroup              // agentID/actionGroupID → group
	agentAliases               map[string]*AgentAlias                    // agentID/aliasID → alias
	agentKBAssociations        map[string]*AgentKnowledgeBaseAssociation // agentID/kbID → assoc
	knowledgeBases             map[string]*KnowledgeBase                 // kbID → kb
	kbByName                   map[string]string                         // kbName → kbID
	dataSources                map[string]*DataSource                    // kbID/dsID → ds
	ingestionJobs              map[string]*IngestionJob                  // kbID/dsID/jobID → job
	mu                         *lockmetrics.RWMutex
	accountID                  string
	region                     string
	foundationModels           []*FoundationModelSummary
	guardrailCounter           int
	guardrailVersionCounter    int
	provisionedCounter         int
	evaluationJobCounter       int
	arpCounter                 int
	arpWorkflowCounter         int
	arpTestCaseCounter         int
	customModelCounter         int
	customModelDeployCounter   int
	customizationJobCounter    int
	copyJobCounter             int
	importJobCounter           int
	inferenceProfileCounter    int
	marketplaceEndpointCounter int
	agentCounter               int
	actionGroupCounter         int
	agentAliasCounter          int
	kbCounter                  int
	dataSourceCounter          int
	ingestionJobCounter        int
}

// NewInMemoryBackend creates a new InMemoryBackend pre-seeded with foundation models.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		guardrails:                  make(map[string]*Guardrail),
		guardrailVersions:           make(map[string]*GuardrailVersion),
		provisionedModelThroughputs: make(map[string]*ProvisionedModelThroughput),
		evaluationJobs:              make(map[string]*EvaluationJob),
		automatedReasoningPolicies:  make(map[string]*AutomatedReasoningPolicy),
		arpBuildWorkflows:           make(map[string]*AutomatedReasoningPolicyBuildWorkflow),
		arpTestCases:                make(map[string]*AutomatedReasoningPolicyTestCase),
		arpVersions:                 make(map[string]*AutomatedReasoningPolicyVersion),
		arpVersionCountByPolicy:     make(map[string]int),
		customModels:                make(map[string]*CustomModel),
		customModelDeployments:      make(map[string]*CustomModelDeployment),
		foundationModelAgreements:   make(map[string]*FoundationModelAgreement),
		modelCustomizationJobs:      make(map[string]*ModelCustomizationJob),
		modelCopyJobs:               make(map[string]*ModelCopyJob),
		modelImportJobs:             make(map[string]*ModelImportJob),
		inferenceProfiles:           make(map[string]*InferenceProfile),
		marketplaceEndpoints:        make(map[string]*MarketplaceModelEndpoint),
		guardrailsByName:            make(map[string]string),
		guardrailsByARN:             make(map[string]string),
		pmtsByName:                  make(map[string]string),
		arpByName:                   make(map[string]string),
		customModelsByName:          make(map[string]string),
		customModelDeployByName:     make(map[string]string),
		evaluationJobsByName:        make(map[string]string),
		customizationJobsByName:     make(map[string]string),
		inferenceProfilesByName:     make(map[string]string),
		marketplaceEndpointsByName:  make(map[string]string),
		agents:                      make(map[string]*Agent),
		agentsByName:                make(map[string]string),
		agentActionGroups:           make(map[string]*AgentActionGroup),
		agentAliases:                make(map[string]*AgentAlias),
		agentKBAssociations:         make(map[string]*AgentKnowledgeBaseAssociation),
		knowledgeBases:              make(map[string]*KnowledgeBase),
		kbByName:                    make(map[string]string),
		dataSources:                 make(map[string]*DataSource),
		ingestionJobs:               make(map[string]*IngestionJob),
		accountID:                   accountID,
		region:                      region,
		mu:                          lockmetrics.New("bedrock"),
	}
	b.seedFoundationModels()

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state, returning the backend to its initial seeded state.
// The accountID, region, and seeded foundation models are preserved.
//
//nolint:funlen // Reset must clear all maps; the length is proportional to the number of resource types
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.guardrails = make(map[string]*Guardrail)
	b.guardrailVersions = make(map[string]*GuardrailVersion)
	b.provisionedModelThroughputs = make(map[string]*ProvisionedModelThroughput)
	b.evaluationJobs = make(map[string]*EvaluationJob)
	b.automatedReasoningPolicies = make(map[string]*AutomatedReasoningPolicy)
	b.arpBuildWorkflows = make(map[string]*AutomatedReasoningPolicyBuildWorkflow)
	b.arpTestCases = make(map[string]*AutomatedReasoningPolicyTestCase)
	b.arpVersions = make(map[string]*AutomatedReasoningPolicyVersion)
	b.arpVersionCountByPolicy = make(map[string]int)
	b.customModels = make(map[string]*CustomModel)
	b.customModelDeployments = make(map[string]*CustomModelDeployment)
	b.foundationModelAgreements = make(map[string]*FoundationModelAgreement)
	b.modelCustomizationJobs = make(map[string]*ModelCustomizationJob)
	b.modelCopyJobs = make(map[string]*ModelCopyJob)
	b.modelImportJobs = make(map[string]*ModelImportJob)
	b.inferenceProfiles = make(map[string]*InferenceProfile)
	b.marketplaceEndpoints = make(map[string]*MarketplaceModelEndpoint)
	b.loggingConfig = nil
	b.guardrailsByName = make(map[string]string)
	b.guardrailsByARN = make(map[string]string)
	b.pmtsByName = make(map[string]string)
	b.arpByName = make(map[string]string)
	b.customModelsByName = make(map[string]string)
	b.customModelDeployByName = make(map[string]string)
	b.evaluationJobsByName = make(map[string]string)
	b.customizationJobsByName = make(map[string]string)
	b.inferenceProfilesByName = make(map[string]string)
	b.marketplaceEndpointsByName = make(map[string]string)
	b.agents = make(map[string]*Agent)
	b.agentsByName = make(map[string]string)
	b.agentActionGroups = make(map[string]*AgentActionGroup)
	b.agentAliases = make(map[string]*AgentAlias)
	b.agentKBAssociations = make(map[string]*AgentKnowledgeBaseAssociation)
	b.knowledgeBases = make(map[string]*KnowledgeBase)
	b.kbByName = make(map[string]string)
	b.dataSources = make(map[string]*DataSource)
	b.ingestionJobs = make(map[string]*IngestionJob)
	b.agentCounter = 0
	b.actionGroupCounter = 0
	b.agentAliasCounter = 0
	b.kbCounter = 0
	b.dataSourceCounter = 0
	b.ingestionJobCounter = 0
	b.guardrailCounter = 0
	b.guardrailVersionCounter = 0
	b.provisionedCounter = 0
	b.evaluationJobCounter = 0
	b.arpCounter = 0
	b.arpWorkflowCounter = 0
	b.arpTestCaseCounter = 0
	b.customModelCounter = 0
	b.customModelDeployCounter = 0
	b.customizationJobCounter = 0
	b.copyJobCounter = 0
	b.importJobCounter = 0
	b.inferenceProfileCounter = 0
	b.marketplaceEndpointCounter = 0
}

func (b *InMemoryBackend) seedFoundationModels() {
	partition := "aws"
	prefix := "arn:" + partition + ":bedrock::" + b.accountID + ":foundation-model/"

	b.foundationModels = []*FoundationModelSummary{
		{
			ModelID:          "amazon.titan-text-express-v1",
			ModelName:        "Titan Text G1 - Express",
			ProviderName:     "Amazon",
			ModelArn:         prefix + "amazon.titan-text-express-v1",
			InputModalities:  []string{textTypeText},
			OutputModalities: []string{textTypeText},
		},
		{
			ModelID:          "amazon.titan-embed-text-v1",
			ModelName:        "Titan Embeddings G1 - Text",
			ProviderName:     "Amazon",
			ModelArn:         prefix + "amazon.titan-embed-text-v1",
			InputModalities:  []string{textTypeText},
			OutputModalities: []string{"EMBEDDING"},
		},
		{
			ModelID:          "anthropic.claude-v2",
			ModelName:        "Claude",
			ProviderName:     "Anthropic",
			ModelArn:         prefix + "anthropic.claude-v2",
			InputModalities:  []string{textTypeText},
			OutputModalities: []string{textTypeText},
		},
		{
			ModelID:          "anthropic.claude-3-sonnet-20240229-v1:0",
			ModelName:        "Claude 3 Sonnet",
			ProviderName:     "Anthropic",
			ModelArn:         prefix + "anthropic.claude-3-sonnet-20240229-v1:0",
			InputModalities:  []string{textTypeText, "IMAGE"},
			OutputModalities: []string{textTypeText},
		},
		{
			ModelID:          "meta.llama3-8b-instruct-v1:0",
			ModelName:        "Llama 3 8B Instruct",
			ProviderName:     "Meta",
			ModelArn:         prefix + "meta.llama3-8b-instruct-v1:0",
			InputModalities:  []string{textTypeText},
			OutputModalities: []string{textTypeText},
		},
	}
}

// newGuardrailID generates a unique guardrail ID.
func (b *InMemoryBackend) newGuardrailID() string {
	b.guardrailCounter++

	return fmt.Sprintf("bedrock-guardrail-%07d", b.guardrailCounter)
}

// newProvisionedID generates a unique provisioned model throughput ID.
func (b *InMemoryBackend) newProvisionedID() string {
	b.provisionedCounter++

	return fmt.Sprintf("pmt-%07d", b.provisionedCounter)
}

// newEvaluationJobID generates a unique evaluation job ID.
func (b *InMemoryBackend) newEvaluationJobID() string {
	b.evaluationJobCounter++

	return fmt.Sprintf("eval-job-%07d", b.evaluationJobCounter)
}

// newARPID generates a unique automated reasoning policy ID.
func (b *InMemoryBackend) newARPID() string {
	b.arpCounter++

	return fmt.Sprintf("arp-%07d", b.arpCounter)
}

// newARPTestCaseID generates a unique test case ID.
func (b *InMemoryBackend) newARPTestCaseID() string {
	b.arpTestCaseCounter++

	return fmt.Sprintf("tc-%07d", b.arpTestCaseCounter)
}

// newARPVersionNum generates a monotonically increasing version number for a policy.
// Must be called with the write lock held.
func (b *InMemoryBackend) newARPVersionNum(policyARN string) string {
	b.arpVersionCountByPolicy[policyARN]++

	return strconv.Itoa(b.arpVersionCountByPolicy[policyARN])
}

// newCustomModelID generates a unique custom model ID.
func (b *InMemoryBackend) newCustomModelID() string {
	b.customModelCounter++

	return fmt.Sprintf("cm-%07d", b.customModelCounter)
}

// newCustomModelDeployID generates a unique custom model deployment ID.
func (b *InMemoryBackend) newCustomModelDeployID() string {
	b.customModelDeployCounter++

	return fmt.Sprintf("cmd-%07d", b.customModelDeployCounter)
}

// CreateGuardrail creates a new guardrail.
func (b *InMemoryBackend) CreateGuardrail(
	name, description, blockedInput, blockedOutput string,
	tags []Tag,
) (*Guardrail, error) {
	b.mu.Lock("CreateGuardrail")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if _, exists := b.guardrailsByName[name]; exists {
		return nil, fmt.Errorf("%w: guardrail %s already exists", ErrAlreadyExists, name)
	}

	id := b.newGuardrailID()
	guardrailARN := arn.Build("bedrock", b.region, b.accountID, "guardrail/"+id)
	now := time.Now().UTC()

	tagsCopy := make([]Tag, len(tags))
	copy(tagsCopy, tags)

	g := &Guardrail{
		GuardrailID:             id,
		GuardrailArn:            guardrailARN,
		Name:                    name,
		Description:             description,
		Status:                  "READY",
		Version:                 "DRAFT",
		BlockedInputMessaging:   blockedInput,
		BlockedOutputsMessaging: blockedOutput,
		Tags:                    tagsCopy,
		CreatedAt:               now,
		UpdatedAt:               now,
	}
	b.guardrails[id] = g
	b.guardrailsByName[name] = id
	b.guardrailsByARN[guardrailARN] = id
	cp := *g

	return &cp, nil
}
func (b *InMemoryBackend) GetGuardrail(idOrARN string) (*Guardrail, error) {
	b.mu.RLock("GetGuardrail")
	defer b.mu.RUnlock()

	g, ok := b.findGuardrailByIDOrARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: guardrail %s not found", ErrNotFound, idOrARN)
	}

	cp := *g
	cp.Tags = copyTags(g.Tags)

	return &cp, nil
}

// ListGuardrails returns guardrails with optional pagination.
// If guardrailIdentifier is non-empty, results are filtered to guardrails whose
// ID, ARN, or name equals the identifier (case-sensitive).
func (b *InMemoryBackend) ListGuardrails(nextToken, guardrailIdentifier string) ([]*GuardrailSummary, string) {
	b.mu.RLock("ListGuardrails")
	defer b.mu.RUnlock()

	list := make([]*GuardrailSummary, 0, len(b.guardrails))

	for _, g := range b.guardrails {
		if guardrailIdentifier != "" &&
			g.GuardrailID != guardrailIdentifier &&
			g.GuardrailArn != guardrailIdentifier &&
			g.Name != guardrailIdentifier {
			continue
		}

		list = append(list, &GuardrailSummary{
			GuardrailID: g.GuardrailID,
			Arn:         g.GuardrailArn,
			Name:        g.Name,
			Description: g.Description,
			Status:      g.Status,
			Version:     g.Version,
			CreatedAt:   g.CreatedAt,
			UpdatedAt:   g.UpdatedAt,
		})
	}

	sort.Slice(list, func(i, j int) bool { return list[i].GuardrailID < list[j].GuardrailID })

	return paginateBedrockSlice(list, nextToken)
}

// hasPublishedVersions reports whether a guardrail has any published (non-DRAFT) versions.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) hasPublishedVersions(guardrailID string) bool {
	for key := range b.guardrailVersions {
		prefix := guardrailID + ":"
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			return true
		}
	}

	return false
}

// UpdateGuardrail updates a guardrail's name, description and messaging.
// Mutations are rejected when the guardrail has published (numbered) versions,
// as AWS rejects updates to guardrails that have been versioned.
func (b *InMemoryBackend) UpdateGuardrail(
	idOrARN, name, description, blockedInput, blockedOutput string,
) (*Guardrail, error) {
	b.mu.Lock("UpdateGuardrail")
	defer b.mu.Unlock()

	g, ok := b.findGuardrailByIDOrARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: guardrail %s not found", ErrNotFound, idOrARN)
	}

	if b.hasPublishedVersions(g.GuardrailID) {
		return nil, fmt.Errorf(
			"%w: guardrail %s has published versions and cannot be mutated",
			ErrAlreadyExists,
			idOrARN,
		)
	}

	// Update name with index maintenance.
	if name != "" && name != g.Name {
		if _, exists := b.guardrailsByName[name]; exists {
			return nil, fmt.Errorf("%w: guardrail name %s already in use", ErrAlreadyExists, name)
		}

		delete(b.guardrailsByName, g.Name)
		b.guardrailsByName[name] = g.GuardrailID
		g.Name = name
	}

	if description != "" {
		g.Description = description
	}

	if blockedInput != "" {
		g.BlockedInputMessaging = blockedInput
	}

	if blockedOutput != "" {
		g.BlockedOutputsMessaging = blockedOutput
	}

	g.UpdatedAt = time.Now().UTC()
	cp := *g
	cp.Tags = copyTags(g.Tags)

	return &cp, nil
}

// DeleteGuardrail removes a guardrail by ID or ARN.
func (b *InMemoryBackend) DeleteGuardrail(idOrARN string) error {
	b.mu.Lock("DeleteGuardrail")
	defer b.mu.Unlock()

	g, ok := b.findGuardrailByIDOrARN(idOrARN)
	if !ok {
		return fmt.Errorf("%w: guardrail %s not found", ErrNotFound, idOrARN)
	}

	delete(b.guardrails, g.GuardrailID)
	delete(b.guardrailsByName, g.Name)
	delete(b.guardrailsByARN, g.GuardrailArn)

	return nil
}

// findGuardrailByIDOrARN finds a guardrail by ID or ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findGuardrailByIDOrARN(idOrARN string) (*Guardrail, bool) {
	if g, ok := b.guardrails[idOrARN]; ok {
		return g, true
	}

	if id, ok := b.guardrailsByARN[idOrARN]; ok {
		return b.guardrails[id], true
	}

	return nil, false
}

// ListFoundationModels returns seeded foundation models with optional pagination.
func (b *InMemoryBackend) ListFoundationModels(nextToken string) ([]*FoundationModelSummary, string) {
	b.mu.RLock("ListFoundationModels")
	defer b.mu.RUnlock()

	list := make([]*FoundationModelSummary, len(b.foundationModels))
	copy(list, b.foundationModels)

	return paginateBedrockSlice(list, nextToken)
}

// GetFoundationModel returns a single foundation model by ID.
func (b *InMemoryBackend) GetFoundationModel(modelID string) (*FoundationModelSummary, error) {
	b.mu.RLock("GetFoundationModel")
	defer b.mu.RUnlock()

	for _, m := range b.foundationModels {
		if m.ModelID == modelID {
			cp := *m

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: foundation model %s not found", ErrNotFound, modelID)
}

// CreateProvisionedModelThroughput creates a new provisioned model throughput.
func (b *InMemoryBackend) CreateProvisionedModelThroughput(
	name, modelID string,
	modelUnits int32,
	commitmentDuration string,
	tags []Tag,
) (*ProvisionedModelThroughput, error) {
	b.mu.Lock("CreateProvisionedModelThroughput")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: provisionedModelName is required", ErrValidation)
	}

	if modelUnits <= 0 {
		return nil, fmt.Errorf("%w: modelUnits must be greater than 0", ErrValidation)
	}

	if _, exists := b.pmtsByName[name]; exists {
		return nil, fmt.Errorf("%w: provisioned model throughput %s already exists", ErrAlreadyExists, name)
	}

	id := b.newProvisionedID()
	pmtARN := arn.Build("bedrock", b.region, b.accountID, "provisioned-model/"+id)
	modelARN := arn.Build("bedrock", b.region, b.accountID, "foundation-model/"+modelID)
	now := time.Now().UTC()

	tagsCopy := make([]Tag, len(tags))
	copy(tagsCopy, tags)

	pmt := &ProvisionedModelThroughput{
		ProvisionedModelArn:  pmtARN,
		ProvisionedModelName: name,
		ModelArn:             modelARN,
		DesiredModelArn:      modelARN,
		FoundationModelArn:   modelARN,
		Status:               statusInService,
		ModelUnits:           modelUnits,
		DesiredModelUnits:    modelUnits,
		CommitmentDuration:   commitmentDuration,
		CreationTime:         now,
		LastModifiedTime:     now,
		Tags:                 tagsCopy,
	}
	b.provisionedModelThroughputs[pmtARN] = pmt
	b.pmtsByName[name] = pmtARN
	cp := *pmt

	return &cp, nil
}
func (b *InMemoryBackend) GetProvisionedModelThroughput(idOrARN string) (*ProvisionedModelThroughput, error) {
	b.mu.RLock("GetProvisionedModelThroughput")
	defer b.mu.RUnlock()

	pmt, ok := b.findPMTByIDOrARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: provisioned model throughput %s not found", ErrNotFound, idOrARN)
	}

	cp := *pmt

	return &cp, nil
}

// ListProvisionedModelThroughputs returns provisioned model throughputs with optional pagination.
func (b *InMemoryBackend) ListProvisionedModelThroughputs(nextToken string) ([]*ProvisionedModelThroughput, string) {
	b.mu.RLock("ListProvisionedModelThroughputs")
	defer b.mu.RUnlock()

	list := make([]*ProvisionedModelThroughput, 0, len(b.provisionedModelThroughputs))

	for _, pmt := range b.provisionedModelThroughputs {
		cp := *pmt
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ProvisionedModelArn < list[j].ProvisionedModelArn })

	return paginateBedrockSlice(list, nextToken)
}

// UpdateProvisionedModelThroughput updates a provisioned model throughput.
func (b *InMemoryBackend) UpdateProvisionedModelThroughput(
	idOrARN, modelID string,
	modelUnits *int32,
) (*ProvisionedModelThroughput, error) {
	b.mu.Lock("UpdateProvisionedModelThroughput")
	defer b.mu.Unlock()

	pmt, ok := b.findPMTByIDOrARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: provisioned model throughput %s not found", ErrNotFound, idOrARN)
	}

	if modelID != "" {
		modelARN := arn.Build("bedrock", b.region, b.accountID, "foundation-model/"+modelID)
		pmt.DesiredModelArn = modelARN
	}

	if modelUnits != nil {
		pmt.DesiredModelUnits = *modelUnits
	}

	pmt.LastModifiedTime = time.Now().UTC()
	cp := *pmt

	return &cp, nil
}

// DeleteProvisionedModelThroughput removes a provisioned model throughput by ID or ARN.
func (b *InMemoryBackend) DeleteProvisionedModelThroughput(idOrARN string) error {
	b.mu.Lock("DeleteProvisionedModelThroughput")
	defer b.mu.Unlock()

	pmt, ok := b.findPMTByIDOrARN(idOrARN)
	if !ok {
		return fmt.Errorf("%w: provisioned model throughput %s not found", ErrNotFound, idOrARN)
	}

	delete(b.provisionedModelThroughputs, pmt.ProvisionedModelArn)
	delete(b.pmtsByName, pmt.ProvisionedModelName)

	return nil
}

// AdvanceProvisionedModelThroughputStatuses transitions PMTs from Creating → InService.
// Called by the janitor after the creation delay has elapsed.
func (b *InMemoryBackend) AdvanceProvisionedModelThroughputStatuses() int {
	b.mu.Lock("AdvanceProvisionedModelThroughputStatuses")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	advanced := 0

	for _, pmt := range b.provisionedModelThroughputs {
		if pmt.Status != statusCreating {
			continue
		}
		pmt.Status = statusInService
		pmt.ModelArn = pmt.DesiredModelArn
		pmt.ModelUnits = pmt.DesiredModelUnits
		pmt.LastModifiedTime = now
		advanced++
	}

	return advanced
}

// findPMTByIDOrARN finds a provisioned model throughput by ID or ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findPMTByIDOrARN(idOrARN string) (*ProvisionedModelThroughput, bool) {
	if pmt, ok := b.provisionedModelThroughputs[idOrARN]; ok {
		return pmt, true
	}

	if pmtARN, ok := b.pmtsByName[idOrARN]; ok {
		return b.provisionedModelThroughputs[pmtARN], true
	}

	return nil, false
}

// copyTags returns a new slice with a deep copy of tags.
func copyTags(src []Tag) []Tag {
	if len(src) == 0 {
		return nil
	}

	dst := make([]Tag, len(src))
	copy(dst, src)

	return dst
}

// mergeTags merges new tags into existing ones and returns a sorted result.
// Existing tags with the same key are overwritten by the new values.
func mergeTags(existing, newTags []Tag) []Tag {
	tagMap := make(map[string]string, len(existing)+len(newTags))
	for _, t := range existing {
		tagMap[t.Key] = t.Value
	}

	for _, t := range newTags {
		tagMap[t.Key] = t.Value
	}

	merged := make([]Tag, 0, len(tagMap))
	for k, v := range tagMap {
		merged = append(merged, Tag{Key: k, Value: v})
	}

	sort.Slice(merged, func(i, j int) bool { return merged[i].Key < merged[j].Key })

	return merged
}

// filterTags returns a new slice with the specified keys removed.
func filterTags(existing []Tag, removeKeys map[string]bool) []Tag {
	result := make([]Tag, 0, len(existing))
	for _, t := range existing {
		if !removeKeys[t.Key] {
			result = append(result, t)
		}
	}

	return result
}

// ListTagsForResource returns tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) ([]Tag, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	tags, ok := b.findTagsByARN(resourceARN)
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	return copyTags(tags), nil
}

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags []Tag) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if id, ok := b.guardrailsByARN[resourceARN]; ok {
		g := b.guardrails[id]
		g.Tags = mergeTags(g.Tags, tags)

		return nil
	}

	if pmt, ok := b.provisionedModelThroughputs[resourceARN]; ok {
		pmt.Tags = mergeTags(pmt.Tags, tags)

		return nil
	}

	if job, ok := b.evaluationJobs[resourceARN]; ok {
		job.Tags = mergeTags(job.Tags, tags)

		return nil
	}

	if policy, ok := b.automatedReasoningPolicies[resourceARN]; ok {
		policy.Tags = mergeTags(policy.Tags, tags)

		return nil
	}

	if model, ok := b.customModels[resourceARN]; ok {
		model.Tags = mergeTags(model.Tags, tags)

		return nil
	}

	if deployment, ok := b.customModelDeployments[resourceARN]; ok {
		deployment.Tags = mergeTags(deployment.Tags, tags)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// UntagResource removes tags from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	removeSet := make(map[string]bool, len(tagKeys))
	for _, k := range tagKeys {
		removeSet[k] = true
	}

	if id, ok := b.guardrailsByARN[resourceARN]; ok {
		g := b.guardrails[id]
		g.Tags = filterTags(g.Tags, removeSet)

		return nil
	}

	if pmt, ok := b.provisionedModelThroughputs[resourceARN]; ok {
		pmt.Tags = filterTags(pmt.Tags, removeSet)

		return nil
	}

	if job, ok := b.evaluationJobs[resourceARN]; ok {
		job.Tags = filterTags(job.Tags, removeSet)

		return nil
	}

	if policy, ok := b.automatedReasoningPolicies[resourceARN]; ok {
		policy.Tags = filterTags(policy.Tags, removeSet)

		return nil
	}

	if model, ok := b.customModels[resourceARN]; ok {
		model.Tags = filterTags(model.Tags, removeSet)

		return nil
	}

	if deployment, ok := b.customModelDeployments[resourceARN]; ok {
		deployment.Tags = filterTags(deployment.Tags, removeSet)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// findTagsByARN returns the tags slice pointer for a resource by ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findTagsByARN(resourceARN string) ([]Tag, bool) {
	if id, ok := b.guardrailsByARN[resourceARN]; ok {
		return b.guardrails[id].Tags, true
	}

	if pmt, ok := b.provisionedModelThroughputs[resourceARN]; ok {
		return pmt.Tags, true
	}

	if job, ok := b.evaluationJobs[resourceARN]; ok {
		return job.Tags, true
	}

	if policy, ok := b.automatedReasoningPolicies[resourceARN]; ok {
		return policy.Tags, true
	}

	if model, ok := b.customModels[resourceARN]; ok {
		return model.Tags, true
	}

	if deployment, ok := b.customModelDeployments[resourceARN]; ok {
		return deployment.Tags, true
	}

	return nil, false
}

// --- EvaluationJob methods ---

// CreateEvaluationJob creates a new evaluation job.
func (b *InMemoryBackend) CreateEvaluationJob(name string, tags []Tag) (*EvaluationJob, error) {
	b.mu.Lock("CreateEvaluationJob")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: jobName is required", ErrValidation)
	}

	if _, exists := b.evaluationJobsByName[name]; exists {
		return nil, fmt.Errorf("%w: evaluation job %s already exists", ErrAlreadyExists, name)
	}

	id := b.newEvaluationJobID()
	jobARN := arn.Build("bedrock", b.region, b.accountID, "evaluation-job/"+id)
	now := time.Now().UTC()

	job := &EvaluationJob{
		JobArn:           jobARN,
		JobName:          name,
		Status:           statusInProgress,
		CreationTime:     now,
		LastModifiedTime: now,
		Tags:             copyTags(tags),
	}
	b.evaluationJobs[jobARN] = job
	b.evaluationJobsByName[name] = jobARN
	cp := *job
	cp.Tags = copyTags(job.Tags)

	return &cp, nil
}

// BatchDeleteEvaluationJobError describes a single job deletion failure.
type BatchDeleteEvaluationJobError struct {
	JobARN  string `json:"jobIdentifier"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// BatchDeleteEvaluationJobItem describes a successfully scheduled deletion.
type BatchDeleteEvaluationJobItem struct {
	JobARN string `json:"jobIdentifier"`
	Status string `json:"jobStatus"`
}

// BatchDeleteEvaluationJob deletes multiple evaluation jobs.
func (b *InMemoryBackend) BatchDeleteEvaluationJob(jobARNs []string) (
	[]BatchDeleteEvaluationJobError, []BatchDeleteEvaluationJobItem, error,
) {
	b.mu.Lock("BatchDeleteEvaluationJob")
	defer b.mu.Unlock()

	if len(jobARNs) == 0 {
		return nil, nil, fmt.Errorf("%w: jobIdentifiers must not be empty", ErrValidation)
	}

	var errs []BatchDeleteEvaluationJobError

	deleted := make([]BatchDeleteEvaluationJobItem, 0, len(jobARNs))

	for _, jobARN := range jobARNs {
		job, ok := b.evaluationJobs[jobARN]
		if !ok {
			errs = append(errs, BatchDeleteEvaluationJobError{
				JobARN:  jobARN,
				Code:    "ResourceNotFoundException",
				Message: fmt.Sprintf("evaluation job %s not found", jobARN),
			})

			continue
		}

		delete(b.evaluationJobsByName, job.JobName)
		delete(b.evaluationJobs, jobARN)
		deleted = append(deleted, BatchDeleteEvaluationJobItem{
			JobARN: jobARN,
			Status: "Deleting",
		})
	}

	if errs == nil {
		errs = []BatchDeleteEvaluationJobError{}
	}

	if deleted == nil {
		deleted = []BatchDeleteEvaluationJobItem{}
	}

	return errs, deleted, nil
}

// --- AutomatedReasoningPolicy methods ---

// CreateAutomatedReasoningPolicy creates a new Automated Reasoning policy.
func (b *InMemoryBackend) CreateAutomatedReasoningPolicy(
	name, description string,
	tags []Tag,
) (*AutomatedReasoningPolicy, error) {
	b.mu.Lock("CreateAutomatedReasoningPolicy")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if _, exists := b.arpByName[name]; exists {
		return nil, fmt.Errorf("%w: automated reasoning policy %s already exists", ErrAlreadyExists, name)
	}

	id := b.newARPID()
	policyARN := arn.Build("bedrock", b.region, b.accountID, "automated-reasoning-policy/"+id)
	now := time.Now().UTC()

	policy := &AutomatedReasoningPolicy{
		PolicyArn:   policyARN,
		Name:        name,
		Description: description,
		Status:      "ACTIVE",
		CreatedAt:   now,
		UpdatedAt:   now,
		Tags:        copyTags(tags),
	}
	b.automatedReasoningPolicies[policyARN] = policy
	b.arpByName[name] = policyARN
	cp := *policy
	cp.Tags = copyTags(policy.Tags)

	return &cp, nil
}

// CancelAutomatedReasoningPolicyBuildWorkflow cancels a running build workflow.
func (b *InMemoryBackend) CancelAutomatedReasoningPolicyBuildWorkflow(policyARN, workflowID string) error {
	b.mu.Lock("CancelAutomatedReasoningPolicyBuildWorkflow")
	defer b.mu.Unlock()

	if _, ok := b.automatedReasoningPolicies[policyARN]; !ok {
		return fmt.Errorf("%w: automated reasoning policy %s not found", ErrNotFound, policyARN)
	}

	wf, ok := b.arpBuildWorkflows[workflowID]
	if !ok {
		return fmt.Errorf("%w: build workflow %s not found", ErrNotFound, workflowID)
	}

	if wf.PolicyArn != policyARN {
		return fmt.Errorf("%w: build workflow %s does not belong to policy %s", ErrNotFound, workflowID, policyARN)
	}

	wf.Status = "Cancelled"

	return nil
}

// CreateAutomatedReasoningPolicyTestCase creates a test case for an Automated Reasoning policy.
func (b *InMemoryBackend) CreateAutomatedReasoningPolicyTestCase(
	policyARN string,
) (*AutomatedReasoningPolicyTestCase, error) {
	b.mu.Lock("CreateAutomatedReasoningPolicyTestCase")
	defer b.mu.Unlock()

	if _, ok := b.automatedReasoningPolicies[policyARN]; !ok {
		return nil, fmt.Errorf("%w: automated reasoning policy %s not found", ErrNotFound, policyARN)
	}

	id := b.newARPTestCaseID()
	tc := &AutomatedReasoningPolicyTestCase{
		TestCaseID: id,
		PolicyArn:  policyARN,
	}
	b.arpTestCases[id] = tc
	cp := *tc

	return &cp, nil
}

// CreateAutomatedReasoningPolicyVersion creates a new version of an Automated Reasoning policy.
func (b *InMemoryBackend) CreateAutomatedReasoningPolicyVersion(
	policyARN, definitionHash string,
) (*AutomatedReasoningPolicyVersion, error) {
	b.mu.Lock("CreateAutomatedReasoningPolicyVersion")
	defer b.mu.Unlock()

	policy, ok := b.automatedReasoningPolicies[policyARN]
	if !ok {
		return nil, fmt.Errorf("%w: automated reasoning policy %s not found", ErrNotFound, policyARN)
	}

	// Use per-policy version counter for realistic monotonic versioning.
	versionNum := b.newARPVersionNum(policyARN)
	versionedARN := policyARN + "/version/" + versionNum

	version := &AutomatedReasoningPolicyVersion{
		PolicyArn:      versionedARN,
		Name:           policy.Name,
		DefinitionHash: definitionHash,
		Version:        versionNum,
		CreatedAt:      time.Now().UTC(),
	}
	key := policyARN + ":" + versionNum
	b.arpVersions[key] = version
	cp := *version

	return &cp, nil
}

// --- CustomModel methods ---

// CreateCustomModel creates a new custom model.
func (b *InMemoryBackend) CreateCustomModel(modelName string, tags []Tag) (*CustomModel, error) {
	b.mu.Lock("CreateCustomModel")
	defer b.mu.Unlock()

	if modelName == "" {
		return nil, fmt.Errorf("%w: modelName is required", ErrValidation)
	}

	if _, exists := b.customModelsByName[modelName]; exists {
		return nil, fmt.Errorf("%w: custom model %s already exists", ErrAlreadyExists, modelName)
	}

	id := b.newCustomModelID()
	modelARN := arn.Build("bedrock", b.region, b.accountID, "custom-model/"+id)

	model := &CustomModel{
		ModelArn:     modelARN,
		ModelName:    modelName,
		CreationTime: time.Now().UTC(),
		Tags:         copyTags(tags),
	}
	b.customModels[modelARN] = model
	b.customModelsByName[modelName] = modelARN
	cp := *model
	cp.Tags = copyTags(model.Tags)

	return &cp, nil
}

// --- CustomModelDeployment methods ---

// CreateCustomModelDeployment creates a new deployment for a custom model.
func (b *InMemoryBackend) CreateCustomModelDeployment(
	modelARN, deploymentName string,
	tags []Tag,
) (*CustomModelDeployment, error) {
	b.mu.Lock("CreateCustomModelDeployment")
	defer b.mu.Unlock()

	if modelARN == "" {
		return nil, fmt.Errorf("%w: modelArn is required", ErrValidation)
	}

	if deploymentName == "" {
		return nil, fmt.Errorf("%w: modelDeploymentName is required", ErrValidation)
	}

	if _, exists := b.customModelDeployByName[deploymentName]; exists {
		return nil, fmt.Errorf("%w: custom model deployment %s already exists", ErrAlreadyExists, deploymentName)
	}

	id := b.newCustomModelDeployID()
	deploymentARN := arn.Build("bedrock", b.region, b.accountID, "custom-model-deployment/"+id)
	now := time.Now().UTC()

	deployment := &CustomModelDeployment{
		CustomModelDeploymentArn: deploymentARN,
		ModelDeploymentName:      deploymentName,
		ModelArn:                 modelARN,
		Status:                   statusCreating,
		CreationTime:             now,
		LastModifiedTime:         now,
		Tags:                     copyTags(tags),
	}
	b.customModelDeployments[deploymentARN] = deployment
	b.customModelDeployByName[deploymentName] = deploymentARN
	cp := *deployment
	cp.Tags = copyTags(deployment.Tags)

	return &cp, nil
}

// --- FoundationModelAgreement methods ---

// CreateFoundationModelAgreement creates a foundation model access agreement.
func (b *InMemoryBackend) CreateFoundationModelAgreement(modelID string) (*FoundationModelAgreement, error) {
	b.mu.Lock("CreateFoundationModelAgreement")
	defer b.mu.Unlock()

	if modelID == "" {
		return nil, fmt.Errorf("%w: modelId is required", ErrValidation)
	}

	agreement := &FoundationModelAgreement{
		ModelID: modelID,
	}
	b.foundationModelAgreements[modelID] = agreement
	cp := *agreement

	return &cp, nil
}

// --- GetCustomModel / ListCustomModels / DeleteCustomModel methods ---

// findCustomModelARN resolves a custom model ID or name to its ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findCustomModelARN(idOrARN string) (string, bool) {
	if _, ok := b.customModels[idOrARN]; ok {
		return idOrARN, true
	}

	if a := b.customModelsByName[idOrARN]; a != "" {
		return a, true
	}

	return "", false
}

// resolveCustomModelARN resolves to ARN or returns ErrNotFound.
func (b *InMemoryBackend) resolveCustomModelARN(idOrARN string) (string, error) {
	if a, ok := b.findCustomModelARN(idOrARN); ok {
		return a, nil
	}

	return "", fmt.Errorf("%w: custom model %s not found", ErrNotFound, idOrARN)
}

// GetCustomModel returns a custom model by ARN or name.
func (b *InMemoryBackend) GetCustomModel(idOrARN string) (*CustomModel, error) {
	b.mu.RLock("GetCustomModel")
	defer b.mu.RUnlock()

	modelARN, err := b.resolveCustomModelARN(idOrARN)
	if err != nil {
		return nil, err
	}

	m := b.customModels[modelARN]
	cp := *m
	cp.Tags = copyTags(m.Tags)

	return &cp, nil
}

// ListCustomModels returns all custom models with optional pagination.
func (b *InMemoryBackend) ListCustomModels(nextToken string) ([]*CustomModel, string) {
	b.mu.RLock("ListCustomModels")
	defer b.mu.RUnlock()

	list := make([]*CustomModel, 0, len(b.customModels))

	for _, m := range b.customModels {
		cp := *m
		cp.Tags = copyTags(m.Tags)
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ModelArn < list[j].ModelArn })

	return paginateBedrockSlice(list, nextToken)
}

// DeleteCustomModel removes a custom model by ARN or name.
func (b *InMemoryBackend) DeleteCustomModel(idOrARN string) error {
	b.mu.Lock("DeleteCustomModel")
	defer b.mu.Unlock()

	modelARN, err := b.resolveCustomModelARN(idOrARN)
	if err != nil {
		return err
	}

	m := b.customModels[modelARN]
	delete(b.customModelsByName, m.ModelName)
	delete(b.customModels, modelARN)

	return nil
}

// --- ModelCustomizationJob methods ---

// newCustomizationJobID generates a unique customization job ID.
func (b *InMemoryBackend) newCustomizationJobID() string {
	b.customizationJobCounter++

	return fmt.Sprintf("cj-%07d", b.customizationJobCounter)
}

// CreateModelCustomizationJob creates a new model customization job.
func (b *InMemoryBackend) CreateModelCustomizationJob(
	jobName, baseModelID, customizationType string,
	tags []Tag,
) (*ModelCustomizationJob, error) {
	b.mu.Lock("CreateModelCustomizationJob")
	defer b.mu.Unlock()

	if jobName == "" {
		return nil, fmt.Errorf("%w: jobName is required", ErrValidation)
	}

	if _, exists := b.customizationJobsByName[jobName]; exists {
		return nil, fmt.Errorf("%w: customization job %s already exists", ErrAlreadyExists, jobName)
	}

	id := b.newCustomizationJobID()
	jobARN := arn.Build("bedrock", b.region, b.accountID, "model-customization-job/"+id)
	outputModelARN := arn.Build("bedrock", b.region, b.accountID, "custom-model/output-"+id)
	baseModelARN := arn.Build("bedrock", b.region, b.accountID, "foundation-model/"+baseModelID)
	now := time.Now().UTC()

	job := &ModelCustomizationJob{
		JobArn:            jobARN,
		JobName:           jobName,
		BaseModelArn:      baseModelARN,
		OutputModelArn:    outputModelARN,
		Status:            statusInProgress,
		CustomizationType: customizationType,
		CreationTime:      now,
		LastModifiedTime:  now,
		Tags:              copyTags(tags),
	}
	b.modelCustomizationJobs[jobARN] = job
	b.customizationJobsByName[jobName] = jobARN
	cp := *job
	cp.Tags = copyTags(job.Tags)

	return &cp, nil
}

// findCustomizationJobARN resolves a job ID or name to its ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findCustomizationJobARN(idOrARN string) (string, bool) {
	if _, ok := b.modelCustomizationJobs[idOrARN]; ok {
		return idOrARN, true
	}

	if a := b.customizationJobsByName[idOrARN]; a != "" {
		return a, true
	}

	return "", false
}

// GetModelCustomizationJob returns a model customization job by ARN or name.
func (b *InMemoryBackend) GetModelCustomizationJob(idOrARN string) (*ModelCustomizationJob, error) {
	b.mu.RLock("GetModelCustomizationJob")
	defer b.mu.RUnlock()

	jobARN, ok := b.findCustomizationJobARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: model customization job %s not found", ErrNotFound, idOrARN)
	}

	j := b.modelCustomizationJobs[jobARN]
	cp := *j
	cp.Tags = copyTags(j.Tags)

	return &cp, nil
}

// ListModelCustomizationJobs returns all customization jobs with optional pagination.
func (b *InMemoryBackend) ListModelCustomizationJobs(nextToken string) ([]*ModelCustomizationJob, string) {
	b.mu.RLock("ListModelCustomizationJobs")
	defer b.mu.RUnlock()

	list := make([]*ModelCustomizationJob, 0, len(b.modelCustomizationJobs))

	for _, j := range b.modelCustomizationJobs {
		cp := *j
		cp.Tags = copyTags(j.Tags)
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].JobArn < list[j].JobArn })

	return paginateBedrockSlice(list, nextToken)
}

// StopModelCustomizationJob stops a running customization job.
func (b *InMemoryBackend) StopModelCustomizationJob(idOrARN string) error {
	b.mu.Lock("StopModelCustomizationJob")
	defer b.mu.Unlock()

	jobARN, ok := b.findCustomizationJobARN(idOrARN)
	if !ok {
		return fmt.Errorf("%w: model customization job %s not found", ErrNotFound, idOrARN)
	}

	b.modelCustomizationJobs[jobARN].Status = statusStopped

	return nil
}

// AdvanceCustomizationJobStatuses moves InProgress customization jobs to Completed.
// Called by the janitor after the simulated training delay has elapsed.
func (b *InMemoryBackend) AdvanceCustomizationJobStatuses(minAge time.Duration) int {
	b.mu.Lock("AdvanceCustomizationJobStatuses")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	advanced := 0

	for _, job := range b.modelCustomizationJobs {
		if job.Status != statusInProgress {
			continue
		}

		if now.Sub(job.CreationTime) >= minAge {
			job.Status = statusCompleted
			job.LastModifiedTime = now
			job.EndTime = now
			advanced++
		}
	}

	return advanced
}

// --- ModelCopyJob methods ---

// CreateModelCopyJob creates a new model copy job.
//
//nolint:dupl // Identical structure to CreateModelImportJob; different types.
func (b *InMemoryBackend) CreateModelCopyJob(sourceModelARN string, tags []Tag) (*ModelCopyJob, error) {
	if sourceModelARN == "" {
		return nil, fmt.Errorf("%w: sourceModelArn is required", ErrValidation)
	}

	b.mu.Lock("CreateModelCopyJob")
	defer b.mu.Unlock()

	b.copyJobCounter++
	id := fmt.Sprintf("mcj-%07d", b.copyJobCounter)
	jobARN := arn.Build("bedrock", b.region, b.accountID, "model-copy-job/"+id)
	targetModelARN := arn.Build("bedrock", b.region, b.accountID, "custom-model/copy-"+id)
	now := time.Now().UTC()

	job := &ModelCopyJob{
		JobArn:           jobARN,
		SourceModelArn:   sourceModelARN,
		TargetModelArn:   targetModelARN,
		Status:           statusInProgress,
		CreationTime:     now,
		LastModifiedTime: now,
		Tags:             copyTags(tags),
	}
	b.modelCopyJobs[jobARN] = job

	cp := *job
	cp.Tags = copyTags(job.Tags)

	return &cp, nil
}

// GetModelCopyJob returns a model copy job by ARN.
func (b *InMemoryBackend) GetModelCopyJob(jobARN string) (*ModelCopyJob, error) {
	b.mu.RLock("GetModelCopyJob")
	defer b.mu.RUnlock()

	job, ok := b.modelCopyJobs[jobARN]
	if !ok {
		return nil, fmt.Errorf("%w: model copy job %s not found", ErrNotFound, jobARN)
	}

	cp := *job
	cp.Tags = copyTags(job.Tags)

	return &cp, nil
}

// ListModelCopyJobs returns all model copy jobs sorted by creation time.
func (b *InMemoryBackend) ListModelCopyJobs() []*ModelCopyJob {
	b.mu.RLock("ListModelCopyJobs")
	defer b.mu.RUnlock()

	list := make([]*ModelCopyJob, 0, len(b.modelCopyJobs))

	for _, j := range b.modelCopyJobs {
		cp := *j
		cp.Tags = copyTags(j.Tags)
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, k int) bool { return list[i].CreationTime.Before(list[k].CreationTime) })

	return list
}

// --- ModelImportJob methods ---

// CreateModelImportJob creates a new model import job.
//
//nolint:dupl // Identical structure to CreateModelCopyJob; different types.
func (b *InMemoryBackend) CreateModelImportJob(jobName string, tags []Tag) (*ModelImportJob, error) {
	if jobName == "" {
		return nil, fmt.Errorf("%w: jobName is required", ErrValidation)
	}

	b.mu.Lock("CreateModelImportJob")
	defer b.mu.Unlock()

	b.importJobCounter++
	id := fmt.Sprintf("mij-%07d", b.importJobCounter)
	jobARN := arn.Build("bedrock", b.region, b.accountID, "model-import-job/"+id)
	importedModelARN := arn.Build("bedrock", b.region, b.accountID, "imported-model/"+id)
	now := time.Now().UTC()

	job := &ModelImportJob{
		JobArn:           jobARN,
		JobName:          jobName,
		ImportedModelArn: importedModelARN,
		Status:           statusInProgress,
		CreationTime:     now,
		LastModifiedTime: now,
		Tags:             copyTags(tags),
	}
	b.modelImportJobs[jobARN] = job

	cp := *job
	cp.Tags = copyTags(job.Tags)

	return &cp, nil
}

// GetModelImportJob returns a model import job by ARN.
func (b *InMemoryBackend) GetModelImportJob(jobARN string) (*ModelImportJob, error) {
	b.mu.RLock("GetModelImportJob")
	defer b.mu.RUnlock()

	job, ok := b.modelImportJobs[jobARN]
	if !ok {
		return nil, fmt.Errorf("%w: model import job %s not found", ErrNotFound, jobARN)
	}

	cp := *job
	cp.Tags = copyTags(job.Tags)

	return &cp, nil
}

// ListModelImportJobs returns all model import jobs sorted by creation time.
func (b *InMemoryBackend) ListModelImportJobs() []*ModelImportJob {
	b.mu.RLock("ListModelImportJobs")
	defer b.mu.RUnlock()

	list := make([]*ModelImportJob, 0, len(b.modelImportJobs))

	for _, j := range b.modelImportJobs {
		cp := *j
		cp.Tags = copyTags(j.Tags)
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, k int) bool { return list[i].CreationTime.Before(list[k].CreationTime) })

	return list
}

// AdvanceCopyImportJobStatuses moves InProgress copy/import jobs to Completed after the min age elapses.
func (b *InMemoryBackend) AdvanceCopyImportJobStatuses(minAge time.Duration) int {
	b.mu.Lock("AdvanceCopyImportJobStatuses")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	advanced := 0

	for _, job := range b.modelCopyJobs {
		if job.Status == statusInProgress && now.Sub(job.CreationTime) >= minAge {
			job.Status = statusCompleted
			job.LastModifiedTime = now
			advanced++
		}
	}

	for _, job := range b.modelImportJobs {
		if job.Status == statusInProgress && now.Sub(job.CreationTime) >= minAge {
			endTime := now
			job.Status = "Complete"
			job.LastModifiedTime = now
			job.EndTime = &endTime
			advanced++
		}
	}

	return advanced
}

// --- InferenceProfile methods ---

// newInferenceProfileID generates a unique inference profile ID.
func (b *InMemoryBackend) newInferenceProfileID() string {
	b.inferenceProfileCounter++

	return fmt.Sprintf("ip-%07d", b.inferenceProfileCounter)
}

// CreateInferenceProfile creates a new inference profile.
func (b *InMemoryBackend) CreateInferenceProfile(
	name, description string,
	tags []Tag,
) (*InferenceProfile, error) {
	b.mu.Lock("CreateInferenceProfile")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: inferenceProfileName is required", ErrValidation)
	}

	if _, exists := b.inferenceProfilesByName[name]; exists {
		return nil, fmt.Errorf("%w: inference profile %s already exists", ErrAlreadyExists, name)
	}

	id := b.newInferenceProfileID()
	profileARN := arn.Build("bedrock", b.region, b.accountID, "inference-profile/"+id)
	now := time.Now().UTC()

	profile := &InferenceProfile{
		InferenceProfileArn:  profileARN,
		InferenceProfileID:   id,
		InferenceProfileName: name,
		Description:          description,
		Status:               "ACTIVE",
		Type:                 "APPLICATION",
		CreatedAt:            now,
		UpdatedAt:            now,
		Tags:                 copyTags(tags),
	}
	b.inferenceProfiles[profileARN] = profile
	b.inferenceProfilesByName[name] = profileARN
	cp := *profile
	cp.Tags = copyTags(profile.Tags)

	return &cp, nil
}

// findInferenceProfileARN resolves a profile ID or name to its ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findInferenceProfileARN(idOrARN string) (string, bool) {
	if _, ok := b.inferenceProfiles[idOrARN]; ok {
		return idOrARN, true
	}

	if a := b.inferenceProfilesByName[idOrARN]; a != "" {
		return a, true
	}

	return "", false
}

// GetInferenceProfile returns an inference profile by ARN or name.
func (b *InMemoryBackend) GetInferenceProfile(idOrARN string) (*InferenceProfile, error) {
	b.mu.RLock("GetInferenceProfile")
	defer b.mu.RUnlock()

	profileARN, ok := b.findInferenceProfileARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: inference profile %s not found", ErrNotFound, idOrARN)
	}

	p := b.inferenceProfiles[profileARN]
	cp := *p
	cp.Tags = copyTags(p.Tags)

	return &cp, nil
}

// ListInferenceProfiles returns all inference profiles with optional pagination.
func (b *InMemoryBackend) ListInferenceProfiles(nextToken string) ([]*InferenceProfile, string) {
	b.mu.RLock("ListInferenceProfiles")
	defer b.mu.RUnlock()

	list := make([]*InferenceProfile, 0, len(b.inferenceProfiles))

	for _, p := range b.inferenceProfiles {
		cp := *p
		cp.Tags = copyTags(p.Tags)
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].InferenceProfileArn < list[j].InferenceProfileArn
	})

	return paginateBedrockSlice(list, nextToken)
}

// DeleteInferenceProfile removes an inference profile by ARN or name.
func (b *InMemoryBackend) DeleteInferenceProfile(idOrARN string) error {
	b.mu.Lock("DeleteInferenceProfile")
	defer b.mu.Unlock()

	profileARN, ok := b.findInferenceProfileARN(idOrARN)
	if !ok {
		return fmt.Errorf("%w: inference profile %s not found", ErrNotFound, idOrARN)
	}

	p := b.inferenceProfiles[profileARN]
	delete(b.inferenceProfilesByName, p.InferenceProfileName)
	delete(b.inferenceProfiles, profileARN)

	return nil
}

// --- MarketplaceModelEndpoint methods ---

// newMarketplaceEndpointID generates a unique marketplace endpoint ID.
func (b *InMemoryBackend) newMarketplaceEndpointID() string {
	b.marketplaceEndpointCounter++

	return fmt.Sprintf("mme-%07d", b.marketplaceEndpointCounter)
}

// CreateMarketplaceModelEndpoint creates a new marketplace model endpoint.
func (b *InMemoryBackend) CreateMarketplaceModelEndpoint(
	endpointName, modelSourceID string,
	tags []Tag,
) (*MarketplaceModelEndpoint, error) {
	b.mu.Lock("CreateMarketplaceModelEndpoint")
	defer b.mu.Unlock()

	if endpointName == "" {
		return nil, fmt.Errorf("%w: endpointName is required", ErrValidation)
	}

	if _, exists := b.marketplaceEndpointsByName[endpointName]; exists {
		return nil, fmt.Errorf("%w: marketplace endpoint %s already exists", ErrAlreadyExists, endpointName)
	}

	id := b.newMarketplaceEndpointID()
	endpointARN := arn.Build("bedrock", b.region, b.accountID, "marketplace-model-endpoint/"+id)
	now := time.Now().UTC()

	ep := &MarketplaceModelEndpoint{
		EndpointArn:   endpointARN,
		EndpointName:  endpointName,
		ModelSourceID: modelSourceID,
		Status:        statusCreating,
		CreatedAt:     now,
		UpdatedAt:     now,
		Tags:          copyTags(tags),
	}
	b.marketplaceEndpoints[endpointARN] = ep
	b.marketplaceEndpointsByName[endpointName] = endpointARN
	cp := *ep
	cp.Tags = copyTags(ep.Tags)

	return &cp, nil
}

// findMarketplaceEndpointARN resolves an endpoint ID or name to its ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findMarketplaceEndpointARN(idOrARN string) (string, bool) {
	if _, ok := b.marketplaceEndpoints[idOrARN]; ok {
		return idOrARN, true
	}

	if a := b.marketplaceEndpointsByName[idOrARN]; a != "" {
		return a, true
	}

	return "", false
}

// GetMarketplaceModelEndpoint returns a marketplace endpoint by ARN or name.
func (b *InMemoryBackend) GetMarketplaceModelEndpoint(idOrARN string) (*MarketplaceModelEndpoint, error) {
	b.mu.RLock("GetMarketplaceModelEndpoint")
	defer b.mu.RUnlock()

	epARN, ok := b.findMarketplaceEndpointARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: marketplace endpoint %s not found", ErrNotFound, idOrARN)
	}

	ep := b.marketplaceEndpoints[epARN]
	cp := *ep
	cp.Tags = copyTags(ep.Tags)

	return &cp, nil
}

// ListMarketplaceModelEndpoints returns all marketplace endpoints with optional pagination.
func (b *InMemoryBackend) ListMarketplaceModelEndpoints(nextToken string) ([]*MarketplaceModelEndpoint, string) {
	b.mu.RLock("ListMarketplaceModelEndpoints")
	defer b.mu.RUnlock()

	list := make([]*MarketplaceModelEndpoint, 0, len(b.marketplaceEndpoints))

	for _, ep := range b.marketplaceEndpoints {
		cp := *ep
		cp.Tags = copyTags(ep.Tags)
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].EndpointArn < list[j].EndpointArn })

	return paginateBedrockSlice(list, nextToken)
}

// DeleteMarketplaceModelEndpoint removes a marketplace endpoint by ARN or name.
func (b *InMemoryBackend) DeleteMarketplaceModelEndpoint(idOrARN string) error {
	b.mu.Lock("DeleteMarketplaceModelEndpoint")
	defer b.mu.Unlock()

	epARN, ok := b.findMarketplaceEndpointARN(idOrARN)
	if !ok {
		return fmt.Errorf("%w: marketplace endpoint %s not found", ErrNotFound, idOrARN)
	}

	ep := b.marketplaceEndpoints[epARN]
	delete(b.marketplaceEndpointsByName, ep.EndpointName)
	delete(b.marketplaceEndpoints, epARN)

	return nil
}

// UpdateMarketplaceModelEndpoint updates a marketplace endpoint status.
func (b *InMemoryBackend) UpdateMarketplaceModelEndpoint(idOrARN string) (*MarketplaceModelEndpoint, error) {
	b.mu.Lock("UpdateMarketplaceModelEndpoint")
	defer b.mu.Unlock()

	epARN, ok := b.findMarketplaceEndpointARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: marketplace endpoint %s not found", ErrNotFound, idOrARN)
	}

	ep := b.marketplaceEndpoints[epARN]
	ep.UpdatedAt = time.Now().UTC()
	cp := *ep
	cp.Tags = copyTags(ep.Tags)

	return &cp, nil
}

// RegisterMarketplaceModelEndpoint transitions endpoint status to Active.
func (b *InMemoryBackend) RegisterMarketplaceModelEndpoint(idOrARN string) error {
	b.mu.Lock("RegisterMarketplaceModelEndpoint")
	defer b.mu.Unlock()

	epARN, ok := b.findMarketplaceEndpointARN(idOrARN)
	if !ok {
		return fmt.Errorf("%w: marketplace endpoint %s not found", ErrNotFound, idOrARN)
	}

	b.marketplaceEndpoints[epARN].Status = "Active"

	return nil
}

// DeregisterMarketplaceModelEndpoint transitions endpoint status to Deregistered.
func (b *InMemoryBackend) DeregisterMarketplaceModelEndpoint(idOrARN string) error {
	b.mu.Lock("DeregisterMarketplaceModelEndpoint")
	defer b.mu.Unlock()

	epARN, ok := b.findMarketplaceEndpointARN(idOrARN)
	if !ok {
		return fmt.Errorf("%w: marketplace endpoint %s not found", ErrNotFound, idOrARN)
	}

	b.marketplaceEndpoints[epARN].Status = "Deregistered"

	return nil
}

// --- ModelInvocationLoggingConfiguration methods ---

// GetModelInvocationLoggingConfiguration returns the current logging configuration.
func (b *InMemoryBackend) GetModelInvocationLoggingConfiguration() *ModelInvocationLoggingConfiguration {
	b.mu.RLock("GetModelInvocationLoggingConfiguration")
	defer b.mu.RUnlock()

	if b.loggingConfig == nil {
		return &ModelInvocationLoggingConfiguration{}
	}

	cp := *b.loggingConfig

	return &cp
}

// PutModelInvocationLoggingConfiguration sets the logging configuration.
func (b *InMemoryBackend) PutModelInvocationLoggingConfiguration(cfg *ModelInvocationLoggingConfiguration) {
	b.mu.Lock("PutModelInvocationLoggingConfiguration")
	defer b.mu.Unlock()

	cp := *cfg
	b.loggingConfig = &cp
}

// DeleteModelInvocationLoggingConfiguration removes the logging configuration.
func (b *InMemoryBackend) DeleteModelInvocationLoggingConfiguration() {
	b.mu.Lock("DeleteModelInvocationLoggingConfiguration")
	defer b.mu.Unlock()

	b.loggingConfig = nil
}

// --- GuardrailVersion methods ---

// CreateGuardrailVersion creates a new numbered version snapshot of a guardrail.
// Each guardrail maintains its own monotonically increasing version counter.
func (b *InMemoryBackend) CreateGuardrailVersion(idOrARN, description string) (*GuardrailVersion, error) {
	b.mu.Lock("CreateGuardrailVersion")
	defer b.mu.Unlock()

	g, ok := b.findGuardrailByIDOrARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: guardrail %s not found", ErrNotFound, idOrARN)
	}

	// Use per-guardrail version counter for isolated, predictable numbering.
	g.versionCounter++
	versionNum := strconv.Itoa(g.versionCounter)

	gv := &GuardrailVersion{
		GuardrailID: g.GuardrailID,
		Version:     versionNum,
		Description: description,
	}

	key := g.GuardrailID + ":" + versionNum
	b.guardrailVersions[key] = gv

	return gv, nil
}

// paginateBedrockSlice applies pagination to a slice using an integer-offset NextToken.
func paginateBedrockSlice[T any](list []T, nextToken string) ([]T, string) {
	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []T{}, ""
	}
	end := startIdx + bedrockDefaultPageSize
	var outToken string
	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// parseNextToken parses a pagination token (integer offset) into a slice index.
func parseNextToken(token string) int {
	if token == "" {
		return 0
	}

	idx, err := strconv.Atoi(token)
	if err != nil || idx < 0 {
		return 0
	}

	return idx
}
