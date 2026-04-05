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

const bedrockDefaultPageSize = 100

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
	CreationTime time.Time `json:"creationTime"`
	JobArn       string    `json:"jobArn"`
	JobName      string    `json:"jobName"`
	Status       string    `json:"status"`
}

// AutomatedReasoningPolicy represents an Automated Reasoning policy.
type AutomatedReasoningPolicy struct {
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
	PolicyArn   string    `json:"policyArn"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Status      string    `json:"status"`
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
	ModelArn  string `json:"modelArn"`
	ModelName string `json:"modelName"`
}

// CustomModelDeployment represents a custom model deployment.
type CustomModelDeployment struct {
	CustomModelDeploymentArn string `json:"customModelDeploymentArn"`
	ModelDeploymentName      string `json:"modelDeploymentName"`
	ModelArn                 string `json:"modelArn"`
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

// InMemoryBackend stores Amazon Bedrock state in memory.
type InMemoryBackend struct {
	guardrails                  map[string]*Guardrail
	provisionedModelThroughputs map[string]*ProvisionedModelThroughput
	evaluationJobs              map[string]*EvaluationJob
	automatedReasoningPolicies  map[string]*AutomatedReasoningPolicy
	arpBuildWorkflows           map[string]*AutomatedReasoningPolicyBuildWorkflow // workflowID → workflow
	arpTestCases                map[string]*AutomatedReasoningPolicyTestCase      // testCaseID → test case
	arpVersions                 map[string]*AutomatedReasoningPolicyVersion       // policyArn+version → version
	customModels                map[string]*CustomModel                           // modelArn → model
	customModelDeployments      map[string]*CustomModelDeployment                 // deploymentArn → deployment
	foundationModelAgreements   map[string]*FoundationModelAgreement              // modelID → agreement
	guardrailsByName            map[string]string                                 // guardrail name → ID
	guardrailsByARN             map[string]string                                 // guardrail ARN → ID
	pmtsByName                  map[string]string                                 // PMT name → ARN
	arpByName                   map[string]string                                 // policy name → ARN
	customModelsByName          map[string]string                                 // model name → ARN
	customModelDeployByName     map[string]string                                 // deployment name → ARN
	mu                          *lockmetrics.RWMutex
	accountID                   string
	region                      string
	foundationModels            []*FoundationModelSummary
	guardrailCounter            int
	guardrailVersionCounter     int
	provisionedCounter          int
	evaluationJobCounter        int
	arpCounter                  int
	arpWorkflowCounter          int
	arpTestCaseCounter          int
	arpVersionCounter           int
	customModelCounter          int
	customModelDeployCounter    int
}

// NewInMemoryBackend creates a new InMemoryBackend pre-seeded with foundation models.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		guardrails:                  make(map[string]*Guardrail),
		provisionedModelThroughputs: make(map[string]*ProvisionedModelThroughput),
		evaluationJobs:              make(map[string]*EvaluationJob),
		automatedReasoningPolicies:  make(map[string]*AutomatedReasoningPolicy),
		arpBuildWorkflows:           make(map[string]*AutomatedReasoningPolicyBuildWorkflow),
		arpTestCases:                make(map[string]*AutomatedReasoningPolicyTestCase),
		arpVersions:                 make(map[string]*AutomatedReasoningPolicyVersion),
		customModels:                make(map[string]*CustomModel),
		customModelDeployments:      make(map[string]*CustomModelDeployment),
		foundationModelAgreements:   make(map[string]*FoundationModelAgreement),
		guardrailsByName:            make(map[string]string),
		guardrailsByARN:             make(map[string]string),
		pmtsByName:                  make(map[string]string),
		arpByName:                   make(map[string]string),
		customModelsByName:          make(map[string]string),
		customModelDeployByName:     make(map[string]string),
		accountID:                   accountID,
		region:                      region,
		mu:                          lockmetrics.New("bedrock"),
	}
	b.seedFoundationModels()

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

func (b *InMemoryBackend) seedFoundationModels() {
	partition := "aws"
	prefix := "arn:" + partition + ":bedrock::" + b.accountID + ":foundation-model/"

	b.foundationModels = []*FoundationModelSummary{
		{
			ModelID:          "amazon.titan-text-express-v1",
			ModelName:        "Titan Text G1 - Express",
			ProviderName:     "Amazon",
			ModelArn:         prefix + "amazon.titan-text-express-v1",
			InputModalities:  []string{"TEXT"},
			OutputModalities: []string{"TEXT"},
		},
		{
			ModelID:          "amazon.titan-embed-text-v1",
			ModelName:        "Titan Embeddings G1 - Text",
			ProviderName:     "Amazon",
			ModelArn:         prefix + "amazon.titan-embed-text-v1",
			InputModalities:  []string{"TEXT"},
			OutputModalities: []string{"EMBEDDING"},
		},
		{
			ModelID:          "anthropic.claude-v2",
			ModelName:        "Claude",
			ProviderName:     "Anthropic",
			ModelArn:         prefix + "anthropic.claude-v2",
			InputModalities:  []string{"TEXT"},
			OutputModalities: []string{"TEXT"},
		},
		{
			ModelID:          "anthropic.claude-3-sonnet-20240229-v1:0",
			ModelName:        "Claude 3 Sonnet",
			ProviderName:     "Anthropic",
			ModelArn:         prefix + "anthropic.claude-3-sonnet-20240229-v1:0",
			InputModalities:  []string{"TEXT", "IMAGE"},
			OutputModalities: []string{"TEXT"},
		},
		{
			ModelID:          "meta.llama3-8b-instruct-v1:0",
			ModelName:        "Llama 3 8B Instruct",
			ProviderName:     "Meta",
			ModelArn:         prefix + "meta.llama3-8b-instruct-v1:0",
			InputModalities:  []string{"TEXT"},
			OutputModalities: []string{"TEXT"},
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

// newARPVersionNum generates a unique version number string.
func (b *InMemoryBackend) newARPVersionNum() string {
	b.arpVersionCounter++

	return strconv.Itoa(b.arpVersionCounter)
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

	return &cp, nil
}

// ListGuardrails returns guardrails with optional pagination.
func (b *InMemoryBackend) ListGuardrails(nextToken string) ([]*GuardrailSummary, string) {
	b.mu.RLock("ListGuardrails")
	defer b.mu.RUnlock()

	list := make([]*GuardrailSummary, 0, len(b.guardrails))

	for _, g := range b.guardrails {
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

// UpdateGuardrail updates a guardrail's description and messaging.
func (b *InMemoryBackend) UpdateGuardrail(
	idOrARN, description, blockedInput, blockedOutput string,
) (*Guardrail, error) {
	b.mu.Lock("UpdateGuardrail")
	defer b.mu.Unlock()

	g, ok := b.findGuardrailByIDOrARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: guardrail %s not found", ErrNotFound, idOrARN)
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
		Status:               "InService",
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

// ListTagsForResource returns tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) ([]Tag, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	tags, ok := b.findTagsByARN(resourceARN)
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	result := make([]Tag, len(tags))
	copy(result, tags)

	return result, nil
}

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags []Tag) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if id, ok := b.guardrailsByARN[resourceARN]; ok {
		g := b.guardrails[id]
		tagMap := make(map[string]string, len(g.Tags))
		for _, t := range g.Tags {
			tagMap[t.Key] = t.Value
		}
		for _, t := range tags {
			tagMap[t.Key] = t.Value
		}
		merged := make([]Tag, 0, len(tagMap))
		for k, v := range tagMap {
			merged = append(merged, Tag{Key: k, Value: v})
		}
		g.Tags = merged

		return nil
	}

	if pmt, ok := b.provisionedModelThroughputs[resourceARN]; ok {
		tagMap := make(map[string]string, len(pmt.Tags))
		for _, t := range pmt.Tags {
			tagMap[t.Key] = t.Value
		}
		for _, t := range tags {
			tagMap[t.Key] = t.Value
		}
		merged := make([]Tag, 0, len(tagMap))
		for k, v := range tagMap {
			merged = append(merged, Tag{Key: k, Value: v})
		}
		pmt.Tags = merged

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
		filtered := g.Tags[:0]
		for _, t := range g.Tags {
			if !removeSet[t.Key] {
				filtered = append(filtered, t)
			}
		}
		g.Tags = filtered

		return nil
	}

	if pmt, ok := b.provisionedModelThroughputs[resourceARN]; ok {
		filtered := pmt.Tags[:0]
		for _, t := range pmt.Tags {
			if !removeSet[t.Key] {
				filtered = append(filtered, t)
			}
		}
		pmt.Tags = filtered

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// findTagsByARN returns a copy of the tags for a resource by ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findTagsByARN(resourceARN string) ([]Tag, bool) {
	if id, ok := b.guardrailsByARN[resourceARN]; ok {
		g := b.guardrails[id]
		result := make([]Tag, len(g.Tags))
		copy(result, g.Tags)

		return result, true
	}

	if pmt, ok := b.provisionedModelThroughputs[resourceARN]; ok {
		result := make([]Tag, len(pmt.Tags))
		copy(result, pmt.Tags)

		return result, true
	}

	return nil, false
}

// --- EvaluationJob methods ---

// CreateEvaluationJob creates a new evaluation job.
func (b *InMemoryBackend) CreateEvaluationJob(name string) (*EvaluationJob, error) {
	b.mu.Lock("CreateEvaluationJob")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: jobName is required", ErrValidation)
	}

	id := b.newEvaluationJobID()
	jobARN := arn.Build("bedrock", b.region, b.accountID, "evaluation-job/"+id)

	job := &EvaluationJob{
		JobArn:       jobARN,
		JobName:      name,
		Status:       "InProgress",
		CreationTime: time.Now().UTC(),
	}
	b.evaluationJobs[jobARN] = job
	cp := *job

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
	[]BatchDeleteEvaluationJobError, []BatchDeleteEvaluationJobItem,
) {
	b.mu.Lock("BatchDeleteEvaluationJob")
	defer b.mu.Unlock()

	var errs []BatchDeleteEvaluationJobError

	var deleted []BatchDeleteEvaluationJobItem

	for _, jobARN := range jobARNs {
		if _, ok := b.evaluationJobs[jobARN]; !ok {
			errs = append(errs, BatchDeleteEvaluationJobError{
				JobARN:  jobARN,
				Code:    "ResourceNotFoundException",
				Message: fmt.Sprintf("evaluation job %s not found", jobARN),
			})

			continue
		}

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

	return errs, deleted
}

// --- AutomatedReasoningPolicy methods ---

// CreateAutomatedReasoningPolicy creates a new Automated Reasoning policy.
func (b *InMemoryBackend) CreateAutomatedReasoningPolicy(name, description string) (*AutomatedReasoningPolicy, error) {
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
	}
	b.automatedReasoningPolicies[policyARN] = policy
	b.arpByName[name] = policyARN
	cp := *policy

	return &cp, nil
}

// CancelAutomatedReasoningPolicyBuildWorkflow cancels a running build workflow.
func (b *InMemoryBackend) CancelAutomatedReasoningPolicyBuildWorkflow(policyARN, workflowID string) error {
	b.mu.Lock("CancelAutomatedReasoningPolicyBuildWorkflow")
	defer b.mu.Unlock()

	if _, ok := b.automatedReasoningPolicies[policyARN]; !ok {
		return fmt.Errorf("%w: automated reasoning policy %s not found", ErrNotFound, policyARN)
	}

	if _, ok := b.arpBuildWorkflows[workflowID]; !ok {
		return fmt.Errorf("%w: build workflow %s not found", ErrNotFound, workflowID)
	}

	b.arpBuildWorkflows[workflowID].Status = "Cancelled"

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

	versionNum := b.newARPVersionNum()
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
func (b *InMemoryBackend) CreateCustomModel(modelName string) (*CustomModel, error) {
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
		ModelArn:  modelARN,
		ModelName: modelName,
	}
	b.customModels[modelARN] = model
	b.customModelsByName[modelName] = modelARN
	cp := *model

	return &cp, nil
}

// --- CustomModelDeployment methods ---

// CreateCustomModelDeployment creates a new deployment for a custom model.
func (b *InMemoryBackend) CreateCustomModelDeployment(modelARN, deploymentName string) (*CustomModelDeployment, error) {
	b.mu.Lock("CreateCustomModelDeployment")
	defer b.mu.Unlock()

	if deploymentName == "" {
		return nil, fmt.Errorf("%w: modelDeploymentName is required", ErrValidation)
	}

	if _, exists := b.customModelDeployByName[deploymentName]; exists {
		return nil, fmt.Errorf("%w: custom model deployment %s already exists", ErrAlreadyExists, deploymentName)
	}

	id := b.newCustomModelDeployID()
	deploymentARN := arn.Build("bedrock", b.region, b.accountID, "custom-model-deployment/"+id)

	deployment := &CustomModelDeployment{
		CustomModelDeploymentArn: deploymentARN,
		ModelDeploymentName:      deploymentName,
		ModelArn:                 modelARN,
	}
	b.customModelDeployments[deploymentARN] = deployment
	b.customModelDeployByName[deploymentName] = deploymentARN
	cp := *deployment

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

// --- GuardrailVersion methods ---

// CreateGuardrailVersion creates a new numbered version snapshot of a guardrail.
func (b *InMemoryBackend) CreateGuardrailVersion(idOrARN, description string) (*GuardrailVersion, error) {
	b.mu.Lock("CreateGuardrailVersion")
	defer b.mu.Unlock()

	g, ok := b.findGuardrailByIDOrARN(idOrARN)
	if !ok {
		return nil, fmt.Errorf("%w: guardrail %s not found", ErrNotFound, idOrARN)
	}

	// Guardrail versions use a dedicated counter to avoid collisions with guardrail IDs.
	b.guardrailVersionCounter++
	versionNum := strconv.Itoa(b.guardrailVersionCounter)

	gv := &GuardrailVersion{
		GuardrailID: g.GuardrailID,
		Version:     versionNum,
		Description: description,
	}

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
