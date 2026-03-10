package bedrock

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
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

// InMemoryBackend stores Amazon Bedrock state in memory.
type InMemoryBackend struct {
	guardrails                  map[string]*Guardrail
	provisionedModelThroughputs map[string]*ProvisionedModelThroughput
	mu                          *lockmetrics.RWMutex
	accountID                   string
	region                      string
	foundationModels            []*FoundationModelSummary
	guardrailCounter            int
	provisionedCounter          int
}

// NewInMemoryBackend creates a new InMemoryBackend pre-seeded with foundation models.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		guardrails:                  make(map[string]*Guardrail),
		provisionedModelThroughputs: make(map[string]*ProvisionedModelThroughput),
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

// CreateGuardrail creates a new guardrail.
func (b *InMemoryBackend) CreateGuardrail(
	name, description, blockedInput, blockedOutput string,
	tags []Tag,
) (*Guardrail, error) {
	b.mu.Lock("CreateGuardrail")
	defer b.mu.Unlock()

	for _, g := range b.guardrails {
		if g.Name == name {
			return nil, fmt.Errorf("%w: guardrail %s already exists", ErrAlreadyExists, name)
		}
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
	cp := *g

	return &cp, nil
}

// GetGuardrail returns a guardrail by ID or ARN.
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

// ListGuardrails returns all guardrails.
func (b *InMemoryBackend) ListGuardrails() []*GuardrailSummary {
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

	return list
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

	return nil
}

// findGuardrailByIDOrARN finds a guardrail by ID or ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findGuardrailByIDOrARN(idOrARN string) (*Guardrail, bool) {
	if g, ok := b.guardrails[idOrARN]; ok {
		return g, true
	}

	for _, g := range b.guardrails {
		if g.GuardrailArn == idOrARN {
			return g, true
		}
	}

	return nil, false
}

// ListFoundationModels returns all seeded foundation models.
func (b *InMemoryBackend) ListFoundationModels() []*FoundationModelSummary {
	b.mu.RLock("ListFoundationModels")
	defer b.mu.RUnlock()

	list := make([]*FoundationModelSummary, len(b.foundationModels))
	copy(list, b.foundationModels)

	return list
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

	for _, p := range b.provisionedModelThroughputs {
		if p.ProvisionedModelName == name {
			return nil, fmt.Errorf("%w: provisioned model throughput %s already exists", ErrAlreadyExists, name)
		}
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
	cp := *pmt

	return &cp, nil
}

// GetProvisionedModelThroughput returns a provisioned model throughput by ID or ARN.
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

// ListProvisionedModelThroughputs returns all provisioned model throughputs.
func (b *InMemoryBackend) ListProvisionedModelThroughputs() []*ProvisionedModelThroughput {
	b.mu.RLock("ListProvisionedModelThroughputs")
	defer b.mu.RUnlock()

	list := make([]*ProvisionedModelThroughput, 0, len(b.provisionedModelThroughputs))

	for _, pmt := range b.provisionedModelThroughputs {
		cp := *pmt
		list = append(list, &cp)
	}

	return list
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

	return nil
}

// findPMTByIDOrARN finds a provisioned model throughput by ID or ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findPMTByIDOrARN(idOrARN string) (*ProvisionedModelThroughput, bool) {
	if pmt, ok := b.provisionedModelThroughputs[idOrARN]; ok {
		return pmt, true
	}

	for _, pmt := range b.provisionedModelThroughputs {
		if pmt.ProvisionedModelName == idOrARN {
			return pmt, true
		}
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

	existing, ok := b.findTagsByARNPointer(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	tagMap := make(map[string]string, len(*existing))
	for _, t := range *existing {
		tagMap[t.Key] = t.Value
	}

	for _, t := range tags {
		tagMap[t.Key] = t.Value
	}

	merged := make([]Tag, 0, len(tagMap))
	for k, v := range tagMap {
		merged = append(merged, Tag{Key: k, Value: v})
	}

	*existing = merged

	return nil
}

// UntagResource removes tags from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing, ok := b.findTagsByARNPointer(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	removeSet := make(map[string]bool, len(tagKeys))
	for _, k := range tagKeys {
		removeSet[k] = true
	}

	filtered := (*existing)[:0]
	for _, t := range *existing {
		if !removeSet[t.Key] {
			filtered = append(filtered, t)
		}
	}

	*existing = filtered

	return nil
}

// findTagsByARN returns a copy of the tags for a resource by ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findTagsByARN(resourceARN string) ([]Tag, bool) {
	for _, g := range b.guardrails {
		if g.GuardrailArn == resourceARN {
			result := make([]Tag, len(g.Tags))
			copy(result, g.Tags)

			return result, true
		}
	}

	for _, pmt := range b.provisionedModelThroughputs {
		if pmt.ProvisionedModelArn == resourceARN {
			result := make([]Tag, len(pmt.Tags))
			copy(result, pmt.Tags)

			return result, true
		}
	}

	return nil, false
}

// findTagsByARNPointer returns a pointer to the tags slice for a resource by ARN.
// Caller must hold the write lock.
func (b *InMemoryBackend) findTagsByARNPointer(resourceARN string) (*[]Tag, bool) {
	for _, g := range b.guardrails {
		if g.GuardrailArn == resourceARN {
			return &g.Tags, true
		}
	}

	for _, pmt := range b.provisionedModelThroughputs {
		if pmt.ProvisionedModelArn == resourceARN {
			return &pmt.Tags, true
		}
	}

	return nil, false
}
