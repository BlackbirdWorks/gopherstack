package transcribe

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"
)

// supportedBaseModelNames returns the set of base model names for custom language models.
func supportedBaseModelNames() []string { return []string{"NarrowBand", "WideBand"} }

// validateBaseModelName checks that a CLM base model name is valid.
func validateBaseModelName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: BaseModelName is required", ErrValidation)
	}

	if !slices.Contains(supportedBaseModelNames(), name) {
		return fmt.Errorf("%w: BaseModelName %q must be one of %v",
			ErrValidation, name, supportedBaseModelNames())
	}

	return nil
}

// CreateLanguageModel creates a new custom language model.
func (b *InMemoryBackend) CreateLanguageModel(input *LanguageModel) (*LanguageModel, error) {
	if err := validateResourceName("ModelName", input.ModelName); err != nil {
		return nil, err
	}

	if err := validateBaseModelName(input.BaseModelName); err != nil {
		return nil, err
	}

	if err := validateLanguageCode(input.LanguageCode); err != nil {
		return nil, err
	}

	if input.LanguageCode == "" {
		return nil, fmt.Errorf("%w: LanguageCode is required", ErrValidation)
	}

	if input.InputDataConfig == nil {
		return nil, fmt.Errorf("%w: InputDataConfig is required", ErrValidation)
	}

	if input.InputDataConfig.S3Uri == "" {
		return nil, fmt.Errorf("%w: InputDataConfig.S3Uri is required", ErrValidation)
	}

	if input.InputDataConfig.DataAccessRoleArn == "" {
		return nil, fmt.Errorf("%w: InputDataConfig.DataAccessRoleArn is required", ErrValidation)
	}

	b.mu.Lock("CreateLanguageModel")
	defer b.mu.Unlock()

	if b.languageModels.Has(input.ModelName) {
		return nil, fmt.Errorf("%w: language model %s already exists", ErrAlreadyExists, input.ModelName)
	}

	now := time.Now()
	m := *input
	m.ModelStatus = modelStatusCompleted
	m.CreateTime = now
	m.LastModifiedTime = now
	b.languageModels.Put(&m)
	b.recordResourceTagsLocked(b.resourceARN(resourceTypeLanguageModel, m.ModelName), m.Tags)

	cp := m
	b.observeLanguageModel(&cp)

	return &cp, nil
}

// UpdateLanguageModel changes the data access role and/or encryption
// configuration of a model that is not IN_PROGRESS.
func (b *InMemoryBackend) UpdateLanguageModel(
	modelName, dataAccessRoleArn string, enc *EncryptionConfig,
) (*LanguageModel, error) {
	if modelName == "" {
		return nil, fmt.Errorf("%w: ModelName is required", ErrValidation)
	}

	if dataAccessRoleArn == "" && enc == nil {
		return nil, fmt.Errorf("%w: DataAccessRoleArn or EncryptionConfiguration is required", ErrValidation)
	}

	if enc != nil && !isKMSKeyARN(enc.KMSKey) {
		return nil, fmt.Errorf("%w: EncryptionConfiguration.KMSKey must be a full KMS key ARN", ErrValidation)
	}

	b.mu.Lock("UpdateLanguageModel")
	defer b.mu.Unlock()

	m, ok := b.languageModels.Get(modelName)
	if !ok {
		return nil, fmt.Errorf("%w: language model %s not found", ErrNotFound, modelName)
	}

	cur := *m
	b.observeLanguageModel(&cur)

	if cur.ModelStatus == jobStatusInProgress {
		return nil, fmt.Errorf("%w: language model %s is IN_PROGRESS", ErrValidation, modelName)
	}

	if dataAccessRoleArn != "" {
		idc := InputDataConfig{}
		if m.InputDataConfig != nil {
			idc = *m.InputDataConfig
		}

		idc.DataAccessRoleArn = dataAccessRoleArn
		m.InputDataConfig = &idc
	}

	if enc != nil {
		cp := *enc
		cp.KMSEncryptionContext = maps.Clone(enc.KMSEncryptionContext)
		m.Encryption = &cp
	}

	m.LastModifiedTime = time.Now()

	out := *m
	b.observeLanguageModel(&out)

	return &out, nil
}

func isKMSKeyARN(s string) bool {
	return strings.HasPrefix(s, "arn:") && strings.Contains(s, ":kms:") && strings.Contains(s, ":key/")
}

// DeleteLanguageModel removes a custom language model by name.
func (b *InMemoryBackend) DeleteLanguageModel(modelName string) error {
	if modelName == "" {
		return fmt.Errorf("%w: ModelName is required", ErrValidation)
	}

	b.mu.Lock("DeleteLanguageModel")
	defer b.mu.Unlock()

	if !b.languageModels.Delete(modelName) {
		return fmt.Errorf("%w: language model %s not found", ErrNotFound, modelName)
	}

	b.forgetResourceTagsLocked(b.resourceARN(resourceTypeLanguageModel, modelName))

	return nil
}

// AddLanguageModelInternal seeds a language model directly (test helper).
func (b *InMemoryBackend) AddLanguageModelInternal(m *LanguageModel) {
	b.mu.Lock("AddLanguageModelInternal")
	defer b.mu.Unlock()

	cp := *m
	b.languageModels.Put(&cp)
}

// DescribeLanguageModel returns a language model by name.
func (b *InMemoryBackend) DescribeLanguageModel(modelName string) (*LanguageModel, error) {
	b.mu.RLock("DescribeLanguageModel")
	defer b.mu.RUnlock()

	m, ok := b.languageModels.Get(modelName)
	if !ok {
		return nil, fmt.Errorf("%w: language model %s not found", ErrNotFound, modelName)
	}

	cp := *m
	b.observeLanguageModel(&cp)

	return &cp, nil
}

// ListLanguageModels returns language models with optional status filter, name
// substring filter, and pagination.
func (b *InMemoryBackend) ListLanguageModels(
	statusFilter, nameContains, nextToken string, maxResults int32,
) ([]LanguageModel, string) {
	b.mu.RLock("ListLanguageModels")
	defer b.mu.RUnlock()

	all := make([]LanguageModel, 0, b.languageModels.Len())
	for _, m := range b.languageModels.All() {
		cp := *m
		b.observeLanguageModel(&cp)

		if (statusFilter == "" || cp.ModelStatus == statusFilter) && matchesNameContains(m.ModelName, nameContains) {
			all = append(all, cp)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ModelName < all[j].ModelName })

	return paginateList(all, nextToken, maxResults)
}
