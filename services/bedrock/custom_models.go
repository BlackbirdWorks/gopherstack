package bedrock

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// newCustomModelID generates a unique custom model ID.
func (b *InMemoryBackend) newCustomModelID() string {
	b.customModelCounter++

	return fmt.Sprintf("cm-%07d", b.customModelCounter)
}

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
	b.customModels.Put(model)
	b.customModelsByName[modelName] = modelARN
	cp := *model
	cp.Tags = copyTags(model.Tags)

	return &cp, nil
}

// findCustomModelARN resolves a custom model ID or name to its ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findCustomModelARN(idOrARN string) (string, bool) {
	if _, ok := b.customModels.Get(idOrARN); ok {
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

	m, _ := b.customModels.Get(modelARN)
	cp := *m
	cp.Tags = copyTags(m.Tags)

	return &cp, nil
}

// ListCustomModels returns all custom models with optional pagination.
func (b *InMemoryBackend) ListCustomModels(nextToken string) ([]*CustomModel, string) {
	b.mu.RLock("ListCustomModels")
	defer b.mu.RUnlock()

	list := make([]*CustomModel, 0, b.customModels.Len())

	for _, m := range b.customModels.All() {
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

	m, _ := b.customModels.Get(modelARN)
	delete(b.customModelsByName, m.ModelName)
	b.customModels.Delete(modelARN)

	return nil
}
