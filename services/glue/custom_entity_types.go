package glue

import (
	"fmt"
	"sort"
)

// BatchGetCustomEntityTypes retrieves multiple custom entity types by name.
func (b *InMemoryBackend) BatchGetCustomEntityTypes(
	names []string,
) ([]*CustomEntityType, []string) {
	b.mu.RLock("BatchGetCustomEntityTypes")
	defer b.mu.RUnlock()

	found := make([]*CustomEntityType, 0, len(names))
	missing := make([]string, 0, len(names))

	for _, name := range names {
		cet, ok := b.customEntityTypes.Get(name)
		if !ok {
			missing = append(missing, name)

			continue
		}

		cp := *cet
		cp.ContextWords = append([]string(nil), cet.ContextWords...)
		found = append(found, &cp)
	}

	return found, missing
}

// AddCustomEntityTypeInternal adds a custom entity type directly to the backend without validation.
func (b *InMemoryBackend) AddCustomEntityTypeInternal(cet *CustomEntityType) {
	b.mu.Lock("AddCustomEntityTypeInternal")
	defer b.mu.Unlock()

	cp := *cet
	cp.ContextWords = append([]string(nil), cet.ContextWords...)
	b.customEntityTypes.Put(&cp)
}

// CreateCustomEntityType stores a new custom entity type.
func (b *InMemoryBackend) CreateCustomEntityType(
	name, regexString string,
	contextWords []string,
) (*CustomEntityType, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: CustomEntityType Name is required", ErrValidation)
	}
	if regexString == "" {
		return nil, fmt.Errorf("%w: CustomEntityType RegexString is required", ErrValidation)
	}

	b.mu.Lock("CreateCustomEntityType")
	defer b.mu.Unlock()

	cet := &CustomEntityType{
		Name:         name,
		RegexString:  regexString,
		ContextWords: contextWords,
	}
	b.customEntityTypes.Put(cet)
	cp := *cet

	return &cp, nil
}

// GetCustomEntityType returns a custom entity type by name.
func (b *InMemoryBackend) GetCustomEntityType(name string) (*CustomEntityType, error) {
	b.mu.RLock("GetCustomEntityType")
	defer b.mu.RUnlock()

	cet, ok := b.customEntityTypes.Get(name)
	if !ok {
		return nil, fmt.Errorf("custom entity type %q not found: %w", name, ErrNotFound)
	}

	cp := *cet

	return &cp, nil
}

// DeleteCustomEntityType removes a custom entity type.
func (b *InMemoryBackend) DeleteCustomEntityType(name string) error {
	b.mu.Lock("DeleteCustomEntityType")
	defer b.mu.Unlock()

	if !b.customEntityTypes.Has(name) {
		return fmt.Errorf("custom entity type %q not found: %w", name, ErrNotFound)
	}

	b.customEntityTypes.Delete(name)

	return nil
}

// ListCustomEntityTypes returns all custom entity type names.
func (b *InMemoryBackend) ListCustomEntityTypes() []*CustomEntityType {
	b.mu.RLock("ListCustomEntityTypes")
	defer b.mu.RUnlock()

	src := b.customEntityTypes.All()
	list := make([]*CustomEntityType, 0, len(src))
	for _, cet := range src {
		cp := *cet
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, k int) bool {
		return list[i].Name < list[k].Name
	})

	return list
}
