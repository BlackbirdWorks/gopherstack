package transcribe

import (
	"fmt"
	"sort"
	"time"
)

// maxVocabularyFilterWords is the maximum number of words in a vocabulary filter.
const maxVocabularyFilterWords = 10000

// validateVocabularyFilterWords checks the word count limit.
func validateVocabularyFilterWords(words []string) error {
	if len(words) > maxVocabularyFilterWords {
		return fmt.Errorf("%w: Words list exceeds maximum of %d items", ErrValidation, maxVocabularyFilterWords)
	}

	return nil
}

// CreateVocabularyFilter creates a new custom vocabulary filter.
func (b *InMemoryBackend) CreateVocabularyFilter(input *VocabularyFilter) (*VocabularyFilter, error) {
	if input.VocabularyFilterName == "" {
		return nil, fmt.Errorf("%w: VocabularyFilterName is required", ErrValidation)
	}

	if err := validateLanguageCode(input.LanguageCode); err != nil {
		return nil, err
	}

	if input.LanguageCode == "" {
		return nil, fmt.Errorf("%w: LanguageCode is required", ErrValidation)
	}

	// Exactly one of Words or VocabularyFilterFileURI must be set.
	if len(input.Words) > 0 && input.VocabularyFilterFileURI != "" {
		return nil, fmt.Errorf("%w: provide either Words or VocabularyFilterFileURI, not both", ErrValidation)
	}

	if len(input.Words) == 0 && input.VocabularyFilterFileURI == "" {
		return nil, fmt.Errorf("%w: one of Words or VocabularyFilterFileURI is required", ErrValidation)
	}

	if err := validateVocabularyFilterWords(input.Words); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateVocabularyFilter")
	defer b.mu.Unlock()

	if b.vocabularyFilters.Has(input.VocabularyFilterName) {
		return nil, fmt.Errorf(
			"%w: vocabulary filter %s already exists",
			ErrAlreadyExists,
			input.VocabularyFilterName,
		)
	}

	now := time.Now()
	f := *input
	f.LastModifiedTime = now
	b.vocabularyFilters.Put(&f)
	b.recordResourceTagsLocked(resourceARN(resourceTypeVocabularyFilter, f.VocabularyFilterName), f.Tags)

	cp := f

	return &cp, nil
}

// AddVocabularyFilterInternal seeds a vocabulary filter directly (test helper).
func (b *InMemoryBackend) AddVocabularyFilterInternal(f *VocabularyFilter) {
	b.mu.Lock("AddVocabularyFilterInternal")
	defer b.mu.Unlock()

	cp := *f
	b.vocabularyFilters.Put(&cp)
}

// GetVocabularyFilter returns a custom vocabulary filter by name.
func (b *InMemoryBackend) GetVocabularyFilter(
	vocabularyFilterName string,
) (*VocabularyFilter, error) {
	b.mu.RLock("GetVocabularyFilter")
	defer b.mu.RUnlock()

	f, ok := b.vocabularyFilters.Get(vocabularyFilterName)
	if !ok {
		return nil, fmt.Errorf(
			"%w: vocabulary filter %s not found",
			ErrNotFound,
			vocabularyFilterName,
		)
	}

	cp := *f

	return &cp, nil
}

// UpdateVocabularyFilter updates an existing vocabulary filter.
func (b *InMemoryBackend) UpdateVocabularyFilter(
	input *VocabularyFilter,
) (*VocabularyFilter, error) {
	if input.VocabularyFilterName == "" {
		return nil, fmt.Errorf("%w: VocabularyFilterName is required", ErrValidation)
	}

	if err := validateVocabularyFilterWords(input.Words); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateVocabularyFilter")
	defer b.mu.Unlock()

	f, ok := b.vocabularyFilters.Get(input.VocabularyFilterName)
	if !ok {
		return nil, fmt.Errorf(
			"%w: vocabulary filter %s not found",
			ErrNotFound,
			input.VocabularyFilterName,
		)
	}

	if input.LanguageCode != "" {
		f.LanguageCode = input.LanguageCode
	}

	f.Words, f.VocabularyFilterFileURI = applyListOrURI(
		input.Words, input.VocabularyFilterFileURI, f.Words, f.VocabularyFilterFileURI,
	)
	f.LastModifiedTime = time.Now()

	cp := *f

	return &cp, nil
}

// DeleteVocabularyFilter removes a vocabulary filter by name.
func (b *InMemoryBackend) DeleteVocabularyFilter(vocabularyFilterName string) error {
	if vocabularyFilterName == "" {
		return fmt.Errorf("%w: VocabularyFilterName is required", ErrValidation)
	}

	b.mu.Lock("DeleteVocabularyFilter")
	defer b.mu.Unlock()

	if !b.vocabularyFilters.Delete(vocabularyFilterName) {
		return fmt.Errorf("%w: vocabulary filter %s not found", ErrNotFound, vocabularyFilterName)
	}

	b.forgetResourceTagsLocked(resourceARN(resourceTypeVocabularyFilter, vocabularyFilterName))

	return nil
}

// ListVocabularyFilters returns all vocabulary filters with pagination.
func (b *InMemoryBackend) ListVocabularyFilters(nextToken string) ([]VocabularyFilter, string) {
	b.mu.RLock("ListVocabularyFilters")
	defer b.mu.RUnlock()

	all := make([]VocabularyFilter, 0, b.vocabularyFilters.Len())
	for _, f := range b.vocabularyFilters.All() {
		all = append(all, *f)
	}

	sort.Slice(
		all,
		func(i, j int) bool { return all[i].VocabularyFilterName < all[j].VocabularyFilterName },
	)

	return paginateList(all, nextToken)
}
