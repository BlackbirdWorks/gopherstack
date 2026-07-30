package transcribe

import (
	"fmt"
	"sort"
	"time"
)

// CreateMedicalVocabulary creates a new medical custom vocabulary.
func (b *InMemoryBackend) CreateMedicalVocabulary(
	vocabularyName, languageCode, vocabularyFileURI string, tags map[string]string,
) (*MedicalVocabulary, error) {
	if vocabularyName == "" {
		return nil, fmt.Errorf("%w: VocabularyName is required", ErrValidation)
	}

	if languageCode == "" {
		return nil, fmt.Errorf("%w: LanguageCode is required", ErrValidation)
	}

	if vocabularyFileURI == "" {
		return nil, fmt.Errorf("%w: VocabularyFileURI is required", ErrValidation)
	}

	b.mu.Lock("CreateMedicalVocabulary")
	defer b.mu.Unlock()

	if b.medicalVocabularies.Has(vocabularyName) {
		return nil, fmt.Errorf(
			"%w: medical vocabulary %s already exists",
			ErrAlreadyExists,
			vocabularyName,
		)
	}

	now := time.Now()
	v := &MedicalVocabulary{
		VocabularyName:    vocabularyName,
		LanguageCode:      languageCode,
		VocabularyState:   vocabStateReady,
		VocabularyFileURI: vocabularyFileURI,
		LastModifiedTime:  now,
		Tags:              tags,
	}
	b.medicalVocabularies.Put(v)
	b.recordResourceTagsLocked(resourceARN(resourceTypeMedicalVocabulary, v.VocabularyName), v.Tags)

	cp := *v

	return &cp, nil
}

// AddMedicalVocabularyInternal seeds a medical vocabulary directly (test helper).
func (b *InMemoryBackend) AddMedicalVocabularyInternal(v *MedicalVocabulary) {
	b.mu.Lock("AddMedicalVocabularyInternal")
	defer b.mu.Unlock()

	cp := *v
	b.medicalVocabularies.Put(&cp)
}

// GetMedicalVocabulary returns a medical vocabulary by name.
func (b *InMemoryBackend) GetMedicalVocabulary(vocabularyName string) (*MedicalVocabulary, error) {
	b.mu.RLock("GetMedicalVocabulary")
	defer b.mu.RUnlock()

	v, ok := b.medicalVocabularies.Get(vocabularyName)
	if !ok {
		return nil, fmt.Errorf("%w: medical vocabulary %s not found", ErrNotFound, vocabularyName)
	}

	cp := *v

	return &cp, nil
}

// UpdateMedicalVocabulary updates an existing medical vocabulary.
func (b *InMemoryBackend) UpdateMedicalVocabulary(
	vocabularyName, languageCode, vocabularyFileURI string,
) (*MedicalVocabulary, error) {
	if vocabularyName == "" {
		return nil, fmt.Errorf("%w: VocabularyName is required", ErrValidation)
	}

	b.mu.Lock("UpdateMedicalVocabulary")
	defer b.mu.Unlock()

	v, ok := b.medicalVocabularies.Get(vocabularyName)
	if !ok {
		return nil, fmt.Errorf("%w: medical vocabulary %s not found", ErrNotFound, vocabularyName)
	}

	if languageCode != "" {
		v.LanguageCode = languageCode
	}

	if vocabularyFileURI != "" {
		v.VocabularyFileURI = vocabularyFileURI
	}

	v.LastModifiedTime = time.Now()

	cp := *v

	return &cp, nil
}

// DeleteMedicalVocabulary removes a medical vocabulary by name.
func (b *InMemoryBackend) DeleteMedicalVocabulary(vocabularyName string) error {
	if vocabularyName == "" {
		return fmt.Errorf("%w: VocabularyName is required", ErrValidation)
	}

	b.mu.Lock("DeleteMedicalVocabulary")
	defer b.mu.Unlock()

	if !b.medicalVocabularies.Delete(vocabularyName) {
		return fmt.Errorf("%w: medical vocabulary %s not found", ErrNotFound, vocabularyName)
	}

	b.forgetResourceTagsLocked(resourceARN(resourceTypeMedicalVocabulary, vocabularyName))

	return nil
}

// ListMedicalVocabularies returns medical vocabularies with optional state filter,
// name substring filter, and pagination.
func (b *InMemoryBackend) ListMedicalVocabularies(
	stateFilter, nameContains, nextToken string, maxResults int32,
) ([]MedicalVocabulary, string) {
	b.mu.RLock("ListMedicalVocabularies")
	defer b.mu.RUnlock()

	all := make([]MedicalVocabulary, 0, b.medicalVocabularies.Len())
	for _, v := range b.medicalVocabularies.All() {
		if (stateFilter == "" || v.VocabularyState == stateFilter) &&
			matchesNameContains(v.VocabularyName, nameContains) {
			all = append(all, *v)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].VocabularyName < all[j].VocabularyName })

	return paginateList(all, nextToken, maxResults)
}
