package transcribe

import (
	"fmt"
	"sort"
	"time"
)

// maxVocabularyPhrases is the maximum number of phrases in a vocabulary.
const maxVocabularyPhrases = 256

// maxVocabularyPhraseLen is the maximum length of a vocabulary phrase.
const maxVocabularyPhraseLen = 256

// validateVocabularyPhrases checks phrase count and length limits.
func validateVocabularyPhrases(phrases []string) error {
	if len(phrases) > maxVocabularyPhrases {
		return fmt.Errorf("%w: Phrases list exceeds maximum of %d items", ErrValidation, maxVocabularyPhrases)
	}

	for i, p := range phrases {
		if len(p) > maxVocabularyPhraseLen {
			return fmt.Errorf("%w: phrase[%d] exceeds maximum length of %d characters",
				ErrValidation, i, maxVocabularyPhraseLen)
		}
	}

	return nil
}

// CreateVocabulary creates a new custom vocabulary.
func (b *InMemoryBackend) CreateVocabulary(input *Vocabulary) (*Vocabulary, error) {
	if input.VocabularyName == "" {
		return nil, fmt.Errorf("%w: VocabularyName is required", ErrValidation)
	}

	if err := validateLanguageCode(input.LanguageCode); err != nil {
		return nil, err
	}

	if input.LanguageCode == "" {
		return nil, fmt.Errorf("%w: LanguageCode is required", ErrValidation)
	}

	// Exactly one of Phrases or VocabularyFileURI must be set.
	if len(input.Phrases) > 0 && input.VocabularyFileURI != "" {
		return nil, fmt.Errorf("%w: provide either Phrases or VocabularyFileURI, not both", ErrValidation)
	}

	if err := validateVocabularyPhrases(input.Phrases); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateVocabulary")
	defer b.mu.Unlock()

	if b.vocabularies.Has(input.VocabularyName) {
		return nil, fmt.Errorf("%w: vocabulary %s already exists", ErrAlreadyExists, input.VocabularyName)
	}

	now := time.Now()
	v := *input
	v.VocabularyState = vocabStateReady
	v.LastModifiedTime = now
	b.vocabularies.Put(&v)
	b.recordResourceTagsLocked(resourceARN(resourceTypeVocabulary, v.VocabularyName), v.Tags)

	cp := v

	return &cp, nil
}

// AddVocabularyInternal seeds a vocabulary directly (test helper).
func (b *InMemoryBackend) AddVocabularyInternal(v *Vocabulary) {
	b.mu.Lock("AddVocabularyInternal")
	defer b.mu.Unlock()

	cp := *v
	b.vocabularies.Put(&cp)
}

// GetVocabulary returns a custom vocabulary by name.
func (b *InMemoryBackend) GetVocabulary(vocabularyName string) (*Vocabulary, error) {
	b.mu.RLock("GetVocabulary")
	defer b.mu.RUnlock()

	v, ok := b.vocabularies.Get(vocabularyName)
	if !ok {
		return nil, fmt.Errorf("%w: vocabulary %s not found", ErrVocabularyNotFound, vocabularyName)
	}

	cp := *v

	return &cp, nil
}

// UpdateVocabulary updates an existing custom vocabulary.
func (b *InMemoryBackend) UpdateVocabulary(
	input *Vocabulary,
) (*Vocabulary, error) {
	if input.VocabularyName == "" {
		return nil, fmt.Errorf("%w: VocabularyName is required", ErrValidation)
	}

	if err := validateVocabularyPhrases(input.Phrases); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateVocabulary")
	defer b.mu.Unlock()

	v, ok := b.vocabularies.Get(input.VocabularyName)
	if !ok {
		return nil, fmt.Errorf("%w: vocabulary %s not found", ErrNotFound, input.VocabularyName)
	}

	if input.LanguageCode != "" {
		v.LanguageCode = input.LanguageCode
	}

	v.Phrases, v.VocabularyFileURI = applyListOrURI(
		input.Phrases, input.VocabularyFileURI, v.Phrases, v.VocabularyFileURI,
	)
	v.LastModifiedTime = time.Now()

	cp := *v

	return &cp, nil
}

// DeleteVocabulary removes a custom vocabulary by name.
func (b *InMemoryBackend) DeleteVocabulary(vocabularyName string) error {
	if vocabularyName == "" {
		return fmt.Errorf("%w: VocabularyName is required", ErrValidation)
	}

	b.mu.Lock("DeleteVocabulary")
	defer b.mu.Unlock()

	if !b.vocabularies.Delete(vocabularyName) {
		return fmt.Errorf("%w: vocabulary %s not found", ErrNotFound, vocabularyName)
	}

	b.forgetResourceTagsLocked(resourceARN(resourceTypeVocabulary, vocabularyName))

	return nil
}

// ListVocabularies returns custom vocabularies with optional state filter and pagination.
func (b *InMemoryBackend) ListVocabularies(stateFilter, nextToken string) ([]Vocabulary, string) {
	b.mu.RLock("ListVocabularies")
	defer b.mu.RUnlock()

	all := make([]Vocabulary, 0, b.vocabularies.Len())
	for _, v := range b.vocabularies.All() {
		if stateFilter == "" || v.VocabularyState == stateFilter {
			all = append(all, *v)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].VocabularyName < all[j].VocabularyName })

	return paginateList(all, nextToken)
}
