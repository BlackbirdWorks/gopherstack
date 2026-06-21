package transcribe

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type backendSnapshot struct {
	Jobs                     map[string]*TranscriptionJob        `json:"jobs"`
	CallAnalyticsCategories  map[string]*CallAnalyticsCategory   `json:"callAnalyticsCategories"`
	LanguageModels           map[string]*LanguageModel           `json:"languageModels"`
	MedicalVocabularies      map[string]*MedicalVocabulary       `json:"medicalVocabularies"`
	Vocabularies             map[string]*Vocabulary              `json:"vocabularies"`
	VocabularyFilters        map[string]*VocabularyFilter        `json:"vocabularyFilters"`
	CallAnalyticsJobs        map[string]*CallAnalyticsJob        `json:"callAnalyticsJobs"`
	MedicalScribeJobs        map[string]*MedicalScribeJob        `json:"medicalScribeJobs"`
	MedicalTranscriptionJobs map[string]*MedicalTranscriptionJob `json:"medicalTranscriptionJobs"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	jobsCopy := make(map[string]*TranscriptionJob, len(b.jobs))
	for k, v := range b.jobs {
		cp := *v
		jobsCopy[k] = &cp
	}

	catsCopy := make(map[string]*CallAnalyticsCategory, len(b.callAnalyticsCategories))
	for k, v := range b.callAnalyticsCategories {
		cp := *v
		catsCopy[k] = &cp
	}

	modelsCopy := make(map[string]*LanguageModel, len(b.languageModels))
	for k, v := range b.languageModels {
		cp := *v
		modelsCopy[k] = &cp
	}

	medVocabsCopy := make(map[string]*MedicalVocabulary, len(b.medicalVocabularies))
	for k, v := range b.medicalVocabularies {
		cp := *v
		medVocabsCopy[k] = &cp
	}

	vocabsCopy := make(map[string]*Vocabulary, len(b.vocabularies))
	for k, v := range b.vocabularies {
		cp := *v
		vocabsCopy[k] = &cp
	}

	filtersCopy := make(map[string]*VocabularyFilter, len(b.vocabularyFilters))
	for k, v := range b.vocabularyFilters {
		cp := *v
		filtersCopy[k] = &cp
	}

	caJobsCopy := make(map[string]*CallAnalyticsJob, len(b.callAnalyticsJobs))
	for k, v := range b.callAnalyticsJobs {
		cp := *v
		caJobsCopy[k] = &cp
	}

	msJobsCopy := make(map[string]*MedicalScribeJob, len(b.medicalScribeJobs))
	for k, v := range b.medicalScribeJobs {
		cp := *v
		msJobsCopy[k] = &cp
	}

	mtJobsCopy := make(map[string]*MedicalTranscriptionJob, len(b.medicalTranscriptionJobs))
	for k, v := range b.medicalTranscriptionJobs {
		cp := *v
		mtJobsCopy[k] = &cp
	}

	snap := backendSnapshot{
		Jobs:                     jobsCopy,
		CallAnalyticsCategories:  catsCopy,
		LanguageModels:           modelsCopy,
		MedicalVocabularies:      medVocabsCopy,
		Vocabularies:             vocabsCopy,
		VocabularyFilters:        filtersCopy,
		CallAnalyticsJobs:        caJobsCopy,
		MedicalScribeJobs:        msJobsCopy,
		MedicalTranscriptionJobs: mtJobsCopy,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "transcribe: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	// Restore maps, replacing nil with empty maps so the backend is always usable.
	b.jobs = nonNilMap(snap.Jobs)
	b.callAnalyticsCategories = nonNilMap(snap.CallAnalyticsCategories)
	b.languageModels = nonNilMap(snap.LanguageModels)
	b.medicalVocabularies = nonNilMap(snap.MedicalVocabularies)
	b.vocabularies = nonNilMap(snap.Vocabularies)
	b.vocabularyFilters = nonNilMap(snap.VocabularyFilters)
	b.callAnalyticsJobs = nonNilMap(snap.CallAnalyticsJobs)
	b.medicalScribeJobs = nonNilMap(snap.MedicalScribeJobs)
	b.medicalTranscriptionJobs = nonNilMap(snap.MedicalTranscriptionJobs)

	return nil
}

// nonNilMap returns m if it is not nil, otherwise returns a new empty map.
func nonNilMap[K comparable, V any](m map[K]V) map[K]V {
	if m != nil {
		return m
	}

	return make(map[K]V)
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
