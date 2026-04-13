package transcribe

import (
	"encoding/json"
	"log/slog"
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
func (b *InMemoryBackend) Snapshot() []byte {
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
		slog.Default().Warn("transcribe: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Jobs == nil {
		snap.Jobs = make(map[string]*TranscriptionJob)
	}

	if snap.CallAnalyticsCategories == nil {
		snap.CallAnalyticsCategories = make(map[string]*CallAnalyticsCategory)
	}

	if snap.LanguageModels == nil {
		snap.LanguageModels = make(map[string]*LanguageModel)
	}

	if snap.MedicalVocabularies == nil {
		snap.MedicalVocabularies = make(map[string]*MedicalVocabulary)
	}

	if snap.Vocabularies == nil {
		snap.Vocabularies = make(map[string]*Vocabulary)
	}

	if snap.VocabularyFilters == nil {
		snap.VocabularyFilters = make(map[string]*VocabularyFilter)
	}

	if snap.CallAnalyticsJobs == nil {
		snap.CallAnalyticsJobs = make(map[string]*CallAnalyticsJob)
	}

	if snap.MedicalScribeJobs == nil {
		snap.MedicalScribeJobs = make(map[string]*MedicalScribeJob)
	}

	if snap.MedicalTranscriptionJobs == nil {
		snap.MedicalTranscriptionJobs = make(map[string]*MedicalTranscriptionJob)
	}

	b.jobs = snap.Jobs
	b.callAnalyticsCategories = snap.CallAnalyticsCategories
	b.languageModels = snap.LanguageModels
	b.medicalVocabularies = snap.MedicalVocabularies
	b.vocabularies = snap.Vocabularies
	b.vocabularyFilters = snap.VocabularyFilters
	b.callAnalyticsJobs = snap.CallAnalyticsJobs
	b.medicalScribeJobs = snap.MedicalScribeJobs
	b.medicalTranscriptionJobs = snap.MedicalTranscriptionJobs

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
