package transcribe

// JobCount returns the number of transcription jobs stored in the backend.
// Exported for use in tests only.
func JobCount(b *InMemoryBackend) int {
	b.mu.RLock("JobCount")
	defer b.mu.RUnlock()

	return b.jobs.Len()
}

// CallAnalyticsCategoryCount returns the number of Call Analytics categories.
// Exported for use in tests only.
func CallAnalyticsCategoryCount(b *InMemoryBackend) int {
	b.mu.RLock("CallAnalyticsCategoryCount")
	defer b.mu.RUnlock()

	return b.callAnalyticsCategories.Len()
}

// LanguageModelCount returns the number of language models stored.
// Exported for use in tests only.
func LanguageModelCount(b *InMemoryBackend) int {
	b.mu.RLock("LanguageModelCount")
	defer b.mu.RUnlock()

	return b.languageModels.Len()
}

// MedicalVocabularyCount returns the number of medical vocabularies stored.
// Exported for use in tests only.
func MedicalVocabularyCount(b *InMemoryBackend) int {
	b.mu.RLock("MedicalVocabularyCount")
	defer b.mu.RUnlock()

	return b.medicalVocabularies.Len()
}

// VocabularyCount returns the number of custom vocabularies stored.
// Exported for use in tests only.
func VocabularyCount(b *InMemoryBackend) int {
	b.mu.RLock("VocabularyCount")
	defer b.mu.RUnlock()

	return b.vocabularies.Len()
}

// VocabularyFilterCount returns the number of vocabulary filters stored.
// Exported for use in tests only.
func VocabularyFilterCount(b *InMemoryBackend) int {
	b.mu.RLock("VocabularyFilterCount")
	defer b.mu.RUnlock()

	return b.vocabularyFilters.Len()
}

// CallAnalyticsJobCount returns the number of Call Analytics jobs stored.
// Exported for use in tests only.
func CallAnalyticsJobCount(b *InMemoryBackend) int {
	b.mu.RLock("CallAnalyticsJobCount")
	defer b.mu.RUnlock()

	return b.callAnalyticsJobs.Len()
}

// MedicalScribeJobCount returns the number of Medical Scribe jobs stored.
// Exported for use in tests only.
func MedicalScribeJobCount(b *InMemoryBackend) int {
	b.mu.RLock("MedicalScribeJobCount")
	defer b.mu.RUnlock()

	return b.medicalScribeJobs.Len()
}

// MedicalTranscriptionJobCount returns the number of Medical Transcription jobs stored.
// Exported for use in tests only.
func MedicalTranscriptionJobCount(b *InMemoryBackend) int {
	b.mu.RLock("MedicalTranscriptionJobCount")
	defer b.mu.RUnlock()

	return b.medicalTranscriptionJobs.Len()
}

// HandlerOpsLen returns the number of operations in the handler dispatch map.
// Exported for use in tests only.
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}
